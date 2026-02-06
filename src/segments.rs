use crate::encoding::{
    crc32, decode_series_block_v1_all_rows, decode_series_block_v1_for_query,
    decode_series_block_v2_all_rows, decode_series_block_v2_for_query, encode_series_block,
    read_u32, read_u64, write_u32, write_u64, SegmentEncodingConfig,
};
use crate::error::DbError;
use crate::types::{Row, TagSet, Timestamp, Value};

use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SEG_MAGIC: &[u8; 8] = b"UGNSEG01";
const SEG_FOOTER_MAGIC: &[u8; 8] = b"UGNSEGF1";
const MANIFEST_MAGIC: &[u8; 8] = b"UGNMAN01";

const SEG_VERSION: u32 = 2;
const MANIFEST_VERSION: u32 = 1;

const FOOTER_LEN: u64 = 8 + 8 + 8 + 4; // magic + index_off + index_len + crc32

#[derive(Debug, Clone)]
pub struct SegmentStoreConfig {
    pub compaction_check_interval: Duration,
    pub l0_compaction_trigger_segment_count: usize,
    /// Optional trigger: compact L0 when total bytes across all L0 segments reaches/exceeds this value.
    ///
    /// Note: bytes are computed from on-disk file sizes (best-effort).
    pub l0_compaction_trigger_total_bytes: Option<u64>,
    /// Optional trigger: compact L0 when the oldest L0 segment age reaches/exceeds this value.
    pub l0_compaction_trigger_max_age: Option<Duration>,
    /// Encoding & compression configuration for new series blocks.
    pub encoding: SegmentEncodingConfig,
}

impl Default for SegmentStoreConfig {
    fn default() -> Self {
        Self {
            compaction_check_interval: Duration::from_secs(1),
            l0_compaction_trigger_segment_count: 4,
            l0_compaction_trigger_total_bytes: None,
            l0_compaction_trigger_max_age: None,
            encoding: SegmentEncodingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompactionStats {
    pub input_segments: usize,
    pub output_segments: usize,
}

#[derive(Debug)]
pub struct SegmentStore {
    manifest_path: PathBuf,
    segments_dir: PathBuf,
    tmp_dir: PathBuf,

    state: Arc<RwLock<StoreState>>,

    encoding: SegmentEncodingConfig,

    compaction_tx: mpsc::Sender<CompactionCmd>,
    compaction_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct StoreState {
    manifest: Manifest,
    // Active segments used for reads/compaction. Readers clone Arcs.
    active: Vec<Arc<Segment>>,
    // Removed from active; may be deleted by the reaper once no readers hold them.
    obsolete: Vec<Arc<Segment>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Manifest {
    version: u32,
    next_segment_id: u64,
    delete_before: Option<Timestamp>,
    // Persisted copy of segment metadata for faster startup.
    segments: Vec<SegmentRecord>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SegmentRecord {
    id: u64,
    level: u8,
    created_at: Timestamp,
    max_seq: u64,
    min_ts: Timestamp,
    max_ts: Timestamp,
    file_name: String,
    series: BTreeMap<String, SeriesBlockMeta>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SeriesBlockMeta {
    pub offset: u64,
    pub len: u64,
    pub row_count: u32,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub crc32: u32,
}

#[derive(Debug)]
struct Segment {
    rec: SegmentRecord,
    path: PathBuf,
}

#[derive(Debug)]
enum CompactionCmd {
    Maybe,
    Force {
        ack: mpsc::Sender<Result<CompactionStats, DbError>>,
    },
    Shutdown,
}

impl SegmentStore {
    pub fn open<P: AsRef<Path>>(dir: P, config: SegmentStoreConfig) -> Result<Self, DbError> {
        let dir = dir.as_ref().to_path_buf();
        let segments_dir = dir.join("segments");
        let tmp_dir = segments_dir.join("tmp");
        let manifest_path = segments_dir.join("MANIFEST.bin");

        fs::create_dir_all(&tmp_dir)?;

        let manifest = if manifest_path.exists() {
            read_manifest(&manifest_path)?
        } else {
            Manifest {
                version: MANIFEST_VERSION,
                next_segment_id: 1,
                delete_before: None,
                segments: Vec::new(),
            }
        };

        // Ensure we can load all referenced segments; if any is missing/corrupt, fail fast.
        let mut active = Vec::with_capacity(manifest.segments.len());
        for rec in &manifest.segments {
            let path = segments_dir.join(&rec.file_name);
            // Validate footer/index CRC early; also normalizes any future upgrades.
            let _ = load_segment_index(&path)?;
            active.push(Arc::new(Segment {
                rec: rec.clone(),
                path,
            }));
        }

        // Persist manifest on first creation (so header/magic exists).
        if !manifest_path.exists() {
            write_manifest_atomic(&manifest_path, &tmp_dir, &manifest)?;
        }

        let state = Arc::new(RwLock::new(StoreState {
            manifest,
            active,
            obsolete: Vec::new(),
        }));
        let (tx, rx) = mpsc::channel();
        let state_clone = Arc::clone(&state);
        let segments_dir_clone = segments_dir.clone();
        let tmp_dir_clone = tmp_dir.clone();
        let manifest_path_clone = manifest_path.clone();
        let cfg_clone = config.clone();
        let encoding = config.encoding.clone();

        let handle = thread::spawn(move || {
            compaction_loop(
                rx,
                state_clone,
                &segments_dir_clone,
                &tmp_dir_clone,
                &manifest_path_clone,
                cfg_clone,
            )
        });

        Ok(Self {
            manifest_path,
            segments_dir,
            tmp_dir,
            state,
            encoding,
            compaction_tx: tx,
            compaction_handle: Mutex::new(Some(handle)),
        })
    }

    pub fn delete_before(&self) -> Option<Timestamp> {
        self.state
            .read()
            .ok()
            .and_then(|s| s.manifest.delete_before)
    }

    /// Advances the delete-before tombstone watermark (never decreases).
    ///
    /// This makes retention effective immediately for reads, and compaction will later
    /// physically drop the data.
    pub fn advance_delete_before(&self, delete_before: Timestamp) -> Result<(), DbError> {
        {
            let mut st = self.state.write()?;
            let cur = st.manifest.delete_before.unwrap_or(0);
            if delete_before <= cur {
                return Ok(());
            }
            st.manifest.delete_before = Some(delete_before);
            write_manifest_atomic(&self.manifest_path, &self.tmp_dir, &st.manifest)?;
        }

        // Nudge the background compaction/reclaim loop so TTL advances lead to timely reclamation.
        let _ = self.compaction_tx.send(CompactionCmd::Maybe);
        Ok(())
    }

    pub fn max_persisted_seq(&self) -> u64 {
        self.state
            .read()
            .map(|s| {
                s.active
                    .iter()
                    .map(|seg| seg.rec.max_seq)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }

    pub(crate) fn ingest_l0(
        &self,
        mut rows_by_series: HashMap<String, Vec<Row>>,
    ) -> Result<(), DbError> {
        // Sort each series (timestamp, seq) for deterministic layout & query binary search.
        for rows in rows_by_series.values_mut() {
            rows.sort_unstable_by(|a, b| (a.timestamp, a.seq).cmp(&(b.timestamp, b.seq)));
        }

        let created_at = now_ns();

        // Read the current retention watermark first so we can filter without allocating an id.
        let delete_before = { self.state.read()?.manifest.delete_before };

        // Apply retention to newly created segments too (prevents reintroducing expired data).
        let delete_before_ts = delete_before.unwrap_or(0);
        if delete_before_ts > 0 {
            for rows in rows_by_series.values_mut() {
                rows.retain(|r| r.timestamp >= delete_before_ts);
            }
            rows_by_series.retain(|_, rows| !rows.is_empty());
            if rows_by_series.is_empty() {
                // Nothing survives retention; treat as a no-op (do not error the flush/recovery path).
                return Ok(());
            }
        }

        // Assign id under manifest lock.
        let id = {
            let mut st = self.state.write()?;
            let id = st.manifest.next_segment_id;
            st.manifest.next_segment_id = st.manifest.next_segment_id.saturating_add(1);
            id
        };

        let file_name = format!("seg_{:020}_l0.seg", id);
        let final_path = self.segments_dir.join(&file_name);
        let tmp_path = self.tmp_dir.join(format!("{}.tmp", &file_name));

        let rec = write_segment_file(
            &tmp_path,
            &final_path,
            id,
            0,
            created_at,
            delete_before,
            rows_by_series,
            &self.encoding,
        )?;

        // Install into manifest + active set atomically.
        {
            let mut st = self.state.write()?;
            st.manifest.segments.push(rec.clone());
            write_manifest_atomic(&self.manifest_path, &self.tmp_dir, &st.manifest)?;
            st.active.push(Arc::new(Segment {
                rec: rec.clone(),
                path: final_path,
            }));
        }

        // Trigger compaction opportunistically.
        let _ = self.compaction_tx.send(CompactionCmd::Maybe);
        Ok(())
    }

    pub fn query(
        &self,
        series: &str,
        time_range: std::ops::Range<Timestamp>,
        tag_filter: Option<&TagSet>,
    ) -> Result<Vec<(Timestamp, Value)>, DbError> {
        if time_range.start >= time_range.end {
            return Err(DbError::InvalidTimeRange {
                start: time_range.start,
                end: time_range.end,
            });
        }

        let (segments, delete_before) = {
            let st = self.state.read()?;
            (st.active.clone(), st.manifest.delete_before)
        };

        let delete_before = delete_before.unwrap_or(0);
        let mut out = Vec::new();
        let mut seen_series = false;

        for seg in segments {
            // Segment-level min/max filter first.
            if seg.rec.max_ts < time_range.start || seg.rec.min_ts >= time_range.end {
                continue;
            }

            let Some(meta) = seg.rec.series.get(series) else {
                continue;
            };
            seen_series = true;
            if meta.max_ts < time_range.start || meta.min_ts >= time_range.end {
                continue;
            }

            let mut results = read_series_range(
                &seg.path,
                meta,
                time_range.clone(),
                tag_filter,
                delete_before,
            )?;
            out.append(&mut results);
        }

        if !seen_series {
            return Err(DbError::SeriesNotFound(series.to_string()));
        }
        Ok(out)
    }

    /// Forces a full L0 compaction now and waits for completion.
    pub fn compact_blocking(&self) -> Result<CompactionStats, DbError> {
        let (tx, rx) = mpsc::channel();
        self.compaction_tx
            .send(CompactionCmd::Force { ack: tx })
            .map_err(|e| {
                DbError::BackgroundTaskError(format!("Failed to request compaction: {}", e))
            })?;
        rx.recv().map_err(|e| {
            DbError::BackgroundTaskError(format!("Failed to receive compaction ack: {}", e))
        })?
    }
}

impl Drop for SegmentStore {
    fn drop(&mut self) {
        let _ = self.compaction_tx.send(CompactionCmd::Shutdown);
        if let Ok(mut h) = self.compaction_handle.lock() {
            if let Some(handle) = h.take() {
                let _ = handle.join();
            }
        }
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn compaction_loop(
    rx: mpsc::Receiver<CompactionCmd>,
    state: Arc<RwLock<StoreState>>,
    segments_dir: &Path,
    tmp_dir: &Path,
    manifest_path: &Path,
    cfg: SegmentStoreConfig,
) {
    let mut last_check = SystemTime::now();
    loop {
        let timeout = cfg.compaction_check_interval;
        match rx.recv_timeout(timeout) {
            Ok(CompactionCmd::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Ok(CompactionCmd::Force { ack }) => {
                let res =
                    compact_l0_once(&state, segments_dir, tmp_dir, manifest_path, &cfg.encoding);
                let _ = ack.send(res);
            }
            Ok(CompactionCmd::Maybe) | Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = last_check; // reserved for future time-based triggers
                let _ = maybe_compact(&state, segments_dir, tmp_dir, manifest_path, &cfg);
                last_check = SystemTime::now();
                let _ = last_check;
            }
        }
    }
}

fn maybe_compact(
    state: &Arc<RwLock<StoreState>>,
    segments_dir: &Path,
    tmp_dir: &Path,
    manifest_path: &Path,
    cfg: &SegmentStoreConfig,
) -> Result<(), DbError> {
    // Retention reclamation is correctness-agnostic (queries already obey tombstone), but critical for
    // enterprise operational guarantees: eventually reclaim disk for expired data even if no new
    // flushes/compactions happen and even if only a single segment exists.
    reclaim_retention(state, segments_dir, tmp_dir, manifest_path, &cfg.encoding)?;

    let (l0, l0_total_bytes, oldest_created_at) = {
        let st = state.read()?;
        let l0: Vec<_> = st
            .active
            .iter()
            .filter(|s| s.rec.level == 0)
            .cloned()
            .collect();
        let oldest = l0.iter().map(|s| s.rec.created_at).min();
        let mut total = 0u64;
        for seg in &l0 {
            total = total.saturating_add(fs::metadata(&seg.path).map(|m| m.len()).unwrap_or(0));
        }
        (l0, total, oldest)
    };

    let mut should_compact = false;
    if l0.len() >= cfg.l0_compaction_trigger_segment_count {
        should_compact = true;
    }
    if let Some(bytes) = cfg.l0_compaction_trigger_total_bytes {
        if l0_total_bytes >= bytes {
            should_compact = true;
        }
    }
    if let Some(max_age) = cfg.l0_compaction_trigger_max_age {
        if let Some(oldest) = oldest_created_at {
            let age = Duration::from_nanos(now_ns().saturating_sub(oldest));
            if age >= max_age {
                should_compact = true;
            }
        }
    }

    // L0 compaction is a merge; require at least two segments.
    if should_compact && l0.len() >= 2 {
        let _ = compact_l0_once(state, segments_dir, tmp_dir, manifest_path, &cfg.encoding)?;
    }
    Ok(())
}

fn reclaim_retention(
    state: &Arc<RwLock<StoreState>>,
    segments_dir: &Path,
    tmp_dir: &Path,
    manifest_path: &Path,
    encoding: &SegmentEncodingConfig,
) -> Result<(), DbError> {
    let (delete_before, segments) = {
        let st = state.read()?;
        let delete_before = st.manifest.delete_before.unwrap_or(0);
        if delete_before == 0 {
            return Ok(());
        }
        (delete_before, st.active.clone())
    };

    // Identify segments that are fully expired or partially overlapping the watermark.
    let mut fully_expired: Vec<Arc<Segment>> = Vec::new();
    let mut partial: Vec<Arc<Segment>> = Vec::new();
    for seg in segments {
        if seg.rec.max_ts < delete_before {
            fully_expired.push(seg);
        } else if seg.rec.min_ts < delete_before {
            partial.push(seg);
        }
    }

    // Rewrite partially expired segments one-by-one so their persisted metadata reflects the watermark.
    for seg in partial {
        let mut filtered: HashMap<String, Vec<Row>> = HashMap::new();
        for (series, meta) in &seg.rec.series {
            // Fast path: entire series is expired.
            if meta.max_ts < delete_before {
                continue;
            }
            let mut rows = read_series_all_rows(&seg.path, meta)?;
            rows.retain(|r| r.timestamp >= delete_before);
            if rows.is_empty() {
                continue;
            }
            rows.sort_unstable_by(|a, b| (a.timestamp, a.seq).cmp(&(b.timestamp, b.seq)));
            filtered.insert(series.clone(), rows);
        }

        if filtered.is_empty() {
            fully_expired.push(seg);
            continue;
        }

        // Allocate a new id for the rewritten segment.
        let new_id = {
            let mut st = state.write()?;
            let id = st.manifest.next_segment_id;
            st.manifest.next_segment_id = st.manifest.next_segment_id.saturating_add(1);
            id
        };
        let created_at = now_ns();
        let level = seg.rec.level;

        let file_name = format!("seg_{:020}_l{}.seg", new_id, level);
        let final_path = segments_dir.join(&file_name);
        let tmp_path = tmp_dir.join(format!("{}.tmp", &file_name));

        let new_rec = write_segment_file(
            &tmp_path,
            &final_path,
            new_id,
            level,
            created_at,
            Some(delete_before),
            filtered,
            encoding,
        )?;

        // Atomically replace the segment in manifest + active set.
        {
            let mut st = state.write()?;

            st.manifest.segments.retain(|r| r.id != seg.rec.id);
            st.manifest.segments.push(new_rec.clone());
            write_manifest_atomic(manifest_path, tmp_dir, &st.manifest)?;

            let mut new_active = Vec::with_capacity(st.active.len() + 1);
            let mut new_obsolete = Vec::new();
            for s in st.active.drain(..) {
                if s.rec.id == seg.rec.id {
                    new_obsolete.push(s);
                } else {
                    new_active.push(s);
                }
            }
            st.obsolete.extend(new_obsolete);
            new_active.push(Arc::new(Segment {
                rec: new_rec.clone(),
                path: final_path,
            }));
            st.active = new_active;
        }
    }

    if !fully_expired.is_empty() {
        let old_ids: std::collections::HashSet<u64> =
            fully_expired.iter().map(|s| s.rec.id).collect();
        {
            let mut st = state.write()?;
            st.manifest.segments.retain(|r| !old_ids.contains(&r.id));
            write_manifest_atomic(manifest_path, tmp_dir, &st.manifest)?;

            let mut new_active = Vec::with_capacity(st.active.len());
            let mut new_obsolete = Vec::new();
            for s in st.active.drain(..) {
                if old_ids.contains(&s.rec.id) {
                    new_obsolete.push(s);
                } else {
                    new_active.push(s);
                }
            }
            st.obsolete.extend(new_obsolete);
            st.active = new_active;
        }
    }

    reap_obsolete(state)?;
    Ok(())
}

fn compact_l0_once(
    state: &Arc<RwLock<StoreState>>,
    segments_dir: &Path,
    tmp_dir: &Path,
    manifest_path: &Path,
    encoding: &SegmentEncodingConfig,
) -> Result<CompactionStats, DbError> {
    // Select L0 segments to compact.
    let (to_compact, delete_before) = {
        let st = state.read()?;
        let l0: Vec<_> = st
            .active
            .iter()
            .filter(|s| s.rec.level == 0)
            .cloned()
            .collect();
        (l0, st.manifest.delete_before)
    };

    if to_compact.len() < 2 {
        reap_obsolete(state)?;
        return Ok(CompactionStats {
            input_segments: 0,
            output_segments: 0,
        });
    }

    // Build merged rows by series.
    let mut merged: HashMap<String, Vec<Row>> = HashMap::new();
    let mut max_seq = 0u64;

    for seg in &to_compact {
        max_seq = max_seq.max(seg.rec.max_seq);

        for (series, meta) in &seg.rec.series {
            let rows = read_series_all_rows(&seg.path, meta)?;
            let entry = merged.entry(series.clone()).or_insert_with(Vec::new);
            entry.extend(rows);
        }
    }

    // Apply retention tombstone watermark.
    let delete_before = delete_before.unwrap_or(0);
    for rows in merged.values_mut() {
        rows.retain(|r| r.timestamp >= delete_before);
        rows.sort_unstable_by(|a, b| (a.timestamp, a.seq).cmp(&(b.timestamp, b.seq)));
    }
    merged.retain(|_, rows| !rows.is_empty());

    // If retention removes everything, delete the input segments without producing an output segment.
    if merged.is_empty() {
        let old_ids: std::collections::HashSet<u64> = to_compact.iter().map(|s| s.rec.id).collect();
        {
            let mut st = state.write()?;
            st.manifest.segments.retain(|r| !old_ids.contains(&r.id));
            write_manifest_atomic(manifest_path, tmp_dir, &st.manifest)?;

            let mut new_active = Vec::with_capacity(st.active.len());
            let mut new_obsolete = Vec::new();
            for seg in st.active.drain(..) {
                if old_ids.contains(&seg.rec.id) {
                    new_obsolete.push(seg);
                } else {
                    new_active.push(seg);
                }
            }
            st.obsolete.extend(new_obsolete);
            st.active = new_active;
        }
        reap_obsolete(state)?;
        return Ok(CompactionStats {
            input_segments: to_compact.len(),
            output_segments: 0,
        });
    }

    // Allocate new id and write output segment.
    let (new_id, created_at) = {
        let mut st = state.write()?;
        let id = st.manifest.next_segment_id;
        st.manifest.next_segment_id = st.manifest.next_segment_id.saturating_add(1);
        (id, now_ns())
    };

    let file_name = format!("seg_{:020}_l1.seg", new_id);
    let final_path = segments_dir.join(&file_name);
    let tmp_path = tmp_dir.join(format!("{}.tmp", &file_name));

    let new_rec = write_segment_file(
        &tmp_path,
        &final_path,
        new_id,
        1,
        created_at,
        Some(delete_before),
        merged,
        encoding,
    )?;

    // Install: remove old L0 from active list, add new L1, persist manifest.
    {
        let mut st = state.write()?;
        let old_ids: std::collections::HashSet<u64> = to_compact.iter().map(|s| s.rec.id).collect();

        // Update manifest records.
        st.manifest.segments.retain(|r| !old_ids.contains(&r.id));
        st.manifest.segments.push(new_rec.clone());
        write_manifest_atomic(manifest_path, tmp_dir, &st.manifest)?;

        // Update active list.
        let mut new_active = Vec::with_capacity(st.active.len() + 1);
        let mut new_obsolete = Vec::new();
        for seg in st.active.drain(..) {
            if old_ids.contains(&seg.rec.id) {
                new_obsolete.push(seg);
            } else {
                new_active.push(seg);
            }
        }
        st.obsolete.extend(new_obsolete);
        new_active.push(Arc::new(Segment {
            rec: new_rec.clone(),
            path: final_path,
        }));
        st.active = new_active;
    }

    reap_obsolete(state)?;
    Ok(CompactionStats {
        input_segments: to_compact.len(),
        output_segments: 1,
    })
}

fn reap_obsolete(state: &Arc<RwLock<StoreState>>) -> Result<(), DbError> {
    let mut to_delete: Vec<PathBuf> = Vec::new();
    {
        let mut st = state.write()?;
        let mut keep = Vec::new();
        for seg in st.obsolete.drain(..) {
            if Arc::strong_count(&seg) == 1 {
                to_delete.push(seg.path.clone());
            } else {
                keep.push(seg);
            }
        }
        st.obsolete = keep;
    }

    for p in to_delete {
        let _ = fs::remove_file(&p);
    }
    Ok(())
}

fn write_segment_file(
    tmp_path: &Path,
    final_path: &Path,
    id: u64,
    level: u8,
    created_at: Timestamp,
    delete_before: Option<Timestamp>,
    rows_by_series: HashMap<String, Vec<Row>>,
    encoding: &SegmentEncodingConfig,
) -> Result<SegmentRecord, DbError> {
    // Build series blocks in a deterministic order.
    let mut series_names: Vec<String> = rows_by_series.keys().cloned().collect();
    series_names.sort();

    // Track segment stats.
    let mut seg_min_ts = Timestamp::MAX;
    let mut seg_max_ts = 0u64;
    let mut seg_max_seq = 0u64;
    let mut series_meta: BTreeMap<String, SeriesBlockMeta> = BTreeMap::new();

    // Create tmp file
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(tmp_path)?;
    let mut w = BufWriter::new(file);

    // Header
    w.write_all(SEG_MAGIC)?;
    w.write_all(&SEG_VERSION.to_le_bytes())?;
    w.write_all(&id.to_le_bytes())?;
    w.write_all(&[level])?;
    w.write_all(&created_at.to_le_bytes())?;

    // max_seq placeholder (we’ll fill it after computing)
    let max_seq_pos = w.stream_position()?;
    w.write_all(&0u64.to_le_bytes())?;

    // delete_before watermark persisted in the segment header for self-description (optional).
    let db = delete_before.unwrap_or(0);
    w.write_all(&db.to_le_bytes())?;

    // Series blocks
    for series in &series_names {
        let rows = rows_by_series.get(series).expect("series exists");
        if rows.is_empty() {
            continue;
        }

        let block_offset = w.stream_position()?;
        let block_bytes = encode_series_block(rows, encoding)?;
        let block_crc32 = crc32(&block_bytes);

        w.write_all(&block_bytes)?;
        let block_len = block_bytes.len() as u64;

        let row_count = rows.len() as u32;
        let min_ts = rows.first().unwrap().timestamp;
        let max_ts = rows.last().unwrap().timestamp;
        let max_seq = rows.iter().map(|r| r.seq).max().unwrap_or(0);

        seg_min_ts = seg_min_ts.min(min_ts);
        seg_max_ts = seg_max_ts.max(max_ts);
        seg_max_seq = seg_max_seq.max(max_seq);

        series_meta.insert(
            series.clone(),
            SeriesBlockMeta {
                offset: block_offset,
                len: block_len,
                row_count,
                min_ts,
                max_ts,
                crc32: block_crc32,
            },
        );
    }

    if series_meta.is_empty() {
        return Err(DbError::Internal(
            "Refusing to write an empty segment".to_string(),
        ));
    }

    // Index
    let index_offset = w.stream_position()?;
    let mut index_buf = Vec::new();
    write_u32(&mut index_buf, series_meta.len() as u32);
    for (name, meta) in &series_meta {
        write_string(&mut index_buf, name);
        write_u64(&mut index_buf, meta.offset);
        write_u64(&mut index_buf, meta.len);
        write_u32(&mut index_buf, meta.row_count);
        write_u64(&mut index_buf, meta.min_ts);
        write_u64(&mut index_buf, meta.max_ts);
        write_u32(&mut index_buf, meta.crc32);
    }
    let index_len = index_buf.len() as u64;
    w.write_all(&index_buf)?;

    // Footer CRC covers (index_offset,index_len)
    let mut footer_hasher = Crc32::new();
    footer_hasher.update(&index_offset.to_le_bytes());
    footer_hasher.update(&index_len.to_le_bytes());
    let footer_crc = footer_hasher.finalize();

    w.write_all(SEG_FOOTER_MAGIC)?;
    w.write_all(&index_offset.to_le_bytes())?;
    w.write_all(&index_len.to_le_bytes())?;
    w.write_all(&footer_crc.to_le_bytes())?;

    // Backpatch max_seq.
    w.flush()?;
    let mut f = w.into_inner().map_err(|e| DbError::Io(e.into_error()))?;
    f.seek(SeekFrom::Start(max_seq_pos))?;
    f.write_all(&seg_max_seq.to_le_bytes())?;
    f.flush()?;
    f.sync_data()?;

    // Atomic install: rename tmp -> final.
    fs::rename(tmp_path, final_path)?;
    sync_parent_dir(final_path)?;

    let size = fs::metadata(final_path)?.len();
    let rec = SegmentRecord {
        id,
        level,
        created_at,
        max_seq: seg_max_seq,
        min_ts: seg_min_ts,
        max_ts: seg_max_ts,
        file_name: final_path
            .file_name()
            .ok_or_else(|| DbError::Internal("Invalid segment filename".to_string()))?
            .to_string_lossy()
            .into_owned(),
        series: series_meta,
    };

    // Sanity: index can be read back.
    let _ = load_segment_index(final_path)?;
    let _ = size;
    Ok(rec)
}

fn read_series_range(
    path: &Path,
    meta: &SeriesBlockMeta,
    time_range: std::ops::Range<Timestamp>,
    tag_filter: Option<&TagSet>,
    delete_before: Timestamp,
) -> Result<Vec<(Timestamp, Value)>, DbError> {
    let mut f = File::open(path)?;
    let seg_version = read_segment_version(&mut f, path)?;
    f.seek(SeekFrom::Start(meta.offset))?;
    let mut block = vec![0u8; meta.len as usize];
    f.read_exact(&mut block)?;

    let mut hasher = Crc32::new();
    hasher.update(&block);
    let actual = hasher.finalize();
    if actual != meta.crc32 {
        return Err(DbError::Corruption {
            details: format!("Segment block CRC mismatch in {:?}", path),
            series: None,
            timestamp: None,
        });
    }

    match seg_version {
        1 => {
            let decoded = decode_series_block_v1_for_query(&block, path)?;
            let start_idx = decoded
                .timestamps
                .partition_point(|&ts| ts < time_range.start.max(delete_before));
            let end_idx = decoded
                .timestamps
                .partition_point(|&ts| ts < time_range.end);
            if start_idx >= end_idx {
                return Ok(Vec::new());
            }

            let mut out = Vec::new();
            for i in start_idx..end_idx {
                if let Some(filter) = tag_filter {
                    if !decoded.row_matches_filter_v1(i, filter)? {
                        continue;
                    }
                }
                out.push((decoded.timestamps[i], decoded.values[i]));
            }
            Ok(out)
        }
        2 => {
            let decoded = decode_series_block_v2_for_query(&block, path)?;
            let start_idx = decoded
                .timestamps
                .partition_point(|&ts| ts < time_range.start.max(delete_before));
            let end_idx = decoded
                .timestamps
                .partition_point(|&ts| ts < time_range.end);
            if start_idx >= end_idx {
                return Ok(Vec::new());
            }

            let mut out = Vec::new();
            if let Some(filter) = tag_filter {
                let matcher = decoded.build_tag_filter_matcher(filter);
                if matcher.is_none() {
                    return Ok(Vec::new());
                }
                let matcher = matcher.unwrap();
                for i in start_idx..end_idx {
                    if !decoded.row_matches_filter_v2(i, &matcher)? {
                        continue;
                    }
                    out.push((decoded.timestamps[i], decoded.values[i]));
                }
            } else {
                for i in start_idx..end_idx {
                    out.push((decoded.timestamps[i], decoded.values[i]));
                }
            }
            Ok(out)
        }
        other => Err(DbError::Corruption {
            details: format!("Unsupported segment version {} in {:?}", other, path),
            series: None,
            timestamp: None,
        }),
    }
}

fn read_series_all_rows(path: &Path, meta: &SeriesBlockMeta) -> Result<Vec<Row>, DbError> {
    let mut f = File::open(path)?;
    let seg_version = read_segment_version(&mut f, path)?;
    f.seek(SeekFrom::Start(meta.offset))?;
    let mut block = vec![0u8; meta.len as usize];
    f.read_exact(&mut block)?;

    let mut hasher = Crc32::new();
    hasher.update(&block);
    let actual = hasher.finalize();
    if actual != meta.crc32 {
        return Err(DbError::Corruption {
            details: format!("Segment block CRC mismatch in {:?}", path),
            series: None,
            timestamp: None,
        });
    }

    match seg_version {
        1 => decode_series_block_v1_all_rows(&block, path),
        2 => decode_series_block_v2_all_rows(&block, path),
        other => Err(DbError::Corruption {
            details: format!("Unsupported segment version {} in {:?}", other, path),
            series: None,
            timestamp: None,
        }),
    }
}

fn read_segment_version(f: &mut File, path: &Path) -> Result<u32, DbError> {
    use std::io::Seek;
    let pos = f.stream_position().unwrap_or(0);
    f.seek(SeekFrom::Start(0))?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != SEG_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad segment magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let v = read_u32(f)?;
    // Restore position for callers that may rely on it (best-effort).
    let _ = f.seek(SeekFrom::Start(pos));
    Ok(v)
}

fn load_segment_index(path: &Path) -> Result<BTreeMap<String, SeriesBlockMeta>, DbError> {
    let mut f = File::open(path)?;
    // Validate header magic.
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != SEG_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad segment magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    // version
    let version = read_u32(&mut f)?;
    if version != 1 && version != SEG_VERSION {
        return Err(DbError::Corruption {
            details: format!("Unsupported segment version {} in {:?}", version, path),
            series: None,
            timestamp: None,
        });
    }

    // Footer
    let file_len = f.metadata()?.len();
    if file_len < FOOTER_LEN {
        return Err(DbError::Corruption {
            details: format!("Truncated segment file {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    f.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
    let mut footer_magic = [0u8; 8];
    f.read_exact(&mut footer_magic)?;
    if &footer_magic != SEG_FOOTER_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad segment footer magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let index_offset = read_u64(&mut f)?;
    let index_len = read_u64(&mut f)?;
    let footer_crc = read_u32(&mut f)?;

    let mut footer_hasher = Crc32::new();
    footer_hasher.update(&index_offset.to_le_bytes());
    footer_hasher.update(&index_len.to_le_bytes());
    if footer_hasher.finalize() != footer_crc {
        return Err(DbError::Corruption {
            details: format!("Segment footer CRC mismatch in {:?}", path),
            series: None,
            timestamp: None,
        });
    }

    // Index
    f.seek(SeekFrom::Start(index_offset))?;
    let mut index_bytes = vec![0u8; index_len as usize];
    f.read_exact(&mut index_bytes)?;
    let mut cur = std::io::Cursor::new(index_bytes);

    let series_count = read_u32(&mut cur)? as usize;
    let mut out = BTreeMap::new();
    for _ in 0..series_count {
        let name = read_string(&mut cur)?;
        let offset = read_u64(&mut cur)?;
        let len = read_u64(&mut cur)?;
        let row_count = read_u32(&mut cur)?;
        let min_ts = read_u64(&mut cur)?;
        let max_ts = read_u64(&mut cur)?;
        let crc32 = read_u32(&mut cur)?;
        out.insert(
            name,
            SeriesBlockMeta {
                offset,
                len,
                row_count,
                min_ts,
                max_ts,
                crc32,
            },
        );
    }
    Ok(out)
}

fn read_manifest(path: &Path) -> Result<Manifest, DbError> {
    let mut f = File::open(path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != MANIFEST_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad manifest magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let version = read_u32(&mut f)?;
    if version != MANIFEST_VERSION {
        return Err(DbError::Corruption {
            details: format!("Unsupported manifest version {} in {:?}", version, path),
            series: None,
            timestamp: None,
        });
    }
    let len = read_u64(&mut f)? as usize;
    let crc = read_u32(&mut f)?;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)?;
    let mut hasher = Crc32::new();
    hasher.update(&buf);
    if hasher.finalize() != crc {
        return Err(DbError::Corruption {
            details: format!("Manifest CRC mismatch in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let manifest: Manifest =
        bincode::deserialize(&buf).map_err(|e| DbError::Serialization(e.to_string()))?;
    Ok(manifest)
}

fn write_manifest_atomic(path: &Path, tmp_dir: &Path, manifest: &Manifest) -> Result<(), DbError> {
    let bytes = bincode::serialize(manifest).map_err(|e| DbError::Serialization(e.to_string()))?;
    let mut hasher = Crc32::new();
    hasher.update(&bytes);
    let crc = hasher.finalize();

    let tmp_path = tmp_dir.join("MANIFEST.bin.tmp");
    let mut w = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?,
    );
    w.write_all(MANIFEST_MAGIC)?;
    w.write_all(&MANIFEST_VERSION.to_le_bytes())?;
    w.write_all(&(bytes.len() as u64).to_le_bytes())?;
    w.write_all(&crc.to_le_bytes())?;
    w.write_all(&bytes)?;
    w.flush()?;
    w.get_ref().sync_data()?;
    drop(w);

    fs::rename(&tmp_path, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), DbError> {
    let parent = path
        .parent()
        .ok_or_else(|| DbError::Internal("Missing parent dir".to_string()))?;
    let dir = File::open(parent)?;
    dir.sync_data()?;
    Ok(())
}

// --- binary helpers (segment-index string encoding; primitives from encoding) ---

fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

fn read_string<R: Read>(r: &mut R) -> Result<String, DbError> {
    let len = read_u32(r)? as usize;
    let mut b = vec![0u8; len];
    r.read_exact(&mut b)?;
    String::from_utf8(b).map_err(|e| DbError::Internal(format!("Invalid UTF-8: {}", e)))
}
