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
const SER_BLOCK_MAGIC: &[u8; 8] = b"UGNSER01";
const MANIFEST_MAGIC: &[u8; 8] = b"UGNMAN01";

const SEG_VERSION: u32 = 2;
const MANIFEST_VERSION: u32 = 1;

const FOOTER_LEN: u64 = 8 + 8 + 8 + 4; // magic + index_off + index_len + crc32

/// Storage encoding configuration for series blocks within segment files.
///
/// Note: This is persisted into each series block header so readers do not require
/// out-of-band configuration. Configuration only affects *newly written* blocks.
#[derive(Debug, Clone)]
pub struct SegmentEncodingConfig {
    pub float_encoding: FloatEncoding,
    pub tag_encoding: TagEncoding,
    pub compression: BlockCompression,
}

impl Default for SegmentEncodingConfig {
    fn default() -> Self {
        Self {
            // Default to an enterprise-safe baseline: deterministic encoding, minimal CPU overhead.
            float_encoding: FloatEncoding::Raw64,
            tag_encoding: TagEncoding::Dictionary,
            // Keep compression opt-in by default; callers can choose LZ4/Zstd based on workload.
            compression: BlockCompression::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatEncoding {
    /// Store IEEE-754 bits verbatim (8 bytes/value).
    Raw64,
    /// Gorilla-style XOR + leading/trailing-zero bitpacking over consecutive values.
    GorillaXor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagEncoding {
    /// Dictionary encode all tag keys/values within the block and store per-row ids.
    Dictionary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockCompression {
    None,
    /// LZ4 (fast) compression.
    Lz4,
    /// Zstd compression with a specific compression level (recommended 1-3 for low latency).
    Zstd { level: i32 },
}

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
            active.push(Arc::new(Segment { rec: rec.clone(), path }));
        }

        // Persist manifest on first creation (so header/magic exists).
        if !manifest_path.exists() {
            write_manifest_atomic(&manifest_path, &tmp_dir, &manifest)?;
        }

        let state = Arc::new(RwLock::new(StoreState { manifest, active, obsolete: Vec::new() }));
        let (tx, rx) = mpsc::channel();
        let state_clone = Arc::clone(&state);
        let segments_dir_clone = segments_dir.clone();
        let tmp_dir_clone = tmp_dir.clone();
        let manifest_path_clone = manifest_path.clone();
        let cfg_clone = config.clone();
        let encoding = config.encoding.clone();

        let handle = thread::spawn(move || compaction_loop(
            rx,
            state_clone,
            &segments_dir_clone,
            &tmp_dir_clone,
            &manifest_path_clone,
            cfg_clone,
        ));

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
        self.state.read().ok().and_then(|s| s.manifest.delete_before)
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
            .map(|s| s.active.iter().map(|seg| seg.rec.max_seq).max().unwrap_or(0))
            .unwrap_or(0)
    }

    pub(crate) fn ingest_l0(&self, mut rows_by_series: HashMap<String, Vec<Row>>) -> Result<(), DbError> {
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
            st.active.push(Arc::new(Segment { rec: rec.clone(), path: final_path }));
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
            return Err(DbError::InvalidTimeRange { start: time_range.start, end: time_range.end });
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

            let Some(meta) = seg.rec.series.get(series) else { continue };
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
            .map_err(|e| DbError::BackgroundTaskError(format!("Failed to request compaction: {}", e)))?;
        rx.recv().map_err(|e| DbError::BackgroundTaskError(format!("Failed to receive compaction ack: {}", e)))?
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
                let res = compact_l0_once(&state, segments_dir, tmp_dir, manifest_path, &cfg.encoding);
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
        let l0: Vec<_> = st.active.iter().filter(|s| s.rec.level == 0).cloned().collect();
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
            new_active.push(Arc::new(Segment { rec: new_rec.clone(), path: final_path }));
            st.active = new_active;
        }
    }

    if !fully_expired.is_empty() {
        let old_ids: std::collections::HashSet<u64> = fully_expired.iter().map(|s| s.rec.id).collect();
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
        let l0: Vec<_> = st.active.iter().filter(|s| s.rec.level == 0).cloned().collect();
        (l0, st.manifest.delete_before)
    };

    if to_compact.len() < 2 {
        reap_obsolete(state)?;
        return Ok(CompactionStats { input_segments: 0, output_segments: 0 });
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
        return Ok(CompactionStats { input_segments: to_compact.len(), output_segments: 0 });
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
        new_active.push(Arc::new(Segment { rec: new_rec.clone(), path: final_path }));
        st.active = new_active;
    }

    reap_obsolete(state)?;
    Ok(CompactionStats { input_segments: to_compact.len(), output_segments: 1 })
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
    let file = OpenOptions::new().create(true).write(true).truncate(true).open(tmp_path)?;
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
        let mut hasher = Crc32::new();
        hasher.update(&block_bytes);
        let crc32 = hasher.finalize();

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
                crc32,
            },
        );
    }

    if series_meta.is_empty() {
        return Err(DbError::Internal("Refusing to write an empty segment".to_string()));
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

const SER_BLOCK_V2: u32 = 2;

const TS_CODEC_DELTA_VARINT: u8 = 1;
const FLOAT_CODEC_RAW64: u8 = 1;
const FLOAT_CODEC_GORILLA_XOR: u8 = 2;
const TAG_CODEC_DICTIONARY: u8 = 1;
const COMPRESS_NONE: u8 = 0;
const COMPRESS_LZ4: u8 = 1;
const COMPRESS_ZSTD: u8 = 2;

fn encode_series_block(rows: &[Row], encoding: &SegmentEncodingConfig) -> Result<Vec<u8>, DbError> {
    let row_count = rows.len();
    if row_count == 0 {
        return Err(DbError::Internal("Refusing to encode an empty series block".to_string()));
    }
    if row_count > (u32::MAX as usize) {
        return Err(DbError::Internal("Series block too large".to_string()));
    }

    // --- build v2 uncompressed payload ---
    let mut payload: Vec<u8> = Vec::new();

    // seq column: store verbatim u64 (seq is not guaranteed monotonic when rows are sorted by timestamp).
    for r in rows {
        write_u64(&mut payload, r.seq);
    }

    // timestamp column: base + unsigned varint deltas (timestamps are non-decreasing after sort).
    let base_ts = rows[0].timestamp;
    write_u64(&mut payload, base_ts);
    let mut prev = base_ts;
    for r in &rows[1..] {
        let ts = r.timestamp;
        if ts < prev {
            return Err(DbError::Internal("Rows must be sorted by timestamp for delta encoding".to_string()));
        }
        let d = ts - prev;
        write_var_u64(&mut payload, d);
        prev = ts;
    }

    // values column
    match encoding.float_encoding {
        FloatEncoding::Raw64 => {
            for r in rows {
                // Preserve exact IEEE-754 bit patterns (including NaN payloads).
                payload.extend_from_slice(&r.value.to_bits().to_le_bytes());
            }
        }
        FloatEncoding::GorillaXor => {
            let values: Vec<u64> = rows.iter().map(|r| r.value.to_bits()).collect();
            encode_gorilla_xor_u64(&values, &mut payload)?;
        }
    }

    // tags column (dictionary)
    match encoding.tag_encoding {
        TagEncoding::Dictionary => {
            encode_tags_dictionary(rows, &mut payload)?;
        }
    }

    let uncompressed_len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Series block payload too large".to_string()))?;
    let uncompressed_crc32 = crc32(&payload);

    // --- optional compression (per block) ---
    let (compression_codec, compression_param, stored_payload) = compress_block_payload(encoding.compression, &payload)?;

    let stored_len: u32 = stored_payload
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Compressed series block too large".to_string()))?;

    // --- v2 block container header ---
    let mut buf = Vec::with_capacity(64 + stored_payload.len());
    buf.extend_from_slice(SER_BLOCK_MAGIC);
    write_u32(&mut buf, SER_BLOCK_V2);
    write_u32(&mut buf, row_count as u32);
    buf.push(TS_CODEC_DELTA_VARINT);
    buf.push(match encoding.float_encoding {
        FloatEncoding::Raw64 => FLOAT_CODEC_RAW64,
        FloatEncoding::GorillaXor => FLOAT_CODEC_GORILLA_XOR,
    });
    buf.push(match encoding.tag_encoding {
        TagEncoding::Dictionary => TAG_CODEC_DICTIONARY,
    });
    buf.push(compression_codec);
    write_u32(&mut buf, compression_param);
    write_u32(&mut buf, uncompressed_len);
    write_u32(&mut buf, uncompressed_crc32);
    write_u32(&mut buf, stored_len);
    buf.extend_from_slice(&stored_payload);
    Ok(buf)
}

// Legacy (segment v1) encoder retained for test-only fixtures and compatibility verification.
#[cfg(test)]
#[allow(dead_code)]
fn encode_series_block_v1(rows: &[Row]) -> Result<Vec<u8>, DbError> {
    let row_count = rows.len();
    if row_count == 0 {
        return Err(DbError::Internal("Refusing to encode an empty series block".to_string()));
    }
    if row_count > (u32::MAX as usize) {
        return Err(DbError::Internal("Series block too large".to_string()));
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(SER_BLOCK_MAGIC);
    write_u32(&mut buf, row_count as u32);

    // Columns: seq, ts, value
    for r in rows {
        write_u64(&mut buf, r.seq);
    }
    for r in rows {
        write_u64(&mut buf, r.timestamp);
    }
    for r in rows {
        write_f64(&mut buf, r.value);
    }

    // Tags column: offsets + blob (bincode of HashMap, legacy).
    let mut offsets: Vec<u32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut tags_blob: Vec<u8> = Vec::new();
    for r in rows {
        let enc = bincode::serialize(&r.tags).map_err(|e| DbError::Serialization(e.to_string()))?;
        let next = offsets
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(enc.len() as u32)
            .ok_or_else(|| DbError::Internal("Tags blob overflow".to_string()))?;
        tags_blob.extend_from_slice(&enc);
        offsets.push(next);
    }

    // offsets
    for off in offsets {
        write_u32(&mut buf, off);
    }
    write_u32(&mut buf, tags_blob.len() as u32);
    buf.extend_from_slice(&tags_blob);
    Ok(buf)
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
            let end_idx = decoded.timestamps.partition_point(|&ts| ts < time_range.end);
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
            let end_idx = decoded.timestamps.partition_point(|&ts| ts < time_range.end);
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

// --- series block decoders ---

struct DecodedBlockV1Query {
    timestamps: Vec<Timestamp>,
    values: Vec<Value>,
    offsets: Vec<u32>,
    tags_blob: Vec<u8>,
}

impl DecodedBlockV1Query {
    fn row_matches_filter_v1(&self, i: usize, filter: &TagSet) -> Result<bool, DbError> {
        let s = self.offsets[i] as usize;
        let e = self.offsets[i + 1] as usize;
        let tags: TagSet = bincode::deserialize(&self.tags_blob[s..e])
            .map_err(|e| DbError::Serialization(e.to_string()))?;
        Ok(check_tags(&tags, filter))
    }
}

fn decode_series_block_v1_for_query(block: &[u8], path: &Path) -> Result<DecodedBlockV1Query, DbError> {
    let mut cur = std::io::Cursor::new(block);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic)?;
    if &magic != SER_BLOCK_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad series block magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let row_count = read_u32(&mut cur)? as usize;
    if row_count == 0 {
        return Err(DbError::Corruption {
            details: format!("Empty series block in {:?}", path),
            series: None,
            timestamp: None,
        });
    }

    // seq column (unused for query path)
    for _ in 0..row_count {
        let _ = read_u64(&mut cur)?;
    }

    let mut timestamps = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        timestamps.push(read_u64(&mut cur)?);
    }
    let mut values = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        values.push(read_f64(&mut cur)?);
    }

    let mut offsets = Vec::with_capacity(row_count + 1);
    for _ in 0..(row_count + 1) {
        offsets.push(read_u32(&mut cur)?);
    }
    let tags_len = read_u32(&mut cur)? as usize;
    let mut tags_blob = vec![0u8; tags_len];
    cur.read_exact(&mut tags_blob)?;

    Ok(DecodedBlockV1Query { timestamps, values, offsets, tags_blob })
}

fn decode_series_block_v1_all_rows(block: &[u8], path: &Path) -> Result<Vec<Row>, DbError> {
    let mut cur = std::io::Cursor::new(block);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic)?;
    if &magic != SER_BLOCK_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad series block magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let row_count = read_u32(&mut cur)? as usize;
    if row_count == 0 {
        return Err(DbError::Corruption {
            details: format!("Empty series block in {:?}", path),
            series: None,
            timestamp: None,
        });
    }

    let mut seqs = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        seqs.push(read_u64(&mut cur)?);
    }
    let mut timestamps = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        timestamps.push(read_u64(&mut cur)?);
    }
    let mut values = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        values.push(read_f64(&mut cur)?);
    }

    let mut offsets = Vec::with_capacity(row_count + 1);
    for _ in 0..(row_count + 1) {
        offsets.push(read_u32(&mut cur)?);
    }
    let tags_len = read_u32(&mut cur)? as usize;
    let mut tags_blob = vec![0u8; tags_len];
    cur.read_exact(&mut tags_blob)?;

    let mut out = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let s = offsets[i] as usize;
        let e = offsets[i + 1] as usize;
        let tags: TagSet = bincode::deserialize(&tags_blob[s..e])
            .map_err(|e| DbError::Serialization(e.to_string()))?;
        out.push(Row { seq: seqs[i], timestamp: timestamps[i], value: values[i], tags });
    }
    Ok(out)
}

struct TagFilterMatcherV2 {
    pairs: Vec<(u32, u32)>,
}

struct DecodedBlockV2Query {
    timestamps: Vec<Timestamp>,
    values: Vec<Value>,
    dict: Vec<String>,
    offsets: Vec<u32>,
    tags_blob: Vec<u8>,
}

impl DecodedBlockV2Query {
    fn build_tag_filter_matcher(&self, filter: &TagSet) -> Option<TagFilterMatcherV2> {
        if filter.is_empty() {
            return Some(TagFilterMatcherV2 { pairs: Vec::new() });
        }
        let mut map: HashMap<&str, u32> = HashMap::with_capacity(self.dict.len());
        for (i, s) in self.dict.iter().enumerate() {
            // Dict may contain duplicates only if corrupted; we choose the first id deterministically.
            map.entry(s.as_str()).or_insert(i as u32);
        }
        let mut pairs = Vec::with_capacity(filter.len());
        for (k, v) in filter {
            let kid = *map.get(k.as_str())?;
            let vid = *map.get(v.as_str())?;
            pairs.push((kid, vid));
        }
        Some(TagFilterMatcherV2 { pairs })
    }

    fn row_matches_filter_v2(&self, i: usize, matcher: &TagFilterMatcherV2) -> Result<bool, DbError> {
        if matcher.pairs.is_empty() {
            return Ok(true);
        }
        let s = self.offsets[i] as usize;
        let e = self.offsets[i + 1] as usize;
        if s > e || e > self.tags_blob.len() {
            return Err(DbError::Corruption {
                details: "Tag offsets out of bounds".to_string(),
                series: None,
                timestamp: None,
            });
        }
        let mut cur = std::io::Cursor::new(&self.tags_blob[s..e]);
        let pair_count = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
            details,
            series: None,
            timestamp: None,
        })? as usize;

        let mut found = vec![false; matcher.pairs.len()];
        for _ in 0..pair_count {
            let kid = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })?;
            let vid = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })?;
            for (idx, (fk, fv)) in matcher.pairs.iter().copied().enumerate() {
                if !found[idx] && kid == fk && vid == fv {
                    found[idx] = true;
                }
            }
        }
        Ok(found.into_iter().all(|b| b))
    }
}

fn decode_series_block_v2_for_query(block: &[u8], path: &Path) -> Result<DecodedBlockV2Query, DbError> {
    let (row_count, float_codec, tag_codec, _compression, _compression_param, payload) =
        decode_series_block_v2_container(block, path)?;

    // payload decode (v2)
    let mut cur = std::io::Cursor::new(payload);

    // seq column (unused for query path)
    for _ in 0..row_count {
        let _ = read_u64(&mut cur)?;
    }

    // timestamps: base + deltas
    let base = read_u64(&mut cur)?;
    let mut timestamps = Vec::with_capacity(row_count);
    timestamps.push(base);
    let mut prev = base;
    for _ in 1..row_count {
        let d = read_var_u64(&mut cur).map_err(|details| DbError::Corruption {
            details,
            series: None,
            timestamp: None,
        })?;
        prev = prev.checked_add(d).ok_or_else(|| DbError::Corruption {
            details: "Timestamp delta overflow".to_string(),
            series: None,
            timestamp: None,
        })?;
        timestamps.push(prev);
    }

    // values
    let mut values = Vec::with_capacity(row_count);
    match float_codec {
        FLOAT_CODEC_RAW64 => {
            for _ in 0..row_count {
                let bits = read_u64(&mut cur)?;
                values.push(f64::from_bits(bits));
            }
        }
        FLOAT_CODEC_GORILLA_XOR => {
            let bits = decode_gorilla_xor_u64(&mut cur, row_count)?;
            for b in bits {
                values.push(f64::from_bits(b));
            }
        }
        other => {
            return Err(DbError::Corruption {
                details: format!("Unknown float codec {} in {:?}", other, path),
                series: None,
                timestamp: None,
            })
        }
    }

    // tags
    match tag_codec {
        TAG_CODEC_DICTIONARY => {
            let dict_count = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut dict = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                let n = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })? as usize;
                let mut b = vec![0u8; n];
                cur.read_exact(&mut b)?;
                let s = String::from_utf8(b).map_err(|e| DbError::Internal(format!("Invalid UTF-8 in dictionary: {}", e)))?;
                dict.push(s);
            }

            // per-row tagset lengths
            let mut offsets: Vec<u32> = Vec::with_capacity(row_count + 1);
            offsets.push(0);
            let mut total: u32 = 0;
            for _ in 0..row_count {
                let len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })?;
                total = total.checked_add(len).ok_or_else(|| DbError::Corruption {
                    details: "Tag blob overflow".to_string(),
                    series: None,
                    timestamp: None,
                })?;
                offsets.push(total);
            }
            let tags_len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut tags_blob = vec![0u8; tags_len];
            cur.read_exact(&mut tags_blob)?;

            Ok(DecodedBlockV2Query { timestamps, values, dict, offsets, tags_blob })
        }
        other => Err(DbError::Corruption {
            details: format!("Unknown tag codec {} in {:?}", other, path),
            series: None,
            timestamp: None,
        }),
    }
}

fn decode_series_block_v2_all_rows(block: &[u8], path: &Path) -> Result<Vec<Row>, DbError> {
    let (row_count, float_codec, tag_codec, _compression, _compression_param, payload) =
        decode_series_block_v2_container(block, path)?;
    let mut cur = std::io::Cursor::new(payload);

    let mut seqs = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        seqs.push(read_u64(&mut cur)?);
    }

    // timestamps
    let base = read_u64(&mut cur)?;
    let mut timestamps = Vec::with_capacity(row_count);
    timestamps.push(base);
    let mut prev = base;
    for _ in 1..row_count {
        let d = read_var_u64(&mut cur).map_err(|details| DbError::Corruption {
            details,
            series: None,
            timestamp: None,
        })?;
        prev = prev.checked_add(d).ok_or_else(|| DbError::Corruption {
            details: "Timestamp delta overflow".to_string(),
            series: None,
            timestamp: None,
        })?;
        timestamps.push(prev);
    }

    // values
    let mut values = Vec::with_capacity(row_count);
    match float_codec {
        FLOAT_CODEC_RAW64 => {
            for _ in 0..row_count {
                values.push(f64::from_bits(read_u64(&mut cur)?));
            }
        }
        FLOAT_CODEC_GORILLA_XOR => {
            let bits = decode_gorilla_xor_u64(&mut cur, row_count)?;
            for b in bits {
                values.push(f64::from_bits(b));
            }
        }
        other => {
            return Err(DbError::Corruption {
                details: format!("Unknown float codec {} in {:?}", other, path),
                series: None,
                timestamp: None,
            })
        }
    }

    // tags (dictionary)
    match tag_codec {
        TAG_CODEC_DICTIONARY => {
            let dict_count = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut dict = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                let n = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })? as usize;
                let mut b = vec![0u8; n];
                cur.read_exact(&mut b)?;
                let s = String::from_utf8(b).map_err(|e| DbError::Internal(format!("Invalid UTF-8 in dictionary: {}", e)))?;
                dict.push(s);
            }

            let mut offsets: Vec<u32> = Vec::with_capacity(row_count + 1);
            offsets.push(0);
            let mut total: u32 = 0;
            for _ in 0..row_count {
                let len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })?;
                total = total.checked_add(len).ok_or_else(|| DbError::Corruption {
                    details: "Tag blob overflow".to_string(),
                    series: None,
                    timestamp: None,
                })?;
                offsets.push(total);
            }
            let tags_len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut tags_blob = vec![0u8; tags_len];
            cur.read_exact(&mut tags_blob)?;

            let mut out = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let s = offsets[i] as usize;
                let e = offsets[i + 1] as usize;
                if s > e || e > tags_blob.len() {
                    return Err(DbError::Corruption {
                        details: "Tag offsets out of bounds".to_string(),
                        series: None,
                        timestamp: None,
                    });
                }
                let mut tcur = std::io::Cursor::new(&tags_blob[s..e]);
                let pair_count = read_var_u32(&mut tcur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })? as usize;
                let mut tags: TagSet = TagSet::with_capacity(pair_count);
                for _ in 0..pair_count {
                    let kid = read_var_u32(&mut tcur).map_err(|details| DbError::Corruption {
                        details,
                        series: None,
                        timestamp: None,
                    })? as usize;
                    let vid = read_var_u32(&mut tcur).map_err(|details| DbError::Corruption {
                        details,
                        series: None,
                        timestamp: None,
                    })? as usize;
                    let k = dict.get(kid).ok_or_else(|| DbError::Corruption {
                        details: "Dictionary key id out of range".to_string(),
                        series: None,
                        timestamp: None,
                    })?;
                    let v = dict.get(vid).ok_or_else(|| DbError::Corruption {
                        details: "Dictionary value id out of range".to_string(),
                        series: None,
                        timestamp: None,
                    })?;
                    tags.insert(k.clone(), v.clone());
                }
                out.push(Row { seq: seqs[i], timestamp: timestamps[i], value: values[i], tags });
            }
            Ok(out)
        }
        other => Err(DbError::Corruption {
            details: format!("Unknown tag codec {} in {:?}", other, path),
            series: None,
            timestamp: None,
        }),
    }
}

fn decode_series_block_v2_container(
    block: &[u8],
    path: &Path,
) -> Result<(usize, u8, u8, u8, u32, Vec<u8>), DbError> {
    // Header: magic + v2 fields.
    let mut cur = std::io::Cursor::new(block);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic)?;
    if &magic != SER_BLOCK_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad series block magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let block_version = read_u32(&mut cur)?;
    if block_version != SER_BLOCK_V2 {
        return Err(DbError::Corruption {
            details: format!("Unexpected series block version {} in {:?}", block_version, path),
            series: None,
            timestamp: None,
        });
    }
    let row_count = read_u32(&mut cur)? as usize;
    if row_count == 0 {
        return Err(DbError::Corruption {
            details: format!("Empty series block in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let mut codec = [0u8; 4];
    cur.read_exact(&mut codec)?;
    let ts_codec = codec[0];
    let float_codec = codec[1];
    let tag_codec = codec[2];
    let compression = codec[3];
    if ts_codec != TS_CODEC_DELTA_VARINT {
        return Err(DbError::Corruption {
            details: format!("Unknown timestamp codec {} in {:?}", ts_codec, path),
            series: None,
            timestamp: None,
        });
    }
    let compression_param = read_u32(&mut cur)?;
    let uncompressed_len = read_u32(&mut cur)? as usize;
    let expected_crc = read_u32(&mut cur)?;
    let stored_len = read_u32(&mut cur)? as usize;
    if uncompressed_len > 512 * 1024 * 1024 {
        return Err(DbError::Corruption {
            details: "Refusing to allocate oversized series block".to_string(),
            series: None,
            timestamp: None,
        });
    }

    let hdr_len = cur.position() as usize;
    if hdr_len.checked_add(stored_len).unwrap_or(usize::MAX) > block.len() {
        return Err(DbError::Corruption {
            details: "Truncated series block payload".to_string(),
            series: None,
            timestamp: None,
        });
    }
    let stored = &block[hdr_len..hdr_len + stored_len];

    // Decompress if needed.
    let payload: Vec<u8> = match compression {
        COMPRESS_NONE => stored.to_vec(),
        COMPRESS_LZ4 => {
            lz4_flex::decompress_size_prepended(stored).map_err(|e| DbError::Corruption {
                details: format!("LZ4 decompress failed: {}", e),
                series: None,
                timestamp: None,
            })?
        }
        COMPRESS_ZSTD => {
            let level = i32::from_le_bytes(compression_param.to_le_bytes());
            let _ = level; // persisted for observability/debug; not required for decompression
            zstd::bulk::decompress(stored, uncompressed_len).map_err(|e| DbError::Corruption {
                details: format!("Zstd decompress failed: {}", e),
                series: None,
                timestamp: None,
            })?
        }
        other => {
            return Err(DbError::Corruption {
                details: format!("Unknown compression codec {} in {:?}", other, path),
                series: None,
                timestamp: None,
            })
        }
    };

    if payload.len() != uncompressed_len {
        return Err(DbError::Corruption {
            details: "Series block decompressed length mismatch".to_string(),
            series: None,
            timestamp: None,
        });
    }
    let actual_crc = crc32(&payload);
    if actual_crc != expected_crc {
        return Err(DbError::Corruption {
            details: "Series block payload CRC mismatch".to_string(),
            series: None,
            timestamp: None,
        });
    }

    Ok((row_count, float_codec, tag_codec, compression, compression_param, payload))
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
    let manifest: Manifest = bincode::deserialize(&buf).map_err(|e| DbError::Serialization(e.to_string()))?;
    Ok(manifest)
}

fn write_manifest_atomic(path: &Path, tmp_dir: &Path, manifest: &Manifest) -> Result<(), DbError> {
    let bytes = bincode::serialize(manifest).map_err(|e| DbError::Serialization(e.to_string()))?;
    let mut hasher = Crc32::new();
    hasher.update(&bytes);
    let crc = hasher.finalize();

    let tmp_path = tmp_dir.join("MANIFEST.bin.tmp");
    let mut w = BufWriter::new(OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?);
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
    let parent = path.parent().ok_or_else(|| DbError::Internal("Missing parent dir".to_string()))?;
    let dir = File::open(parent)?;
    dir.sync_data()?;
    Ok(())
}

#[inline]
fn check_tags(point_tags: &TagSet, filter_tags: &TagSet) -> bool {
    if point_tags.len() < filter_tags.len() {
        return false;
    }
    filter_tags
        .iter()
        .all(|(key, value)| point_tags.get(key) == Some(value))
}

// --- binary helpers ---

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[cfg(test)]
#[allow(dead_code)]
fn write_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32, DbError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
fn read_u64<R: Read>(r: &mut R) -> Result<u64, DbError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
fn read_f64<R: Read>(r: &mut R) -> Result<f64, DbError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(f64::from_le_bytes(b))
}
fn read_string<R: Read>(r: &mut R) -> Result<String, DbError> {
    let len = read_u32(r)? as usize;
    let mut b = vec![0u8; len];
    r.read_exact(&mut b)?;
    String::from_utf8(b).map_err(|e| DbError::Internal(format!("Invalid UTF-8: {}", e)))
}

#[inline]
fn crc32(bytes: &[u8]) -> u32 {
    let mut h = Crc32::new();
    h.update(bytes);
    h.finalize()
}

// --- varint (unsigned LEB128) helpers ---

fn write_var_u64(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn write_var_u32(buf: &mut Vec<u8>, v: u32) {
    write_var_u64(buf, v as u64);
}

fn read_var_u64<R: Read>(r: &mut R) -> Result<u64, String> {
    let mut out: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)
            .map_err(|e| format!("Truncated varint: {}", e))?;
        let byte = b[0];
        out |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Ok(out);
        }
        shift = shift.saturating_add(7);
    }
    Err("Varint too long".to_string())
}

fn read_var_u32<R: Read>(r: &mut R) -> Result<u32, String> {
    let v = read_var_u64(r)?;
    if v > u32::MAX as u64 {
        return Err("Varint does not fit in u32".to_string());
    }
    Ok(v as u32)
}

// --- compression helpers ---

fn compress_block_payload(
    compression: BlockCompression,
    payload: &[u8],
) -> Result<(u8, u32, Vec<u8>), DbError> {
    match compression {
        BlockCompression::None => Ok((COMPRESS_NONE, 0, payload.to_vec())),
        BlockCompression::Lz4 => Ok((COMPRESS_LZ4, 0, lz4_flex::compress_prepend_size(payload))),
        BlockCompression::Zstd { level } => {
            let compressed = zstd::bulk::compress(payload, level)
                .map_err(|e| DbError::Internal(format!("Zstd compress failed: {}", e)))?;
            let param = u32::from_le_bytes(level.to_le_bytes());
            Ok((COMPRESS_ZSTD, param, compressed))
        }
    }
}

// --- Gorilla XOR float bitpacking (u64 bit patterns) ---

struct BitWriter<'a> {
    out: &'a mut Vec<u8>,
    cur: u8,
    used: u8, // bits used in cur [0..=8)
}

impl<'a> BitWriter<'a> {
    fn new(out: &'a mut Vec<u8>) -> Self {
        Self { out, cur: 0, used: 0 }
    }

    fn push_bit(&mut self, bit: bool) {
        let b = if bit { 1u8 } else { 0u8 };
        self.cur |= b << (7 - self.used);
        self.used += 1;
        if self.used == 8 {
            self.out.push(self.cur);
            self.cur = 0;
            self.used = 0;
        }
    }

    fn push_bits(&mut self, mut v: u64, count: u8) {
        // Write `count` bits, MSB-first among the `count` least-significant bits of `v`.
        if count == 0 {
            return;
        }
        // Keep only the relevant bits.
        if count < 64 {
            v &= (1u64 << count) - 1;
        }
        for i in (0..count).rev() {
            self.push_bit(((v >> i) & 1) == 1);
        }
    }

    fn finish(mut self) {
        if self.used > 0 {
            self.out.push(self.cur);
            self.cur = 0;
            self.used = 0;
        }
    }
}

struct BitReader<'a, R: Read> {
    r: &'a mut R,
    cur: u8,
    left: u8, // bits remaining in cur
}

impl<'a, R: Read> BitReader<'a, R> {
    fn new(r: &'a mut R) -> Self {
        Self { r, cur: 0, left: 0 }
    }

    fn read_bit(&mut self) -> Result<bool, DbError> {
        if self.left == 0 {
            let mut b = [0u8; 1];
            self.r.read_exact(&mut b).map_err(DbError::Io)?;
            self.cur = b[0];
            self.left = 8;
        }
        let bit = (self.cur & (1u8 << (self.left - 1))) != 0;
        self.left -= 1;
        Ok(bit)
    }

    fn read_bits(&mut self, count: u8) -> Result<u64, DbError> {
        let mut out = 0u64;
        for _ in 0..count {
            out <<= 1;
            out |= if self.read_bit()? { 1 } else { 0 };
        }
        Ok(out)
    }
}

fn encode_gorilla_xor_u64(values: &[u64], out: &mut Vec<u8>) -> Result<(), DbError> {
    if values.is_empty() {
        return Err(DbError::Internal("Cannot Gorilla-encode empty values".to_string()));
    }
    out.extend_from_slice(&values[0].to_le_bytes());
    let mut bw = BitWriter::new(out);

    let mut prev = values[0];
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;
    let mut prev_sigbits: u8 = 0;

    for &cur in &values[1..] {
        let x = prev ^ cur;
        if x == 0 {
            bw.push_bit(false);
        } else {
            bw.push_bit(true);
            let leading = x.leading_zeros() as u8;
            let trailing = x.trailing_zeros() as u8;
            let sigbits_u32 = 64u32
                .saturating_sub(leading as u32)
                .saturating_sub(trailing as u32);
            let sigbits: u8 = sigbits_u32
                .try_into()
                .map_err(|_| DbError::Internal("Invalid significant bit width".to_string()))?;

            if prev_sigbits != 0 && leading >= prev_leading && trailing >= prev_trailing {
                // Reuse previous window
                bw.push_bit(false);
                let significant = x >> prev_trailing;
                bw.push_bits(significant, prev_sigbits);
            } else {
                // New window
                bw.push_bit(true);
                bw.push_bits(leading as u64, 6);
                bw.push_bits((sigbits - 1) as u64, 6);
                let significant = x >> trailing;
                bw.push_bits(significant, sigbits);
                prev_leading = leading;
                prev_trailing = trailing;
                prev_sigbits = sigbits;
            }
        }
        prev = cur;
    }
    bw.finish();
    Ok(())
}

fn decode_gorilla_xor_u64<R: Read>(r: &mut R, count: usize) -> Result<Vec<u64>, DbError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let first = read_u64(r)?;
    let mut out = Vec::with_capacity(count);
    out.push(first);
    let mut br = BitReader::new(r);

    let mut prev = first;
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;
    let mut prev_sigbits: u8 = 0;

    for _ in 1..count {
        let control = br.read_bit()?;
        if !control {
            out.push(prev);
            continue;
        }
        let mode = br.read_bit()?;
        let (_leading, sigbits, trailing) = if !mode {
            if prev_sigbits == 0 {
                return Err(DbError::Corruption {
                    details: "Gorilla reuse window before initialization".to_string(),
                    series: None,
                    timestamp: None,
                });
            }
            (prev_leading, prev_sigbits, prev_trailing)
        } else {
            let leading = br.read_bits(6)? as u8;
            let sigbits = (br.read_bits(6)? as u8).saturating_add(1);
            if leading > 63 || sigbits == 0 || sigbits > 64 {
                return Err(DbError::Corruption {
                    details: "Invalid Gorilla bit widths".to_string(),
                    series: None,
                    timestamp: None,
                });
            }
            let trailing = 64u8
                .saturating_sub(leading)
                .saturating_sub(sigbits);
            prev_leading = leading;
            prev_trailing = trailing;
            prev_sigbits = sigbits;
            (leading, sigbits, trailing)
        };

        let significant = br.read_bits(sigbits)?;
        let x = significant << trailing;
        let cur = prev ^ x;
        out.push(cur);
        prev = cur;
    }
    Ok(out)
}

// --- Tag dictionary encoding (block-local) ---

fn encode_tags_dictionary(rows: &[Row], out: &mut Vec<u8>) -> Result<(), DbError> {
    use std::collections::BTreeSet;

    let mut uniq: BTreeSet<String> = BTreeSet::new();
    for r in rows {
        for (k, v) in &r.tags {
            uniq.insert(k.clone());
            uniq.insert(v.clone());
        }
    }
    let dict: Vec<String> = uniq.into_iter().collect();

    let mut map: HashMap<&str, u32> = HashMap::with_capacity(dict.len());
    for (i, s) in dict.iter().enumerate() {
        map.insert(s.as_str(), i as u32);
    }

    write_var_u32(out, dict.len() as u32);
    for s in &dict {
        let b = s.as_bytes();
        let n: u32 = b
            .len()
            .try_into()
            .map_err(|_| DbError::Internal("Dictionary string too large".to_string()))?;
        write_var_u32(out, n);
        out.extend_from_slice(b);
    }

    let mut tags_blob: Vec<u8> = Vec::new();
    let mut lengths: Vec<u32> = Vec::with_capacity(rows.len());
    for r in rows {
        let mut kv: Vec<(&String, &String)> = r.tags.iter().collect();
        kv.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));

        let mut row_enc: Vec<u8> = Vec::new();
        write_var_u32(&mut row_enc, kv.len() as u32);
        for (k, v) in kv {
            let kid = *map
                .get(k.as_str())
                .ok_or_else(|| DbError::Internal("Missing dictionary key".to_string()))?;
            let vid = *map
                .get(v.as_str())
                .ok_or_else(|| DbError::Internal("Missing dictionary value".to_string()))?;
            write_var_u32(&mut row_enc, kid);
            write_var_u32(&mut row_enc, vid);
        }

        let len_u32: u32 = row_enc
            .len()
            .try_into()
            .map_err(|_| DbError::Internal("Tags blob too large".to_string()))?;
        lengths.push(len_u32);
        tags_blob.extend_from_slice(&row_enc);
    }

    for l in lengths {
        write_var_u32(out, l);
    }

    let tags_len_u32: u32 = tags_blob
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Tags blob too large".to_string()))?;
    write_var_u32(out, tags_len_u32);
    out.extend_from_slice(&tags_blob);
    Ok(())
}

#[cfg(test)]
mod encoding_compression_acceptance_tests {
    use super::*;

    fn make_rows() -> Vec<Row> {
        let mut t0: TagSet = TagSet::new();
        t0.insert("host".to_string(), "a".to_string());
        t0.insert("region".to_string(), "us-east-1".to_string());

        vec![
            Row {
                seq: 1,
                timestamp: 100,
                value: 1.25,
                tags: t0.clone(),
            },
            Row {
                seq: 2,
                timestamp: 105,
                value: 1.5,
                tags: t0,
            },
        ]
    }

    #[test]
    fn series_block_container_has_and_enforces_versioning() {
        let rows = make_rows();
        let enc = SegmentEncodingConfig {
            float_encoding: FloatEncoding::Raw64,
            tag_encoding: TagEncoding::Dictionary,
            compression: BlockCompression::None,
        };
        let mut block = encode_series_block(&rows, &enc).expect("encode");

        // Container header: [magic:8][block_version:u32]...
        block[8..12].copy_from_slice(&u32::MAX.to_le_bytes());

        let err = decode_series_block_v2_container(&block, std::path::Path::new("dummy.seg")).unwrap_err();
        match err {
            DbError::Corruption { details, .. } => {
                assert!(
                    details.contains("Unexpected series block version"),
                    "unexpected details: {details}"
                );
            }
            other => panic!("expected corruption error, got {other:?}"),
        }
    }

    #[test]
    fn series_block_container_has_and_enforces_payload_checksum() {
        let rows = make_rows();
        let enc = SegmentEncodingConfig {
            float_encoding: FloatEncoding::GorillaXor,
            tag_encoding: TagEncoding::Dictionary,
            compression: BlockCompression::Zstd { level: 1 },
        };
        let mut block = encode_series_block(&rows, &enc).expect("encode");

        // Corrupt expected CRC32 in the header while leaving stored payload intact.
        // Layout (bytes): magic(8) + ver(4) + row_count(4) + codec(4) + comp_param(4) +
        //                 uncompressed_len(4) + expected_crc(4) + stored_len(4) + stored_payload...
        let expected_crc_off = 8 + 4 + 4 + 4 + 4 + 4;
        block[expected_crc_off..expected_crc_off + 4].copy_from_slice(&0u32.to_le_bytes());

        let err = decode_series_block_v2_container(&block, std::path::Path::new("dummy.seg")).unwrap_err();
        match err {
            DbError::Corruption { details, .. } => {
                assert!(
                    details.contains("payload CRC mismatch"),
                    "unexpected details: {details}"
                );
            }
            other => panic!("expected corruption error, got {other:?}"),
        }
    }
}

