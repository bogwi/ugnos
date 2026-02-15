use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;

use ugnos::{DbConfig, DbCore, TagSet, Timestamp, Value};

fn make_segments_config(dir: &Path) -> DbConfig {
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.to_path_buf();
    cfg.enable_segments = true;
    cfg.enable_wal = true;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(3600); // prevent background flush noise
    cfg.wal_buffer_size = 1; // force frequent WAL flushes

    // Make compaction eager so tests can trigger meaningful rewrites.
    cfg.segment_store.compaction_check_interval = Duration::from_millis(10);
    cfg.segment_store.l0_compaction_trigger_segment_count = 2;
    cfg
}

fn sort_results(v: &mut Vec<(Timestamp, Value)>) {
    // Project contract (README): query results are not guaranteed globally sorted across segments.
    // Tests that compare query outputs across compaction/recovery should normalize order.
    v.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.total_cmp(&b.1)));
}

// --- test-only manifest reader (black-box, validates CRC) ---

#[derive(serde::Deserialize)]
struct TestManifest {
    version: u32,
    next_segment_id: u64,
    delete_before: Option<u64>,
    segments: Vec<TestSegmentRecord>,
}

#[derive(serde::Deserialize)]
struct TestSegmentRecord {
    id: u64,
    level: u8,
    created_at: u64,
    max_seq: u64,
    min_ts: u64,
    max_ts: u64,
    file_name: String,
    series: std::collections::BTreeMap<String, TestSeriesBlockMeta>,
    #[serde(default)]
    tag_postings_offset: u64,
    #[serde(default)]
    tag_postings_len: u32,
}

#[derive(serde::Deserialize)]
struct TestSeriesBlockMeta {
    offset: u64,
    len: u64,
    row_count: u32,
    min_ts: u64,
    max_ts: u64,
    crc32: u32,
    #[serde(default)]
    tag_index_offset: u64,
    #[serde(default)]
    tag_index_len: u32,
}

fn assert_manifest_invariants(m: &TestManifest) {
    assert_eq!(m.version, 1, "manifest version mismatch");
    assert!(
        !m.segments.is_empty(),
        "expected manifest to contain at least one segment"
    );
    if let Some(db) = m.delete_before {
        assert!(db > 0, "delete_before must be > 0 when present");
    }

    let max_id = m.segments.iter().map(|s| s.id).max().unwrap_or(0);
    assert!(
        m.next_segment_id >= max_id.saturating_add(1),
        "next_segment_id must be >= max(segment_id)+1 (max_id={}, next={})",
        max_id,
        m.next_segment_id
    );

    for s in &m.segments {
        assert!(s.id > 0, "segment id must be > 0");
        assert!(
            !s.file_name.is_empty(),
            "segment file_name must be non-empty"
        );
        assert!(
            s.level <= 1,
            "segment level must be 0 or 1 in tests (got {})",
            s.level
        );
        assert!(s.created_at > 0, "segment created_at must be > 0");
        assert!(s.max_seq > 0, "segment max_seq must be > 0");
        assert!(s.min_ts <= s.max_ts, "segment min_ts must be <= max_ts");

        // Postings index fields should be consistent (both present or both absent).
        assert!(
            (s.tag_postings_offset == 0 && s.tag_postings_len == 0)
                || (s.tag_postings_offset > 0 && s.tag_postings_len > 0),
            "tag postings offset/len must be both zero or both non-zero (off={}, len={})",
            s.tag_postings_offset,
            s.tag_postings_len
        );

        // Series block metadata should be self-consistent.
        for (name, meta) in &s.series {
            assert!(!name.is_empty(), "series name must be non-empty");
            assert!(meta.offset > 0, "series block offset must be > 0");
            assert!(meta.len > 0, "series block len must be > 0");
            assert!(meta.row_count > 0, "series block row_count must be > 0");
            assert!(meta.min_ts <= meta.max_ts, "series meta min_ts <= max_ts");
            assert!(meta.crc32 != 0, "series block crc32 must be non-zero");
            assert!(
                (meta.tag_index_offset == 0 && meta.tag_index_len == 0)
                    || (meta.tag_index_offset > 0 && meta.tag_index_len > 0),
                "tag index offset/len must be both zero or both non-zero (off={}, len={})",
                meta.tag_index_offset,
                meta.tag_index_len
            );
        }
    }
}

fn read_manifest(dir: &Path) -> TestManifest {
    use crc32fast::Hasher as Crc32;
    use std::fs::File;
    use std::io::Read;

    // `SegmentStore::open` stores manifest under `data_dir/engine/segments/MANIFEST.bin`
    let manifest_path = dir.join("engine").join("segments").join("MANIFEST.bin");
    let mut f = File::open(&manifest_path).unwrap();
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic).unwrap();
    assert_eq!(&magic, b"UGNMAN01");
    let mut b4 = [0u8; 4];
    f.read_exact(&mut b4).unwrap();
    let _version = u32::from_le_bytes(b4);
    let mut b8 = [0u8; 8];
    f.read_exact(&mut b8).unwrap();
    let len = u64::from_le_bytes(b8) as usize;
    f.read_exact(&mut b4).unwrap();
    let expected_crc = u32::from_le_bytes(b4);
    let mut payload = vec![0u8; len];
    f.read_exact(&mut payload).unwrap();
    let mut hasher = Crc32::new();
    hasher.update(&payload);
    let actual_crc = hasher.finalize();
    assert_eq!(actual_crc, expected_crc, "manifest CRC mismatch");
    let m = bincode::deserialize::<TestManifest>(&payload).unwrap();
    assert_manifest_invariants(&m);
    m
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(cond(), "condition not met within {:?}", timeout);
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 1000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_compaction_preserves_query_results(
        // Generate a proptest strategy that produces a vector of 1 to 199 tuples.
        // Each tuple contains:
        //  - a u8 value in the range 0..3 (i.e., 0, 1, or 2),
        //  - a u64 timestamp in the range 0..9_999,
        //  - and a random normal (non-NaN, non-infinite) f64 value (prop::num::f64::NORMAL).
        // The vector simulates a randomized batch of (series_id, timestamp, value) operations
        ops in prop::collection::vec((0u8..3, 0u64..10_000, prop::num::f64::NORMAL), 1..200)
    ) {
        let dir = tempdir().unwrap();
        let cfg = make_segments_config(dir.path());
        let db = DbCore::with_config(cfg).unwrap();

        let mut used: HashSet<String> = HashSet::new();
        let empty_tags: TagSet = TagSet::new();

        // Insert in bursts to create multiple L0 segments.
        for (i, (series_id, ts, val)) in ops.iter().cloned().enumerate() {
            let series = format!("series_{}", series_id);
            used.insert(series.clone());
            db.insert(&series, ts, val, empty_tags.clone()).unwrap();

            if i % 10 == 0 {
                db.flush().unwrap();
            }
        }
        db.flush().unwrap();

        // Capture baseline results.
        let mut baseline: HashMap<String, Vec<(Timestamp, Value)>> = HashMap::new();
        for series in &used {
            let mut r = db.query(series, 0..u64::MAX, None).unwrap();
            sort_results(&mut r);
            baseline.insert(series.clone(), r);
        }

        // Force compaction and verify results are identical.
        db.compact().unwrap();

        for series in &used {
            let mut r = db.query(series, 0..u64::MAX, None).unwrap();
            sort_results(&mut r);
            prop_assert_eq!(&r, baseline.get(series).unwrap());
        }
    }
}

#[test]
fn test_retention_tombstone_is_immediate_and_compaction_safe() {
    let dir = tempdir().unwrap();
    let cfg = make_segments_config(dir.path());
    let db = DbCore::with_config(cfg.clone()).unwrap();

    let series = "retention_series";
    let tags: TagSet = TagSet::new();

    // Insert points across a wide range.
    for ts in 0u64..200u64 {
        db.insert(series, ts, ts as f64, tags.clone()).unwrap();
        if ts % 25 == 0 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();

    // Apply retention: delete everything before 100.
    db.set_delete_before(100).unwrap();

    let mut r = db.query(series, 0..u64::MAX, None).unwrap();
    sort_results(&mut r);
    assert!(!r.is_empty());
    assert!(r.iter().all(|(ts, _)| *ts >= 100));

    // Compaction must not reintroduce deleted data.
    db.compact().unwrap();
    let mut r2 = db.query(series, 0..u64::MAX, None).unwrap();
    sort_results(&mut r2);
    assert_eq!(r2, r);

    drop(db);

    // Restart: retention watermark must persist via manifest.
    let mut db2 = DbCore::with_config(cfg).unwrap();
    db2.recover().unwrap();

    let mut r3 = db2.query(series, 0..u64::MAX, None).unwrap();
    sort_results(&mut r3);
    assert_eq!(r3, r2);
}

#[test]
fn test_segments_enable_fast_restart_without_full_wal_replay() {
    let dir = tempdir().unwrap();
    let cfg = make_segments_config(dir.path());

    // Create DB and write durable segments.
    {
        let db = DbCore::with_config(cfg.clone()).unwrap();
        let series = "restart_series";
        let tags: TagSet = TagSet::new();

        for i in 0..500u64 {
            db.insert(series, i, (i as f64) * 1.25, tags.clone())
                .unwrap();
            if i % 50 == 0 {
                db.flush().unwrap();
            }
        }
        db.flush().unwrap();
    }

    // Restart and recover. WAL should be truncated after tail materialization.
    let mut db2 = DbCore::with_config(cfg.clone()).unwrap();
    db2.recover().unwrap();

    let mut r = db2.query("restart_series", 0..u64::MAX, None).unwrap();
    sort_results(&mut r);
    assert_eq!(r.len(), 500);

    // WAL should be reset to just the header after recover.
    let wal_path = cfg.data_dir.join("wal").join("wal.log");
    let len = std::fs::metadata(&wal_path).unwrap().len();
    assert!(
        len <= 12,
        "expected WAL to be truncated to header, got {} bytes",
        len
    );
}

#[test]
fn test_retention_physically_reclaims_single_segment_without_explicit_compact() {
    let dir = tempdir().unwrap();
    let mut cfg = make_segments_config(dir.path());
    // Ensure count-based compaction cannot fire; we want retention reclamation to be responsible.
    cfg.segment_store.l0_compaction_trigger_segment_count = usize::MAX;

    let db = DbCore::with_config(cfg.clone()).unwrap();
    let series = "retention_reclaim_single_seg";
    let tags: TagSet = TagSet::new();

    // Create exactly one segment (single flush).
    for ts in 0u64..200u64 {
        db.insert(series, ts, ts as f64, tags.clone()).unwrap();
    }
    db.flush().unwrap();

    let m0 = read_manifest(&cfg.data_dir);
    assert_eq!(m0.segments.len(), 1);
    assert_eq!(m0.segments[0].min_ts, 0);
    assert_eq!(m0.segments[0].max_ts, 199);

    // Advance retention watermark: expect physical reclaim to eventually rewrite the segment so
    // the on-disk segment metadata no longer includes expired timestamps.
    db.set_delete_before(100).unwrap();

    wait_until(Duration::from_secs(2), || {
        let m = read_manifest(&cfg.data_dir);
        m.segments.len() == 1 && m.segments.iter().all(|s| s.min_ts >= 100)
    });
}

#[test]
fn test_retention_physically_reclaims_l1_segments_after_compaction() {
    let dir = tempdir().unwrap();
    let mut cfg = make_segments_config(dir.path());
    cfg.segment_store.l0_compaction_trigger_segment_count = 2;

    let db = DbCore::with_config(cfg.clone()).unwrap();
    let series = "retention_reclaim_l1";
    let tags: TagSet = TagSet::new();

    // Create multiple L0 segments then compact to L1.
    for ts in 0u64..150u64 {
        db.insert(series, ts, ts as f64, tags.clone()).unwrap();
        if ts % 50 == 0 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
    db.compact().unwrap();

    let before = read_manifest(&cfg.data_dir);
    assert!(
        before.segments.iter().any(|s| s.level == 1),
        "expected at least one L1 segment before retention reclaim"
    );
    assert!(
        before.segments.iter().any(|s| s.min_ts < 100),
        "expected some pre-retention data to exist on disk"
    );

    db.set_delete_before(100).unwrap();

    wait_until(Duration::from_secs(2), || {
        let m = read_manifest(&cfg.data_dir);
        // After retention reclaim, all persisted segments must have metadata consistent with the watermark.
        m.segments.iter().all(|s| s.min_ts >= 100)
    });
}

#[test]
fn test_compaction_triggers_on_total_bytes_threshold() {
    let dir = tempdir().unwrap();
    let mut cfg = make_segments_config(dir.path());
    cfg.segment_store.compaction_check_interval = Duration::from_millis(10);
    cfg.segment_store.l0_compaction_trigger_segment_count = usize::MAX; // disable count trigger
    cfg.segment_store.l0_compaction_trigger_total_bytes = Some(1); // effectively "always" once there are >=2 L0

    let db = DbCore::with_config(cfg.clone()).unwrap();
    let series = "bytes_trigger";
    let tags: TagSet = TagSet::new();

    // Produce >=2 L0 segments.
    for ts in 0u64..100u64 {
        db.insert(series, ts, ts as f64, tags.clone()).unwrap();
    }
    db.flush().unwrap();
    for ts in 100u64..200u64 {
        db.insert(series, ts, ts as f64, tags.clone()).unwrap();
    }
    db.flush().unwrap();

    wait_until(Duration::from_secs(2), || {
        let m = read_manifest(&cfg.data_dir);
        m.segments.iter().any(|s| s.level == 1)
    });
}

#[test]
fn test_compaction_triggers_on_age_threshold() {
    let dir = tempdir().unwrap();
    let mut cfg = make_segments_config(dir.path());
    cfg.segment_store.compaction_check_interval = Duration::from_millis(10);
    cfg.segment_store.l0_compaction_trigger_segment_count = usize::MAX; // disable count trigger
    cfg.segment_store.l0_compaction_trigger_total_bytes = None;
    cfg.segment_store.l0_compaction_trigger_max_age = Some(Duration::from_millis(1));

    let db = DbCore::with_config(cfg.clone()).unwrap();
    let series = "age_trigger";
    let tags: TagSet = TagSet::new();

    // Produce >=2 L0 segments.
    for ts in 0u64..50u64 {
        db.insert(series, ts, ts as f64, tags.clone()).unwrap();
    }
    db.flush().unwrap();
    for ts in 50u64..100u64 {
        db.insert(series, ts, ts as f64, tags.clone()).unwrap();
    }
    db.flush().unwrap();

    // Ensure the segments become "old enough".
    std::thread::sleep(Duration::from_millis(5));

    wait_until(Duration::from_secs(2), || {
        let m = read_manifest(&cfg.data_dir);
        m.segments.iter().any(|s| s.level == 1)
    });
}
