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
    v.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.total_cmp(&b.1)));
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 1000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_compaction_preserves_query_results(
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
            db.insert(series, i, (i as f64) * 1.25, tags.clone()).unwrap();
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
    assert!(len <= 12, "expected WAL to be truncated to header, got {} bytes", len);
}

