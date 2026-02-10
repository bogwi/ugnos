use std::collections::HashMap;
use std::fs;
use std::time::Duration;

use tempfile::tempdir;

use ugnos::{DbConfig, DbCore, DbError, Snapshotter, TagSet, WriteAheadLog};

mod test_manifest;

#[test]
fn test_recovery_after_crash_wal_append() {
    // Simulate "crash after WAL append": WAL contains inserts, nothing else is durable.
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // Write WAL directly (durable), then "crash".
    {
        let wal_dir = data_dir.join("wal");
        let mut wal = WriteAheadLog::new(&wal_dir, 1).unwrap();
        wal.log_insert(1, "series_a", 10, 1.25, TagSet::new())
            .unwrap();
        wal.log_insert(2, "series_a", 11, 2.25, TagSet::new())
            .unwrap();
        wal.flush_to_disk().unwrap();
        // Intentionally do not call `close()` to mimic abrupt termination.
    }

    // Restart and recover from WAL.
    let mut cfg = DbConfig::default();
    cfg.data_dir = data_dir;
    cfg.enable_wal = true;
    cfg.enable_snapshots = false;
    cfg.enable_segments = false;
    cfg.flush_interval = Duration::from_secs(3600);
    cfg.wal_buffer_size = 1;

    let mut db = DbCore::with_config(cfg).unwrap();
    db.recover().unwrap();

    let r = db.query("series_a", 0..u64::MAX, None).unwrap();
    assert!(
        r.windows(2).all(|w| w[1].0 >= w[0].0),
        "query must return points in timestamp order"
    );
    assert_eq!(r, vec![(10, 1.25), (11, 2.25)]);
}

#[test]
fn test_recovery_after_crash_during_flush_wal_rotated_segments_enabled() {
    // Simulate "crash during flush": WAL was rotated, but segments were not created/installed.
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // Create WAL with some data, then rotate it to a wal_*.log file (as flush would).
    {
        let wal_dir = data_dir.join("wal");
        let mut wal = WriteAheadLog::new(&wal_dir, 1).unwrap();
        wal.log_insert(1, "series_b", 100, 10.0, TagSet::new())
            .unwrap();
        wal.log_insert(2, "series_b", 101, 11.0, TagSet::new())
            .unwrap();
        wal.flush_to_disk().unwrap();
        wal.rotate(123_456_789).unwrap();
    }

    // Restart with segments enabled: recovery must materialize WAL tail into a new L0 segment.
    let mut cfg = DbConfig::default();
    cfg.data_dir = data_dir.clone();
    cfg.enable_wal = true;
    cfg.enable_snapshots = false;
    cfg.enable_segments = true;
    cfg.flush_interval = Duration::from_secs(3600);
    cfg.wal_buffer_size = 1;

    let mut db = DbCore::with_config(cfg.clone()).unwrap();
    db.recover().unwrap();

    // Make the ordering assertion airtight: this scenario should materialize exactly one segment,
    // so the query is effectively single-segment (global ordering across multiple segments is not
    // guaranteed per the project contract).
    let m = test_manifest::read_manifest(&cfg.data_dir);
    assert_eq!(
        m.segments.len(),
        1,
        "expected exactly one segment after recovery; if this becomes multi-segment, do not assume query order"
    );

    let r = db.query("series_b", 0..u64::MAX, None).unwrap();
    assert!(
        r.windows(2).all(|w| w[1].0 >= w[0].0),
        "query must return points in timestamp order"
    );
    assert_eq!(r, vec![(100, 10.0), (101, 11.0)]);

    // WAL should be truncated to header after recovery (bounded restart cost).
    let wal_path = cfg.data_dir.join("wal").join("wal.log");
    let len = fs::metadata(&wal_path).unwrap().len();
    assert!(
        len <= 12,
        "expected WAL to be truncated to header, got {} bytes",
        len
    );
}

#[test]
fn test_recovery_after_crash_during_snapshot_write_tmp_is_ignored() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // Create one valid snapshot.
    {
        let snapshots_dir = data_dir.join("snapshots");
        let snapshotter = Snapshotter::new(&snapshots_dir).unwrap();
        let mut series_data = HashMap::new();
        let chunk = ugnos::types::TimeSeriesChunk {
            timestamps: vec![1, 2],
            values: vec![1.0, 2.0],
            tags: vec![TagSet::new(), TagSet::new()],
        };
        series_data.insert(
            "series_s".to_string(),
            std::sync::Arc::new(std::sync::RwLock::new(chunk)),
        );
        let _ = snapshotter.create_snapshot(&series_data, 100).unwrap();

        // Simulate crash during snapshot write of a newer snapshot: temp file exists, never renamed.
        let tmp = snapshots_dir.join(".tmp_snapshot_200.bin");
        fs::write(&tmp, b"partial").unwrap();
    }

    // DB recovery should ignore tmp files and load the valid snapshot.
    let mut cfg = DbConfig::default();
    cfg.data_dir = data_dir;
    cfg.enable_wal = false;
    cfg.enable_snapshots = true;
    cfg.enable_segments = false;
    cfg.flush_interval = Duration::from_secs(3600);

    let mut db = DbCore::with_config(cfg).unwrap();
    db.recover().unwrap();

    let r = db.query("series_s", 0..u64::MAX, None).unwrap();
    assert!(
        r.windows(2).all(|w| w[1].0 >= w[0].0),
        "query must return points in timestamp order"
    );
    assert_eq!(r, vec![(1, 1.0), (2, 2.0)]);
}

#[test]
fn test_wal_corruption_is_detected_with_series_and_timestamp_context() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let series = "series_corrupt";
    let ts = 999u64;

    // Create a WAL with one record.
    {
        let mut wal = WriteAheadLog::new(&wal_dir, 1).unwrap();
        let mut tags = TagSet::new();
        tags.insert("k".to_string(), "v".to_string());
        wal.log_insert(1, series, ts, 3.14, tags).unwrap();
        wal.flush_to_disk().unwrap();
    }

    // Corrupt a byte in the payload (not the header fields), so CRC mismatch is triggered.
    let wal_path = wal_dir.join("wal.log");
    let mut bytes = fs::read(&wal_path).unwrap();
    // Header (magic+version) = 12 bytes, record header = len(4) + crc(4).
    let rec_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let payload_start = 12 + 4 + 4;
    let flip_idx = payload_start + (rec_len.saturating_sub(1));
    bytes[flip_idx] ^= 0xFF;
    fs::write(&wal_path, bytes).unwrap();

    // Reading must fail with a corruption error that includes series/timestamp context.
    let wal = WriteAheadLog::new(&wal_dir, 1).unwrap();
    let err = wal.read_all_entries().unwrap_err();
    match err {
        DbError::Corruption {
            series: s,
            timestamp: t,
            ..
        } => {
            assert_eq!(s.as_deref(), Some(series));
            assert_eq!(t, Some(ts));
        }
        other => panic!("expected DbError::Corruption, got {other:?}"),
    }
}
