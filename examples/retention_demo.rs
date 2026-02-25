//! Demonstrates retention (TTL): `retention_ttl` and `retention_check_interval`.
//!
//! Creates a DB with a short TTL (e.g. 10s), inserts points with timestamps in the past
//! (one older than TTL, one within TTL), flushes so the background thread advances the
//! retention watermark, then queries to show that only the recent point is returned.
//! Optionally triggers compaction so expired data can be reclaimed on disk.
//!
//! Run with: `cargo run --example retention_demo`

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, DbError, TagSet};

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn main() -> Result<(), DbError> {
    let dir = TempDir::new().expect("temp dir");
    let ttl = Duration::from_secs(10);
    // Use ZERO so the first flush always runs the retention check (no dependency on thread timing).
    let retention_check = Duration::ZERO;

    let cfg = DbConfig {
        data_dir: PathBuf::from(dir.path()),
        flush_interval: Duration::from_millis(50),
        enable_wal: false,
        enable_snapshots: false,
        enable_segments: true,
        retention_ttl: Some(ttl),
        retention_check_interval: retention_check,
        ..DbConfig::default()
    };

    let db = DbCore::with_config(cfg)?;
    let now = now_ns();
    let ns_per_sec = 1_000_000_000u64;
    let old_ts = now.saturating_sub(15 * ns_per_sec);   // 15s ago
    let recent_ts = now.saturating_sub(2 * ns_per_sec); // 2s ago

    let mut tags = TagSet::new();
    tags.insert("host".to_string(), "demo".to_string());

    // Insert one point older than TTL and one within TTL.
    db.insert("temperature", old_ts, 10.0, tags.clone())?;
    db.insert("temperature", recent_ts, 20.0, tags)?;

    // Flush: the background thread runs retention (delete_before = now - TTL), then
    // ingests the buffer. Rows with timestamp < delete_before are excluded on ingest
    // and from future queries.
    db.flush()?;

    // Query over a wide range: only the recent point should be visible.
    let time_range = 0..u64::MAX;
    let results = db.query("temperature", time_range.clone(), None)?;
    assert_eq!(
        results.len(),
        1,
        "only the point within TTL should be visible; got {} points",
        results.len()
    );
    assert_eq!(results[0].0, recent_ts);
    assert_eq!(results[0].1, 20.0);
    println!(
        "retention_demo: TTL {}s applied; query returned 1 point (ts={}, value={}); older point excluded",
        ttl.as_secs(),
        recent_ts,
        results[0].1
    );

    // Optional: compaction so segments can drop expired data and reclaim disk.
    db.compact()?;
    let after_compact = db.query("temperature", time_range, None)?;
    assert_eq!(after_compact.len(), 1, "compaction must not change visible data");
    println!("retention_demo: compaction completed; query still returns 1 point");

    Ok(())
}
