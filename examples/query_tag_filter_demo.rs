//! Demonstrates tag filter in query: `db.query(series, range, Some(&tags))` restricts results
//! to points whose tags contain all filter key-value pairs. Inserts multiple tag sets, then
//! queries with and without a filter to show the contrast.
//!
//! Run with: `cargo run --example query_tag_filter_demo`

use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, DbError, TagSet};

fn main() -> Result<(), DbError> {
    let dir = TempDir::new().expect("temp dir");
    let cfg = DbConfig {
        data_dir: PathBuf::from(dir.path()),
        flush_interval: Duration::from_millis(100),
        enable_wal: true,
        enable_snapshots: true,
        enable_segments: true,
        ..DbConfig::default()
    };

    let db = DbCore::with_config(cfg)?;
    let t0 = 1_700_000_000_000_000_000u64;

    // Insert three distinct series (same metric, different tag sets).
    let mut tags1 = TagSet::new();
    tags1.insert("host".to_string(), "server1".to_string());
    tags1.insert("region".to_string(), "us-east".to_string());

    let mut tags2 = TagSet::new();
    tags2.insert("host".to_string(), "server2".to_string());
    tags2.insert("region".to_string(), "us-east".to_string());

    let mut tags3 = TagSet::new();
    tags3.insert("host".to_string(), "server3".to_string());
    tags3.insert("region".to_string(), "eu-west".to_string());

    db.insert("cpu_usage", t0, 0.5, tags1.clone())?;
    db.insert("cpu_usage", t0 + 1, 0.6, tags1)?;

    db.insert("cpu_usage", t0 + 2, 0.7, tags2.clone())?;
    db.insert("cpu_usage", t0 + 3, 0.8, tags2)?;

    db.insert("cpu_usage", t0 + 4, 0.9, tags3.clone())?;
    db.insert("cpu_usage", t0 + 5, 1.0, tags3)?;

    db.flush()?;

    // Query without filter: all 6 points.
    let all = db.query("cpu_usage", 0..u64::MAX, None)?;
    assert_eq!(all.len(), 6, "query without filter must return all points");

    // Query with filter host=server1: only the 2 points for that series.
    let mut filter = TagSet::new();
    filter.insert("host".to_string(), "server1".to_string());
    let filtered = db.query("cpu_usage", 0..u64::MAX, Some(&filter))?;
    assert_eq!(
        filtered.len(),
        2,
        "query with host=server1 must return only that series"
    );

    // Query with partial filter region=us-east: 4 points (server1 and server2).
    let mut region_filter = TagSet::new();
    region_filter.insert("region".to_string(), "us-east".to_string());
    let by_region = db.query("cpu_usage", 0..u64::MAX, Some(&region_filter))?;
    assert_eq!(
        by_region.len(),
        4,
        "query with region=us-east must return server1 and server2"
    );

    println!(
        "query_tag_filter_demo: no filter={}, host=server1={}, region=us-east={}",
        all.len(),
        filtered.len(),
        by_region.len()
    );
    Ok(())
}
