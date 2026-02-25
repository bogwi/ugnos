//! Demonstrates series cardinality limits: `max_series_cardinality` and optional
//! `cardinality_scope_tag_key`. Inserts up to the limit succeed; a third distinct
//! series returns `DbError::SeriesCardinalityLimitExceeded`; same-series inserts
//! still succeed.
//!
//! Run with: `cargo run --example cardinality_demo`

use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, DbError, TagSet};

fn main() -> Result<(), DbError> {
    let dir = TempDir::new().expect("temp dir");
    let cfg = DbConfig {
        data_dir: PathBuf::from(dir.path()),
        flush_interval: Duration::from_secs(60),
        enable_wal: false,
        enable_snapshots: false,
        enable_segments: true,
        max_series_cardinality: Some(2),
        cardinality_scope_tag_key: Some("tenant".to_string()),
        ..DbConfig::default()
    };

    let db = DbCore::with_config(cfg)?;
    let t0 = 1_700_000_000_000_000_000u64;

    // Two distinct series (same metric, different tag sets) in scope "api" → succeed.
    let mut tags1 = TagSet::new();
    tags1.insert("tenant".to_string(), "api".to_string());
    tags1.insert("instance".to_string(), "1".to_string());
    db.insert("requests_total", t0, 100.0, tags1.clone())?;

    let mut tags2 = TagSet::new();
    tags2.insert("tenant".to_string(), "api".to_string());
    tags2.insert("instance".to_string(), "2".to_string());
    db.insert("requests_total", t0 + 1, 200.0, tags2)?;

    // Third distinct series in the same scope → SeriesCardinalityLimitExceeded.
    let mut tags3 = TagSet::new();
    tags3.insert("tenant".to_string(), "api".to_string());
    tags3.insert("instance".to_string(), "3".to_string());
    let r = db.insert("requests_total", t0 + 2, 300.0, tags3);
    match r {
        Err(DbError::SeriesCardinalityLimitExceeded {
            current,
            limit,
            scope,
        }) => {
            assert_eq!(current, 2, "current cardinality should be 2");
            assert_eq!(limit, 2, "limit should be 2");
            assert_eq!(scope, "api", "scope should be derived from tenant tag");
            println!(
                "cardinality_demo: third distinct series correctly rejected (current={}, limit={}, scope={})",
                current, limit, scope
            );
        }
        Ok(()) => panic!("expected SeriesCardinalityLimitExceeded for third distinct series"),
        Err(e) => panic!("expected SeriesCardinalityLimitExceeded, got {:?}", e),
    }

    // Same-series insert (same metric + same tags as first) → still succeeds.
    db.insert("requests_total", t0 + 3, 101.0, tags1)?;
    println!("cardinality_demo: same-series insert still succeeds");
    Ok(())
}
