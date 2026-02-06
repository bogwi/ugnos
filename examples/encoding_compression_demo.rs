//! Example: configuring series block encoding and compression for the segment engine.
//!
//! This shows how to use timestamp delta encoding (varint), float encoding (Raw64 or GorillaXor),
//! tag dictionary encoding, and per-block compression (LZ4 or Zstd) when writing to segment files.

use std::path::PathBuf;
use std::time::Duration;
use ugnos::encoding::{BlockCompression, FloatEncoding};
use ugnos::{DbConfig, DbCore, DbError, TagSet};

fn main() -> Result<(), DbError> {
    let mut config = DbConfig::default();
    config.data_dir = PathBuf::from("./demo_encoding_data");
    config.enable_segments = true;
    config.flush_interval = Duration::from_millis(50);
    config.segment_store.encoding.float_encoding = FloatEncoding::GorillaXor;
    config.segment_store.encoding.compression = BlockCompression::Zstd { level: 3 };

    let mut db = DbCore::with_config(config)?;
    db.recover()?;

    let before = db
        .query("sensor_temp", 0..u64::MAX, None)
        .map(|rows| rows.len())
        // err means the first run, when no "./demo_encoding_data" exists, so we expect 0 rows
        // `.unwrap_or(0)` is a small hack to keep this example running
        // in production you have all means to handle this .query() error:
        // DbError::SeriesNotFound
        .unwrap_or(0); 

    let mut tags_a = TagSet::new();
    tags_a.insert("host".to_string(), "server1".to_string());
    tags_a.insert("region".to_string(), "west".to_string());
    db.insert("sensor_temp", 1_000_000_000, 22.5, tags_a.clone())?;
    db.insert("sensor_temp", 1_000_000_100, 22.7, tags_a.clone())?;
    db.insert("sensor_temp", 1_000_000_200, 22.4, tags_a)?;

    let mut tags_b = TagSet::new();
    tags_b.insert("host".to_string(), "server2".to_string());
    tags_b.insert("region".to_string(), "east".to_string());
    db.insert("sensor_temp", 1_000_000_300, 23.1, tags_b.clone())?;
    db.insert("sensor_temp", 1_000_000_400, 23.0, tags_b)?;

    db.flush()?;

    let after = db.query("sensor_temp", 0..u64::MAX, None)?.len();
    assert_eq!(after, before + 5);

    Ok(())
}
