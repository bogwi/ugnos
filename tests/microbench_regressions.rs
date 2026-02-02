//! Microbench regression gates.
//!
//! These are intentionally `#[ignore]` because they are environment-sensitive.
//! CI should run them in `--release` with explicit thresholds configured via env vars.

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, TagSet};

const SEED: u64 = 0x_5547_4E4F_535F_4D42; // fixed seed for microbench fixtures

#[derive(Clone, Debug)]
struct InsertOp {
    series: String,
    ts: u64,
    val: f64,
    tags: TagSet,
}

fn generate_ops(points: usize, series_count: usize, tag_pairs: usize, tag_cardinality: u32) -> Vec<InsertOp> {
    let mut rng = ChaCha8Rng::seed_from_u64(SEED);
    let mut ops = Vec::with_capacity(points);
    for i in 0..points {
        let series = format!("series_{}", i % series_count);
        let ts = i as u64;
        let val = rng.random::<u32>() as f64 * 0.001;
        let mut tags = TagSet::new();
        for k in 0..tag_pairs {
            let key = format!("k{}", k);
            let v = rng.random_range(0..tag_cardinality);
            let value = format!("v{}", v);
            tags.insert(key, value);
        }
        ops.push(InsertOp { series, ts, val, tags });
    }
    ops
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok())
}

#[test]
#[ignore]
fn microbench_ingest_throughput_guard() {
    let min_pts_per_sec = match env_u64("UGNOS_MIN_INGEST_PTS_PER_SEC") {
        Some(v) => v as f64,
        None => return, // not configured; CI must set this
    };

    let points = env_u64("UGNOS_MICROBENCH_POINTS").unwrap_or(50_000) as usize;
    let ops = generate_ops(points, 64, 4, 16);

    let dir = TempDir::new().expect("tempdir");
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.enable_segments = false;
    cfg.enable_wal = false;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(60 * 60);

    let db = DbCore::with_config(cfg).expect("db init");

    let started = Instant::now();
    for op in ops {
        db.insert(&op.series, op.ts, op.val, op.tags).unwrap();
    }
    db.flush().unwrap();
    let elapsed = started.elapsed().as_secs_f64();

    let pts_per_sec = (points as f64) / elapsed.max(1e-9);
    assert!(
        pts_per_sec >= min_pts_per_sec,
        "ingest throughput regression: got {:.0} pts/s, expected >= {} pts/s",
        pts_per_sec,
        min_pts_per_sec as u64
    );
}

#[test]
#[ignore]
fn microbench_flush_latency_guard() {
    let max_flush_ms = match env_u64("UGNOS_MAX_FLUSH_MS") {
        Some(v) => v as f64,
        None => return, // not configured; CI must set this
    };

    let points = env_u64("UGNOS_MICROBENCH_POINTS").unwrap_or(50_000) as usize;
    let ops = generate_ops(points, 64, 4, 16);

    let dir = TempDir::new().expect("tempdir");
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.enable_segments = false;
    cfg.enable_wal = false;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(60 * 60);

    let db = DbCore::with_config(cfg).expect("db init");
    for op in ops {
        db.insert(&op.series, op.ts, op.val, op.tags).unwrap();
    }

    let started = Instant::now();
    db.flush().unwrap();
    let flush_ms = started.elapsed().as_secs_f64() * 1000.0;

    assert!(
        flush_ms <= max_flush_ms,
        "flush latency regression: got {:.2}ms, expected <= {:.2}ms",
        flush_ms,
        max_flush_ms
    );
}

