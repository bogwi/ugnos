use iai_callgrind::{library_benchmark, library_benchmark_group, main, LibraryBenchmarkConfig};
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::hint::black_box;
use std::time::Duration;
use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, TagSet};

const SEED: u64 = 0x_5547_4E4F_535F_4245; // "UGNOS_BE" (ish) — fixed seed for stable fixtures

#[derive(Debug)]
struct InsertOp {
    series: String,
    ts: u64,
    val: f64,
    tags: TagSet,
}

#[derive(Debug)]
struct Fixture {
    _dir: TempDir,
    db: DbCore,
    ops: Vec<InsertOp>,
}

fn gen_tags(rng: &mut impl Rng, tag_pairs: usize) -> TagSet {
    let mut tags = TagSet::new();
    for i in 0..tag_pairs {
        // Small controlled cardinality to avoid accidental label explosions.
        let k = format!("k{}", i);
        let v = format!("v{}", rng.random_range(0..16u32));
        tags.insert(k, v);
    }
    tags
}

fn gen_ops(points: usize, series_count: usize, tag_pairs: usize) -> Vec<InsertOp> {
    let mut rng = ChaCha8Rng::seed_from_u64(SEED);
    let mut ops = Vec::with_capacity(points);

    for i in 0..points {
        let series = format!("series_{}", (i % series_count));
        let ts = i as u64;
        let val = rng.random::<u32>() as f64 * 0.001;
        let tags = gen_tags(&mut rng, tag_pairs);
        ops.push(InsertOp {
            series,
            ts,
            val,
            tags,
        });
    }

    ops
}

fn setup_fixture(points: usize, enable_wal: bool, enable_snapshots: bool) -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.enable_segments = false;
    cfg.enable_wal = enable_wal;
    cfg.wal_buffer_size = 1024;
    cfg.enable_snapshots = enable_snapshots;
    cfg.snapshot_interval = Duration::from_secs(60 * 60);
    cfg.flush_interval = Duration::from_secs(60 * 60);

    let db = DbCore::with_config(cfg).expect("db init");
    let ops = gen_ops(points, 64, 4);
    Fixture { _dir: dir, db, ops }
}

#[library_benchmark]
#[bench::wal_enabled_10k(setup_fixture(10_000, true, false))]
#[bench::wal_disabled_10k(setup_fixture(10_000, false, false))]
fn ingest_then_flush(mut fx: Fixture) -> u64 {
    let mut n = 0u64;
    for op in fx.ops.drain(..) {
        fx.db
            .insert(
                black_box(&op.series),
                black_box(op.ts),
                black_box(op.val),
                black_box(op.tags),
            )
            .unwrap();
        n += 1;
    }
    fx.db.flush().unwrap();
    black_box(n)
}

#[library_benchmark]
#[bench::snapshot_5k(setup_fixture(5_000, true, true))]
fn snapshot_after_ingest(mut fx: Fixture) -> usize {
    for op in fx.ops.drain(..) {
        fx.db.insert(&op.series, op.ts, op.val, op.tags).unwrap();
    }
    fx.db.flush().unwrap();
    fx.db.snapshot().unwrap();
    black_box(1usize)
}

library_benchmark_group!(
    name = microbench_group;
    config = LibraryBenchmarkConfig::default();
    benchmarks = ingest_then_flush, snapshot_after_ingest
);

main!(library_benchmark_groups = microbench_group);
