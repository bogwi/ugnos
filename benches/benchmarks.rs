mod datasets;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, TagSet};

use std::time::Duration;

fn make_config(data_dir: &std::path::Path, enable_wal: bool) -> DbConfig {
    let mut cfg = DbConfig::default();
    cfg.data_dir = data_dir.to_path_buf();
    cfg.enable_segments = false; // microbench suite focuses on in-memory path determinism
    cfg.enable_wal = enable_wal;
    cfg.wal_buffer_size = 1024;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(60 * 60);
    cfg
}

fn bench_ingest_fixed_dataset(c: &mut Criterion) {
    let ops = datasets::generate_insert_ops(datasets::DEFAULT_SEED, 20_000, 64, 4, 16);

    #[derive(Debug)]
    struct Fixture {
        _dir: TempDir,
        db: DbCore,
    }

    let mut group = c.benchmark_group("ingest");

    for (name, enable_wal) in [("wal_enabled", true), ("wal_disabled", false)] {
        group.bench_function(format!("ingest_20k_{}", name), |b| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().expect("tempdir");
                    let cfg = make_config(dir.path(), enable_wal);
                    let db = DbCore::with_config(cfg).expect("db init");
                    Fixture { _dir: dir, db }
                },
                |fx| {
                    for op in &ops {
                        fx.db
                            .insert(
                                black_box(&op.series),
                                black_box(op.ts),
                                black_box(op.val),
                                black_box(op.tags.clone()),
                            )
                            .unwrap();
                    }
                    fx.db.flush().unwrap();
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

fn bench_query_fixed_dataset(c: &mut Criterion) {
    let ops = datasets::generate_insert_ops(datasets::DEFAULT_SEED, 120_000, 64, 4, 16);

    let dir = TempDir::new().expect("tempdir");
    let mut cfg = make_config(dir.path(), true);
    cfg.enable_wal = false; // keep query setup fast + deterministic

    let db = DbCore::with_config(cfg).expect("db init");
    for op in &ops {
        db.insert(&op.series, op.ts, op.val, op.tags.clone()).unwrap();
    }
    db.flush().unwrap();

    // Query a stable slice of one series.
    let series = "series_7";
    let range = 40_000u64..90_000u64;

    // Deterministic filter: "k0=v0" matches ~1/16 of points.
    let mut filter: TagSet = TagSet::new();
    filter.insert("k0".to_string(), "v0".to_string());

    let mut group = c.benchmark_group("query");
    group.bench_function("range_no_tags", |b| {
        b.iter(|| {
            let _ = db
                .query(black_box(series), black_box(range.clone()), black_box(None))
                .unwrap();
        })
    });
    group.bench_function("range_with_tag_filter", |b| {
        b.iter(|| {
            let _ = db
                .query(black_box(series), black_box(range.clone()), black_box(Some(&filter)))
                .unwrap();
        })
    });
    group.finish();
}

criterion_group!(benches, bench_ingest_fixed_dataset, bench_query_fixed_dataset);
criterion_main!(benches);

