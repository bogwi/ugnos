use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ugnos::{DbCore, DbConfig, TagSet};
use std::time::Duration;
use std::env;
use std::path::PathBuf;
use std::fs;

// Helper to create tags
fn tags_from(pairs: &[(&str, &str)]) -> TagSet {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// Check if WAL should be disabled via environment variable
fn is_nowal_enabled() -> bool {
    env::var("NOWAL").is_ok()
}

// Create DbConfig based on flags
fn create_db_config(flush_interval: Duration) -> DbConfig {
    let mut config = DbConfig::default();
    config.flush_interval = flush_interval;
    config.enable_wal = !is_nowal_enabled();
    config
}

// Benchmark for inserting single points
fn bench_insert_single(c: &mut Criterion) {
    let config = create_db_config(Duration::from_secs(60)); // Use longer flush interval for benchmark
    let db = DbCore::with_config(config).unwrap();
    
    let wal_status = if is_nowal_enabled() { "disabled" } else { "enabled" };
    println!("Running insert benchmark with WAL {}", wal_status);
    
    let series_name = "bench_insert_series";
    let tags = tags_from(&[("host", "server_bench"), ("region", "bench_region")]);

    c.bench_function("insert_single", |b| {
        let mut i = 0u64;
        b.iter(|| {
            db.insert(
                black_box(series_name),
                black_box(i),
                black_box(i as f64 * 1.1),
                black_box(tags.clone()),
            )
            .unwrap();
            i += 1;
        })
    });
}

// Benchmark for querying data
fn bench_query(c: &mut Criterion) {
    let config = create_db_config(Duration::from_millis(100)); // Faster flush for setup
    let db = DbCore::with_config(config).unwrap();
    
    let wal_status = if is_nowal_enabled() { "disabled" } else { "enabled" };
    println!("Running query benchmark with WAL {}", wal_status);
    
    let series_name = "bench_query_series";
    let num_points = 100_000;
    let tags = tags_from(&[("host", "query_server"), ("dc", "dc1")]);

    // Pre-populate data
    println!("Setting up data for query benchmark...");
    for i in 0..num_points {
        db.insert(series_name, i, i as f64 * 0.9, tags.clone())
            .unwrap();
    }
    db.flush().unwrap();
    std::thread::sleep(Duration::from_secs(2)); // Ensure flush completes
    println!("Data setup complete.");

    let query_range = 50_000..(num_points - 10_000); // Query a large middle chunk
    let filter_tags = tags_from(&[("dc", "dc1")]);

    let mut group = c.benchmark_group("query_operations");

    group.bench_function("query_range_no_tags", |b| {
        b.iter(|| {
            let _ = db.query(
                black_box(series_name),
                black_box(query_range.clone()),
                black_box(None),
            )
            .unwrap();
        })
    });

    group.bench_function("query_range_with_tags", |b| {
        b.iter(|| {
            let _ = db.query(
                black_box(series_name),
                black_box(query_range.clone()),
                black_box(Some(&filter_tags)),
            )
            .unwrap();
        })
    });

    group.finish();
}

// Cleanup function that runs after all benchmarks
fn cleanup(_c: &mut Criterion) {
    println!("Cleaning up benchmark data directory...");
    let _ = fs::remove_dir_all(PathBuf::from("./data"));
    println!("Cleanup complete.");
}

criterion_group!(benches, bench_insert_single, bench_query, cleanup);
criterion_main!(benches);

