use std::sync::OnceLock;
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, TagSet};

fn install_prometheus_once() -> &'static ugnos::telemetry::db_metrics::InProcessPrometheus {
    static PROM: OnceLock<ugnos::telemetry::db_metrics::InProcessPrometheus> = OnceLock::new();
    PROM.get_or_init(|| {
        ugnos::telemetry::db_metrics::InProcessPrometheus::install(Duration::from_millis(25))
            .expect("failed to install in-process prometheus recorder")
    })
}

fn parse_sample(rendered: &str, metric: &str) -> Option<f64> {
    for line in rendered.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let key = parts.next()?;
        let val = parts.next()?;
        // Support exact match and label form: `name{...} value`.
        if key == metric || key.starts_with(metric) && key.as_bytes().get(metric.len()).copied() == Some(b'{') {
            if let Ok(v) = val.parse::<f64>() {
                return Some(v);
            }
        }
    }
    None
}

fn parse_counter(rendered: &str, base_name: &str) -> f64 {
    parse_sample(rendered, &format!("{}_total", base_name))
        .or_else(|| parse_sample(rendered, base_name))
        .unwrap_or(0.0)
}

fn extract_ugnos_lines(rendered: &str) -> String {
    rendered
        .lines()
        .filter(|l| l.contains("ugnos_"))
        .take(200)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn emits_ingest_flush_wal_snapshot_metrics() {
    let prom = install_prometheus_once();

    let before = prom.render();
    let before_ingest = parse_counter(&before, "ugnos_ingest_points");
    let before_flush_points = parse_counter(&before, "ugnos_flush_points");
    let before_wal_bytes = parse_counter(&before, "ugnos_wal_bytes_written");
    let before_wal_fsync_count = parse_sample(&before, "ugnos_wal_fsync_duration_seconds_count").unwrap_or(0.0);
    let before_flush_count = parse_sample(&before, "ugnos_flush_duration_seconds_count").unwrap_or(0.0);
    let before_snapshot_count = parse_sample(&before, "ugnos_snapshot_duration_seconds_count").unwrap_or(0.0);

    let dir = TempDir::new().expect("tempdir");
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.enable_segments = false;
    cfg.enable_wal = true;
    cfg.wal_buffer_size = 1; // force WAL flush+fsync per insert
    cfg.enable_snapshots = true;
    cfg.snapshot_interval = Duration::from_secs(60 * 60);
    cfg.flush_interval = Duration::from_secs(60 * 60);

    let db = DbCore::with_config(cfg).expect("db init");

    let mut tags: TagSet = TagSet::new();
    tags.insert("host".to_string(), "test".to_string());

    let points = 10u64;
    for i in 0..points {
        db.insert("metrics_series", i, (i as f64) * 1.25, tags.clone())
            .expect("insert");
    }

    db.flush().expect("flush");
    db.snapshot().expect("snapshot");

    // Give the exporter time to run upkeep at least once.
    std::thread::sleep(Duration::from_millis(60));

    let after = prom.render();
    let after_ingest = parse_counter(&after, "ugnos_ingest_points");
    let after_flush_points = parse_counter(&after, "ugnos_flush_points");
    let after_wal_bytes = parse_counter(&after, "ugnos_wal_bytes_written");
    let after_wal_fsync_count = parse_sample(&after, "ugnos_wal_fsync_duration_seconds_count").unwrap_or(0.0);
    let after_flush_count = parse_sample(&after, "ugnos_flush_duration_seconds_count").unwrap_or(0.0);
    let after_snapshot_count = parse_sample(&after, "ugnos_snapshot_duration_seconds_count").unwrap_or(0.0);
    let snapshot_size = parse_sample(&after, "ugnos_snapshot_size_bytes").unwrap_or(0.0);

    assert!(
        after_ingest >= before_ingest + (points as f64),
        "expected ingest counter to increase by at least {}, before={}, after={}\n\nRendered ugnos_* metrics:\n{}",
        points,
        before_ingest,
        after_ingest,
        extract_ugnos_lines(&after)
    );

    assert!(
        after_flush_points >= before_flush_points + (points as f64),
        "expected flush points counter to increase by at least {}, before={}, after={}",
        points,
        before_flush_points,
        after_flush_points
    );

    assert!(
        after_wal_bytes > before_wal_bytes,
        "expected WAL bytes written to increase, before={}, after={}",
        before_wal_bytes,
        after_wal_bytes
    );

    assert!(
        after_wal_fsync_count >= before_wal_fsync_count + (points as f64),
        "expected WAL fsync histogram count to increase by at least {}, before={}, after={}",
        points,
        before_wal_fsync_count,
        after_wal_fsync_count
    );

    assert!(
        after_flush_count >= before_flush_count + 1.0,
        "expected flush duration histogram count to increase, before={}, after={}",
        before_flush_count,
        after_flush_count
    );

    assert!(
        after_snapshot_count >= before_snapshot_count + 1.0,
        "expected snapshot duration histogram count to increase, before={}, after={}",
        before_snapshot_count,
        after_snapshot_count
    );

    assert!(snapshot_size > 0.0, "expected snapshot size gauge to be > 0");
}

