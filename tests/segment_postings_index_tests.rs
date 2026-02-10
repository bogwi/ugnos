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
        if key == metric || (key.starts_with(metric) && key.as_bytes().get(metric.len()) == Some(&b'{')) {
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

fn tags(pairs: &[(&str, &str)]) -> TagSet {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Segment-level postings index must allow skipping whole segments/blocks before reading series blocks.
///
/// This test asserts the skip counter increases when querying for a tag that can only match a
/// subset of segments (forcing at least one skip).
#[test]
fn segment_postings_index_skips_non_matching_segments() {
    let prom = install_prometheus_once();
    let before = prom.render();
    let before_skips = parse_counter(&before, "ugnos_tag_postings_segment_skips");

    let dir = TempDir::new().unwrap();
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.enable_segments = true;
    cfg.enable_wal = false;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(3600);
    cfg.segment_store.enable_tag_index = true;

    let db = DbCore::with_config(cfg).unwrap();

    // Segment 1: host=a
    db.insert("m", 1, 1.0, tags(&[("host", "a")])).unwrap();
    db.flush().unwrap();

    // Segment 2: host=b
    db.insert("m", 2, 2.0, tags(&[("host", "b")])).unwrap();
    db.flush().unwrap();

    let mut filter = TagSet::new();
    filter.insert("host".to_string(), "b".to_string());
    let out = db.query("m", 0..10, Some(&filter)).unwrap();
    assert_eq!(out, vec![(2, 2.0)]);

    // Give exporter upkeep a moment to run.
    std::thread::sleep(Duration::from_millis(60));
    let after = prom.render();
    let after_skips = parse_counter(&after, "ugnos_tag_postings_segment_skips");

    assert!(
        after_skips >= before_skips + 1.0,
        "expected postings skip counter to increase by at least 1; before={}, after={}\nRendered:\n{}",
        before_skips,
        after_skips,
        after
    );
}

