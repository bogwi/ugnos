use std::sync::OnceLock;
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, DbError, TagSet};

fn install_prometheus_once() -> &'static ugnos::telemetry::db_metrics::InProcessPrometheus {
    static PROM: OnceLock<ugnos::telemetry::db_metrics::InProcessPrometheus> = OnceLock::new();
    PROM.get_or_init(|| {
        ugnos::telemetry::db_metrics::InProcessPrometheus::install(Duration::from_millis(25))
            .expect("failed to install in-process prometheus recorder")
    })
}

fn find_metric_value(rendered: &str, metric: &str, must_contain: Option<&str>) -> Option<f64> {
    for line in rendered.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let key = parts.next()?;
        let val = parts.next()?;
        if key != metric && !key.starts_with(metric) {
            continue;
        }
        if let Some(needle) = must_contain {
            if !key.contains(needle) {
                continue;
            }
        }
        if let Ok(v) = val.parse::<f64>() {
            return Some(v);
        }
    }
    None
}

fn parse_counter_with_scope(rendered: &str, base_name: &str, scope: &str) -> f64 {
    let label = format!("scope=\"{}\"", scope);
    find_metric_value(rendered, &format!("{}_total", base_name), Some(&label))
        .or_else(|| find_metric_value(rendered, base_name, Some(&label)))
        .unwrap_or(0.0)
}

fn parse_gauge_with_scope(rendered: &str, name: &str, scope: &str) -> Option<f64> {
    let label = format!("scope=\"{}\"", scope);
    find_metric_value(rendered, name, Some(&label))
}

fn tags(pairs: &[(&str, &str)]) -> TagSet {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Acceptance criterion: "Configurable hard limit for series cardinality returns explicit error + metrics."
///
/// Break-it posture:
/// - If the implementation forgets to record metrics on rejection, this test will fail.
/// - If the implementation returns a generic error, this test will fail.
#[test]
fn cardinality_limit_rejection_emits_explicit_error_and_metrics() {
    let prom = install_prometheus_once();

    // Use an unlikely-to-collide scope label so other tests don't pollute this sample.
    let scope = "tenant_cardinality_metrics_test_scope";

    let before = prom.render();
    let before_rejections =
        parse_counter_with_scope(&before, "ugnos_cardinality_limit_rejections", scope);
    let before_cardinality =
        parse_gauge_with_scope(&before, "ugnos_series_cardinality", scope).unwrap_or(0.0);

    let dir = TempDir::new().expect("tempdir");
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.enable_segments = true;
    cfg.enable_wal = false;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(60 * 60);
    cfg.max_series_cardinality = Some(2);
    cfg.cardinality_scope_tag_key = Some("tenant".to_string());

    let db = DbCore::with_config(cfg).expect("db init");

    // Two distinct series keys (same series name, different tagsets) under the same tenant scope.
    db.insert(
        "m",
        1,
        1.0,
        tags(&[("tenant", scope), ("host", "a")]),
    )
    .unwrap();
    db.insert(
        "m",
        2,
        2.0,
        tags(&[("tenant", scope), ("host", "b")]),
    )
    .unwrap();

    // Third distinct series key => must be rejected.
    let r = db.insert(
        "m",
        3,
        3.0,
        tags(&[("tenant", scope), ("host", "c")]),
    );
    assert!(
        matches!(r, Err(DbError::SeriesCardinalityLimitExceeded { .. })),
        "expected explicit SeriesCardinalityLimitExceeded; got: {r:?}"
    );

    // Give exporter upkeep a moment to run.
    std::thread::sleep(Duration::from_millis(60));
    let after = prom.render();

    let after_rejections = parse_counter_with_scope(&after, "ugnos_cardinality_limit_rejections", scope);
    let after_cardinality =
        parse_gauge_with_scope(&after, "ugnos_series_cardinality", scope).unwrap_or(0.0);

    assert!(
        after_rejections >= before_rejections + 1.0,
        "expected per-scope cardinality rejection counter to increase by at least 1; before={}, after={}\nRendered:\n{}",
        before_rejections,
        after_rejections,
        after
    );

    // Gauge is updated on successful inserts; the rejection path should not inflate it.
    assert!(
        (after_cardinality - 2.0).abs() < f64::EPSILON && after_cardinality >= before_cardinality,
        "expected per-scope series cardinality gauge to settle at 2; before={}, after={}\nRendered:\n{}",
        before_cardinality,
        after_cardinality,
        after
    );
}

