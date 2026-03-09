//! PromQL library API: single entry point for running PromQL against a [`DbCore`].
//!
//! Exposes instant query, and (in follow-up work) range query, labels, label values, and series
//! with the same semantics as the Prometheus HTTP API (`GET /api/v1/query`, etc.),
//! using typed programmatic contracts and a unified error type.

use crate::core::DbCore;
use crate::error::DbError;
use crate::prometheus_api::eval_vector;
use crate::query_surface::parse_eval_expr;
use std::collections::HashMap;
use std::sync::Arc;

/// Unified error type for PromQL library operations.
///
/// Callers can distinguish parse failures (invalid PromQL), parameter errors
/// (missing/invalid time or step), and execution errors (storage/`DbError`)
/// without string matching.
#[derive(Debug)]
pub enum PromqlError {
    /// Invalid PromQL (e.g. unsupported syntax, parse failure).
    Parse(String),
    /// Missing or invalid time/step/range parameter.
    BadParameter(String),
    /// Storage or execution failure.
    Execution(DbError),
}

impl std::fmt::Display for PromqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PromqlError::Parse(s) => write!(f, "parse error: {}", s),
            PromqlError::BadParameter(s) => write!(f, "bad parameter: {}", s),
            PromqlError::Execution(e) => write!(f, "execution error: {}", e),
        }
    }
}

impl std::error::Error for PromqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PromqlError::Execution(e) => Some(e),
            _ => None,
        }
    }
}

impl From<DbError> for PromqlError {
    fn from(e: DbError) -> Self {
        PromqlError::Execution(e)
    }
}

/// One sample in an instant-vector result: metric labels, timestamp (nanoseconds), and value.
///
/// Same semantics as a single element of the `result` array in `GET /api/v1/query`
/// when `resultType` is `vector`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstantSample {
    /// Metric labels including `__name__`.
    pub metric: HashMap<String, String>,
    /// Timestamp of the sample in nanoseconds since Unix epoch.
    pub ts_ns: u64,
    /// Sample value (may be NaN or ±Inf per PromQL).
    pub value: f64,
}

/// Runs an instant query at a single evaluation time.
///
/// Same semantics as `GET /api/v1/query`: parses the PromQL string, evaluates
/// at `time_ns`, and returns a vector of samples (one per series for instant/range
/// selectors, or aggregated per group for aggregations).
///
/// # Arguments
/// * `db` - Database handle (shared).
/// * `query` - PromQL expression (e.g. `http_requests_total`, `rate(metric[5m])`, `sum by (job)(rate(x[5m]))`).
/// * `time_ns` - Evaluation time in nanoseconds since Unix epoch.
///
/// # Returns
/// * `Ok(vec)` - Instant vector of samples; may be empty if no series match.
/// * `Err(PromqlError::Parse(_))` - Invalid or unsupported PromQL.
/// * `Err(PromqlError::Execution(_))` - Storage error during evaluation.
///
/// # Example
///
/// ```ignore
/// use ugnos::promql::{query_instant, InstantSample, PromqlError};
/// use ugnos::DbCore;
/// use std::sync::Arc;
///
/// let db = Arc::new(DbCore::default());
/// let time_ns = 2_000_000_000; // 2s after epoch
/// match query_instant(&db, "http_requests_total", time_ns) {
///     Ok(samples) => { /* use samples */ }
///     Err(PromqlError::Parse(msg)) => { /* invalid query */ }
///     Err(PromqlError::Execution(e)) => { /* db error */ }
///     Err(PromqlError::BadParameter(_)) => { /* not used for instant */ }
/// }
/// ```
pub fn query_instant(
    db: &Arc<DbCore>,
    query: &str,
    time_ns: u64,
) -> Result<Vec<InstantSample>, PromqlError> {
    let q = query.trim();
    if q.is_empty() {
        return Err(PromqlError::BadParameter("query string is empty".to_string()));
    }
    let expr = parse_eval_expr(q).map_err(PromqlError::Parse)?;
    let internal = eval_vector(&expr, time_ns, db).map_err(PromqlError::Execution)?;
    Ok(internal
        .into_iter()
        .map(|s| InstantSample {
            metric: s.metric,
            ts_ns: s.ts_ns,
            value: s.value,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Builds a Db with known input series for instant-query tests (promtool-style input_series).
    /// Series: http_requests_total{job="api"} at 1s=10, 2s=20; http_requests_total{job="web"} at 1.5s=15.
    fn make_db_with_series() -> (Arc<DbCore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::DbConfig {
            data_dir: dir.path().to_path_buf(),
            max_series_cardinality: Some(1000),
            ..Default::default()
        };
        let mut db = DbCore::with_config(config).unwrap();
        db.recover().unwrap();
        let db = Arc::new(db);
        db.insert(
            "http_requests_total",
            1_000_000_000,
            10.0,
            [("job".to_string(), "api".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        db.insert(
            "http_requests_total",
            2_000_000_000,
            20.0,
            [("job".to_string(), "api".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        db.insert(
            "http_requests_total",
            1_500_000_000,
            15.0,
            [("job".to_string(), "web".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        db.flush().unwrap();
        (db, dir)
    }

    /// Db with multiple series for aggregation tests: job=api (instance a/b), job=web (instance c).
    fn make_db_for_aggregation() -> (Arc<DbCore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::DbConfig {
            data_dir: dir.path().to_path_buf(),
            max_series_cardinality: Some(1000),
            ..Default::default()
        };
        let mut db = DbCore::with_config(config).unwrap();
        db.recover().unwrap();
        let db = Arc::new(db);
        for i in 0..5 {
            db.insert(
                "http_requests_total",
                (i + 1) * 1_000_000_000,
                (i as f64 + 1.0) * 10.0,
                [
                    ("job".to_string(), "api".to_string()),
                    ("instance".to_string(), "a".to_string()),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        }
        for i in 0..5 {
            db.insert(
                "http_requests_total",
                (i + 1) * 1_000_000_000,
                (i as f64 + 1.0) * 5.0,
                [
                    ("job".to_string(), "api".to_string()),
                    ("instance".to_string(), "b".to_string()),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        }
        for i in 0..5 {
            db.insert(
                "http_requests_total",
                (i + 1) * 1_000_000_000,
                (i as f64 + 1.0) * 3.0,
                [
                    ("job".to_string(), "web".to_string()),
                    ("instance".to_string(), "c".to_string()),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        }
        db.flush().unwrap();
        (db, dir)
    }

    fn metric_sort_key(m: &HashMap<String, String>) -> String {
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        pairs
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn sort_samples(samples: &mut [InstantSample]) {
        samples.sort_by_key(|a| metric_sort_key(&a.metric));
    }

    // --- Instant query: same semantics as GET /api/v1/query ---

    #[test]
    fn instant_selector_returns_matching_series() {
        let (db, _guard) = make_db_with_series();
        let time_ns = 2_000_000_000u64;
        let result = query_instant(&db, "http_requests_total", time_ns).unwrap();
        let mut samples = result;
        sort_samples(&mut samples);
        assert_eq!(samples.len(), 2, "api and web series");
        let names: Vec<_> = samples
            .iter()
            .map(|s| s.metric.get("__name__").map(|x| x.as_str()))
            .collect();
        assert!(names.iter().all(|n| *n == Some("http_requests_total")));
        let jobs: Vec<_> = samples
            .iter()
            .map(|s| s.metric.get("job").map(|x| x.as_str()))
            .collect();
        assert!(jobs.contains(&Some("api")));
        assert!(jobs.contains(&Some("web")));
        // At time 2s: api has latest 20, web has latest 15 (1.5s)
        let api_sample = samples.iter().find(|s| s.metric.get("job") == Some(&"api".to_string()));
        let web_sample = samples.iter().find(|s| s.metric.get("job") == Some(&"web".to_string()));
        assert_eq!(api_sample.map(|s| s.value), Some(20.0));
        assert_eq!(web_sample.map(|s| s.value), Some(15.0));
        assert_eq!(api_sample.map(|s| s.ts_ns), Some(2_000_000_000));
        assert_eq!(web_sample.map(|s| s.ts_ns), Some(1_500_000_000));
    }

    #[test]
    fn instant_selector_with_label_matcher_equality() {
        let (db, _guard) = make_db_with_series();
        let result = query_instant(&db, r#"http_requests_total{job="api"}"#, 2_000_000_000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metric.get("job"), Some(&"api".to_string()));
        assert_eq!(result[0].value, 20.0);
    }

    #[test]
    fn instant_selector_with_label_matcher_not_equal() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, r#"http_requests_total{job!="web"}"#, 2_000_000_000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metric.get("job"), Some(&"api".to_string()));
    }

    #[test]
    fn instant_selector_with_regex_matcher() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, r#"http_requests_total{job=~"api|web"}"#, 2_000_000_000).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn instant_empty_query_returns_bad_parameter() {
        let (db, _guard) = make_db_with_series();
        let err = query_instant(&db, "", 2_000_000_000).unwrap_err();
        match &err {
            PromqlError::BadParameter(msg) => assert!(msg.contains("empty")),
            _ => panic!("expected BadParameter, got {:?}", err),
        }
    }

    #[test]
    fn instant_whitespace_only_query_returns_bad_parameter() {
        let (db, _guard) = make_db_with_series();
        let err = query_instant(&db, "   ", 2_000_000_000).unwrap_err();
        match &err {
            PromqlError::BadParameter(msg) => assert!(!msg.is_empty()),
            _ => panic!("expected BadParameter, got {:?}", err),
        }
    }

    #[test]
    fn instant_parse_error_binary_expr() {
        let (db, _guard) = make_db_with_series();
        let err = query_instant(&db, "metric_a + metric_b", 2_000_000_000).unwrap_err();
        match &err {
            PromqlError::Parse(_) => {}
            _ => panic!("expected Parse, got {:?}", err),
        }
    }

    #[test]
    fn instant_parse_error_bare_range_vector() {
        let (db, _guard) = make_db_with_series();
        let err = query_instant(&db, "http_requests_total[5m]", 2_000_000_000).unwrap_err();
        match &err {
            PromqlError::Parse(_) => {}
            _ => panic!("expected Parse, got {:?}", err),
        }
    }

    #[test]
    fn instant_parse_error_unsupported_function() {
        let (db, _guard) = make_db_with_series();
        let err = query_instant(
            &db,
            "histogram_quantile(0.9, rate(http_duration_bucket[5m]))",
            2_000_000_000,
        )
        .unwrap_err();
        match &err {
            PromqlError::Parse(_) => {}
            _ => panic!("expected Parse, got {:?}", err),
        }
    }

    #[test]
    fn instant_nonexistent_metric_returns_empty_vector() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, "nonexistent_metric", 2_000_000_000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn instant_nonexistent_selector_returns_empty_vector() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, r#"nonexistent{job="x"}"#, 2_000_000_000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn instant_rate_returns_vector_same_semantics_as_http() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, "rate(http_requests_total[5m])", 2_000_000_000).unwrap();
        // At least one series (api or web) has two points in [0,2] so rate is defined
        assert!(!result.is_empty());
        for s in &result {
            assert!(s.metric.contains_key("__name__"));
            assert!(s.metric.contains_key("job"));
            assert!(s.ts_ns == 2_000_000_000);
        }
    }

    #[test]
    fn instant_increase_returns_vector() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, "increase(http_requests_total[5m])", 2_000_000_000).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn instant_avg_over_time_returns_vector() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, "avg_over_time(http_requests_total[5m])", 2_000_000_000).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn instant_sum_by_returns_grouped_vector() {
        let (db, _guard) = make_db_for_aggregation();
        // At time 5s: api has 10+20+30+40+50 = 150 (inst a) + 5+10+15+20+25 = 75 (inst b); web 3+6+9+12+15 = 45
        let result = query_instant(
            &db,
            r#"sum by (job) (http_requests_total)"#,
            5_000_000_000,
        )
        .unwrap();
        let mut samples = result;
        sort_samples(&mut samples);
        assert_eq!(samples.len(), 2, "one group per job");
        let api = samples.iter().find(|s| s.metric.get("job") == Some(&"api".to_string()));
        let web = samples.iter().find(|s| s.metric.get("job") == Some(&"web".to_string()));
        assert!(api.is_some());
        assert!(web.is_some());
        // At 5s instant: latest per series is (5s, 50) for api/a, (5s, 25) for api/b, (5s, 15) for web/c. Sum by job: api = 75, web = 15.
        assert_eq!(api.map(|s| s.value), Some(75.0));
        assert_eq!(web.map(|s| s.value), Some(15.0));
    }

    #[test]
    fn instant_count_returns_series_count() {
        let (db, _guard) = make_db_for_aggregation();
        let result = query_instant(&db, "count(http_requests_total)", 5_000_000_000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 3.0, "three series");
    }

    #[test]
    fn instant_sum_of_empty_set_returns_empty_vector() {
        let (db, _guard) = make_db_with_series();
        let result =
            query_instant(&db, r#"sum by (job) (nonexistent)"#, 2_000_000_000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn instant_error_variants_distinguishable() {
        let (db, _guard) = make_db_with_series();
        let parse_err = query_instant(&db, "invalid {", 2_000_000_000).unwrap_err();
        assert!(matches!(parse_err, PromqlError::Parse(_)));
        let param_err = query_instant(&db, "", 2_000_000_000).unwrap_err();
        assert!(matches!(param_err, PromqlError::BadParameter(_)));
    }
}
