//! PromQL library API: single entry point for running PromQL against a [`DbCore`].
//!
//! Exposes instant query, range query, labels, and label values with the same semantics as the
//! Prometheus HTTP API (`GET /api/v1/query`, `GET /api/v1/query_range`, `GET /api/v1/labels`,
//! `GET /api/v1/label/<name>/values`, etc.), using typed programmatic contracts and a unified error type.

use crate::core::DbCore;
use crate::error::DbError;
use crate::prometheus_api::{eval_matrix, eval_vector};
use crate::query_surface::{parse_eval_expr, parse_instant_selector, series_matches_selector};
use std::collections::{HashMap, HashSet};
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

/// One series in a range (matrix) result: metric labels and step-aligned `(timestamp_ns, value)` pairs.
///
/// Same semantics as a single element of the `result` array in `GET /api/v1/query_range`
/// when `resultType` is `matrix`.
///
/// **Step invariant:** Each `(ts_ns, value)` in `steps` satisfies:
/// - `ts_ns` is on the query grid: `ts_ns == start_ns + k * step_ns` for some `k`, with `start_ns <= ts_ns <= end_ns`.
/// - Timestamps in `steps` are strictly increasing.
/// - `steps` is a *subset* of the full grid: a grid time is omitted when the series has no value at that
///   evaluation time (e.g. no sample at or before that time for an instant selector). Thus different
///   series can have different `steps.len()`.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSeries {
    /// Metric labels including `__name__`.
    pub metric: HashMap<String, String>,
    /// Step-aligned samples: `(ts_ns, value)` in ascending time order (subset of query grid).
    pub steps: Vec<(u64, f64)>,
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

/// Runs a range query over `[start_ns, end_ns]` with the given step.
///
/// Same semantics as `GET /api/v1/query_range`: parses the PromQL string, evaluates
/// at each step `start_ns`, `start_ns + step_ns`, … up to `end_ns`, and returns a matrix
/// of series, each with step-aligned `(ts_ns, value)` pairs.
///
/// # Arguments
/// * `db` - Database handle (shared).
/// * `query` - PromQL expression (instant selector, range function, or aggregation).
/// * `start_ns` - Start of the range (inclusive), nanoseconds since Unix epoch.
/// * `end_ns` - End of the range (inclusive), nanoseconds since Unix epoch.
/// * `step_ns` - Step between evaluation points in nanoseconds (must be positive).
///
/// # Returns
/// * `Ok(vec)` - Matrix of series; may be empty if no series match.
/// * `Err(PromqlError::Parse(_))` - Invalid or unsupported PromQL.
/// * `Err(PromqlError::BadParameter(_))` - Empty query, `start_ns >= end_ns`, or `step_ns == 0`.
/// * `Err(PromqlError::Execution(_))` - Storage error during evaluation.
///
/// # Example
///
/// ```ignore
/// use ugnos::promql::{query_range, RangeSeries, PromqlError};
/// use ugnos::DbCore;
/// use std::sync::Arc;
///
/// let db = Arc::new(DbCore::default());
/// let start_ns = 1_000_000_000;
/// let end_ns = 3_000_000_000;
/// let step_ns = 1_000_000_000;
/// match query_range(&db, "http_requests_total", start_ns, end_ns, step_ns) {
///     Ok(series) => { /* each series has metric + steps */ }
///     Err(PromqlError::BadParameter(msg)) => { /* invalid range/step */ }
///     Err(PromqlError::Parse(msg)) => { /* invalid query */ }
///     Err(PromqlError::Execution(e)) => { /* db error */ }
/// }
/// ```
pub fn query_range(
    db: &Arc<DbCore>,
    query: &str,
    start_ns: u64,
    end_ns: u64,
    step_ns: u64,
) -> Result<Vec<RangeSeries>, PromqlError> {
    let q = query.trim();
    if q.is_empty() {
        return Err(PromqlError::BadParameter("query string is empty".to_string()));
    }
    if start_ns >= end_ns {
        return Err(PromqlError::BadParameter(
            "start must be before end".to_string(),
        ));
    }
    if step_ns == 0 {
        return Err(PromqlError::BadParameter(
            "step must be positive".to_string(),
        ));
    }
    let expr = parse_eval_expr(q).map_err(PromqlError::Parse)?;
    let internal = eval_matrix(&expr, start_ns, end_ns, step_ns, db).map_err(PromqlError::Execution)?;
    Ok(internal
        .into_iter()
        .map(|sd| RangeSeries {
            metric: sd.metric,
            steps: sd.steps,
        })
        .collect())
}

/// Returns the set of label names present in series that match the optional selectors
/// and have at least one sample in the time range `[start_ns, end_ns]` (inclusive).
///
/// Same semantics as `GET /api/v1/labels`: optional `match[]` restricts to series matching
/// any of the given PromQL instant selectors; required `start`/`end` restrict to an
/// approximate time range (only series with data in that range contribute their labels).
///
/// # Arguments
/// * `db` - Database handle (shared).
/// * `match_selectors` - Optional list of PromQL instant selectors (e.g. `http_requests_total`, `metric{job="api"}`).
///   If `None` or empty, all series in the store are considered (within the time range).
/// * `start_ns` - Start of the time range (inclusive), nanoseconds since Unix epoch.
/// * `end_ns` - End of the time range (inclusive), nanoseconds since Unix epoch.
///
/// # Returns
/// * `Ok(vec)` - Sorted list of unique label names (including `__name__`); may be empty.
/// * `Err(PromqlError::BadParameter(_))` - `start_ns >= end_ns`.
/// * `Err(PromqlError::Parse(_))` - A selector in `match_selectors` is invalid PromQL.
/// * `Err(PromqlError::Execution(_))` - Storage error while checking series in range.
///
/// # Example
///
/// ```ignore
/// use ugnos::promql::labels;
/// use std::sync::Arc;
///
/// let db: Arc<DbCore> = /* ... */;
/// let start_ns = 1_000_000_000;
/// let end_ns = 3_000_000_000;
/// // All labels in the time range
/// let names = labels(&db, None::<&[String]>, start_ns, end_ns)?;
/// // Only labels from series matching the selector
/// let names = labels(&db, Some(&["http_requests_total{job=\"api\"}".into()]), start_ns, end_ns)?;
/// ```
pub fn labels(
    db: &Arc<DbCore>,
    match_selectors: Option<&[impl AsRef<str>]>,
    start_ns: u64,
    end_ns: u64,
) -> Result<Vec<String>, PromqlError> {
    if start_ns >= end_ns {
        return Err(PromqlError::BadParameter(
            "start must be before end".to_string(),
        ));
    }
    // Inclusive end: query range is [start_ns, end_ns]; storage uses [start, end).
    let query_end = end_ns.saturating_add(1);

    let keys = db.list_series_keys();

    // If match_selectors provided, parse them and filter keys to those matching any selector.
    let keys_to_consider: Vec<(String, HashMap<String, String>)> = match match_selectors {
        None => keys.clone(),
        Some(s) if s.is_empty() => keys.clone(),
        Some(selectors) => {
            let parsed: Vec<_> = selectors
                .iter()
                .map(|s| parse_instant_selector(s.as_ref().trim()).map_err(PromqlError::Parse))
                .collect::<Result<Vec<_>, _>>()?;
            keys.into_iter()
                .filter(|(name, tags)| {
                    parsed.iter().any(|inst| {
                        series_matches_selector(name, tags, &inst.selector)
                    })
                })
                .collect()
        }
    };

    let mut names: HashSet<String> = HashSet::new();
    names.insert("__name__".to_string());
    for (series_name, tags) in keys_to_consider {
        // Only include labels from series that have at least one point in [start_ns, end_ns].
        let range = start_ns..query_end;
        match db.query(&series_name, range, Some(&tags)) {
            Ok(points) if !points.is_empty() => {
                for k in tags.keys() {
                    names.insert(k.clone());
                }
            }
            Ok(_) | Err(_) => {}
        }
    }
    let mut data: Vec<String> = names.into_iter().collect();
    data.sort();
    Ok(data)
}

/// Returns the set of values for a given label name from series that match the optional selectors
/// and have at least one sample in the time range `[start_ns, end_ns]` (inclusive).
///
/// Same semantics as `GET /api/v1/label/<name>/values`: optional `match[]` restricts to series
/// matching any of the given PromQL instant selectors; required `start`/`end` restrict to an
/// approximate time range (only series with data in that range contribute their label values).
/// For the special label `__name__`, returns metric names (series names) instead of a tag value.
///
/// # Arguments
/// * `db` - Database handle (shared).
/// * `label_name` - Label whose values to return (use `__name__` for metric names).
/// * `match_selectors` - Optional list of PromQL instant selectors. If `None` or empty, all series
///   in the store are considered (within the time range).
/// * `start_ns` - Start of the time range (inclusive), nanoseconds since Unix epoch.
/// * `end_ns` - End of the time range (inclusive), nanoseconds since Unix epoch.
///
/// # Returns
/// * `Ok(vec)` - Sorted list of unique label values; may be empty if no series have the label or
///   no series have data in range.
/// * `Err(PromqlError::BadParameter(_))` - `start_ns >= end_ns`.
/// * `Err(PromqlError::Parse(_))` - A selector in `match_selectors` is invalid PromQL.
/// * `Err(PromqlError::Execution(_))` - Storage error while checking series in range.
///
/// # Example
///
/// ```ignore
/// use ugnos::promql::label_values;
/// use std::sync::Arc;
///
/// let db: Arc<DbCore> = /* ... */;
/// let start_ns = 1_000_000_000;
/// let end_ns = 3_000_000_000;
/// // All values for "job" in the time range
/// let values = label_values(&db, "job", None::<&[String]>, start_ns, end_ns)?;
/// // Only values from series matching the selector
/// let values = label_values(&db, "job", Some(&["http_requests_total{instance=\"a\"}".into()]), start_ns, end_ns)?;
/// // Metric names (same as values for __name__)
/// let names = label_values(&db, "__name__", None::<&[String]>, start_ns, end_ns)?;
/// ```
pub fn label_values(
    db: &Arc<DbCore>,
    label_name: &str,
    match_selectors: Option<&[impl AsRef<str>]>,
    start_ns: u64,
    end_ns: u64,
) -> Result<Vec<String>, PromqlError> {
    if start_ns >= end_ns {
        return Err(PromqlError::BadParameter(
            "start must be before end".to_string(),
        ));
    }
    let query_end = end_ns.saturating_add(1);

    let keys = db.list_series_keys();

    let keys_to_consider: Vec<(String, HashMap<String, String>)> = match match_selectors {
        None => keys.clone(),
        Some(s) if s.is_empty() => keys.clone(),
        Some(selectors) => {
            let parsed: Vec<_> = selectors
                .iter()
                .map(|s| parse_instant_selector(s.as_ref().trim()).map_err(PromqlError::Parse))
                .collect::<Result<Vec<_>, _>>()?;
            keys.into_iter()
                .filter(|(name, tags)| {
                    parsed.iter().any(|inst| {
                        series_matches_selector(name, tags, &inst.selector)
                    })
                })
                .collect()
        }
    };

    let mut values: HashSet<String> = HashSet::new();
    for (series_name, tags) in keys_to_consider {
        let range = start_ns..query_end;
        match db.query(&series_name, range, Some(&tags)) {
            Ok(points) if !points.is_empty() => {
                if label_name == "__name__" {
                    values.insert(series_name);
                } else if let Some(v) = tags.get(label_name) {
                    values.insert(v.clone());
                }
            }
            Ok(_) | Err(_) => {}
        }
    }
    let mut data: Vec<String> = values.into_iter().collect();
    data.sort();
    Ok(data)
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

    // --- Labels: same semantics as GET /api/v1/labels (library-only, no HTTP) ---
    // Tests follow time-series metadata API practices: unique sorted label names,
    // optional match[] filtering (OR semantics), and time-range restriction so only
    // series with data in [start_ns, end_ns] contribute (see Prometheus PR #7288, #8301).

    #[test]
    fn labels_returns_sorted_unique_names_including_metric_name() {
        let (db, _guard) = make_db_with_series();
        let names = labels(&db, None::<&[String]>, 0, 3_000_000_000).unwrap();
        assert!(
            names.contains(&"__name__".to_string()),
            "labels must include __name__"
        );
        assert!(names.contains(&"job".to_string()), "labels must include job");
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "labels must be returned sorted");
    }

    #[test]
    fn labels_with_match_selector_restricts_to_matching_series() {
        let (db, _guard) = make_db_with_series();
        // Only series with job="api" contribute; we still get __name__ and job.
        let names = labels(
            &db,
            Some(&["http_requests_total{job=\"api\"}".to_string()]),
            0,
            3_000_000_000,
        )
        .unwrap();
        assert!(names.contains(&"__name__".to_string()));
        assert!(names.contains(&"job".to_string()));
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn labels_time_range_excludes_series_with_no_data_in_range() {
        let (db, _guard) = make_db_with_series();
        // Data at 1s, 1.5s, 2s. Range [2.5s, 3s] contains no samples -> only __name__.
        let names = labels(&db, None::<&[String]>, 2_500_000_000, 3_000_000_000).unwrap();
        assert_eq!(names, ["__name__"]);
    }

    #[test]
    fn labels_time_range_includes_only_series_with_points_in_range() {
        let (db, _guard) = make_db_with_series();
        // Range [1s, 1.2s]: only api series has a point at 1s; web has 1.5s. So only api contributes.
        let names = labels(&db, None::<&[String]>, 1_000_000_000, 1_200_000_000).unwrap();
        assert!(names.contains(&"__name__".to_string()));
        assert!(names.contains(&"job".to_string()));
    }

    #[test]
    fn labels_start_ge_end_returns_bad_parameter() {
        let (db, _guard) = make_db_with_series();
        let err = labels(&db, None::<&[String]>, 2_000_000_000, 1_000_000_000).unwrap_err();
        match &err {
            PromqlError::BadParameter(msg) => assert!(msg.contains("start") && msg.contains("end")),
            _ => panic!("expected BadParameter, got {:?}", err),
        }
    }

    #[test]
    fn labels_invalid_match_selector_returns_parse_error() {
        let (db, _guard) = make_db_with_series();
        let err = labels(
            &db,
            Some(&["sum(rate(x[5m]))".to_string()]),
            0,
            3_000_000_000,
        )
        .unwrap_err();
        assert!(matches!(err, PromqlError::Parse(_)));
    }

    #[test]
    fn labels_library_parity_with_instant_and_range() {
        // Acceptance: caller can run instant, range, and labels from library only; no duplicated logic.
        let (db, _guard) = make_db_for_aggregation();
        let time_ns = 3_000_000_000u64;
        let _samples = query_instant(&db, "http_requests_total", time_ns).unwrap();
        let _series = query_range(
            &db,
            "http_requests_total",
            1_000_000_000,
            5_000_000_000,
            1_000_000_000,
        )
        .unwrap();
        let names = labels(
            &db,
            Some(&["http_requests_total".to_string()]),
            1_000_000_000,
            5_000_000_000,
        )
        .unwrap();
        assert!(names.contains(&"__name__".to_string()));
        assert!(names.contains(&"job".to_string()));
        assert!(names.contains(&"instance".to_string()));
    }

    // --- Label values: same semantics as GET /api/v1/label/<name>/values (library-only, no HTTP) ---
    // Tests follow Prometheus metadata API and time-series DB practices: time-range restriction
    // so only series with data in [start_ns, end_ns] contribute; optional match[] filtering;
    // __name__ returns metric names; unknown label returns empty (see Prometheus PR #7288, #8301).

    #[test]
    fn label_values_returns_sorted_unique_values_for_label() {
        let (db, _guard) = make_db_with_series();
        let values = label_values(&db, "job", None::<&[String]>, 0, 3_000_000_000).unwrap();
        assert!(values.contains(&"api".to_string()));
        assert!(values.contains(&"web".to_string()));
        let mut sorted = values.clone();
        sorted.sort();
        assert_eq!(values, sorted, "label values must be returned sorted");
    }

    #[test]
    fn label_values_for_name_returns_metric_names() {
        let (db, _guard) = make_db_with_series();
        let names = label_values(&db, "__name__", None::<&[String]>, 0, 3_000_000_000).unwrap();
        assert_eq!(names, ["http_requests_total"]);
    }

    #[test]
    fn label_values_with_match_selector_restricts_to_matching_series() {
        let (db, _guard) = make_db_with_series();
        let values = label_values(
            &db,
            "job",
            Some(&["http_requests_total{job=\"api\"}".to_string()]),
            0,
            3_000_000_000,
        )
        .unwrap();
        assert_eq!(values, ["api"]);
    }

    #[test]
    fn label_values_time_range_excludes_series_with_no_data_in_range() {
        let (db, _guard) = make_db_with_series();
        // Data at 1s, 1.5s, 2s. Range [2.5s, 3s] contains no samples -> empty values for "job".
        let values = label_values(&db, "job", None::<&[String]>, 2_500_000_000, 3_000_000_000).unwrap();
        assert!(values.is_empty());
        let names = label_values(&db, "__name__", None::<&[String]>, 2_500_000_000, 3_000_000_000).unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn label_values_time_range_includes_only_series_with_points_in_range() {
        let (db, _guard) = make_db_with_series();
        // Range [1s, 1.2s]: only api series has a point at 1s; web has 1.5s. So only "api" for job.
        let values = label_values(&db, "job", None::<&[String]>, 1_000_000_000, 1_200_000_000).unwrap();
        assert_eq!(values, ["api"]);
    }

    #[test]
    fn label_values_start_ge_end_returns_bad_parameter() {
        let (db, _guard) = make_db_with_series();
        let err = label_values(&db, "job", None::<&[String]>, 2_000_000_000, 1_000_000_000).unwrap_err();
        match &err {
            PromqlError::BadParameter(msg) => assert!(msg.contains("start") && msg.contains("end")),
            _ => panic!("expected BadParameter, got {:?}", err),
        }
    }

    #[test]
    fn label_values_invalid_match_selector_returns_parse_error() {
        let (db, _guard) = make_db_with_series();
        let err = label_values(
            &db,
            "job",
            Some(&["sum(rate(x[5m]))".to_string()]),
            0,
            3_000_000_000,
        )
        .unwrap_err();
        assert!(matches!(err, PromqlError::Parse(_)));
    }

    #[test]
    fn label_values_unknown_label_returns_empty() {
        let (db, _guard) = make_db_with_series();
        let values = label_values(&db, "nonexistent_label", None::<&[String]>, 0, 3_000_000_000).unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn label_values_library_parity_with_instant_and_labels() {
        // Acceptance: caller can execute label values from library only; no duplicated metadata logic.
        let (db, _guard) = make_db_for_aggregation();
        let _samples = query_instant(&db, "http_requests_total", 3_000_000_000).unwrap();
        let _names = labels(
            &db,
            Some(&["http_requests_total".to_string()]),
            1_000_000_000,
            5_000_000_000,
        )
        .unwrap();
        let job_values = label_values(
            &db,
            "job",
            Some(&["http_requests_total".to_string()]),
            1_000_000_000,
            5_000_000_000,
        )
        .unwrap();
        assert!(job_values.contains(&"api".to_string()));
        assert!(job_values.contains(&"web".to_string()));
        let instance_values = label_values(
            &db,
            "instance",
            None::<&[String]>,
            1_000_000_000,
            5_000_000_000,
        )
        .unwrap();
        assert!(instance_values.contains(&"a".to_string()));
        assert!(instance_values.contains(&"b".to_string()));
        assert!(instance_values.contains(&"c".to_string()));
    }

    // --- Range query: same semantics as GET /api/v1/query_range (library-only, no HTTP) ---
    // Tests follow time-series DB best practices: deterministic step alignment, explicit time
    // ranges, and assertion on matrix shape and step count (see promtool-style range testing).

    fn sort_range_series(series: &mut [super::RangeSeries]) {
        series.sort_by_key(|a| metric_sort_key(&a.metric));
    }

    /// Specification for range query matrix results (invariant from eval_instant_matrix / eval_range_fn_matrix):
    /// - Every stored step timestamp is on the query grid: start_ns + k*step_ns for some k, with start_ns <= t <= end_ns.
    /// - Steps per series are strictly increasing.
    /// - Stored steps are a *subset* of the full grid: a step is omitted when the series has no value at that time
    ///   (e.g. instant selector: no point at or before eval_ts; range function: window yields None).
    /// So step count can differ per series.
    fn assert_range_series_invariant(
        series: &[super::RangeSeries],
        start_ns: u64,
        end_ns: u64,
        step_ns: u64,
    ) {
        for s in series {
            assert!(s.metric.contains_key("__name__"));
            assert!(!s.steps.is_empty());
            let mut prev_ts = 0u64;
            for (ts, _) in &s.steps {
                assert!(
                    *ts >= start_ns && *ts <= end_ns && (ts - start_ns) % step_ns == 0,
                    "steps must be on query grid"
                );
                assert!(*ts > prev_ts, "steps must be strictly increasing");
                prev_ts = *ts;
            }
        }
    }

    #[test]
    fn range_instant_selector_returns_step_aligned_matrix() {
        let (db, _guard) = make_db_with_series();
        let start_ns = 1_000_000_000;
        let end_ns = 3_000_000_000;
        let step_ns = 1_000_000_000;
        let result = query_range(&db, "http_requests_total", start_ns, end_ns, step_ns).unwrap();
        let mut series = result;
        sort_range_series(&mut series);
        assert!(!series.is_empty(), "at least one series");

        assert_range_series_invariant(&series, start_ns, end_ns, step_ns);

        // Proof from test data: make_db_with_series has api at 1s, 2s and web at 1.5s only.
        // Grid steps: 1s, 2s, 3s. At each step we take latest point with pt <= eval_ts.
        // - api: at 1s exists point 1s → step; at 2s exists 2s → step; at 3s exists 2s → step. So 3 steps.
        // - web: at 1s no point <= 1s → omit; at 2s exists 1.5s → step; at 3s exists 1.5s → step. So 2 steps.
        // So differing step counts are *required* by the semantics; asserting exact counts proves the spec.
        let api_series = series.iter().find(|s| s.metric.get("job") == Some(&"api".to_string()));
        let web_series = series.iter().find(|s| s.metric.get("job") == Some(&"web".to_string()));
        assert!(api_series.is_some(), "api series must be present");
        assert!(web_series.is_some(), "web series must be present");
        assert_eq!(
            api_series.unwrap().steps.len(),
            3,
            "api has points at 1s and 2s so has a value at every grid step 1s,2s,3s"
        );
        assert_eq!(
            web_series.unwrap().steps.len(),
            2,
            "web has point only at 1.5s; no point <= 1s so step 1s omitted; steps at 2s and 3s only"
        );
    }

    #[test]
    fn range_start_after_end_returns_bad_parameter() {
        let (db, _guard) = make_db_with_series();
        let err = query_range(
            &db,
            "http_requests_total",
            3_000_000_000,
            1_000_000_000,
            1_000_000_000,
        )
        .unwrap_err();
        match &err {
            PromqlError::BadParameter(msg) => assert!(msg.contains("start") && msg.contains("end")),
            _ => panic!("expected BadParameter, got {:?}", err),
        }
    }

    #[test]
    fn range_zero_step_returns_bad_parameter() {
        let (db, _guard) = make_db_with_series();
        let err = query_range(
            &db,
            "http_requests_total",
            1_000_000_000,
            3_000_000_000,
            0,
        )
        .unwrap_err();
        match &err {
            PromqlError::BadParameter(msg) => assert!(msg.contains("step")),
            _ => panic!("expected BadParameter, got {:?}", err),
        }
    }

    #[test]
    fn range_empty_query_returns_bad_parameter() {
        let (db, _guard) = make_db_with_series();
        let err = query_range(&db, "", 1_000_000_000, 3_000_000_000, 1_000_000_000).unwrap_err();
        match &err {
            PromqlError::BadParameter(msg) => assert!(msg.contains("empty")),
            _ => panic!("expected BadParameter, got {:?}", err),
        }
    }

    #[test]
    fn range_parse_error_returns_parse() {
        let (db, _guard) = make_db_with_series();
        let err = query_range(
            &db,
            "metric_a + metric_b",
            1_000_000_000,
            3_000_000_000,
            1_000_000_000,
        )
        .unwrap_err();
        assert!(matches!(err, PromqlError::Parse(_)));
    }

    #[test]
    fn range_rate_returns_matrix_with_steps() {
        let (db, _guard) = make_db_with_series();
        let result = query_range(
            &db,
            "rate(http_requests_total[2s])",
            1_000_000_000,
            3_000_000_000,
            1_000_000_000,
        )
        .unwrap();
        assert!(!result.is_empty());
        for s in &result {
            assert!(s.metric.contains_key("__name__"));
            assert!(!s.steps.is_empty());
            for (_, v) in &s.steps {
                assert!(!v.is_nan(), "rate should not produce NaN for this data");
            }
        }
    }

    #[test]
    fn range_sum_by_returns_grouped_matrix() {
        let (db, _guard) = make_db_for_aggregation();
        let result = query_range(
            &db,
            r#"sum by (job) (http_requests_total)"#,
            1_000_000_000,
            5_000_000_000,
            1_000_000_000,
        )
        .unwrap();
        let mut series = result;
        sort_range_series(&mut series);
        assert_eq!(series.len(), 2, "one series per job group");
        let api = series.iter().find(|s| s.metric.get("job") == Some(&"api".to_string()));
        let web = series.iter().find(|s| s.metric.get("job") == Some(&"web".to_string()));
        assert!(api.is_some());
        assert!(web.is_some());
        // 5 steps: 1s..5s
        assert_eq!(api.unwrap().steps.len(), 5);
        assert_eq!(web.unwrap().steps.len(), 5);
    }

    #[test]
    fn range_nonexistent_metric_returns_empty_matrix() {
        let (db, _guard) = make_db_with_series();
        let result = query_range(
            &db,
            "nonexistent_metric",
            1_000_000_000,
            3_000_000_000,
            1_000_000_000,
        )
        .unwrap();
        assert!(result.is_empty());
    }

    /// Library range query and HTTP query_range use the same evaluation path; result shape must match.
    #[test]
    fn range_library_and_http_same_result_shape() {
        let (db, _guard) = make_db_with_series();
        let start_ns = 1_000_000_000;
        let end_ns = 3_000_000_000;
        let step_ns = 1_000_000_000;

        let lib_series = query_range(&db, "http_requests_total", start_ns, end_ns, step_ns).unwrap();

        let r = crate::prometheus_api::handle_query_range(
            Some("http_requests_total"),
            Some("1"),
            Some("3"),
            Some("1s"),
            &db,
        );
        assert_eq!(r.status, http::StatusCode::OK);
        let body: crate::prometheus_api::ApiEnvelope<crate::prometheus_api::QueryData> =
            serde_json::from_slice(&r.body).unwrap();
        let data = body.data.unwrap();
        let crate::prometheus_api::QueryResult::Matrix(http_matrix) = data.result else {
            panic!("expected matrix")
        };

        assert_eq!(
            lib_series.len(),
            http_matrix.len(),
            "library and HTTP must return same number of series"
        );
        let mut lib_sorted = lib_series;
        sort_range_series(&mut lib_sorted);
        let mut http_sorted = http_matrix;
        http_sorted.sort_by_key(|a| metric_sort_key(&a.metric));
        for (lib_s, http_s) in lib_sorted.iter().zip(http_sorted.iter()) {
            assert_eq!(lib_s.metric, http_s.metric);
            assert_eq!(
                lib_s.steps.len(),
                http_s.values.len(),
                "same step count per series"
            );
        }
    }
}
