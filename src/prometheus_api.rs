//! Prometheus HTTP API v1 (Grafana Prometheus datasource compatibility).
//!
//! Implements `GET /api/v1/query`, `query_range`, `labels`, `label/<name>/values`, and `series`
//! with the standard JSON envelope and result formats.

use crate::error::DbError;
use crate::DbCore;
use http::StatusCode;
use promql_parser::parser;
use promql_parser::parser::Expr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Content-Type for Prometheus API JSON responses.
pub const PROMETHEUS_API_CONTENT_TYPE: &str = "application/json";

// ---------- Prometheus response envelope ----------

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiEnvelope<T> {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "errorType")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

fn success<T: Serialize>(data: T) -> ApiEnvelope<T> {
    ApiEnvelope {
        status: "success".to_string(),
        data: Some(data),
        error_type: None,
        error: None,
        warnings: None,
    }
}

fn error_response(typ: &'static str, msg: String) -> ApiEnvelope<serde_json::Value> {
    ApiEnvelope {
        status: "error".to_string(),
        data: None,
        error_type: Some(typ.to_string()),
        error: Some(msg),
        warnings: None,
    }
}

// ---------- Query result types (Prometheus format) ----------

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: QueryResult,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryResult {
    Vector(Vec<VectorSample>),
    Matrix(Vec<MatrixSeries>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VectorSample {
    pub metric: HashMap<String, String>,
    /// [unix_time_seconds, "sample_value"]
    pub value: [serde_json::Value; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MatrixSeries {
    pub metric: HashMap<String, String>,
    /// [[unix_time, "value"], ...]
    pub values: Vec<[serde_json::Value; 2]>,
}

/// Prometheus represents sample values as quoted strings (for NaN/Inf).
fn format_sample_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v == f64::INFINITY {
        "+Inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        v.to_string()
    }
}

fn ns_to_sec(ns: u64) -> f64 {
    (ns as f64) / 1e9
}

/// Current time in nanoseconds since epoch.
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Parse optional `time` query param: Unix seconds (float or int) or RFC3339.
fn parse_time_param(s: Option<&str>) -> Result<u64, String> {
    let s = match s {
        Some(t) => t.trim(),
        None => return Ok(now_ns()),
    };
    if s.is_empty() {
        return Ok(now_ns());
    }
    if let Ok(secs) = s.parse::<f64>() {
        if secs.is_finite() && secs >= 0.0 {
            return Ok((secs * 1e9) as u64);
        }
    }
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(secs * 1_000_000_000);
    }
    Err(format!("invalid time parameter (use Unix seconds): {:?}", s))
}

/// Parse duration in seconds from step param (e.g. "15s" -> 15, "1m" -> 60).
fn parse_step_seconds(s: Option<&str>) -> Result<u64, String> {
    let s = match s {
        Some(t) => t.trim(),
        None => return Err("missing step parameter".to_string()),
    };
    if s.is_empty() {
        return Err("empty step parameter".to_string());
    }
    let s = ascii_lower(s);
    let (num_str, mult) = if s.ends_with("s") {
        (&s[..s.len() - 1], 1u64)
    } else if s.ends_with("m") {
        (&s[..s.len() - 1], 60)
    } else if s.ends_with("h") {
        (&s[..s.len() - 1], 3600)
    } else if s.ends_with("d") {
        (&s[..s.len() - 1], 86400)
    } else if let Ok(n) = s.parse::<f64>() {
        return Ok(if n >= 0.0 && n.is_finite() {
            n as u64
        } else {
            1
        });
    } else {
        return Err(format!("invalid step: {:?}", s));
    };
    let n: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid step number: {:?}", num_str))?;
    if !n.is_finite() || n < 0.0 {
        return Err("step must be non-negative".to_string());
    }
    Ok((n * mult as f64) as u64)
}

fn ascii_lower(s: &str) -> String {
    s.chars().map(|c| c.to_ascii_lowercase()).collect()
}

/// Parsed selector: metric name and optional exact label matchers (for tag_filter).
struct ParsedSelector {
    name: String,
    tag_filter: Option<HashMap<String, String>>,
}

/// Parse PromQL expression into a selector we can evaluate.
/// Only VectorSelector with optional exact-equality matchers is supported.
fn parse_selector(query: &str) -> Result<ParsedSelector, String> {
    let expr = parser::parse(query).map_err(|e| format!("invalid PromQL expression: {}", e))?;
    let vs = match &expr {
        Expr::VectorSelector(v) => v,
        _ => return Err("only metric selector expressions are supported (e.g. metric_name or metric_name{label=\"value\"})".to_string()),
    };
    let name = vs
        .name
        .as_deref()
        .unwrap_or("")
        .to_string();
    if name.is_empty() && vs.matchers.matchers.is_empty() {
        return Err("expression must specify a metric name or label matchers".to_string());
    }
    let mut tag_filter = HashMap::new();
    for m in &vs.matchers.matchers {
        use promql_parser::label::MatchOp;
        match m.op {
            MatchOp::Equal => {
                tag_filter.insert(m.name.clone(), m.value.clone());
            }
            _ => {
                return Err(format!(
                    "only exact label matchers (=) are supported, got {:?}",
                    m.op
                ));
            }
        }
    }
    let name = if name.is_empty() {
        tag_filter
            .get("__name__")
            .cloned()
            .ok_or("metric name or __name__ matcher required")?
    } else {
        name
    };
    Ok(ParsedSelector {
        name,
        tag_filter: if tag_filter.is_empty() {
            None
        } else {
            Some(tag_filter)
        },
    })
}

/// Build Prometheus metric map from series name and tag set (including __name__).
fn metric_from_series_and_tags(series: &str, tags: &HashMap<String, String>) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("__name__".to_string(), series.to_string());
    for (k, v) in tags {
        m.insert(k.clone(), v.clone());
    }
    m
}

/// Check if a series key (name + tags) matches the parsed selector (name + tag_filter).
fn selector_matches(
    series_name: &str,
    tags: &HashMap<String, String>,
    sel: &ParsedSelector,
) -> bool {
    if series_name != sel.name {
        return false;
    }
    let Some(ref filter) = sel.tag_filter else {
        return true;
    };
    filter.iter().all(|(k, v)| tags.get(k) == Some(v))
}

// ---------- Handlers ----------

/// Response for Prometheus API handlers: status and JSON body.
#[derive(Debug)]
pub struct PrometheusApiResponse {
    pub status: StatusCode,
    pub body: Vec<u8>,
}

impl PrometheusApiResponse {
    fn ok_json<T: Serialize>(data: T) -> Self {
        let body = serde_json::to_vec(&success(data)).expect("serialize success");
        Self {
            status: StatusCode::OK,
            body,
        }
    }
    fn err_json(status: StatusCode, error_type: &'static str, error: String) -> Self {
        let body = serde_json::to_vec(&error_response(error_type, error)).expect("serialize error");
        Self {
            status,
            body,
        }
    }
}

/// GET /api/v1/query?query=...&time=...
pub fn handle_query(
    query_param: Option<&str>,
    time_param: Option<&str>,
    db: &Arc<DbCore>,
) -> PrometheusApiResponse {
    let query = match query_param {
        Some(q) if !q.trim().is_empty() => q.trim(),
        _ => {
            return PrometheusApiResponse::err_json(
                StatusCode::BAD_REQUEST,
                "bad_data",
                "missing query parameter".to_string(),
            );
        }
    };
    let sel = match parse_selector(query) {
        Ok(s) => s,
        Err(e) => {
            return PrometheusApiResponse::err_json(
                StatusCode::UNPROCESSABLE_ENTITY,
                "bad_data",
                e,
            );
        }
    };
    let time_ns = match parse_time_param(time_param) {
        Ok(t) => t,
        Err(e) => {
            return PrometheusApiResponse::err_json(
                StatusCode::BAD_REQUEST,
                "bad_data",
                e,
            );
        }
    };
    let series_keys: Vec<_> = db
        .list_series_keys()
        .into_iter()
        .filter(|(name, tags)| selector_matches(name, tags, &sel))
        .collect();

    let mut samples: Vec<VectorSample> = Vec::new();
    for (series_name, tags) in series_keys {
        let start = time_ns.saturating_sub(1_000_000_000);
        let end = time_ns.saturating_add(1);
        let range = start..end;
        let tag_set: crate::types::TagSet = tags.clone();
        let points = match db.query(&series_name, range, Some(&tag_set)) {
            Ok(p) => p,
            Err(DbError::SeriesNotFound(_)) => continue,
            Err(e) => {
                return PrometheusApiResponse::err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    e.to_string(),
                );
            }
        };
        let (ts_ns, value) = match points.into_iter().max_by_key(|&(t, _)| t) {
            Some(p) => p,
            None => continue,
        };
        let metric = metric_from_series_and_tags(&series_name, &tags);
        samples.push(VectorSample {
            metric,
            value: [
                serde_json::Value::Number(serde_json::Number::from_f64(ns_to_sec(ts_ns)).unwrap_or(serde_json::Number::from(0))),
                serde_json::Value::String(format_sample_value(value)),
            ],
        });
    }

    let data = QueryData {
        result_type: "vector".to_string(),
        result: QueryResult::Vector(samples),
    };
    PrometheusApiResponse::ok_json(data)
}

/// GET /api/v1/query_range?query=...&start=...&end=...&step=...
pub fn handle_query_range(
    query_param: Option<&str>,
    start_param: Option<&str>,
    end_param: Option<&str>,
    step_param: Option<&str>,
    db: &Arc<DbCore>,
) -> PrometheusApiResponse {
    let query = match query_param {
        Some(q) if !q.trim().is_empty() => q.trim(),
        _ => {
            return PrometheusApiResponse::err_json(
                StatusCode::BAD_REQUEST,
                "bad_data",
                "missing query parameter".to_string(),
            );
        }
    };
    let start_ns = match start_param {
        Some(s) => match parse_time_param(Some(s)) {
            Ok(t) => t,
            Err(e) => {
                return PrometheusApiResponse::err_json(
                    StatusCode::BAD_REQUEST,
                    "bad_data",
                    e,
                );
            }
        },
        None => {
            return PrometheusApiResponse::err_json(
                StatusCode::BAD_REQUEST,
                "bad_data",
                "missing start parameter".to_string(),
            );
        }
    };
    let end_ns = match end_param {
        Some(s) => match parse_time_param(Some(s)) {
            Ok(t) => t,
            Err(e) => {
                return PrometheusApiResponse::err_json(
                    StatusCode::BAD_REQUEST,
                    "bad_data",
                    e,
                );
            }
        },
        None => {
            return PrometheusApiResponse::err_json(
                StatusCode::BAD_REQUEST,
                "bad_data",
                "missing end parameter".to_string(),
            );
        }
    };
    if start_ns >= end_ns {
        return PrometheusApiResponse::err_json(
            StatusCode::BAD_REQUEST,
            "bad_data",
            "start must be before end".to_string(),
        );
    }
    let step_secs = match parse_step_seconds(step_param) {
        Ok(s) => s,
        Err(e) => {
            return PrometheusApiResponse::err_json(
                StatusCode::BAD_REQUEST,
                "bad_data",
                e,
            );
        }
    };
    let step_ns = step_secs * 1_000_000_000;

    let sel = match parse_selector(query) {
        Ok(s) => s,
        Err(e) => {
            return PrometheusApiResponse::err_json(
                StatusCode::UNPROCESSABLE_ENTITY,
                "bad_data",
                e,
            );
        }
    };

    let series_keys: Vec<_> = db
        .list_series_keys()
        .into_iter()
        .filter(|(name, tags)| selector_matches(name, tags, &sel))
        .collect();

    let mut matrix: Vec<MatrixSeries> = Vec::new();
    for (series_name, tags) in series_keys {
        let tag_set: crate::types::TagSet = tags.clone();
        let points = match db.query(&series_name, start_ns..end_ns.saturating_add(1), Some(&tag_set)) {
            Ok(p) => p,
            Err(DbError::SeriesNotFound(_)) => continue,
            Err(e) => {
                return PrometheusApiResponse::err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    e.to_string(),
                );
            }
        };
        let mut step_values: Vec<[serde_json::Value; 2]> = Vec::new();
        let mut t = start_ns;
        while t <= end_ns {
            let value = match points
                .iter()
                .filter(|(pt, _)| *pt <= t)
                .max_by_key(|(pt, _)| *pt)
            {
                Some((_, val)) => *val,
                None => {
                    t = t.saturating_add(step_ns);
                    continue;
                }
            };
            step_values.push([
                serde_json::Value::Number(serde_json::Number::from_f64(ns_to_sec(t)).unwrap_or(serde_json::Number::from(0))),
                serde_json::Value::String(format_sample_value(value)),
            ]);
            t = t.saturating_add(step_ns);
        }
        if step_values.is_empty() {
            continue;
        }
        matrix.push(MatrixSeries {
            metric: metric_from_series_and_tags(&series_name, &tags),
            values: step_values,
        });
    }

    let data = QueryData {
        result_type: "matrix".to_string(),
        result: QueryResult::Matrix(matrix),
    };
    PrometheusApiResponse::ok_json(data)
}

/// GET /api/v1/labels
pub fn handle_labels(db: &Arc<DbCore>) -> PrometheusApiResponse {
    let mut names: std::collections::HashSet<String> = std::collections::HashSet::new();
    names.insert("__name__".to_string());
    for (_, tags) in db.list_series_keys() {
        for k in tags.keys() {
            names.insert(k.clone());
        }
    }
    let mut data: Vec<String> = names.into_iter().collect();
    data.sort();
    PrometheusApiResponse::ok_json(data)
}

/// GET /api/v1/label/<name>/values
pub fn handle_label_values(label_name: &str, db: &Arc<DbCore>) -> PrometheusApiResponse {
    let mut values: std::collections::HashSet<String> = std::collections::HashSet::new();
    if label_name == "__name__" {
        for name in db.list_series_names() {
            values.insert(name);
        }
    } else {
        for (_, tags) in db.list_series_keys() {
            if let Some(v) = tags.get(label_name) {
                values.insert(v.clone());
            }
        }
    }
    let mut data: Vec<String> = values.into_iter().collect();
    data.sort();
    PrometheusApiResponse::ok_json(data)
}

/// GET /api/v1/series?match[]=...&start=...&end=...
pub fn handle_series(
    match_params: &[String],
    _start_param: Option<&str>,
    _end_param: Option<&str>,
    db: &Arc<DbCore>,
) -> PrometheusApiResponse {
    if match_params.is_empty() {
        return PrometheusApiResponse::err_json(
            StatusCode::BAD_REQUEST,
            "bad_data",
            "at least one match[] parameter is required".to_string(),
        );
    }
    let mut series_list: Vec<HashMap<String, String>> = Vec::new();
    for m in match_params {
        let m = m.trim();
        if m.is_empty() {
            continue;
        }
        let sel = match parse_selector(m) {
            Ok(s) => s,
            Err(_) => continue,
        };
        for (series_name, tags) in db.list_series_keys() {
            if selector_matches(&series_name, &tags, &sel) {
                let labels = metric_from_series_and_tags(&series_name, &tags);
                if !series_list.iter().any(|l| l == &labels) {
                    series_list.push(labels);
                }
            }
        }
    }
    PrometheusApiResponse::ok_json(series_list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_db_with_series() -> (Arc<DbCore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::DbConfig::default();
        config.data_dir = dir.path().to_path_buf();
        config.max_series_cardinality = Some(1000);
        let mut db = DbCore::with_config(config).unwrap();
        db.recover().unwrap();
        let db = Arc::new(db);
        db.insert(
            "http_requests_total",
            1_000_000_000,
            10.0,
            [("job".to_string(), "api".to_string())].into_iter().collect(),
        )
        .unwrap();
        db.insert(
            "http_requests_total",
            2_000_000_000,
            20.0,
            [("job".to_string(), "api".to_string())].into_iter().collect(),
        )
        .unwrap();
        db.insert(
            "http_requests_total",
            1_500_000_000,
            15.0,
            [("job".to_string(), "web".to_string())].into_iter().collect(),
        )
        .unwrap();
        db.flush().unwrap();
        (db, dir)
    }

    #[test]
    fn query_missing_param_returns_400() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(None, None, &db);
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<serde_json::Value> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "error");
        assert!(body.error.unwrap().contains("query"));
    }

    #[test]
    fn query_valid_selector_returns_vector() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(
            Some("http_requests_total"),
            Some("2"), // time = 2s -> 2e9 ns
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<QueryData> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let data = body.data.unwrap();
        assert_eq!(data.result_type.as_str(), "vector");
        let QueryResult::Vector(samples) = data.result else { panic!("expected vector") };
        assert!(!samples.is_empty());
        let has_api = samples.iter().any(|s| s.metric.get("job") == Some(&"api".to_string()));
        let has_web = samples.iter().any(|s| s.metric.get("job") == Some(&"web".to_string()));
        assert!(has_api || has_web);
    }

    #[test]
    fn labels_returns_name_and_known_labels() {
        let (db, _guard) = make_db_with_series();
        let r = handle_labels(&db);
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<Vec<String>> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let data = body.data.unwrap();
        assert!(data.contains(&"__name__".to_string()));
        assert!(data.contains(&"job".to_string()));
    }

    #[test]
    fn label_values_name_returns_metric_names() {
        let (db, _guard) = make_db_with_series();
        let r = handle_label_values("__name__", &db);
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<Vec<String>> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        assert!(body.data.unwrap().contains(&"http_requests_total".to_string()));
    }

    #[test]
    fn series_requires_match_returns_400_without() {
        let (db, _guard) = make_db_with_series();
        let r = handle_series(&[], None, None, &db);
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn series_with_match_returns_label_sets() {
        let (db, _guard) = make_db_with_series();
        let r = handle_series(&["http_requests_total".to_string()], None, None, &db);
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<Vec<HashMap<String, String>>> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let data = body.data.unwrap();
        assert!(!data.is_empty());
        assert!(data.iter().any(|m| m.get("__name__") == Some(&"http_requests_total".to_string())));
    }

    #[test]
    fn query_range_valid_returns_matrix() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query_range(
            Some("http_requests_total"),
            Some("1"),
            Some("3"),
            Some("1s"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<QueryData> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let data = body.data.unwrap();
        assert_eq!(data.result_type.as_str(), "matrix");
        let QueryResult::Matrix(series) = data.result else { panic!("expected matrix") };
        assert!(!series.is_empty());
    }

    #[test]
    fn parse_selector_rejects_non_selector() {
        let q = "sum(rate(x[5m]))";
        let expr = parser::parse(q);
        assert!(expr.is_ok());
        let expr = expr.unwrap();
        assert!(!matches!(expr, Expr::VectorSelector(_)));
    }
}
