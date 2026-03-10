//! Prometheus HTTP API v1 (Grafana Prometheus datasource compatibility).
//!
//! Implements `GET /api/v1/query`, `query_range`, `labels`, `label/<name>/values`, and `series`
//! with the standard JSON envelope and result formats.
//!
//! The evaluation engine supports instant vector selectors, range functions
//! (`rate`, `increase`, `avg_over_time`, `max_over_time`, `min_over_time`,
//! `sum_over_time`), and aggregations (`sum`, `avg`, `min`, `max`, `count`)
//! with `by`/`without` grouping — sufficient for a representative Grafana
//! dashboard subset.

use crate::promql::{self, PromqlError};
use crate::DbCore;
use crate::error::DbError;
use crate::query::{
    compute_avg_over_time, compute_increase, compute_max_over_time, compute_min_over_time,
    compute_rate, compute_sum_over_time,
};
use crate::query_surface::{
    AggOp, EvalExpr, Grouping, InstantSelector, RangeFn, series_matches_selector,
};
use http::StatusCode;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorSample {
    pub metric: HashMap<String, String>,
    /// [unix_time_seconds, "sample_value"]
    pub value: [serde_json::Value; 2],
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

/// Build Prometheus metric map from series name and tag set (including __name__).
fn metric_from_series_and_tags(
    series: &str,
    tags: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("__name__".to_string(), series.to_string());
    for (k, v) in tags {
        m.insert(k.clone(), v.clone());
    }
    m
}

fn json_sample_pair(ts_ns: u64, value: f64) -> [serde_json::Value; 2] {
    [
        serde_json::Value::Number(
            serde_json::Number::from_f64(ns_to_sec(ts_ns)).unwrap_or(serde_json::Number::from(0)),
        ),
        serde_json::Value::String(format_sample_value(value)),
    ]
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
        Self { status, body }
    }
}

// ---------- Internal evaluation types (pub(crate) for promql module) ----------

/// Intermediate per-series data during evaluation (before JSON formatting).
#[allow(missing_docs)]
pub(crate) struct EvalSample {
    pub metric: HashMap<String, String>,
    pub ts_ns: u64,
    pub value: f64,
}

/// Per-series data for range (matrix) evaluation: step-aligned `(ts_ns, value)` pairs.
/// Used by the public [`crate::promql::query_range`] API and by HTTP serialization.
pub(crate) struct EvalSeriesData {
    pub metric: HashMap<String, String>,
    pub steps: Vec<(u64, f64)>,
}

// ---------- Instant query evaluation ----------

/// Evaluate an expression at a single instant, returning a vector of samples.
/// Used by the public [`crate::promql::query_instant`] API.
pub(crate) fn eval_vector(
    expr: &EvalExpr,
    time_ns: u64,
    db: &Arc<DbCore>,
) -> Result<Vec<EvalSample>, DbError> {
    match expr {
        EvalExpr::Instant(selector) => eval_instant_at(selector, time_ns, db),
        EvalExpr::RangeFunction {
            func,
            selector,
            range,
        } => eval_range_fn_at(*func, selector, *range, time_ns, db),
        EvalExpr::Aggregation {
            op,
            inner,
            grouping,
        } => {
            let inner_samples = eval_vector(inner, time_ns, db)?;
            Ok(aggregate_samples(*op, inner_samples, grouping))
        }
    }
}

/// Evaluate an instant vector selector: for each matching series, find the latest
/// sample in a 1-second lookback window ending at `time_ns`.
fn eval_instant_at(
    selector: &InstantSelector,
    time_ns: u64,
    db: &Arc<DbCore>,
) -> Result<Vec<EvalSample>, DbError> {
    let eval_time = match selector.offset {
        Some(off) => time_ns.saturating_sub(off.as_nanos() as u64),
        None => time_ns,
    };
    let series_keys: Vec<_> = db
        .list_series_keys()
        .into_iter()
        .filter(|(name, tags)| series_matches_selector(name, tags, &selector.selector))
        .collect();

    let run_one = |(series_name, tags): &(String, crate::types::TagSet)| -> Result<Option<EvalSample>, DbError> {
        let start = eval_time.saturating_sub(1_000_000_000);
        let end = eval_time.saturating_add(1);
        let points = db.query(series_name, start..end, Some(tags))?;
        match points.into_iter().max_by_key(|&(t, _)| t) {
            Some((ts, val)) => Ok(Some(EvalSample {
                metric: metric_from_series_and_tags(series_name, tags),
                ts_ns: ts,
                value: val,
            })),
            None => Ok(None),
        }
    };

    let results: Vec<Result<Option<EvalSample>, DbError>> = match db.get_query_pool() {
        Some(pool) => pool.install(|| series_keys.par_iter().map(run_one).collect()),
        None => series_keys.par_iter().map(run_one).collect(),
    };

    let mut out = Vec::new();
    for r in results {
        match r {
            Ok(Some(s)) => out.push(s),
            Ok(None) | Err(DbError::SeriesNotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Evaluate a range function at a single instant: for each matching series,
/// fetch samples in `[time - range, time]` and apply the function.
fn eval_range_fn_at(
    func: RangeFn,
    selector: &InstantSelector,
    range: std::time::Duration,
    time_ns: u64,
    db: &Arc<DbCore>,
) -> Result<Vec<EvalSample>, DbError> {
    let range_ns = range.as_nanos() as u64;
    let eval_time = match selector.offset {
        Some(off) => time_ns.saturating_sub(off.as_nanos() as u64),
        None => time_ns,
    };
    let window_start = eval_time.saturating_sub(range_ns);
    let window_end = eval_time.saturating_add(1);

    let series_keys: Vec<_> = db
        .list_series_keys()
        .into_iter()
        .filter(|(name, tags)| series_matches_selector(name, tags, &selector.selector))
        .collect();

    let run_one = |(series_name, tags): &(String, crate::types::TagSet)| -> Result<Option<EvalSample>, DbError> {
        let points = db.query(series_name, window_start..window_end, Some(tags))?;
        let value = apply_range_fn(func, &points)?;
        Ok(value.map(|v| EvalSample {
            metric: metric_from_series_and_tags(series_name, tags),
            ts_ns: time_ns,
            value: v,
        }))
    };

    let results: Vec<Result<Option<EvalSample>, DbError>> = match db.get_query_pool() {
        Some(pool) => pool.install(|| series_keys.par_iter().map(run_one).collect()),
        None => series_keys.par_iter().map(run_one).collect(),
    };

    let mut out = Vec::new();
    for r in results {
        match r {
            Ok(Some(s)) => out.push(s),
            Ok(None) | Err(DbError::SeriesNotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

fn apply_range_fn(
    func: RangeFn,
    points: &[(crate::types::Timestamp, crate::types::Value)],
) -> Result<Option<f64>, DbError> {
    let result = match func {
        RangeFn::Rate => compute_rate(points),
        RangeFn::Increase => compute_increase(points),
        RangeFn::AvgOverTime => compute_avg_over_time(points),
        RangeFn::MaxOverTime => compute_max_over_time(points),
        RangeFn::MinOverTime => compute_min_over_time(points),
        RangeFn::SumOverTime => compute_sum_over_time(points),
    };
    Ok(result)
}

// ---------- Aggregation ----------

/// Per-group state: (metric labels, values, latest timestamp).
type AggGroupEntry = (HashMap<String, String>, Vec<f64>, u64);

/// Group samples by the aggregation grouping clause and apply the operator.
fn aggregate_samples(op: AggOp, samples: Vec<EvalSample>, grouping: &Grouping) -> Vec<EvalSample> {
    let mut groups: HashMap<String, AggGroupEntry> = HashMap::new();

    for sample in samples {
        let key = group_key_string(&sample.metric, grouping);
        let entry = groups.entry(key).or_insert_with(|| {
            (
                group_metric(&sample.metric, grouping),
                Vec::new(),
                sample.ts_ns,
            )
        });
        entry.1.push(sample.value);
        if sample.ts_ns > entry.2 {
            entry.2 = sample.ts_ns;
        }
    }

    groups
        .into_values()
        .map(|(metric, values, ts_ns)| EvalSample {
            metric,
            ts_ns,
            value: apply_agg_op(op, &values),
        })
        .collect()
}

/// Applies a PromQL aggregation over a slice of sample values.
///
/// Semantics follow the [official PromQL operators] (sum/avg/count/min/max).
/// See `query::aggregate_*` and the PromQL rules documented in `query.rs` (vectorized
/// aggregates section).
///
/// [official PromQL operators]: https://prometheus.io/docs/prometheus/latest/querying/operators/
fn apply_agg_op(op: AggOp, values: &[f64]) -> f64 {
    if values.is_empty() {
        return match op {
            AggOp::Sum | AggOp::Count => 0.0,
            _ => f64::NAN,
        };
    }

    match op {
        AggOp::Sum => values.iter().copied().sum(),
        AggOp::Count => values.len() as f64,
        AggOp::Avg => {
            let sum: f64 = values.iter().copied().sum();
            sum / (values.len() as f64)
        }
        AggOp::Min => values.iter().copied().reduce(f64::min).unwrap(),
        AggOp::Max => values.iter().copied().reduce(f64::max).unwrap(),
    }
}

/// Derive the group key (a deterministic string) from a metric's labels.
fn group_key_string(metric: &HashMap<String, String>, grouping: &Grouping) -> String {
    let mut pairs: Vec<(&String, &String)> = match grouping {
        Grouping::By(labels) => metric.iter().filter(|(k, _)| labels.contains(k)).collect(),
        Grouping::Without(labels) => metric.iter().filter(|(k, _)| !labels.contains(k)).collect(),
    };
    pairs.sort_by_key(|(k, _)| *k);
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",")
}

/// Derive the output metric labels from the grouping clause.
fn group_metric(metric: &HashMap<String, String>, grouping: &Grouping) -> HashMap<String, String> {
    match grouping {
        Grouping::By(labels) => metric
            .iter()
            .filter(|(k, _)| labels.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        Grouping::Without(labels) => metric
            .iter()
            .filter(|(k, _)| !labels.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

// ---------- Range (matrix) query evaluation ----------

/// Evaluate an expression over a time range, producing step-aligned matrix data.
/// Used by the public [`crate::promql::query_range`] API; HTTP handler delegates to that.
pub(crate) fn eval_matrix(
    expr: &EvalExpr,
    start_ns: u64,
    end_ns: u64,
    step_ns: u64,
    db: &Arc<DbCore>,
) -> Result<Vec<EvalSeriesData>, DbError> {
    match expr {
        EvalExpr::Instant(selector) => eval_instant_matrix(selector, start_ns, end_ns, step_ns, db),
        EvalExpr::RangeFunction {
            func,
            selector,
            range,
        } => eval_range_fn_matrix(*func, selector, *range, start_ns, end_ns, step_ns, db),
        EvalExpr::Aggregation {
            op,
            inner,
            grouping,
        } => {
            let inner_data = eval_matrix(inner, start_ns, end_ns, step_ns, db)?;
            Ok(aggregate_matrix(
                *op, inner_data, grouping, start_ns, end_ns, step_ns,
            ))
        }
    }
}

/// Evaluate an instant selector over a range: at each step, find the latest sample.
///
/// **Step semantics (proof that stored steps are a subset of the full grid):**
/// The loop iterates over the full grid `t = start_ns, start_ns+step_ns, ... <= end_ns`.
/// A step `(t, val)` is pushed only when there exists a point with `pt <= eval_ts` (see
/// `if let Some(...)` below). If at a grid time there is no such point (e.g. first sample
/// is after that time), that step is omitted. Hence the stored steps are exactly those
/// grid times where the series has a defined value — a *subset* of the full grid, not
/// necessarily the same count across series.
fn eval_instant_matrix(
    selector: &InstantSelector,
    start_ns: u64,
    end_ns: u64,
    step_ns: u64,
    db: &Arc<DbCore>,
) -> Result<Vec<EvalSeriesData>, DbError> {
    let off_ns = selector.offset.map(|d| d.as_nanos() as u64).unwrap_or(0);
    let query_start = start_ns.saturating_sub(off_ns);
    let query_end = end_ns.saturating_sub(off_ns).saturating_add(1);

    let series_keys: Vec<_> = db
        .list_series_keys()
        .into_iter()
        .filter(|(name, tags)| series_matches_selector(name, tags, &selector.selector))
        .collect();

    let run_range = |(series_name, tags): &(String, crate::types::TagSet)| -> Result<Option<EvalSeriesData>, DbError> {
        let points = db.query(series_name, query_start..query_end, Some(tags))?;
        let mut steps = Vec::new();
        let mut t = start_ns;
        while t <= end_ns {
            let eval_ts = t.saturating_sub(off_ns);
            if let Some((_, val)) = points
                .iter()
                .filter(|(pt, _)| *pt <= eval_ts)
                .max_by_key(|(pt, _)| *pt)
            {
                steps.push((t, *val));
            }
            t = t.saturating_add(step_ns);
        }
        if steps.is_empty() {
            return Ok(None);
        }
        Ok(Some(EvalSeriesData {
            metric: metric_from_series_and_tags(series_name, tags),
            steps,
        }))
    };

    let results: Vec<Result<Option<EvalSeriesData>, DbError>> = match db.get_query_pool() {
        Some(pool) => pool.install(|| series_keys.par_iter().map(run_range).collect()),
        None => series_keys.par_iter().map(run_range).collect(),
    };

    let mut out = Vec::new();
    for r in results {
        match r {
            Ok(Some(s)) => out.push(s),
            Ok(None) | Err(DbError::SeriesNotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Evaluate a range function over a time range: pre-fetch the full data window,
/// then compute the function at each step.
///
/// **Step semantics:** Same as instant matrix: we iterate the full grid but push only
/// when `apply_range_fn` returns `Ok(Some(val))`; otherwise that step is omitted.
/// So stored steps are a subset of the full grid.
fn eval_range_fn_matrix(
    func: RangeFn,
    selector: &InstantSelector,
    range: std::time::Duration,
    start_ns: u64,
    end_ns: u64,
    step_ns: u64,
    db: &Arc<DbCore>,
) -> Result<Vec<EvalSeriesData>, DbError> {
    let range_ns = range.as_nanos() as u64;
    let off_ns = selector.offset.map(|d| d.as_nanos() as u64).unwrap_or(0);
    let fetch_start = start_ns.saturating_sub(off_ns).saturating_sub(range_ns);
    let fetch_end = end_ns.saturating_sub(off_ns).saturating_add(1);

    let series_keys: Vec<_> = db
        .list_series_keys()
        .into_iter()
        .filter(|(name, tags)| series_matches_selector(name, tags, &selector.selector))
        .collect();

    let run_range = |(series_name, tags): &(String, crate::types::TagSet)| -> Result<Option<EvalSeriesData>, DbError> {
        let all_points = db.query(series_name, fetch_start..fetch_end, Some(tags))?;
        let mut steps = Vec::new();
        let mut t = start_ns;
        while t <= end_ns {
            let eval_ts = t.saturating_sub(off_ns);
            let window_start = eval_ts.saturating_sub(range_ns);
            let window_points: Vec<_> = all_points
                .iter()
                .filter(|(pt, _)| *pt >= window_start && *pt <= eval_ts)
                .copied()
                .collect();
            if let Ok(Some(val)) = apply_range_fn(func, &window_points) {
                steps.push((t, val));
            }
            t = t.saturating_add(step_ns);
        }
        if steps.is_empty() {
            return Ok(None);
        }
        Ok(Some(EvalSeriesData {
            metric: metric_from_series_and_tags(series_name, tags),
            steps,
        }))
    };

    let results: Vec<Result<Option<EvalSeriesData>, DbError>> = match db.get_query_pool() {
        Some(pool) => pool.install(|| series_keys.par_iter().map(run_range).collect()),
        None => series_keys.par_iter().map(run_range).collect(),
    };

    let mut out = Vec::new();
    for r in results {
        match r {
            Ok(Some(s)) => out.push(s),
            Ok(None) | Err(DbError::SeriesNotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Aggregate matrix data: group inner series by the grouping clause and
/// aggregate per-step values.
fn aggregate_matrix(
    op: AggOp,
    inner: Vec<EvalSeriesData>,
    grouping: &Grouping,
    start_ns: u64,
    end_ns: u64,
    step_ns: u64,
) -> Vec<EvalSeriesData> {
    let mut groups: HashMap<String, (HashMap<String, String>, Vec<&EvalSeriesData>)> =
        HashMap::new();

    for series in &inner {
        let key = group_key_string(&series.metric, grouping);
        let entry = groups
            .entry(key)
            .or_insert_with(|| (group_metric(&series.metric, grouping), Vec::new()));
        entry.1.push(series);
    }

    let mut result = Vec::new();
    for (_, (group_metric, group_series)) in groups {
        let mut steps = Vec::new();
        let mut t = start_ns;
        while t <= end_ns {
            let values: Vec<f64> = group_series
                .iter()
                .filter_map(|s| s.steps.iter().find(|(ts, _)| *ts == t).map(|(_, v)| *v))
                .collect();
            if !values.is_empty() {
                steps.push((t, apply_agg_op(op, &values)));
            }
            t = t.saturating_add(step_ns);
        }
        if !steps.is_empty() {
            result.push(EvalSeriesData {
                metric: group_metric,
                steps,
            });
        }
    }
    result
}

// ---------- Public API handlers ----------

/// GET /api/v1/query?query=...&time=...
///
/// Delegates to [`crate::promql::query_instant`]; parses time param then serializes the result.
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
    let time_ns = match promql::parse_eval_time(time_param) {
        Ok(t) => t,
        Err(e) => {
            return PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e);
        }
    };

    let samples = match crate::promql::query_instant(db, query, time_ns) {
        Ok(s) => s,
        Err(crate::promql::PromqlError::Parse(e)) => {
            return PrometheusApiResponse::err_json(
                StatusCode::UNPROCESSABLE_ENTITY,
                "bad_data",
                e,
            );
        }
        Err(crate::promql::PromqlError::BadParameter(e)) => {
            return PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e);
        }
        Err(crate::promql::PromqlError::Execution(e)) => {
            return PrometheusApiResponse::err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                e.to_string(),
            );
        }
    };

    let vector: Vec<VectorSample> = samples
        .into_iter()
        .map(|s| VectorSample {
            metric: s.metric,
            value: json_sample_pair(s.ts_ns, s.value),
        })
        .collect();

    PrometheusApiResponse::ok_json(QueryData {
        result_type: "vector".to_string(),
        result: QueryResult::Vector(vector),
    })
}

/// GET /api/v1/query_range?query=...&start=...&end=...&step=...
///
/// Delegates to [`promql::query_range`]; no duplicated evaluation logic.
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
        Some(s) => match promql::parse_eval_time(Some(s)) {
            Ok(t) => t,
            Err(e) => {
                return PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e);
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
        Some(s) => match promql::parse_eval_time(Some(s)) {
            Ok(t) => t,
            Err(e) => {
                return PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e);
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
    let step_ns = match step_param {
        Some(s) => match promql::parse_step(s) {
            Ok(ns) => ns,
            Err(e) => {
                return PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e);
            }
        },
        None => {
            return PrometheusApiResponse::err_json(
                StatusCode::BAD_REQUEST,
                "bad_data",
                "missing step parameter".to_string(),
            );
        }
    };

    let series = match promql::query_range(db, query, start_ns, end_ns, step_ns) {
        Ok(s) => s,
        Err(PromqlError::Parse(e)) => {
            return PrometheusApiResponse::err_json(
                StatusCode::UNPROCESSABLE_ENTITY,
                "bad_data",
                e,
            );
        }
        Err(PromqlError::BadParameter(e)) => {
            return PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e);
        }
        Err(PromqlError::Execution(e)) => {
            return PrometheusApiResponse::err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                e.to_string(),
            );
        }
    };

    let matrix: Vec<MatrixSeries> = series
        .into_iter()
        .map(|rs| MatrixSeries {
            metric: rs.metric,
            values: rs
                .steps
                .into_iter()
                .map(|(ts, val)| json_sample_pair(ts, val))
                .collect(),
        })
        .collect();

    PrometheusApiResponse::ok_json(QueryData {
        result_type: "matrix".to_string(),
        result: QueryResult::Matrix(matrix),
    })
}

/// GET /api/v1/labels?match[]=...&start=...&end=...
///
/// Delegates to [`promql::labels`]; no duplicated metadata logic.
/// Optional `start`/`end` (Unix seconds or RFC3339): if both provided, restrict to that time range;
/// if either missing, use 0 to now for backward compatibility.
pub fn handle_labels(
    match_params: &[String],
    start_param: Option<&str>,
    end_param: Option<&str>,
    db: &Arc<DbCore>,
) -> PrometheusApiResponse {
    let start_ns = match start_param.and_then(|s| promql::parse_eval_time(Some(s)).ok()) {
        Some(t) => t,
        None => 0,
    };
    let end_ns = match end_param.and_then(|s| promql::parse_eval_time(Some(s)).ok()) {
        Some(t) => t,
        None => promql::parse_eval_time(None).unwrap(),
    };
    let match_selectors: Option<&[String]> = if match_params.is_empty() {
        None
    } else {
        Some(match_params)
    };
    match promql::labels(db, match_selectors, start_ns, end_ns) {
        Ok(data) => PrometheusApiResponse::ok_json(data),
        Err(promql::PromqlError::BadParameter(e)) => {
            PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e)
        }
        Err(promql::PromqlError::Parse(e)) => {
            PrometheusApiResponse::err_json(StatusCode::UNPROCESSABLE_ENTITY, "bad_data", e)
        }
        Err(promql::PromqlError::Execution(e)) => PrometheusApiResponse::err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            e.to_string(),
        ),
    }
}

/// GET /api/v1/label/<name>/values
///
/// Optional `match[]`, `start`, `end`: same semantics as `handle_labels`; if start/end omitted,
/// uses 0 to now for backward compatibility.
pub fn handle_label_values(
    label_name: &str,
    match_params: &[String],
    start_param: Option<&str>,
    end_param: Option<&str>,
    db: &Arc<DbCore>,
) -> PrometheusApiResponse {
    let start_ns = match start_param.and_then(|s| promql::parse_eval_time(Some(s)).ok()) {
        Some(t) => t,
        None => 0,
    };
    let end_ns = match end_param.and_then(|s| promql::parse_eval_time(Some(s)).ok()) {
        Some(t) => t,
        None => promql::parse_eval_time(None).unwrap(),
    };
    let match_selectors: Option<&[String]> = if match_params.is_empty() {
        None
    } else {
        Some(match_params)
    };
    match promql::label_values(db, label_name, match_selectors, start_ns, end_ns) {
        Ok(data) => PrometheusApiResponse::ok_json(data),
        Err(promql::PromqlError::BadParameter(e)) => {
            PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e)
        }
        Err(promql::PromqlError::Parse(e)) => {
            PrometheusApiResponse::err_json(StatusCode::UNPROCESSABLE_ENTITY, "bad_data", e)
        }
        Err(promql::PromqlError::Execution(e)) => PrometheusApiResponse::err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            e.to_string(),
        ),
    }
}

/// GET /api/v1/series?match[]=...&start=...&end=...
///
/// Delegates to [`promql::series`]; no duplicated metadata logic.
/// Required `match[]` (at least one); optional `start`/`end` (Unix seconds or RFC3339): if both
/// provided, restrict to that time range; if either missing, use 0 to now for backward compatibility.
pub fn handle_series(
    match_params: &[String],
    start_param: Option<&str>,
    end_param: Option<&str>,
    db: &Arc<DbCore>,
) -> PrometheusApiResponse {
    let start_ns = match start_param.and_then(|s| promql::parse_eval_time(Some(s)).ok()) {
        Some(t) => t,
        None => 0,
    };
    let end_ns = match end_param.and_then(|s| promql::parse_eval_time(Some(s)).ok()) {
        Some(t) => t,
        None => promql::parse_eval_time(None).unwrap(),
    };
    match promql::series(db, match_params, start_ns, end_ns) {
        Ok(data) => PrometheusApiResponse::ok_json(data),
        Err(promql::PromqlError::BadParameter(e)) => {
            PrometheusApiResponse::err_json(StatusCode::BAD_REQUEST, "bad_data", e)
        }
        Err(promql::PromqlError::Parse(e)) => {
            PrometheusApiResponse::err_json(StatusCode::UNPROCESSABLE_ENTITY, "bad_data", e)
        }
        Err(promql::PromqlError::Execution(e)) => PrometheusApiResponse::err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            e.to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_db_with_series() -> (Arc<DbCore>, tempfile::TempDir) {
        make_db_with_series_and_parallelism(None)
    }

    fn make_db_with_series_and_parallelism(
        query_max_parallel_series: Option<usize>,
    ) -> (Arc<DbCore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::DbConfig {
            data_dir: dir.path().to_path_buf(),
            max_series_cardinality: Some(1000),
            query_max_parallel_series,
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

    /// Create a DB with multiple series suited for aggregation testing.
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
        // Series: http_requests_total{job="api",instance="a"} = monotonic counter
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
        // Series: http_requests_total{job="api",instance="b"}
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
        // Series: http_requests_total{job="web",instance="c"}
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

    fn sort_vector_samples(samples: &mut [VectorSample]) {
        samples.sort_by_key(|a| metric_sort_key(&a.metric));
    }

    fn sort_matrix_series(series: &mut [MatrixSeries]) {
        series.sort_by_key(|a| metric_sort_key(&a.metric));
    }

    // --- Original tests (backward compat) ---

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
        let r = handle_query(Some("http_requests_total"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<QueryData> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let data = body.data.unwrap();
        assert_eq!(data.result_type.as_str(), "vector");
        let QueryResult::Vector(samples) = data.result else {
            panic!("expected vector")
        };
        assert!(!samples.is_empty());
        let has_api = samples
            .iter()
            .any(|s| s.metric.get("job") == Some(&"api".to_string()));
        let has_web = samples
            .iter()
            .any(|s| s.metric.get("job") == Some(&"web".to_string()));
        assert!(has_api || has_web);
    }

    #[test]
    fn labels_returns_name_and_known_labels() {
        let (db, _guard) = make_db_with_series();
        let r = handle_labels(&[], None, None, &db);
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
        let r = handle_label_values("__name__", &[], None, None, &db);
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<Vec<String>> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        assert!(
            body.data
                .unwrap()
                .contains(&"http_requests_total".to_string())
        );
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
        assert!(
            data.iter()
                .any(|m| m.get("__name__") == Some(&"http_requests_total".to_string()))
        );
    }

    #[test]
    fn series_with_start_end_restricts_to_time_range() {
        // Same fixture: api at 1s, 2s; web at 1.5s. start=2, end=2.5 -> only api has data in range.
        let (db, _guard) = make_db_with_series();
        let r = handle_series(
            &["http_requests_total".to_string()],
            Some("2"),
            Some("2.5"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<Vec<HashMap<String, String>>> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let data = body.data.unwrap();
        assert_eq!(data.len(), 1, "only api has a point in [2s, 2.5s]");
        assert_eq!(data[0].get("job"), Some(&"api".to_string()));
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
        let QueryResult::Matrix(series) = data.result else {
            panic!("expected matrix")
        };
        assert!(!series.is_empty());
    }

    #[test]
    fn query_surface_rejects_aggregation() {
        let e = crate::query_surface::parse_instant_selector("sum(rate(x[5m]))").unwrap_err();
        assert!(
            e.contains("aggregation") || e.contains("function") || e.contains("not yet supported")
        );
    }

    #[test]
    fn query_vector_selector_regex_matcher() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(
            Some(r#"http_requests_total{job=~"api|web"}"#),
            Some("2"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<QueryData> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert_eq!(samples.len(), 2, "api and web both match =~");
    }

    #[test]
    fn query_vector_selector_not_equal_matcher() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some(r#"http_requests_total{job!="web"}"#), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let s = String::from_utf8(r.body).unwrap();
        let body: ApiEnvelope<QueryData> = serde_json::from_str(&s).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].metric.get("job"), Some(&"api".to_string()));
    }

    // --- Parallelism tests ---

    #[test]
    fn query_parallelism_1_same_result_as_default() {
        let (db_default, _g0) = make_db_with_series_and_parallelism(None);
        let (db_pool1, _g1) = make_db_with_series_and_parallelism(Some(1));
        let r0 = handle_query(Some("http_requests_total"), Some("2"), &db_default);
        let r1 = handle_query(Some("http_requests_total"), Some("2"), &db_pool1);
        assert_eq!(r0.status, StatusCode::OK);
        assert_eq!(r1.status, StatusCode::OK);
        let body0: ApiEnvelope<QueryData> = serde_json::from_slice(&r0.body).unwrap();
        let body1: ApiEnvelope<QueryData> = serde_json::from_slice(&r1.body).unwrap();
        let QueryResult::Vector(mut v0) = body0.data.unwrap().result else {
            panic!("vector")
        };
        let QueryResult::Vector(mut v1) = body1.data.unwrap().result else {
            panic!("vector")
        };
        sort_vector_samples(&mut v0);
        sort_vector_samples(&mut v1);
        assert_eq!(v0.len(), v1.len(), "same number of series");
        assert_eq!(v0, v1, "vector result identical with pool(1) vs default");
    }

    #[test]
    fn query_parallelism_2_same_result_as_default() {
        let (db_default, _g0) = make_db_with_series_and_parallelism(None);
        let (db_pool2, _g1) = make_db_with_series_and_parallelism(Some(2));
        let r0 = handle_query(
            Some(r#"http_requests_total{job=~"api|web"}"#),
            Some("2"),
            &db_default,
        );
        let r1 = handle_query(
            Some(r#"http_requests_total{job=~"api|web"}"#),
            Some("2"),
            &db_pool2,
        );
        assert_eq!(r0.status, StatusCode::OK);
        assert_eq!(r1.status, StatusCode::OK);
        let body0: ApiEnvelope<QueryData> = serde_json::from_slice(&r0.body).unwrap();
        let body1: ApiEnvelope<QueryData> = serde_json::from_slice(&r1.body).unwrap();
        let QueryResult::Vector(mut v0) = body0.data.unwrap().result else {
            panic!("vector")
        };
        let QueryResult::Vector(mut v1) = body1.data.unwrap().result else {
            panic!("vector")
        };
        sort_vector_samples(&mut v0);
        sort_vector_samples(&mut v1);
        assert_eq!(v0, v1, "vector result identical with pool(2) vs default");
    }

    #[test]
    fn query_range_parallelism_1_same_result_as_default() {
        let (db_default, _g0) = make_db_with_series_and_parallelism(None);
        let (db_pool1, _g1) = make_db_with_series_and_parallelism(Some(1));
        let r0 = handle_query_range(
            Some("http_requests_total"),
            Some("1"),
            Some("3"),
            Some("1s"),
            &db_default,
        );
        let r1 = handle_query_range(
            Some("http_requests_total"),
            Some("1"),
            Some("3"),
            Some("1s"),
            &db_pool1,
        );
        assert_eq!(r0.status, StatusCode::OK);
        assert_eq!(r1.status, StatusCode::OK);
        let body0: ApiEnvelope<QueryData> = serde_json::from_slice(&r0.body).unwrap();
        let body1: ApiEnvelope<QueryData> = serde_json::from_slice(&r1.body).unwrap();
        let QueryResult::Matrix(mut m0) = body0.data.unwrap().result else {
            panic!("matrix")
        };
        let QueryResult::Matrix(mut m1) = body1.data.unwrap().result else {
            panic!("matrix")
        };
        sort_matrix_series(&mut m0);
        sort_matrix_series(&mut m1);
        assert_eq!(m0.len(), m1.len());
        assert_eq!(m0, m1, "matrix result identical with pool(1) vs default");
    }

    #[test]
    fn query_parallelism_empty_selector_returns_empty_vector() {
        let (db, _guard) = make_db_with_series_and_parallelism(Some(1));
        let r = handle_query(Some(r#"nonexistent_metric{job="none"}"#), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("vector")
        };
        assert!(samples.is_empty(), "no series match → empty vector");
    }

    // --- Range function tests via API ---

    #[test]
    fn query_rate_returns_vector() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some("rate(http_requests_total[5m])"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert!(
            !samples.is_empty(),
            "rate should produce results for matching series"
        );
        for s in &samples {
            let val: f64 = s.value[1].as_str().unwrap().parse().unwrap();
            assert!(
                val >= 0.0,
                "rate should be non-negative for monotonic counter"
            );
        }
    }

    #[test]
    fn query_range_rate_returns_matrix() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query_range(
            Some("rate(http_requests_total[2s])"),
            Some("1"),
            Some("3"),
            Some("1s"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body.status.as_str(), "success");
        let data = body.data.unwrap();
        assert_eq!(data.result_type, "matrix");
        let QueryResult::Matrix(series) = data.result else {
            panic!("expected matrix")
        };
        assert!(!series.is_empty(), "rate range query should produce matrix");
    }

    #[test]
    fn query_increase_returns_vector() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some("increase(http_requests_total[5m])"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert!(!samples.is_empty());
    }

    #[test]
    fn query_avg_over_time_returns_vector() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(
            Some("avg_over_time(http_requests_total[5m])"),
            Some("2"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert!(!samples.is_empty());
    }

    #[test]
    fn query_max_over_time_returns_vector() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(
            Some("max_over_time(http_requests_total[5m])"),
            Some("2"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert!(!samples.is_empty());
    }

    // --- Aggregation tests ---

    #[test]
    fn query_sum_by_returns_grouped_vector() {
        let (db, _guard) = make_db_for_aggregation();
        let r = handle_query(
            Some(r#"sum by (job) (http_requests_total)"#),
            Some("5"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(mut samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        sort_vector_samples(&mut samples);
        assert_eq!(samples.len(), 2, "should have 2 groups: api and web");
        let api = samples
            .iter()
            .find(|s| s.metric.get("job") == Some(&"api".to_string()))
            .unwrap();
        let web = samples
            .iter()
            .find(|s| s.metric.get("job") == Some(&"web".to_string()))
            .unwrap();
        let api_val: f64 = api.value[1].as_str().unwrap().parse().unwrap();
        let web_val: f64 = web.value[1].as_str().unwrap().parse().unwrap();
        // api: instance a=50, instance b=25 → sum=75
        assert!(
            (api_val - 75.0).abs() < 1e-9,
            "api sum should be 75, got {}",
            api_val
        );
        // web: instance c=15 → sum=15
        assert!(
            (web_val - 15.0).abs() < 1e-9,
            "web sum should be 15, got {}",
            web_val
        );
        // Verify __name__ is not in the metric (it's not in the by clause)
        assert!(
            !api.metric.contains_key("__name__"),
            "sum by (job) should not preserve __name__"
        );
    }

    #[test]
    fn query_avg_without_returns_grouped_vector() {
        let (db, _guard) = make_db_for_aggregation();
        let r = handle_query(
            Some(r#"avg without (instance) (http_requests_total)"#),
            Some("5"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(mut samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        sort_vector_samples(&mut samples);
        assert_eq!(samples.len(), 2, "should have 2 groups: api and web");
        let api = samples
            .iter()
            .find(|s| s.metric.get("job") == Some(&"api".to_string()))
            .unwrap();
        let api_val: f64 = api.value[1].as_str().unwrap().parse().unwrap();
        // api: avg of 50 and 25 = 37.5
        assert!(
            (api_val - 37.5).abs() < 1e-9,
            "api avg should be 37.5, got {}",
            api_val
        );
    }

    #[test]
    fn query_count_returns_series_count() {
        let (db, _guard) = make_db_for_aggregation();
        let r = handle_query(Some(r#"count(http_requests_total)"#), Some("5"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert_eq!(samples.len(), 1, "count with no grouping → single result");
        let count: f64 = samples[0].value[1].as_str().unwrap().parse().unwrap();
        assert_eq!(count, 3.0, "should count 3 series");
    }

    #[test]
    fn query_sum_by_rate_composite() {
        let (db, _guard) = make_db_for_aggregation();
        let r = handle_query(
            Some(r#"sum by (job) (rate(http_requests_total[5m]))"#),
            Some("5"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(mut samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        sort_vector_samples(&mut samples);
        assert_eq!(samples.len(), 2, "should have 2 groups: api and web");
        for s in &samples {
            let val: f64 = s.value[1].as_str().unwrap().parse().unwrap();
            assert!(val > 0.0, "sum of rates should be positive");
        }
    }

    #[test]
    fn query_range_sum_by_rate_returns_matrix() {
        let (db, _guard) = make_db_for_aggregation();
        let r = handle_query_range(
            Some(r#"sum by (job) (rate(http_requests_total[3s]))"#),
            Some("3"),
            Some("5"),
            Some("1s"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let data = body.data.unwrap();
        assert_eq!(data.result_type, "matrix");
        let QueryResult::Matrix(mut series) = data.result else {
            panic!("expected matrix")
        };
        sort_matrix_series(&mut series);
        assert_eq!(series.len(), 2, "should have 2 groups: api and web");
        for s in &series {
            assert!(!s.values.is_empty(), "each group should have step values");
        }
    }

    // --- Window aggregate / downsampling test ---

    #[test]
    fn query_range_avg_over_time_downsamples() {
        let (db, _guard) = make_db_for_aggregation();
        let r = handle_query_range(
            Some("avg_over_time(http_requests_total[3s])"),
            Some("2"),
            Some("5"),
            Some("1s"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let data = body.data.unwrap();
        assert_eq!(data.result_type, "matrix");
        let QueryResult::Matrix(series) = data.result else {
            panic!("expected matrix")
        };
        assert!(
            !series.is_empty(),
            "avg_over_time range query should produce results"
        );
        for s in &series {
            assert!(
                s.values.len() >= 2,
                "should have multiple step values (downsampled)"
            );
        }
    }

    // --- Adversarial tests ---

    #[test]
    fn query_rate_no_data_returns_empty_vector() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some(r#"rate(nonexistent[5m])"#), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert!(samples.is_empty());
    }

    #[test]
    fn query_unsupported_function_returns_422() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some("histogram_quantile(0.9, rate(x[5m]))"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn query_binary_expr_returns_422() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some("metric_a + metric_b"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn query_sum_of_empty_set_returns_empty() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some(r#"sum by (job) (nonexistent)"#), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        assert!(samples.is_empty(), "sum of empty set should be empty");
    }

    // ---------- HTTP handler delegation and PromqlError → status mapping (4.1 acceptance) ----------

    /// Parse errors (invalid/unsupported PromQL) must map to 422 and envelope errorType "bad_data".
    #[test]
    fn handler_parse_error_maps_to_422() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some("metric_a + metric_b"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
        let body: ApiEnvelope<serde_json::Value> = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body.status, "error");
        assert_eq!(body.error_type.as_deref(), Some("bad_data"));
        assert!(body.error.is_some());
    }

    /// Bad parameter (invalid time/step/range) must map to 400.
    #[test]
    fn handler_bad_parameter_maps_to_400() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some("http_requests_total"), Some("not-a-number"), &db);
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
        let body: ApiEnvelope<serde_json::Value> = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body.status, "error");
        assert_eq!(body.error_type.as_deref(), Some("bad_data"));
    }

    /// Range query: missing step → 400.
    #[test]
    fn handler_query_range_missing_step_returns_400() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query_range(
            Some("http_requests_total"),
            Some("1"),
            Some("3"),
            None,
            &db,
        );
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
    }

    /// Success response contract: status "success", data present, resultType and result shape.
    #[test]
    fn handler_success_envelope_contract() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query(Some("http_requests_total"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body.status, "success");
        let data = body.data.expect("success response must have data");
        assert_eq!(data.result_type, "vector");
        let QueryResult::Vector(samples) = data.result else {
            panic!("expected vector")
        };
        for s in &samples {
            assert_eq!(s.value.len(), 2, "vector sample must be [timestamp, value]");
            assert!(s.value[0].is_number(), "timestamp must be number");
            assert!(s.value[1].is_string(), "value must be string (Prometheus format)");
        }
    }

    /// Matrix response contract: resultType "matrix", each series has values [[ts, "val"], ...].
    #[test]
    fn handler_matrix_envelope_contract() {
        let (db, _guard) = make_db_with_series();
        let r = handle_query_range(
            Some("http_requests_total"),
            Some("1"),
            Some("3"),
            Some("1s"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body.status, "success");
        let data = body.data.expect("success response must have data");
        assert_eq!(data.result_type, "matrix");
        let QueryResult::Matrix(series) = data.result else {
            panic!("expected matrix")
        };
        for s in &series {
            for pair in &s.values {
                assert_eq!(pair.len(), 2);
                assert!(pair[0].is_number());
                assert!(pair[1].is_string());
            }
        }
    }

    /// Handler delegation: instant query HTTP response matches library `query_instant` result (same samples, same values).
    #[test]
    fn handler_instant_query_matches_library() {
        let (db, _guard) = make_db_with_series();
        let time_ns = 2_000_000_000;
        let lib_result = crate::promql::query_instant(&db, "http_requests_total", time_ns).unwrap();
        let r = handle_query(Some("http_requests_total"), Some("2"), &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Vector(mut samples) = body.data.unwrap().result else {
            panic!("expected vector")
        };
        sort_vector_samples(&mut samples);
        assert_eq!(
            samples.len(),
            lib_result.len(),
            "handler must return same number of samples as library"
        );
        let mut lib_sorted: Vec<_> = lib_result.iter().collect();
        lib_sorted.sort_by_key(|s| metric_sort_key(&s.metric));
        for (http_s, lib_s) in samples.iter().zip(lib_sorted) {
            assert_eq!(http_s.metric, lib_s.metric);
            let http_val: f64 = http_s.value[1].as_str().unwrap().parse().unwrap();
            assert!(
                (http_val - lib_s.value).abs() < 1e-9,
                "value mismatch: HTTP {} vs library {}",
                http_val,
                lib_s.value
            );
        }
    }

    /// Handler delegation: range query HTTP response matches library `query_range` result.
    #[test]
    fn handler_range_query_matches_library() {
        let (db, _guard) = make_db_with_series();
        let start_ns = 1_000_000_000;
        let end_ns = 3_000_000_000;
        let step_ns = 1_000_000_000;
        let lib_result = crate::promql::query_range(
            &db,
            "http_requests_total",
            start_ns,
            end_ns,
            step_ns,
        )
        .unwrap();
        let r = handle_query_range(
            Some("http_requests_total"),
            Some("1"),
            Some("3"),
            Some("1s"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<QueryData> = serde_json::from_slice(&r.body).unwrap();
        let QueryResult::Matrix(mut series) = body.data.unwrap().result else {
            panic!("expected matrix")
        };
        sort_matrix_series(&mut series);
        assert_eq!(
            series.len(),
            lib_result.len(),
            "handler must return same number of series as library"
        );
        let mut lib_sorted: Vec<_> = lib_result.iter().collect();
        lib_sorted.sort_by_key(|s| metric_sort_key(&s.metric));
        for (http_s, lib_s) in series.iter().zip(lib_sorted) {
            assert_eq!(http_s.metric, lib_s.metric);
            assert_eq!(http_s.values.len(), lib_s.steps.len());
            for (hp, lp) in http_s.values.iter().zip(lib_s.steps.iter()) {
                let ts_sec = hp[0].as_f64().unwrap();
                assert!(
                    (ts_sec * 1e9 - lp.0 as f64).abs() < 1.0,
                    "timestamp mismatch"
                );
                let http_val: f64 = hp[1].as_str().unwrap().parse().unwrap();
                assert!(
                    (http_val - lp.1).abs() < 1e-9,
                    "value mismatch at step"
                );
            }
        }
    }

    /// Handler delegation: labels HTTP response matches library `labels` (same set of names).
    #[test]
    fn handler_labels_matches_library() {
        let (db, _guard) = make_db_with_series();
        let start_ns = 0;
        let end_ns = crate::promql::parse_eval_time(None).unwrap();
        let lib_result = crate::promql::labels(&db, None as Option<&[&str]>, start_ns, end_ns).unwrap();
        let r = handle_labels(&[], None, None, &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<Vec<String>> = serde_json::from_slice(&r.body).unwrap();
        let http_data = body.data.unwrap();
        let mut lib_sorted = lib_result.clone();
        lib_sorted.sort();
        let mut http_sorted = http_data.clone();
        http_sorted.sort();
        assert_eq!(http_sorted, lib_sorted, "handler labels must match library");
    }

    /// Handler delegation: label values HTTP response matches library `label_values` (same set of values).
    #[test]
    fn handler_label_values_matches_library() {
        let (db, _guard) = make_db_with_series();
        let start_ns = 0;
        let end_ns = crate::promql::parse_eval_time(None).unwrap();
        let lib_result = crate::promql::label_values(&db, "job", None::<&[&str]>, start_ns, end_ns).unwrap();
        let r = handle_label_values("job", &[], None, None, &db);
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<Vec<String>> = serde_json::from_slice(&r.body).unwrap();
        let http_data = body.data.unwrap();
        let mut lib_sorted = lib_result.clone();
        lib_sorted.sort();
        let mut http_sorted = http_data.clone();
        http_sorted.sort();
        assert_eq!(http_sorted, lib_sorted, "handler label_values must match library");
    }

    /// Contract test: label_values with match[] and start/end — HTTP and library same inputs → identical results.
    #[test]
    fn handler_label_values_with_match_and_time_range_matches_library() {
        let (db, _guard) = make_db_with_series();
        let match_selectors = ["http_requests_total{job=\"api\"}".to_string()];
        let start_ns = 1_000_000_000;
        let end_ns = 3_000_000_000;
        let lib_result = crate::promql::label_values(
            &db,
            "job",
            Some(match_selectors.as_slice()),
            start_ns,
            end_ns,
        )
        .unwrap();
        let r = handle_label_values(
            "job",
            &match_selectors,
            Some("1"),
            Some("3"),
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<Vec<String>> = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body.status, "success");
        let http_data = body.data.unwrap();
        let mut lib_sorted = lib_result.clone();
        lib_sorted.sort();
        let mut http_sorted = http_data.clone();
        http_sorted.sort();
        assert_eq!(http_sorted, lib_sorted, "handler label_values with match and start/end must match library");
    }

    /// Handler delegation: series HTTP response matches library `series` (same label sets).
    #[test]
    fn handler_series_matches_library() {
        let (db, _guard) = make_db_with_series();
        let match_selectors = ["http_requests_total"];
        let start_ns = 0;
        let end_ns = crate::promql::parse_eval_time(None).unwrap();
        let lib_result =
            crate::promql::series(&db, &match_selectors, start_ns, end_ns).unwrap();
        let r = handle_series(
            &match_selectors.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            None,
            None,
            &db,
        );
        assert_eq!(r.status, StatusCode::OK);
        let body: ApiEnvelope<Vec<HashMap<String, String>>> = serde_json::from_slice(&r.body).unwrap();
        let http_data = body.data.unwrap();
        assert_eq!(http_data.len(), lib_result.len());
        let mut http_sorted: Vec<_> = http_data.iter().map(metric_sort_key).collect();
        http_sorted.sort();
        let mut lib_sorted: Vec<_> = lib_result.iter().map(metric_sort_key).collect();
        lib_sorted.sort();
        assert_eq!(http_sorted, lib_sorted, "handler series must match library");
    }
}
