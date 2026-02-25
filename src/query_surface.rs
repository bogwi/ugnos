//! Stable PromQL-like query surface for Grafana-compatible Prometheus HTTP API.
//!
//! This module defines the subset of PromQL that ugnos accepts and evaluates for
//! `/api/v1/query`, `/api/v1/query_range`, and `/api/v1/series`. The goal is
//! maximum adoption: same syntax as Prometheus and Grafana template variables.
//!
//! ## Supported
//!
//! ### Instant vector selector
//! - Metric name: `metric_name` or `{__name__="metric_name"}` or `{__name__=~"pattern"}`
//! - Label matchers: `=`, `!=`, `=~`, `!~` (exact, not-equal, regex, negated regex)
//! - Optional `offset <duration>` (evaluation time is shifted back)
//! - Parentheses: `( metric_name{job="api"} )`
//!
//! ### Range functions
//! - `rate(metric[duration])` — per-second rate of counter increase
//! - `increase(metric[duration])` — total counter increase over window
//! - `avg_over_time(metric[duration])` — average value over window
//! - `max_over_time(metric[duration])` — maximum value over window
//! - `min_over_time(metric[duration])` — minimum value over window
//! - `sum_over_time(metric[duration])` — sum of values over window
//!
//! ### Aggregations with grouping
//! - `sum`, `avg`, `min`, `max`, `count`
//! - Grouping: `by (label, ...)` or `without (label, ...)`
//! - Composition: `sum by (job) (rate(http_requests_total[5m]))`
//!
//! ## Not yet supported (return clear 422)
//! - Binary operators, subqueries
//!
//! All matching uses Prometheus semantics: missing label matches `!=` and `!~`,
//! does not match `=` or `=~`.

use promql_parser::label::METRIC_NAME;
use promql_parser::parser;
use promql_parser::parser::{Expr, LabelModifier, VectorSelector};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Instant vector selector (unchanged public API for backward compatibility)
// ---------------------------------------------------------------------------

/// Parsed instant-vector selector suitable for evaluation at a given time.
#[derive(Debug, Clone)]
pub struct InstantSelector {
    /// The inner vector selector (metric name + matchers).
    pub selector: VectorSelector,
    /// If set, evaluation time should be `query_time - offset`.
    pub offset: Option<Duration>,
}

/// Parses a PromQL query and returns an instant vector selector if the expression
/// is supported.
///
/// Supported: bare `VectorSelector`, `Paren(VectorSelector)`, and either with
/// `offset <duration>`. Returns an error for range vectors, aggregations,
/// functions, and binary expressions.
pub fn parse_instant_selector(query: &str) -> Result<InstantSelector, String> {
    let expr = parser::parse(query.trim()).map_err(|e| format!("invalid PromQL: {}", e))?;
    let (vs, offset) = extract_instant_selector(&expr)?;
    validate_vector_selector(vs)?;
    Ok(InstantSelector {
        selector: vs.clone(),
        offset,
    })
}

fn validate_vector_selector(vs: &VectorSelector) -> Result<(), String> {
    let has_name = vs.name.as_ref().is_some_and(|n| !n.is_empty());
    if !has_name && vs.matchers.is_empty_matchers() {
        return Err(
            "vector selector must specify a metric name or at least one non-empty label matcher"
                .to_string(),
        );
    }
    if !vs.matchers.or_matchers.is_empty() {
        return Err("OR matchers in vector selector are not yet supported".to_string());
    }
    Ok(())
}

/// Unwrap Paren and extract VectorSelector and optional offset.
fn extract_instant_selector(expr: &Expr) -> Result<(&VectorSelector, Option<Duration>), String> {
    let (inner, offset) = unwrap_paren_and_offset(expr)?;
    match inner {
        Expr::VectorSelector(vs) => Ok((vs, offset)),
        Expr::MatrixSelector(_) => Err(
            "range vector and functions (e.g. rate, increase) are not yet supported; use instant vector selector only".to_string(),
        ),
        Expr::Call(_) => Err(
            "functions (rate, increase, avg_over_time, etc.) are not yet supported".to_string(),
        ),
        Expr::Aggregate(_) => Err("aggregations (sum, avg, etc.) are not yet supported".to_string()),
        Expr::Binary(_) => Err("binary expressions are not yet supported".to_string()),
        Expr::Unary(_) => Err("unary expressions are not yet supported on selectors".to_string()),
        Expr::Subquery(_) => Err("subqueries are not yet supported".to_string()),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {
            Err("literal expressions are not supported as query".to_string())
        }
        Expr::Paren(_) => Err("invalid nested paren".to_string()),
        Expr::Extension(_) => Err("extension expressions are not supported".to_string()),
    }
}

fn unwrap_paren_and_offset(expr: &Expr) -> Result<(&Expr, Option<Duration>), String> {
    match expr {
        Expr::Paren(p) => unwrap_paren_and_offset(&p.expr),
        Expr::VectorSelector(vs) => {
            let offset = vs.offset.as_ref().and_then(offset_to_duration);
            Ok((expr, offset))
        }
        Expr::MatrixSelector(ms) => {
            let offset = ms.vs.offset.as_ref().and_then(offset_to_duration);
            Ok((expr, offset))
        }
        _ => Ok((expr, None)),
    }
}

fn offset_to_duration(o: &promql_parser::parser::Offset) -> Option<Duration> {
    use promql_parser::parser::Offset;
    match o {
        Offset::Pos(d) => Some(*d),
        Offset::Neg(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Full evaluation expression tree
// ---------------------------------------------------------------------------

/// Supported range functions applied to a matrix selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFn {
    /// Per-second rate of increase (counter-aware).
    Rate,
    /// Total increase over the range (counter-aware).
    Increase,
    /// Average value over the range.
    AvgOverTime,
    /// Maximum value over the range.
    MaxOverTime,
    /// Minimum value over the range.
    MinOverTime,
    /// Sum of values over the range.
    SumOverTime,
}

impl fmt::Display for RangeFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rate => write!(f, "rate"),
            Self::Increase => write!(f, "increase"),
            Self::AvgOverTime => write!(f, "avg_over_time"),
            Self::MaxOverTime => write!(f, "max_over_time"),
            Self::MinOverTime => write!(f, "min_over_time"),
            Self::SumOverTime => write!(f, "sum_over_time"),
        }
    }
}

/// Supported aggregation operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    /// Sum of values across series.
    Sum,
    /// Arithmetic mean across series.
    Avg,
    /// Minimum value across series.
    Min,
    /// Maximum value across series.
    Max,
    /// Count of series.
    Count,
}

impl fmt::Display for AggOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Count => write!(f, "count"),
        }
    }
}

/// Grouping clause for aggregation operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    /// `by (label1, label2, ...)` — keep only these labels in the result.
    By(Vec<String>),
    /// `without (label1, label2, ...)` — keep all labels except these.
    Without(Vec<String>),
}

/// Full evaluation expression tree for the supported PromQL subset.
///
/// Produced by [`parse_eval_expr`] and consumed by the Prometheus API evaluation engine.
#[derive(Debug, Clone)]
pub enum EvalExpr {
    /// Instant vector selector: `metric{labels}`.
    Instant(InstantSelector),
    /// Range function applied to a matrix selector: `rate(metric[5m])`.
    RangeFunction {
        /// Which range function to apply.
        func: RangeFn,
        /// The underlying vector selector (metric name + label matchers).
        selector: InstantSelector,
        /// Lookback range window.
        range: Duration,
    },
    /// Aggregation with optional grouping: `sum by (job) (inner_expr)`.
    Aggregation {
        /// Which aggregation operator to apply.
        op: AggOp,
        /// The inner expression to evaluate before aggregating.
        inner: Box<EvalExpr>,
        /// Grouping clause (by/without).
        grouping: Grouping,
    },
}

/// Parses a PromQL query into a full evaluation expression tree.
///
/// Accepts instant vector selectors, range functions (`rate`, `increase`,
/// `avg_over_time`, `max_over_time`, `min_over_time`, `sum_over_time`),
/// and aggregations (`sum`, `avg`, `min`, `max`, `count`) with `by`/`without`
/// grouping. Nesting is supported (e.g. `sum by (job) (rate(m[5m]))`).
///
/// Returns a structured error for unsupported expression types (binary ops,
/// subqueries, etc.) with an actionable message.
pub fn parse_eval_expr(query: &str) -> Result<EvalExpr, String> {
    let expr = parser::parse(query.trim()).map_err(|e| format!("invalid PromQL: {}", e))?;
    convert_expr(&expr)
}

fn convert_expr(expr: &Expr) -> Result<EvalExpr, String> {
    match expr {
        Expr::VectorSelector(vs) => {
            validate_vector_selector(vs)?;
            let offset = vs.offset.as_ref().and_then(offset_to_duration);
            Ok(EvalExpr::Instant(InstantSelector {
                selector: vs.clone(),
                offset,
            }))
        }
        Expr::Paren(p) => convert_expr(&p.expr),
        Expr::Call(call) => convert_call(call),
        Expr::Aggregate(agg) => convert_aggregate(agg),
        Expr::Binary(_) => Err("binary expressions are not yet supported".to_string()),
        Expr::Unary(_) => Err("unary expressions are not yet supported".to_string()),
        Expr::Subquery(_) => Err("subqueries are not yet supported".to_string()),
        Expr::MatrixSelector(_) => Err(
            "bare range vector (metric[5m]) must be wrapped in a function like rate()".to_string(),
        ),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {
            Err("literal expressions are not supported as query".to_string())
        }
        Expr::Extension(_) => Err("extension expressions are not supported".to_string()),
    }
}

fn convert_call(call: &promql_parser::parser::Call) -> Result<EvalExpr, String> {
    let func_name = call.func.name;
    let range_fn = match func_name {
        "rate" => RangeFn::Rate,
        "increase" => RangeFn::Increase,
        "avg_over_time" => RangeFn::AvgOverTime,
        "max_over_time" => RangeFn::MaxOverTime,
        "min_over_time" => RangeFn::MinOverTime,
        "sum_over_time" => RangeFn::SumOverTime,
        _ => return Err(format!("unsupported function: {}()", func_name)),
    };

    if call.args.args.len() != 1 {
        return Err(format!(
            "{}() requires exactly one range-vector argument",
            func_name
        ));
    }

    let arg = &call.args.args[0];
    match arg.as_ref() {
        Expr::MatrixSelector(ms) => {
            validate_vector_selector(&ms.vs)?;
            let offset = ms.vs.offset.as_ref().and_then(offset_to_duration);
            Ok(EvalExpr::RangeFunction {
                func: range_fn,
                selector: InstantSelector {
                    selector: ms.vs.clone(),
                    offset,
                },
                range: ms.range,
            })
        }
        _ => Err(format!(
            "{}() requires a range vector selector argument (e.g. metric[5m])",
            func_name
        )),
    }
}

fn convert_aggregate(agg: &promql_parser::parser::AggregateExpr) -> Result<EvalExpr, String> {
    let op = match agg.op.to_string().to_ascii_lowercase().as_str() {
        "sum" => AggOp::Sum,
        "avg" => AggOp::Avg,
        "min" => AggOp::Min,
        "max" => AggOp::Max,
        "count" => AggOp::Count,
        other => {
            return Err(format!("unsupported aggregation operator: {}", other));
        }
    };

    let inner = convert_expr(&agg.expr)?;

    let grouping = match &agg.modifier {
        Some(LabelModifier::Include(labels)) => Grouping::By(labels.labels.clone()),
        Some(LabelModifier::Exclude(labels)) => Grouping::Without(labels.labels.clone()),
        None => Grouping::By(vec![]),
    };

    Ok(EvalExpr::Aggregation {
        op,
        inner: Box::new(inner),
        grouping,
    })
}

// ---------------------------------------------------------------------------
// Query plan explain output
// ---------------------------------------------------------------------------

/// Produces a human-readable tree representation of the evaluation plan.
///
/// Suitable for `EXPLAIN` output, debugging, and regression testing of
/// query planner semantics.
pub fn explain(expr: &EvalExpr) -> String {
    explain_inner(expr, 0)
}

fn explain_inner(expr: &EvalExpr, depth: usize) -> String {
    let indent = "  ".repeat(depth);
    let connector = if depth > 0 { "└─ " } else { "" };
    match expr {
        EvalExpr::Instant(s) => {
            format!(
                "{indent}{connector}VectorSelector({name}{matchers})",
                name = s.selector.name.as_deref().unwrap_or(""),
                matchers = format_matchers(s),
            )
        }
        EvalExpr::RangeFunction {
            func,
            selector,
            range,
        } => {
            let header = format!(
                "{indent}{connector}RangeFunction({func}, range={range}s)",
                range = range.as_secs(),
            );
            let child = explain_inner(&EvalExpr::Instant(selector.clone()), depth + 1);
            format!("{header}\n{child}")
        }
        EvalExpr::Aggregation {
            op,
            inner,
            grouping,
        } => {
            let group_desc = match grouping {
                Grouping::By(labels) if labels.is_empty() => String::new(),
                Grouping::By(labels) => format!(" by({})", labels.join(", ")),
                Grouping::Without(labels) => format!(" without({})", labels.join(", ")),
            };
            let header = format!("{indent}{connector}Aggregation({op}{group_desc})");
            let child = explain_inner(inner, depth + 1);
            format!("{header}\n{child}")
        }
    }
}

fn format_matchers(s: &InstantSelector) -> String {
    if s.selector.matchers.matchers.is_empty() {
        return String::new();
    }
    let items: Vec<String> = s
        .selector
        .matchers
        .matchers
        .iter()
        .map(|m| format!("{}", m))
        .collect();
    format!(", {{{}}}", items.join(", "))
}

// ---------------------------------------------------------------------------
// Series matching (shared by all expression types)
// ---------------------------------------------------------------------------

/// Returns true if the series `(series_name, tags)` matches the vector selector.
///
/// Semantics follow Prometheus: `__name__` matcher applies to the metric name;
/// other matchers apply to labels. Missing label value is treated as empty string
/// (so `=` and `=~` do not match, `!=` and `!~` match).
pub fn series_matches_selector(
    series_name: &str,
    tags: &HashMap<String, String>,
    vs: &VectorSelector,
) -> bool {
    if let Some(ref name) = vs.name {
        if name != series_name {
            return false;
        }
    }

    for m in &vs.matchers.matchers {
        let value = if m.name == METRIC_NAME {
            series_name
        } else {
            tags.get(&m.name).map(|s| s.as_str()).unwrap_or("")
        };
        if !m.is_match(value) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    // --- Instant selector tests (backward compat) ---

    #[test]
    fn parse_instant_selector_metric_only() {
        let s = parse_instant_selector("http_requests_total").unwrap();
        assert_eq!(s.selector.name.as_deref(), Some("http_requests_total"));
        assert!(s.selector.matchers.matchers.is_empty());
        assert!(series_matches_selector(
            "http_requests_total",
            &tags(&[]),
            &s.selector
        ));
        assert!(!series_matches_selector("other", &tags(&[]), &s.selector));
    }

    #[test]
    fn parse_instant_selector_with_equal_matchers() {
        let s = parse_instant_selector(r#"http_requests_total{job="api",method="GET"}"#).unwrap();
        assert!(series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "api"), ("method", "GET")]),
            &s.selector
        ));
        assert!(!series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "api")]),
            &s.selector
        ));
        assert!(!series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "api"), ("method", "POST")]),
            &s.selector
        ));
    }

    #[test]
    fn parse_instant_selector_not_equal() {
        let s = parse_instant_selector(r#"metric{job!="internal"}"#).unwrap();
        assert!(series_matches_selector(
            "metric",
            &tags(&[("job", "api")]),
            &s.selector
        ));
        assert!(!series_matches_selector(
            "metric",
            &tags(&[("job", "internal")]),
            &s.selector
        ));
        assert!(series_matches_selector("metric", &tags(&[]), &s.selector));
    }

    #[test]
    fn parse_instant_selector_regex() {
        let s = parse_instant_selector(r#"http_requests_total{job=~"api|web"}"#).unwrap();
        assert!(series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "api")]),
            &s.selector
        ));
        assert!(series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "web")]),
            &s.selector
        ));
        assert!(!series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "other")]),
            &s.selector
        ));
    }

    #[test]
    fn parse_instant_selector_negated_regex() {
        let s = parse_instant_selector(r#"metric{env!~"prod|staging"}"#).unwrap();
        assert!(series_matches_selector(
            "metric",
            &tags(&[("env", "dev")]),
            &s.selector
        ));
        assert!(!series_matches_selector(
            "metric",
            &tags(&[("env", "prod")]),
            &s.selector
        ));
    }

    #[test]
    fn parse_instant_selector_name_via_matcher() {
        let s = parse_instant_selector(r#"{__name__=~"http_.*"}"#).unwrap();
        assert!(series_matches_selector(
            "http_requests_total",
            &tags(&[]),
            &s.selector
        ));
        assert!(series_matches_selector(
            "http_errors",
            &tags(&[]),
            &s.selector
        ));
        assert!(!series_matches_selector(
            "cpu_usage",
            &tags(&[]),
            &s.selector
        ));
    }

    #[test]
    fn parse_instant_selector_paren() {
        let s = parse_instant_selector(r#"( http_requests_total{job="api"} )"#).unwrap();
        assert_eq!(s.selector.name.as_deref(), Some("http_requests_total"));
        assert!(series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "api")]),
            &s.selector
        ));
    }

    #[test]
    fn parse_rejects_range_vector() {
        let e = parse_instant_selector("http_requests_total[5m]").unwrap_err();
        assert!(e.contains("range vector") || e.contains("not yet supported"));
    }

    #[test]
    fn parse_rejects_rate() {
        let e = parse_instant_selector("rate(http_requests_total[5m])").unwrap_err();
        assert!(e.contains("function") || e.contains("not yet supported"));
    }

    #[test]
    fn parse_rejects_aggregation() {
        let e = parse_instant_selector("sum(http_requests_total)").unwrap_err();
        assert!(e.contains("aggregation") || e.contains("not yet supported"));
    }

    #[test]
    fn parse_rejects_empty_query() {
        let e = parse_instant_selector("").unwrap_err();
        assert!(!e.is_empty());
    }

    #[test]
    fn parse_rejects_invalid_syntax() {
        let e = parse_instant_selector("sum(rate(x[5m]))").unwrap_err();
        assert!(e.contains("invalid") || e.contains("not yet supported"));
    }

    // --- Adversarial selector edge cases ---

    #[test]
    fn series_matches_selector_missing_label_not_equal_matches() {
        let vs = parse_instant_selector(r#"m{job!="x"}"#).unwrap().selector;
        assert!(series_matches_selector("m", &tags(&[]), &vs));
        assert!(!series_matches_selector("m", &tags(&[("job", "x")]), &vs));
    }

    #[test]
    fn series_matches_selector_missing_label_equal_no_match() {
        let vs = parse_instant_selector(r#"m{job="api"}"#).unwrap().selector;
        assert!(!series_matches_selector("m", &tags(&[]), &vs));
        assert!(series_matches_selector("m", &tags(&[("job", "api")]), &vs));
    }

    #[test]
    fn series_matches_selector_name_and_matcher_both_checked() {
        let vs = parse_instant_selector(r#"http_requests_total{job="api"}"#)
            .unwrap()
            .selector;
        assert!(!series_matches_selector(
            "other",
            &tags(&[("job", "api")]),
            &vs
        ));
        assert!(!series_matches_selector(
            "http_requests_total",
            &tags(&[("job", "web")]),
            &vs
        ));
    }

    #[test]
    fn parse_empty_string_returns_error() {
        assert!(parse_instant_selector("   ").is_err());
    }

    // --- Full expression tree tests (parse_eval_expr) ---

    #[test]
    fn eval_expr_instant_selector() {
        let e = parse_eval_expr("http_requests_total").unwrap();
        assert!(matches!(e, EvalExpr::Instant(_)));
    }

    #[test]
    fn eval_expr_rate() {
        let e = parse_eval_expr("rate(http_requests_total[5m])").unwrap();
        match e {
            EvalExpr::RangeFunction {
                func,
                selector,
                range,
            } => {
                assert_eq!(func, RangeFn::Rate);
                assert_eq!(
                    selector.selector.name.as_deref(),
                    Some("http_requests_total")
                );
                assert_eq!(range, Duration::from_secs(300));
            }
            _ => panic!("expected RangeFunction"),
        }
    }

    #[test]
    fn eval_expr_increase() {
        let e = parse_eval_expr("increase(my_counter[1h])").unwrap();
        match e {
            EvalExpr::RangeFunction { func, range, .. } => {
                assert_eq!(func, RangeFn::Increase);
                assert_eq!(range, Duration::from_secs(3600));
            }
            _ => panic!("expected RangeFunction"),
        }
    }

    #[test]
    fn eval_expr_avg_over_time() {
        let e = parse_eval_expr("avg_over_time(cpu_usage[10m])").unwrap();
        match e {
            EvalExpr::RangeFunction { func, range, .. } => {
                assert_eq!(func, RangeFn::AvgOverTime);
                assert_eq!(range, Duration::from_secs(600));
            }
            _ => panic!("expected RangeFunction"),
        }
    }

    #[test]
    fn eval_expr_max_over_time() {
        let e = parse_eval_expr("max_over_time(latency[30s])").unwrap();
        match e {
            EvalExpr::RangeFunction { func, range, .. } => {
                assert_eq!(func, RangeFn::MaxOverTime);
                assert_eq!(range, Duration::from_secs(30));
            }
            _ => panic!("expected RangeFunction"),
        }
    }

    #[test]
    fn eval_expr_min_over_time() {
        let e = parse_eval_expr("min_over_time(temp[2h])").unwrap();
        match &e {
            EvalExpr::RangeFunction { func, .. } => assert_eq!(*func, RangeFn::MinOverTime),
            _ => panic!("expected RangeFunction"),
        }
    }

    #[test]
    fn eval_expr_sum_over_time() {
        let e = parse_eval_expr("sum_over_time(requests[1m])").unwrap();
        match &e {
            EvalExpr::RangeFunction { func, .. } => assert_eq!(*func, RangeFn::SumOverTime),
            _ => panic!("expected RangeFunction"),
        }
    }

    #[test]
    fn eval_expr_sum_by() {
        let e = parse_eval_expr(r#"sum by (job) (rate(http_requests_total{method="GET"}[5m]))"#)
            .unwrap();
        match e {
            EvalExpr::Aggregation {
                op,
                inner,
                grouping,
            } => {
                assert_eq!(op, AggOp::Sum);
                assert_eq!(grouping, Grouping::By(vec!["job".to_string()]));
                match *inner {
                    EvalExpr::RangeFunction { func, .. } => {
                        assert_eq!(func, RangeFn::Rate);
                    }
                    _ => panic!("expected RangeFunction inner"),
                }
            }
            _ => panic!("expected Aggregation"),
        }
    }

    #[test]
    fn eval_expr_avg_without() {
        let e = parse_eval_expr(r#"avg without (instance) (cpu_usage)"#).unwrap();
        match e {
            EvalExpr::Aggregation { op, grouping, .. } => {
                assert_eq!(op, AggOp::Avg);
                assert_eq!(grouping, Grouping::Without(vec!["instance".to_string()]));
            }
            _ => panic!("expected Aggregation"),
        }
    }

    #[test]
    fn eval_expr_count_no_grouping() {
        let e = parse_eval_expr(r#"count(http_requests_total)"#).unwrap();
        match e {
            EvalExpr::Aggregation { op, grouping, .. } => {
                assert_eq!(op, AggOp::Count);
                assert_eq!(grouping, Grouping::By(vec![]));
            }
            _ => panic!("expected Aggregation"),
        }
    }

    #[test]
    fn eval_expr_min_max_agg() {
        let e = parse_eval_expr(r#"min by (env) (avg_over_time(cpu[5m]))"#).unwrap();
        match &e {
            EvalExpr::Aggregation { op, .. } => assert_eq!(*op, AggOp::Min),
            _ => panic!("expected Aggregation"),
        }

        let e = parse_eval_expr(r#"max by (env) (avg_over_time(cpu[5m]))"#).unwrap();
        match &e {
            EvalExpr::Aggregation { op, .. } => assert_eq!(*op, AggOp::Max),
            _ => panic!("expected Aggregation"),
        }
    }

    #[test]
    fn eval_expr_rejects_binary() {
        let e = parse_eval_expr("metric_a + metric_b").unwrap_err();
        assert!(e.contains("binary") || e.contains("not yet supported"));
    }

    #[test]
    fn eval_expr_rejects_subquery() {
        let e = parse_eval_expr("rate(http_requests_total[5m])[30m:1m]").unwrap_err();
        assert!(e.contains("subquer") || e.contains("not yet supported"));
    }

    #[test]
    fn eval_expr_rejects_unsupported_function() {
        let e =
            parse_eval_expr("histogram_quantile(0.9, rate(http_duration_bucket[5m]))").unwrap_err();
        assert!(e.contains("unsupported function"));
    }

    #[test]
    fn eval_expr_rejects_bare_range_vector() {
        let e = parse_eval_expr("http_requests_total[5m]").unwrap_err();
        assert!(e.contains("range vector") || e.contains("wrapped in a function"));
    }

    #[test]
    fn eval_expr_rate_with_label_matchers() {
        let e = parse_eval_expr(r#"rate(http_requests_total{job="api",method=~"GET|POST"}[5m])"#)
            .unwrap();
        match e {
            EvalExpr::RangeFunction { func, selector, .. } => {
                assert_eq!(func, RangeFn::Rate);
                assert_eq!(
                    selector.selector.name.as_deref(),
                    Some("http_requests_total")
                );
                assert_eq!(selector.selector.matchers.matchers.len(), 2);
            }
            _ => panic!("expected RangeFunction"),
        }
    }

    // --- Explain output tests ---

    #[test]
    fn explain_instant_selector() {
        let e = parse_eval_expr("http_requests_total").unwrap();
        let plan = explain(&e);
        assert!(plan.contains("VectorSelector"));
        assert!(plan.contains("http_requests_total"));
    }

    #[test]
    fn explain_range_function() {
        let e = parse_eval_expr("rate(http_requests_total[5m])").unwrap();
        let plan = explain(&e);
        assert!(plan.contains("RangeFunction(rate"));
        assert!(plan.contains("range=300s"));
        assert!(plan.contains("VectorSelector"));
    }

    #[test]
    fn explain_aggregation_with_grouping() {
        let e = parse_eval_expr(r#"sum by (job) (rate(http_requests_total[5m]))"#).unwrap();
        let plan = explain(&e);
        assert!(plan.contains("Aggregation(sum"));
        assert!(plan.contains("by(job)"));
        assert!(plan.contains("RangeFunction(rate"));
    }

    #[test]
    fn explain_without_grouping() {
        let e = parse_eval_expr(r#"avg without (instance) (cpu_usage)"#).unwrap();
        let plan = explain(&e);
        assert!(plan.contains("Aggregation(avg"));
        assert!(plan.contains("without(instance)"));
    }
}
