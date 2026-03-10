//! PromQL library API demo: run PromQL against a `DbCore` from Rust (no HTTP).
//!
//! Demonstrates a **real-world observability scenario** (2025–2026 style): simulate
//! request counters for a small API with jobs and status codes, then use the full
//! PromQL library surface — instant query, range query, labels, label values, and
//! series — with typed results and structured error handling. Typical use: embedded
//! agents, batch jobs, or services that need to evaluate PromQL without calling the
//! daemon’s HTTP API.
//!
//! **Run with:** `cargo run --example promql_library_demo`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use ugnos::promql::{
    label_values, labels, parse_eval_time, parse_step, query_instant, query_range, series,
    InstantSample, MetricLabels, PromqlError, RangeSeries,
};
use ugnos::{DbConfig, DbCore, TagSet};

/// One second in nanoseconds.
const SEC_NS: u64 = 1_000_000_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new().expect("temp dir");
    let cfg = DbConfig {
        data_dir: PathBuf::from(dir.path()),
        flush_interval: Duration::from_millis(100),
        enable_wal: true,
        enable_snapshots: true,
        enable_segments: true,
        max_series_cardinality: Some(1000), // so list_series_keys is populated for labels/series
        ..DbConfig::default()
    };

    let mut db = DbCore::with_config(cfg)?;
    db.recover()?;
    let db = Arc::new(db);

    // --- Seed data: request counters (SLO/observability-style) ---
    // Simulate http_requests_total with job and status over ~10 minutes.
    // Jobs: api, web. Status: success, 5xx (errors). Counters increase over time.
    // Use seconds-scale timestamps (same as tests) for compatibility.
    let t0 = 1_000_000_000; // 1s in ns (epoch + 1s)
    for i in 0..=10 {
        let ts = t0 + i * 60 * SEC_NS; // every minute: 1s, 61s, 121s, ...
        let mut tags_api_ok = TagSet::new();
        tags_api_ok.insert("job".to_string(), "api".to_string());
        tags_api_ok.insert("status".to_string(), "success".to_string());
        db.insert(
            "http_requests_total",
            ts,
            (i as f64 + 1.0) * 100.0,
            tags_api_ok,
        )?;

        let mut tags_api_err = TagSet::new();
        tags_api_err.insert("job".to_string(), "api".to_string());
        tags_api_err.insert("status".to_string(), "5xx".to_string());
        db.insert(
            "http_requests_total",
            ts,
            (i as f64 + 1.0) * 2.0,
            tags_api_err,
        )?;

        let mut tags_web_ok = TagSet::new();
        tags_web_ok.insert("job".to_string(), "web".to_string());
        tags_web_ok.insert("status".to_string(), "success".to_string());
        db.insert(
            "http_requests_total",
            ts,
            (i as f64 + 1.0) * 50.0,
            tags_web_ok,
        )?;
    }
    db.flush()?;

    let start_ns = t0;
    let end_ns = t0 + 10 * 60 * SEC_NS;
    let eval_time = t0 + 5 * 60 * SEC_NS; // 5 min in

    println!("PromQL library demo — request counters in [t0, t0+10m]");
    println!("  start_ns={}, end_ns={}, eval_time={}\n", start_ns, end_ns, eval_time);

    // ---------- 1. Instant query: current value at eval_time ----------
    println!("1. Instant query: http_requests_total at eval_time");
    match query_instant(&db, "http_requests_total", eval_time) {
        Ok(samples) => {
            for s in &samples {
                print_sample(s);
            }
            println!("  -> {} series\n", samples.len());
        }
        Err(e) => handle_promql_error("query_instant", e),
    }

    // ---------- 2. Instant query: rate (SLO-style) ----------
    println!("2. Instant query: rate(http_requests_total[5m]) at eval_time");
    match query_instant(
        &db,
        "rate(http_requests_total[5m])",
        eval_time,
    ) {
        Ok(samples) => {
            for s in &samples {
                print_sample(s);
            }
            println!("  -> {} series\n", samples.len());
        }
        Err(e) => handle_promql_error("rate instant", e),
    }

    // ---------- 3. Instant aggregation: sum by (job) ----------
    println!("3. Instant query: sum by (job) (http_requests_total) at eval_time");
    match query_instant(
        &db,
        r#"sum by (job) (http_requests_total)"#,
        eval_time,
    ) {
        Ok(samples) => {
            for s in &samples {
                print_sample(s);
            }
            println!("  -> {} groups\n", samples.len());
        }
        Err(e) => handle_promql_error("sum by job", e),
    }

    // ---------- 4. Range query: rate over time (for graphing) ----------
    println!("4. Range query: rate(http_requests_total[5m]) over [start,end] step 1m");
    let step_ns = 60 * SEC_NS;
    match query_range(
        &db,
        "rate(http_requests_total[5m])",
        start_ns,
        end_ns,
        step_ns,
    ) {
        Ok(series_list) => {
            for rs in &series_list {
                print_range_series(rs);
            }
            println!("  -> {} series\n", series_list.len());
        }
        Err(e) => handle_promql_error("query_range", e),
    }

    // ---------- 5. Time/step parsing (config/CLI parity) ----------
    println!("5. Time/step parsing (same as HTTP API)");
    let parsed_time = parse_eval_time(Some("1700000100")).unwrap(); // Unix 1700000100
    let parsed_step = parse_step("15s").unwrap();
    println!("  parse_eval_time(Some(\"1700000100\")) = {} ns", parsed_time);
    println!("  parse_step(\"15s\") = {} ns\n", parsed_step);

    // ---------- 6. Labels (dimension discovery) ----------
    println!("6. Labels in range (all series)");
    match labels(&db, None::<&[String]>, start_ns, end_ns) {
        Ok(names) => {
            println!("  {:?}\n", names);
        }
        Err(e) => handle_promql_error("labels", e),
    }

    // ---------- 7. Label values: job and __name__ ----------
    println!("7. Label values: job");
    match label_values(&db, "job", None::<&[String]>, start_ns, end_ns) {
        Ok(values) => println!("  {:?}\n", values),
        Err(e) => handle_promql_error("label_values(job)", e),
    }
    println!("7b. Label values: __name__ (metric names)");
    match label_values(&db, "__name__", None::<&[String]>, start_ns, end_ns) {
        Ok(values) => println!("  {:?}\n", values),
        Err(e) => handle_promql_error("label_values(__name__)", e),
    }

    // ---------- 8. Series (label sets in range) ----------
    println!("8. Series matching http_requests_total in range");
    let match_selectors = ["http_requests_total"];
    match series(&db, &match_selectors, start_ns, end_ns) {
        Ok(metrics) => {
            for m in &metrics {
                print_metric_labels(m);
            }
            println!("  -> {} series\n", metrics.len());
        }
        Err(e) => handle_promql_error("series", e),
    }

    // ---------- 9. Error handling: distinguish Parse vs BadParameter vs Execution ----------
    println!("9. Error handling: PromqlError variants");
    let _ = query_instant(&db, "", eval_time).unwrap_err();
    let _ = query_instant(&db, "metric_a + metric_b", eval_time).unwrap_err();
    println!("  Empty query -> BadParameter; binary op -> Parse (unsupported)");

    println!("\npromql_library_demo finished successfully.");
    Ok(())
}

fn print_sample(s: &InstantSample) {
    let name = s.metric.get("__name__").map(|x| x.as_str()).unwrap_or("?");
    let mut rest: Vec<_> = s
        .metric
        .iter()
        .filter(|(k, _)| *k != "__name__")
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();
    rest.sort();
    println!("  {} {{{}}} @ {} = {}", name, rest.join(", "), s.ts_ns, s.value);
}

fn print_range_series(rs: &RangeSeries) {
    let name = rs.metric.get("__name__").map(|x| x.as_str()).unwrap_or("?");
    let mut rest: Vec<_> = rs
        .metric
        .iter()
        .filter(|(k, _)| *k != "__name__")
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();
    rest.sort();
    println!("  {} {{{}}} -> {} steps", name, rest.join(", "), rs.steps.len());
}

fn print_metric_labels(m: &MetricLabels) {
    let mut pairs: Vec<_> = m.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
    pairs.sort();
    println!("  {{{}}}", pairs.join(", "));
}

fn handle_promql_error(ctx: &str, e: PromqlError) {
    match &e {
        PromqlError::Parse(msg) => eprintln!("  {}: Parse error: {}", ctx, msg),
        PromqlError::BadParameter(msg) => eprintln!("  {}: Bad parameter: {}", ctx, msg),
        PromqlError::Execution(db_err) => eprintln!("  {}: Execution error: {}", ctx, db_err),
    }
}
