//! Prometheus HTTP read API client demo: query, query_range, labels, series.
//!
//! Exercises the Prometheus-compatible read endpoints exposed by ugnosd. Use this
//! to verify connectivity, inspect JSON response shape, or as a reference for
//! building dashboards and scripts.
//!
//! **This example requires a running ugnosd with data.** Running
//! `cargo run --example prometheus_api_client_demo` alone will fail with connection
//! refused. Use the script instead:
//!
//! ```ignore
//! ./scripts/run-prometheus-api-client-demo.sh
//! ```
//!
//! The script starts ugnosd (with auth and `max_series_cardinality`), writes
//! sample data via `gen_minimal_write_request` → POST `/api/v1/write`, waits for
//! flush, then runs this demo with the correct env vars.
//!
//! **Grafana datasource**
//!
//! Add a Prometheus-type datasource with URL `http://<ugnosd-host>:8080`. If auth
//! is enabled, configure "Custom HTTP Headers" → `Authorization: Bearer <token>`.
//! Grafana uses `/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels`, and
//! `/api/v1/label/<name>/values` for panels and variables. This demo mirrors
//! those calls.
//!
//! **Manual run** (when ugnosd is already running with data):
//!
//! ```ignore
//! UGNOS_API_BASE=http://127.0.0.1:8080 UGNOS__HTTP_READ_TOKEN=<token> \
//!   cargo run --example prometheus_api_client_demo
//! ```

use std::collections::HashMap;
use std::env;

use serde::Deserialize;
use ureq::Agent;

/// Prometheus API envelope: `{"status":"success","data":...}` or `{"status":"error","error":"..."}`
#[derive(Debug, Deserialize)]
struct Envelope<T> {
    status: String,
    data: Option<T>,
    #[serde(rename = "errorType")]
    error_type: Option<String>,
    error: Option<String>,
}

/// Query response data for instant (vector) or range (matrix).
#[derive(Debug, Deserialize)]
struct QueryData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: QueryResult,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum QueryResult {
    Vector(Vec<VectorSample>),
    Matrix(Vec<MatrixSeries>),
}

#[derive(Debug, Deserialize)]
struct VectorSample {
    metric: HashMap<String, String>,
    value: [serde_json::Value; 2],
}

#[derive(Debug, Deserialize)]
struct MatrixSeries {
    metric: HashMap<String, String>,
    values: Vec<[serde_json::Value; 2]>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = env::var("UGNOS_API_BASE").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
    let token = env::var("UGNOS__HTTP_READ_TOKEN").ok();
    let agent = Agent::new();

    let req = |path: &str| -> Result<ureq::Response, ureq::Error> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let req = agent.get(&url);
        let req = if let Some(ref t) = token {
            req.set("Authorization", &format!("Bearer {}", t))
        } else {
            req
        };
        req.call()
    };

    println!("Prometheus API client demo — base: {}", base);
    if token.is_some() {
        println!("  (using UGNOS__HTTP_READ_TOKEN for auth)");
    }
    println!();

    // --- /api/v1/query (instant) ---
    let query = "auth_verify_metric";
    let time = "2";
    let path = format!("/api/v1/query?query={}&time={}", urlencoding::encode(query), time);
    println!("GET /api/v1/query?query={}&time={}", query, time);
    match req(&path) {
        Ok(resp) => {
            let status = resp.status();
            let body: String = resp.into_string()?;
            if status != 200 {
                println!("  HTTP {} — {}", status, body.trim());
            } else if let Ok(env) = serde_json::from_str::<Envelope<QueryData>>(&body) {
                if env.status == "success" {
                    if let Some(data) = env.data {
                        println!("  resultType: {}", data.result_type);
                        match data.result {
                            QueryResult::Vector(vec) => {
                                for s in &vec {
                                    println!("    {:?} @ [{}, {}]",
                                        s.metric,
                                        s.value[0],
                                        s.value[1]);
                                }
                                if vec.is_empty() {
                                    println!("    (no samples; ensure data was written and flushed)");
                                }
                            }
                            QueryResult::Matrix(mat) => {
                                for s in mat {
                                    println!("    {:?}: {} points",
                                        s.metric,
                                        s.values.len());
                                }
                            }
                        }
                    }
                } else {
                    println!("  status: {}, errorType: {:?}, error: {:?}",
                        env.status, env.error_type, env.error);
                }
            } else {
                println!("  (raw) {}", if body.len() > 200 { &body[..200] } else { &body });
            }
        }
        Err(e) => println!("  error: {}", e),
    }
    println!();

    // --- /api/v1/query_range ---
    let path = "/api/v1/query_range?query=auth_verify_metric&start=0&end=10&step=1";
    println!("GET {}", path);
    match req(path) {
        Ok(resp) => {
            let status = resp.status();
            let body: String = resp.into_string()?;
            if status != 200 {
                println!("  HTTP {} — {}", status, body.trim());
            } else if let Ok(env) = serde_json::from_str::<Envelope<QueryData>>(&body) {
                if env.status == "success" {
                    if let Some(data) = env.data {
                        println!("  resultType: {}", data.result_type);
                        match data.result {
                            QueryResult::Vector(_) => {}
                            QueryResult::Matrix(mat) => {
                                for s in &mat {
                                    println!("    {:?}: {} samples",
                                        s.metric,
                                        s.values.len());
                                    for (i, [ts, v]) in s.values.iter().enumerate().take(3) {
                                        println!("      [{}] {} = {}", i, ts, v);
                                    }
                                    if s.values.len() > 3 {
                                        println!("      ... and {} more", s.values.len() - 3);
                                    }
                                }
                                if mat.is_empty() {
                                    println!("    (empty matrix)");
                                }
                            }
                        }
                    }
                } else {
                    println!("  status: {}, errorType: {:?}, error: {:?}",
                        env.status, env.error_type, env.error);
                }
            } else {
                println!("  (raw) {}", if body.len() > 150 { &body[..150] } else { &body });
            }
        }
        Err(e) => println!("  error: {}", e),
    }
    println!();

    // --- /api/v1/labels ---
    println!("GET /api/v1/labels");
    match req("/api/v1/labels") {
        Ok(resp) => {
            let status = resp.status();
            let body: String = resp.into_string()?;
            if status != 200 {
                println!("  HTTP {} — {}", status, body.trim());
            } else if let Ok(env) = serde_json::from_str::<Envelope<Vec<String>>>(&body) {
                if env.status == "success" {
                    if let Some(labels) = env.data {
                        println!("  labels ({}): {:?}", labels.len(), labels);
                    }
                } else {
                    println!("  status: {}, errorType: {:?}, error: {:?}",
                        env.status, env.error_type, env.error);
                }
            } else {
                println!("  (raw) {}", if body.len() > 150 { &body[..150] } else { &body });
            }
        }
        Err(e) => println!("  error: {}", e),
    }
    println!();

    // --- /api/v1/label/__name__/values ---
    println!("GET /api/v1/label/__name__/values");
    match req("/api/v1/label/__name__/values") {
        Ok(resp) => {
            let status = resp.status();
            let body: String = resp.into_string()?;
            if status != 200 {
                println!("  HTTP {} — {}", status, body.trim());
            } else if let Ok(env) = serde_json::from_str::<Envelope<Vec<String>>>(&body) {
                if env.status == "success" {
                    if let Some(names) = env.data {
                        println!("  metric names ({}): {:?}", names.len(), names);
                    }
                } else {
                    println!("  status: {}, errorType: {:?}, error: {:?}",
                        env.status, env.error_type, env.error);
                }
            } else {
                println!("  (raw) {}", if body.len() > 150 { &body[..150] } else { &body });
            }
        }
        Err(e) => println!("  error: {}", e),
    }
    println!();

    // --- /api/v1/series?match[]=... ---
    let match_q = urlencoding::encode("auth_verify_metric");
    let path = format!("/api/v1/series?match[]={}", match_q);
    println!("GET /api/v1/series?match[]=auth_verify_metric");
    match req(&path) {
        Ok(resp) => {
            let status = resp.status();
            let body: String = resp.into_string()?;
            if status != 200 {
                println!("  HTTP {} — {}", status, body.trim());
            } else if let Ok(env) = serde_json::from_str::<Envelope<Vec<HashMap<String, String>>>>(&body) {
                if env.status == "success" {
                    if let Some(series) = env.data {
                        println!("  series ({}):", series.len());
                        for s in &series {
                            println!("    {:?}", s);
                        }
                        if series.is_empty() {
                            println!("    (no series matched; ensure data exists and max_series_cardinality is set)");
                        }
                    }
                } else {
                    println!("  status: {}, errorType: {:?}, error: {:?}",
                        env.status, env.error_type, env.error);
                }
            } else {
                println!("  (raw) {}", if body.len() > 150 { &body[..150] } else { &body });
            }
        }
        Err(e) => println!("  error: {}", e),
    }

    Ok(())
}
