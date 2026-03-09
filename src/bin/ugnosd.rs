//! Production daemon for UGNOS time-series database.
//!
//! Loads configuration from (in precedence order): defaults, config file, environment variables
//! (`UGNOS_*`), and CLI flags. Opens the database, runs recovery, serves health/readiness
//! endpoints, and runs until graceful shutdown (SIGINT/SIGTERM).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use std::convert::Infallible;

use bytes::Bytes;
use clap::Parser;
use config::{Config, Environment, File};
use http::StatusCode;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::graceful::GracefulShutdown;
use std::collections::HashSet;
use ugnos::encoding::{BlockCompression, FloatEncoding, SegmentEncodingConfig, TagEncoding};
use ugnos::grpc::{GrpcAuthConfig, GrpcAuthKey, GrpcAuthLayer, GrpcPermission};
use ugnos::http_ops;
use ugnos::prometheus_api;
use ugnos::remote_write;
use ugnos::telemetry::noop_event_listener;
use ugnos::{DbConfig, DbCore};

// ---------- CLI ----------

/// UGNOS time-series database daemon.
#[derive(Parser, Debug)]
#[command(name = "ugnosd", version, about)]
pub struct Cli {
    /// Path to config file (TOML). If omitted, no file is loaded unless default path exists.
    #[arg(long, env = "UGNOS_CONFIG")]
    pub config: Option<PathBuf>,

    /// Override data directory (storage for WAL, snapshots, segments).
    #[arg(long, env = "UGNOS_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Do not load any config file; use defaults + env + CLI only.
    #[arg(long, default_value_t = false)]
    pub no_config: bool,

    /// Load and validate config (file + env + CLI), print effective data_dir, then exit. Does not open DB.
    #[arg(long, default_value_t = false)]
    pub validate_config: bool,

    /// HTTP listen address for health/readiness endpoints (e.g. 127.0.0.1:8080 or 0.0.0.0:8080).
    #[arg(long, env = "UGNOS_HTTP_BIND")]
    pub http_bind: Option<String>,

    /// gRPC listen address for ingest/query/administration (e.g. 127.0.0.1:50051 or 0.0.0.0:50051).
    #[arg(long, env = "UGNOS_GRPC_BIND")]
    pub grpc_bind: Option<String>,
}

// ---------- File/env config (all optional for partial config) ----------
// Encoding types (FloatEncoding, TagEncoding, BlockCompression) use serde in the library
// for TOML string/table parsing; no hand-rolled parsers.

/// Encoding section in config file. All fields optional; serde deserializes from TOML.
#[derive(Debug, Default, serde::Deserialize)]
pub struct EncodingFileConfig {
    pub float_encoding: Option<FloatEncoding>,
    pub tag_encoding: Option<TagEncoding>,
    /// Table form in TOML: `[segment_store.encoding.compression]` with `type = "none"` or `type = "zstd", level = 3`.
    pub compression: Option<BlockCompression>,
}

/// Single key entry for gRPC auth (file config). Permissions: "ingest", "query", "admin".
#[derive(Debug, Default, serde::Deserialize)]
pub struct GrpcAuthKeyFile {
    pub token: Option<String>,
    pub permissions: Option<Vec<String>>,
}

/// gRPC auth section. Deny-by-default: if absent or keys empty, all gRPC requests are denied.
#[derive(Debug, Default, serde::Deserialize)]
pub struct GrpcAuthFileConfig {
    pub keys: Option<Vec<GrpcAuthKeyFile>>,
}

fn parse_grpc_permission(s: &str) -> Option<GrpcPermission> {
    match s.trim().to_lowercase().as_str() {
        "ingest" => Some(GrpcPermission::Ingest),
        "query" => Some(GrpcPermission::Query),
        "admin" => Some(GrpcPermission::Admin),
        _ => None,
    }
}

/// Builds library GrpcAuthConfig from file config. Skips keys with missing token or empty permissions.
fn grpc_auth_config_from_file(c: &Option<GrpcAuthFileConfig>) -> GrpcAuthConfig {
    let keys = match c {
        None => return GrpcAuthConfig::default(),
        Some(cfg) => match &cfg.keys {
            None => return GrpcAuthConfig::default(),
            Some(k) => k,
        },
    };
    let auth_keys: Vec<GrpcAuthKey> = keys
        .iter()
        .filter_map(|k| {
            let token = k.token.as_deref().map(|s| s.as_bytes().to_vec())?;
            let perms: HashSet<GrpcPermission> = k
                .permissions
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter_map(|s| parse_grpc_permission(s))
                .collect();
            if perms.is_empty() {
                return None;
            }
            Some(GrpcAuthKey {
                token,
                permissions: perms,
            })
        })
        .collect();
    GrpcAuthConfig { keys: auth_keys }
}

/// Segment store section in config file.
#[derive(Debug, Default, serde::Deserialize)]
pub struct SegmentStoreFileConfig {
    pub compaction_check_interval_secs: Option<u64>,
    pub l0_compaction_trigger_segment_count: Option<usize>,
    pub l0_compaction_trigger_total_bytes: Option<u64>,
    pub l0_compaction_trigger_max_age_secs: Option<u64>,
    pub enable_tag_index: Option<bool>,
    pub encoding: Option<EncodingFileConfig>,
}

/// Top-level daemon config as read from file + env. Every field optional for layering.
#[derive(Debug, Default, serde::Deserialize)]
pub struct DaemonFileConfig {
    /// HTTP listen address for /healthz and /readyz (e.g. "127.0.0.1:8080").
    pub http_bind: Option<String>,
    /// gRPC listen address for ingest/query/administration (e.g. "127.0.0.1:50051").
    pub grpc_bind: Option<String>,
    pub data_dir: Option<String>,
    pub flush_interval_secs: Option<u64>,
    pub wal_buffer_size: Option<usize>,
    pub enable_wal: Option<bool>,
    pub enable_snapshots: Option<bool>,
    pub snapshot_interval_secs: Option<u64>,
    pub enable_segments: Option<bool>,
    pub retention_ttl_secs: Option<u64>,
    pub retention_check_interval_secs: Option<u64>,
    pub max_series_cardinality: Option<u64>,
    pub cardinality_scope_tag_key: Option<String>,
    /// Max series to scan in parallel for Prometheus API queries. When set, a dedicated pool of this size is used.
    pub query_max_parallel_series: Option<usize>,
    #[serde(rename = "segment_store")]
    pub segment_store: Option<SegmentStoreFileConfig>,
    #[serde(rename = "grpc_auth")]
    pub grpc_auth: Option<GrpcAuthFileConfig>,
    /// Bearer token required for POST /api/v1/write (Prometheus remote write). Deny-by-default: if unset, remote write returns 401.
    pub http_write_token: Option<String>,
    /// Bearer token required for GET/POST /api/v1/* (Prometheus read API). Deny-by-default: if unset, all read API requests return 401.
    pub http_read_token: Option<String>,
}

/// Runtime options for the daemon (HTTP and gRPC bind, gRPC auth, HTTP write auth) derived from config + env + CLI.
#[derive(Debug, Clone)]
pub struct DaemonOptions {
    /// Address to bind the HTTP health/readiness server.
    pub http_bind: SocketAddr,
    /// Address to bind the gRPC server.
    pub grpc_bind: SocketAddr,
    /// gRPC AuthN/AuthZ config. Deny-by-default: empty keys ⇒ all gRPC requests denied.
    pub grpc_auth: GrpcAuthConfig,
    /// If set, POST /api/v1/write requires Authorization: Bearer <this token>. If unset, remote write returns 401 (deny-by-default).
    pub http_write_token: Option<String>,
    /// If set, GET/POST /api/v1/* (Prometheus read API) requires Authorization: Bearer <this token>. If unset, returns 401 (deny-by-default).
    pub http_read_token: Option<String>,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        Self {
            http_bind: "127.0.0.1:8080".parse().expect("default http_bind"),
            grpc_bind: "127.0.0.1:50051".parse().expect("default grpc_bind"),
            grpc_auth: GrpcAuthConfig::default(),
            http_write_token: None,
            http_read_token: None,
        }
    }
}

/// Parse a bind address string to `SocketAddr`. Returns error message on failure.
fn parse_bind_addr(s: &str, name: &str) -> Result<SocketAddr, String> {
    s.parse::<SocketAddr>()
        .map_err(|e| format!("invalid {} {:?}: {}", name, s, e))
}

/// Load merged config and daemon options. CLI overrides file/env for both.
fn load_daemon_config(cli: &Cli) -> Result<(DbConfig, DaemonOptions), String> {
    let mut builder = Config::builder();

    if !cli.no_config {
        if let Some(ref path) = cli.config {
            if !path.exists() {
                return Err(format!("config file not found: {}", path.display()));
            }
            builder = builder.add_source(File::from(path.as_path()).required(false));
        } else {
            let default_path = PathBuf::from("ugnosd.toml");
            if default_path.exists() {
                builder = builder.add_source(File::from(default_path.as_path()).required(false));
            }
        }
    }

    builder = builder.add_source(
        Environment::with_prefix("UGNOS")
            .separator("__")
            .try_parsing(true)
            .ignore_empty(true),
    );

    let merged = builder.build().map_err(|e| e.to_string())?;
    let partial: DaemonFileConfig = merged.try_deserialize().map_err(|e| e.to_string())?;

    let mut db_config = DbConfig {
        event_listener: noop_event_listener(),
        ..Default::default()
    };
    merge_into_db_config(&mut db_config, &partial)?;

    if let Some(ref d) = cli.data_dir {
        db_config.data_dir = d.clone();
    }

    let http_bind_str = cli
        .http_bind
        .as_deref()
        .or(partial.http_bind.as_deref())
        .unwrap_or("127.0.0.1:8080");
    let http_bind = parse_bind_addr(http_bind_str, "http_bind")?;

    let grpc_bind_str = cli
        .grpc_bind
        .as_deref()
        .or(partial.grpc_bind.as_deref())
        .unwrap_or("127.0.0.1:50051");
    let grpc_bind = parse_bind_addr(grpc_bind_str, "grpc_bind")?;

    let grpc_auth = grpc_auth_config_from_file(&partial.grpc_auth);
    let http_write_token = partial.http_write_token;
    let http_read_token = partial.http_read_token;
    let options = DaemonOptions {
        http_bind,
        grpc_bind,
        grpc_auth,
        http_write_token,
        http_read_token,
    };
    Ok((db_config, options))
}

/// Merge file/env partial config onto `DbConfig`. Only overwrites fields that are `Some`.
fn merge_into_db_config(base: &mut DbConfig, partial: &DaemonFileConfig) -> Result<(), String> {
    if let Some(s) = &partial.data_dir {
        base.data_dir = PathBuf::from(s);
    }
    if let Some(secs) = partial.flush_interval_secs {
        base.flush_interval = Duration::from_secs(secs);
    }
    if let Some(n) = partial.wal_buffer_size {
        base.wal_buffer_size = n;
    }
    if let Some(b) = partial.enable_wal {
        base.enable_wal = b;
    }
    if let Some(b) = partial.enable_snapshots {
        base.enable_snapshots = b;
    }
    if let Some(secs) = partial.snapshot_interval_secs {
        base.snapshot_interval = Duration::from_secs(secs);
    }
    if let Some(b) = partial.enable_segments {
        base.enable_segments = b;
    }
    if let Some(secs) = partial.retention_ttl_secs {
        base.retention_ttl = Some(Duration::from_secs(secs));
    }
    if let Some(secs) = partial.retention_check_interval_secs {
        base.retention_check_interval = Duration::from_secs(secs);
    }
    if let Some(n) = partial.max_series_cardinality {
        base.max_series_cardinality = Some(n);
    }
    if let Some(s) = &partial.cardinality_scope_tag_key {
        base.cardinality_scope_tag_key = Some(s.clone());
    }
    if let Some(n) = partial.query_max_parallel_series {
        base.query_max_parallel_series = Some(n);
    }

    if let Some(ss) = &partial.segment_store {
        if let Some(secs) = ss.compaction_check_interval_secs {
            base.segment_store.compaction_check_interval = Duration::from_secs(secs);
        }
        if let Some(n) = ss.l0_compaction_trigger_segment_count {
            base.segment_store.l0_compaction_trigger_segment_count = n;
        }
        if let Some(b) = ss.l0_compaction_trigger_total_bytes {
            base.segment_store.l0_compaction_trigger_total_bytes = Some(b);
        }
        if let Some(secs) = ss.l0_compaction_trigger_max_age_secs {
            base.segment_store.l0_compaction_trigger_max_age = Some(Duration::from_secs(secs));
        }
        if let Some(b) = ss.enable_tag_index {
            base.segment_store.enable_tag_index = b;
        }
        if let Some(ref enc) = ss.encoding {
            base.segment_store.encoding = SegmentEncodingConfig {
                float_encoding: enc.float_encoding.unwrap_or(FloatEncoding::Raw64),
                tag_encoding: enc.tag_encoding.unwrap_or(TagEncoding::Dictionary),
                compression: enc.compression.unwrap_or(BlockCompression::None),
            };
        }
    }
    Ok(())
}

// ---------- Safe startup checks ----------

/// Ensures `data_dir` exists and is writable. Returns an error message on failure.
fn check_data_dir_writable(data_dir: &std::path::Path) -> Result<(), String> {
    std::fs::create_dir_all(data_dir)
        .map_err(|e| format!("data_dir {} cannot be created: {}", data_dir.display(), e))?;
    let probe = data_dir.join(".ugnos_write_probe");
    std::fs::write(&probe, b"")
        .map_err(|e| format!("data_dir {} not writable: {}", data_dir.display(), e))?;
    let _ = std::fs::remove_file(probe);
    Ok(())
}

// ---------- HTTP server (ops + Prometheus remote write) ----------

/// Shared state for the combined HTTP handler (ops + POST /api/v1/write).
struct HttpState {
    ops: std::sync::Arc<http_ops::OpsState>,
    db: std::sync::Arc<DbCore>,
    /// If set, POST /api/v1/write requires Authorization: Bearer <token>. If unset, remote write returns 401.
    http_write_token: Option<String>,
    /// If set, GET/POST /api/v1/* (Prometheus read API) requires Authorization: Bearer <token>. If unset, returns 401.
    http_read_token: Option<String>,
}

fn check_bearer(headers: &http::HeaderMap, expected: &str) -> bool {
    let auth = match headers.get(http::header::AUTHORIZATION) {
        Some(v) => v,
        None => return false,
    };
    let prefix = b"Bearer ";
    let bytes = auth.as_bytes();
    if bytes.len() < prefix.len() {
        return false;
    }
    let (head, rest) = bytes.split_at(prefix.len());
    if head != prefix {
        return false;
    }
    constant_time_eq::constant_time_eq(rest, expected.as_bytes())
}

/// Parse a `key=value&key=value` parameter string (URL query or form-encoded body).
fn parse_params_str(q: &str) -> std::collections::HashMap<String, Vec<String>> {
    let mut out: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    for pair in q.split('&') {
        let (k, v) = match pair.find('=') {
            Some(i) => {
                let k = urlencoding::decode(pair[..i].trim()).unwrap_or_default();
                let v = urlencoding::decode(pair[i + 1..].trim()).unwrap_or_default();
                (k.into_owned(), v.into_owned())
            }
            None => continue,
        };
        if !k.is_empty() {
            out.entry(k).or_default().push(v);
        }
    }
    out
}

/// Parse URI query string into key -> list of values (for match[] etc.).
fn query_params(uri: &http::Uri) -> std::collections::HashMap<String, Vec<String>> {
    match uri.query() {
        Some(q) => parse_params_str(q),
        None => std::collections::HashMap::new(),
    }
}

fn first_param(
    params: &std::collections::HashMap<String, Vec<String>>,
    key: &str,
) -> Option<String> {
    params.get(key).and_then(|v| v.first()).cloned()
}

/// Prometheus JSON error response in the standard envelope format (`{"status":"error",...}`).
fn prom_json_error(status: StatusCode, error_type: &str, error: &str) -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "status": "error",
        "errorType": error_type,
        "error": error,
    });
    Response::builder()
        .status(status)
        .header(
            http::header::CONTENT_TYPE,
            prometheus_api::PROMETHEUS_API_CONTENT_TYPE,
        )
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&body).expect("serialize error"),
        )))
        .expect("response build")
}

async fn http_service(
    state: std::sync::Arc<HttpState>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let raw_path = uri.path();
    let path = raw_path.trim_matches('/');

    let is_get = method == http::Method::GET;
    let is_post = method == http::Method::POST;

    // --- Remote write (POST /api/v1/write) --- auth via http_write_token, deny-by-default
    if is_post && path == "api/v1/write" {
        let expected = match &state.http_write_token {
            Some(t) => t.as_str(),
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Full::new(Bytes::from(
                        "remote write: auth required. Set UGNOS__HTTP_WRITE_TOKEN or http_write_token in config, then use Authorization: Bearer <token>",
                    )))
                    .expect("response build"));
            }
        };
        if !check_bearer(req.headers(), expected) {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Full::new(Bytes::from(
                        "remote write: unauthorized. Use Authorization: Bearer <token> with the configured http_write_token",
                    )))
                    .expect("response build"));
        }
        let body = match req.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Full::new(Bytes::from(format!(
                        "failed to read body: {}",
                        e
                    ))))
                    .expect("response build"));
            }
        };
        let r = remote_write::handle_remote_write(&body, &state.db);
        return Ok(Response::builder()
            .status(r.status)
            .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Full::new(Bytes::from(r.body)))
            .expect("response build"));
    }

    // --- Prometheus read API (GET/POST /api/v1/*) --- auth via http_read_token, deny-by-default
    if (is_get || is_post) && path.starts_with("api/v1/") {
        let expected = match &state.http_read_token {
            Some(t) => t.as_str(),
            None => {
                return Ok(prom_json_error(
                    StatusCode::UNAUTHORIZED,
                    "auth",
                    "authentication required. Set UGNOS__HTTP_READ_TOKEN or http_read_token in config, then use Authorization: Bearer <token>",
                ));
            }
        };
        if !check_bearer(req.headers(), expected) {
            return Ok(prom_json_error(
                StatusCode::UNAUTHORIZED,
                "auth",
                "unauthorized. Use Authorization: Bearer <token> with the configured http_read_token",
            ));
        }

        // GET: URL query params. POST: merge URL params with form-encoded body (Grafana compat).
        let params = if is_post {
            match req.into_body().collect().await {
                Ok(collected) => {
                    let mut merged = query_params(&uri);
                    for (k, vs) in parse_params_str(&String::from_utf8_lossy(&collected.to_bytes()))
                    {
                        merged.entry(k).or_default().extend(vs);
                    }
                    merged
                }
                Err(e) => {
                    return Ok(prom_json_error(
                        StatusCode::BAD_REQUEST,
                        "bad_data",
                        &format!("failed to read body: {}", e),
                    ));
                }
            }
        } else {
            query_params(&uri)
        };

        let sub = path.strip_prefix("api/v1/").unwrap_or(path);
        if sub == "query" {
            let r = prometheus_api::handle_query(
                first_param(&params, "query").as_deref(),
                first_param(&params, "time").as_deref(),
                &state.db,
            );
            return Ok(Response::builder()
                .status(r.status)
                .header(
                    http::header::CONTENT_TYPE,
                    prometheus_api::PROMETHEUS_API_CONTENT_TYPE,
                )
                .body(Full::new(Bytes::from(r.body)))
                .expect("response build"));
        }
        if sub == "query_range" {
            let r = prometheus_api::handle_query_range(
                first_param(&params, "query").as_deref(),
                first_param(&params, "start").as_deref(),
                first_param(&params, "end").as_deref(),
                first_param(&params, "step").as_deref(),
                &state.db,
            );
            return Ok(Response::builder()
                .status(r.status)
                .header(
                    http::header::CONTENT_TYPE,
                    prometheus_api::PROMETHEUS_API_CONTENT_TYPE,
                )
                .body(Full::new(Bytes::from(r.body)))
                .expect("response build"));
        }
        if sub == "labels" {
            let match_list: Vec<String> = params.get("match[]").cloned().unwrap_or_default();
            let r = prometheus_api::handle_labels(
                &match_list,
                first_param(&params, "start").as_deref(),
                first_param(&params, "end").as_deref(),
                &state.db,
            );
            return Ok(Response::builder()
                .status(r.status)
                .header(
                    http::header::CONTENT_TYPE,
                    prometheus_api::PROMETHEUS_API_CONTENT_TYPE,
                )
                .body(Full::new(Bytes::from(r.body)))
                .expect("response build"));
        }
        if sub.starts_with("label/") && sub.ends_with("/values") {
            let name = sub
                .strip_prefix("label/")
                .and_then(|s| s.strip_suffix("/values"))
                .unwrap_or("");
            let r = prometheus_api::handle_label_values(name, &state.db);
            return Ok(Response::builder()
                .status(r.status)
                .header(
                    http::header::CONTENT_TYPE,
                    prometheus_api::PROMETHEUS_API_CONTENT_TYPE,
                )
                .body(Full::new(Bytes::from(r.body)))
                .expect("response build"));
        }
        if sub == "series" {
            let match_list: Vec<String> = params.get("match[]").cloned().unwrap_or_default();
            let r = prometheus_api::handle_series(
                &match_list,
                first_param(&params, "start").as_deref(),
                first_param(&params, "end").as_deref(),
                &state.db,
            );
            return Ok(Response::builder()
                .status(r.status)
                .header(
                    http::header::CONTENT_TYPE,
                    prometheus_api::PROMETHEUS_API_CONTENT_TYPE,
                )
                .body(Full::new(Bytes::from(r.body)))
                .expect("response build"));
        }

        return Ok(prom_json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("unknown API path: /api/v1/{}", sub),
        ));
    }

    // --- Root: home document (GET/HEAD /) ---
    if path.is_empty() && (is_get || method == http::Method::HEAD) {
        const HOME_HTML: &str = include_str!("../../static/index.html");
        let body = if method == http::Method::HEAD {
            Bytes::new()
        } else {
            Bytes::from(HOME_HTML.replace("{{VERSION}}", env!("CARGO_PKG_VERSION")))
        };
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Full::new(body))
            .expect("response build"));
    }

    // --- Ops endpoints (no auth – infrastructure probes for Kubernetes liveness/readiness) ---
    let response = http_ops::handle_ops_request(&method, raw_path, state.ops.is_ready());
    Ok(Response::builder()
        .status(response.status)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(response.body)))
        .expect("response build"))
}

/// Returns a future that completes when SIGINT (Ctrl+C) or SIGTERM is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("ctrl_c handler");
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

/// Runs the daemon: spawns the gRPC server and the ops HTTP server, waits for shutdown signal,
/// then flushes the DB and drops it (triggering WAL flush and compaction stop).
/// On shutdown, readiness is set to false so /readyz returns 503 and orchestrators drain traffic.
async fn run_with_health_server(
    db: std::sync::Arc<DbCore>,
    options: DaemonOptions,
    ops_state: std::sync::Arc<http_ops::OpsState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(options.http_bind)
        .await
        .map_err(|e| format!("failed to bind {}: {}", options.http_bind, e))?;
    eprintln!(
        "ugnosd running (data_dir={}, http={}, grpc={}). Press Ctrl+C or send SIGTERM to stop.",
        db.get_config().data_dir.display(),
        options.http_bind,
        options.grpc_bind
    );

    let server = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());

    let http_state = std::sync::Arc::new(HttpState {
        ops: ops_state.clone(),
        db: db.clone(),
        http_write_token: options.http_write_token.clone(),
        http_read_token: options.http_read_token.clone(),
    });

    loop {
        tokio::select! {
            Ok((stream, _addr)) = listener.accept() => {
                let io = TokioIo::new(Box::pin(stream));
                let state = std::sync::Arc::clone(&http_state);
                let conn = server.serve_connection_with_upgrades(io, service_fn(move |req| {
                    let state = std::sync::Arc::clone(&state);
                    async move { http_service(state, req).await }
                }));
                let fut = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        eprintln!("ops connection error: {:?}", e);
                    }
                });
            }
            _ = &mut shutdown => {
                eprintln!("shutdown signal received");
                ops_state.set_ready(false);
                break;
            }
        }
    }

    drop(listener);
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
    tokio::select! {
        _ = graceful.shutdown() => {
            eprintln!("all health connections closed");
        }
        _ = tokio::time::sleep(SHUTDOWN_TIMEOUT) => {
            eprintln!("shutdown timeout waiting for connections");
        }
    }

    eprintln!("flushing database and closing WAL...");
    if let Err(e) = db.flush() {
        eprintln!("flush during shutdown: {}", e);
    }
    drop(db);
    eprintln!("shutdown complete");
    Ok(())
}

/// Serves the gRPC UgnosService on the given address. Auth layer is always applied (deny-by-default).
async fn serve_grpc(
    db: std::sync::Arc<DbCore>,
    addr: SocketAddr,
    grpc_auth: GrpcAuthConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let svc = ugnos::grpc::UgnosServiceServer::new(ugnos::grpc::UgnosServiceImpl::new(db));
    let auth_layer = GrpcAuthLayer::new(grpc_auth);
    tonic::transport::Server::builder()
        .layer(auth_layer)
        .serve(addr, svc)
        .await
        .map_err(|e| format!("gRPC server error: {}", e))?;
    Ok(())
}

// ---------- Main ----------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    let (db_config, options) = load_daemon_config(&cli).map_err(|e| {
        eprintln!("config error: {}", e);
        e
    })?;

    if cli.validate_config {
        println!("data_dir={}", db_config.data_dir.display());
        println!("flush_interval_secs={}", db_config.flush_interval.as_secs());
        println!("enable_wal={}", db_config.enable_wal);
        println!("enable_segments={}", db_config.enable_segments);
        println!("http_bind={}", options.http_bind);
        println!("grpc_bind={}", options.grpc_bind);
        return Ok(());
    }

    // Safe startup: data_dir must exist and be writable before opening DB.
    check_data_dir_writable(&db_config.data_dir).map_err(|e| {
        eprintln!("startup check failed: {}", e);
        e
    })?;

    let mut db = DbCore::with_config(db_config).map_err(|e| {
        eprintln!("failed to open database: {}", e);
        e
    })?;
    db.recover().map_err(|e| {
        eprintln!("recovery failed: {}", e);
        e
    })?;

    let db = std::sync::Arc::new(db);
    let db_grpc = std::sync::Arc::clone(&db);
    let grpc_addr = options.grpc_bind;
    let grpc_auth = options.grpc_auth.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_grpc(db_grpc, grpc_addr, grpc_auth).await {
            eprintln!("gRPC server: {}", e);
        }
    });

    let ops_state = std::sync::Arc::new(http_ops::OpsState::new(true));
    run_with_health_server(db, options, ops_state).await
}
