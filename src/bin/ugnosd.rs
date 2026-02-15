//! Production daemon for UGNOS time-series database.
//!
//! Loads configuration from (in precedence order): defaults, config file, environment variables
//! (`UGNOS_*`), and CLI flags. Opens the database, runs recovery, serves health/readiness
//! endpoints, and runs until graceful shutdown (SIGINT/SIGTERM).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use std::convert::Infallible;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use clap::Parser;
use config::{Config, Environment, File};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::graceful::GracefulShutdown;
use ugnos::encoding::{BlockCompression, FloatEncoding, SegmentEncodingConfig, TagEncoding};
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
    #[serde(rename = "segment_store")]
    pub segment_store: Option<SegmentStoreFileConfig>,
}

/// Runtime options for the daemon (HTTP bind, etc.) derived from config + env + CLI.
#[derive(Debug, Clone)]
pub struct DaemonOptions {
    /// Address to bind the HTTP health/readiness server.
    pub http_bind: SocketAddr,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        Self {
            http_bind: "127.0.0.1:8080".parse().expect("default http_bind"),
        }
    }
}

/// Parse `http_bind` string to `SocketAddr`. Returns error message on failure.
fn parse_http_bind(s: &str) -> Result<SocketAddr, String> {
    s.parse::<SocketAddr>()
        .map_err(|e| format!("invalid http_bind {:?}: {}", s, e))
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

    let mut db_config = DbConfig::default();
    db_config.event_listener = noop_event_listener();
    merge_into_db_config(&mut db_config, &partial)?;

    if let Some(ref d) = cli.data_dir {
        db_config.data_dir = d.clone();
    }

    let http_bind_str = cli
        .http_bind
        .as_deref()
        .or(partial.http_bind.as_deref())
        .unwrap_or("127.0.0.1:8080");
    let http_bind = parse_http_bind(http_bind_str)?;

    let options = DaemonOptions { http_bind };
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

// ---------- Health HTTP server ----------

/// Shared state for the health service (readiness flag only).
struct HealthState {
    ready: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

async fn health_service(
    state: std::sync::Arc<HealthState>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (status, body) = match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/healthz") => {
            // Liveness: process is alive and responding.
            (hyper::StatusCode::OK, Bytes::from("ok"))
        }
        (&hyper::Method::GET, "/readyz") => {
            // Readiness: DB opened and recovered; safe to send traffic.
            if state.ready.load(Ordering::Acquire) {
                (hyper::StatusCode::OK, Bytes::from("ok"))
            } else {
                (
                    hyper::StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from("not ready"),
                )
            }
        }
        _ => (hyper::StatusCode::NOT_FOUND, Bytes::from("not found")),
    };
    Ok(Response::builder()
        .status(status)
        .body(Full::new(body))
        .expect("response build"))
}

/// Returns a future that completes when SIGINT (Ctrl+C) or SIGTERM is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("ctrl_c handler");
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

/// Runs the daemon: spawns the health HTTP server, waits for shutdown signal, then
/// flushes the DB and drops it (triggering WAL flush and compaction stop).
async fn run_with_health_server(
    db: DbCore,
    options: DaemonOptions,
    ready: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(options.http_bind)
        .await
        .map_err(|e| format!("failed to bind {}: {}", options.http_bind, e))?;
    eprintln!(
        "ugnosd running (data_dir={}, http={}). Press Ctrl+C or send SIGTERM to stop.",
        db.get_config().data_dir.display(),
        options.http_bind
    );

    let state = std::sync::Arc::new(HealthState { ready });
    let server = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());

    loop {
        tokio::select! {
            Ok((stream, _addr)) = listener.accept() => {
                let io = TokioIo::new(Box::pin(stream));
                let state = std::sync::Arc::clone(&state);
                let conn = server.serve_connection_with_upgrades(io, service_fn(move |req| {
                    let state = std::sync::Arc::clone(&state);
                    async move { health_service(state, req).await }
                }));
                let fut = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        eprintln!("health connection error: {:?}", e);
                    }
                });
            }
            _ = &mut shutdown => {
                eprintln!("shutdown signal received");
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

    let ready = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    run_with_health_server(db, options, ready).await
}
