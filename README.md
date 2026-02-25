# ugnos: Concurrent Time-Series Database Core in Rust

`ugnos` is a concurrent, embeddable **time-series storage + query engine** designed for durability and high-throughput ingest in Rust services. The **ugnosd** daemon exposes Prometheus-compatible HTTP APIs (query, Remote Write) and optional gRPC with auth.

For project goals and long-term architecture, see the [whitepaper](Ugnos_Concurrent_Time-Series_Database_Core_Whitepaper.md). Current release focus: **Milestone 2** — query engine + APIs (see [CHANGELOG](CHANGELOG.md) for 0.4.x and 0.5.0).

For latest changelog and version history, see the [CHANGELOG](CHANGELOG.md).

## What this is / what this is not (yet)

This crate is a **library-grade database core** intended to be embedded into a Rust process (service/agent/daemon). The **ugnosd** daemon exposes it as a networked server (HTTP + gRPC) with auth and Prometheus-compatible APIs.

- **This is**:
  - An embeddable time-series ingest + query core with WAL/snapshots/segments (SST-like) and a structured event hook.
  - A production daemon (`ugnosd`) with HTTP ops (liveness/readiness), Prometheus Remote Write, Prometheus HTTP API (instant/range query, labels, series), optional gRPC, and deny-by-default AuthN/AuthZ.
  - A **PromQL-like** query surface (vector/range selectors, label matchers, window functions, aggregations) with vectorized execution and Grafana-compatible Prometheus datasource.
  - Suitable for single-process or single-node deployment where you own deployment, IO, and operational integration.
- **This is not (yet)**:
  - A distributed system (no replication, consensus, sharding across nodes).
  - A full SQL layer (queries are PromQL-like and programmatic APIs).
  - A turnkey operational product (no built-in backup orchestration, migrations tooling, or admin UI).

## Features (today)

- **Concurrent ingest**: sharded write buffering + background flush thread.
- **Durable persistence**:
  - **WAL** with **explicit format versioning** and **per-record CRC32** checksums.
  - **Snapshots** with **explicit format versioning**, **payload CRC32**, and **atomic install** (temp + rename + fsync).
- **On-disk segment engine (SST-like)**:
  - Immutable segment files with per-series columnar blocks; **block-level checksums** and **versioning** (v2 header with payload CRC32 and version field).
  - **Timestamp delta encoding** (varint) for series blocks; configurable float encoding strategies (**Raw64**, **GorillaXor**).
  - **Tag dictionary encoding**; optional per-block compression (**LZ4**, **Zstd** with configurable level).
  - **Time index** per segment/block (time-range via binary search); **tag index** (inverted index with Roaring bitmaps) for tag filters without full scans.
  - Atomic manifest (`MANIFEST.bin`) tracking active segments and retention watermark.
  - Background compaction (L0 → L1 merge) with safe concurrent reads.
- **Indexing & cardinality**:
  - Tag filters use the tag index (bitmap intersection); configurable **series cardinality** hard limit per scope with explicit error and metrics.
- **Retention/TTL**:
  - Immediate logical deletion via tombstone watermark.
  - Physical removal via compaction guarantees.
- **Query & APIs** (daemon / library):
  - **PromQL-like** query surface: vector/range selectors, label matchers (`=`, `!=`, `=~`, `!~`), window functions (`rate`, `increase`, `avg_over_time`, …), aggregations with grouping (`sum by`, `avg without`, …); IEEE 754 semantics (NaN propagation, min/max special case).
  - Query planner with `explain()`; vectorized execution with optional parallelism cap (`query_max_parallel_series`).
  - **Prometheus HTTP API v1**: `GET /api/v1/query`, `GET /api/v1/query_range`, `GET /api/v1/labels`, `GET /api/v1/label/<name>/values`, `GET /api/v1/series` (Grafana Prometheus datasource compatible).
  - **Prometheus Remote Write**: `POST /api/v1/write` (Snappy-compressed protobuf); backpressure/cardinality limit → 429 and metrics.
  - **gRPC** (Tonic) for ingest/query/admin; **AuthN/AuthZ** deny-by-default for HTTP and gRPC (e.g. `http_write_token`, gRPC auth).
- **Observability hooks**:
  - No stdout logging in core hot paths.
  - Structured `DbEvent` stream via `DbConfig.event_listener`.
- **Bench suite** reports segment size per encoding configuration; tests assert p99 query latency within target for encoded/compressed segments.
- **Acceptance and break-it tests** covering format layout validation, checksum/version enforcement, roundtrip correctness, and corruption detection.

## Data layout on disk

All persistence lives under `DbConfig.data_dir`:

- `wal/`
  - `wal.log` (current WAL)
  - `wal_*.log` (rotated WAL segments; may exist briefly)
- `snapshots/`
  - `snapshot_<timestamp>.bin` (atomic, checksummed snapshots)
- `engine/segments/`
  - `MANIFEST.bin` (atomic + checksummed)
  - `seg_<id>_l0.seg`, `seg_<id>_l1.seg`, ...

## Core API notes

- `DbCore::flush()` **blocks until the flush is complete**.
- `DbCore::snapshot()` **blocks until the snapshot is written** (when enabled).
- With segments enabled, `DbCore::recover()`:
  - Uses segment max-seq to replay only the WAL tail.
  - Truncates `wal.log` back to just the header (bounded restart cost).
- Query results are not guaranteed to be globally sorted across multiple segments; **sort by timestamp if you need ordering**.

## Configuration

`DbConfig` is intended to be explicit and production-friendly:

```rust,no_run
use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore};

use ugnos::encoding::{
  BlockCompression::Zstd, 
  FloatEncoding::GorillaXor,
};

let dir = TempDir::new().unwrap();
let mut cfg = DbConfig::default();
cfg.data_dir = PathBuf::from(dir.path());

// Durability toggles
cfg.enable_wal = true;
cfg.enable_snapshots = true;
cfg.enable_segments = true; // segment engine + compaction + retention

// Tuning
cfg.wal_buffer_size = 1_000;
cfg.flush_interval = Duration::from_millis(250);
cfg.snapshot_interval = Duration::from_secs(60 * 15);

// Retention (optional): 
// makes data older than now - ttl invisible, 
// and compaction reclaims disk.
cfg.retention_ttl = Some(Duration::from_secs(60 * 60 * 24 * 7));
cfg.retention_check_interval = Duration::from_secs(1);

// Encoding & compression (series blocks in segments): 
// float (Raw64 | GorillaXor), tag dictionary, LZ4/Zstd.
cfg.segment_store.encoding.float_encoding = GorillaXor;
cfg.segment_store.encoding.compression = Zstd { level: 3 };

// Cardinality (optional): 
// hard limit for distinct series per scope; 
// scope is derived from tags[cardinality_scope_tag_key].
// When exceeded, insert returns DbError::SeriesCardinalityLimitExceeded 
// and metrics:
// - ugnos_cardinality_limit_rejections
// - ugnos_series_cardinality
// cfg.max_series_cardinality = Some(100);
// cfg.cardinality_scope_tag_key = Some("tenant".to_string());

let mut db = DbCore::with_config(cfg).unwrap();
db.recover().unwrap();
```

## Observability (event hook)

Core emits structured events via `DbConfig.event_listener`. With cardinality limits enabled, telemetry exposes `ugnos_cardinality_limit_rejections` and `ugnos_series_cardinality` (when using the Prometheus recorder).

```rust,no_run
use std::sync::{Arc, Mutex};
use ugnos::{DbEvent, DbEventListener};

#[derive(Debug)]
struct MemoryEvents(Arc<Mutex<Vec<DbEvent>>>);

impl DbEventListener for MemoryEvents {
    fn on_event(&self, event: DbEvent) {
        self.0.lock().unwrap().push(event);
    }
}
```

## Basic usage

```rust,no_run
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, DbEvent, DbEventListener, TagSet};

#[derive(Debug)]
struct MemoryEvents(Arc<Mutex<Vec<DbEvent>>>);

impl DbEventListener for MemoryEvents {
    fn on_event(&self, event: DbEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn main() -> Result<(), ugnos::DbError> {
    let events = Arc::new(Mutex::new(Vec::new()));

    let dir = TempDir::new().unwrap();
    let mut cfg = DbConfig::default();
    cfg.data_dir = PathBuf::from(dir.path());
    cfg.enable_wal = true;
    cfg.enable_snapshots = true;
    cfg.enable_segments = true;
    cfg.retention_ttl = Some(
      Duration::from_secs(60 * 60 * 24 * 30) // 30 days
    );
    cfg.event_listener = Arc::new(MemoryEvents(events.clone()));

    let mut db = DbCore::with_config(cfg)?;
    db.recover()?;

    let mut tags = TagSet::new();
    tags.insert("host".to_string(), "server1".to_string());
    tags.insert("region".to_string(), "us-east".to_string());

    db.insert("cpu_usage", 100, 0.75, tags.clone())?;
    db.insert("cpu_usage", 200, 0.80, tags.clone())?;
    db.flush()?; // blocks until durable

    let mut results = db.query(
      "cpu_usage", 0..u64::MAX, Some(&tags))?;
    results.sort_by_key(|(ts, _)| *ts);
    assert_eq!(results.len(), 2);

    Ok(())
}
```

## Daemon (ugnosd)

The `ugnosd` binary runs UGNOS as a production daemon. Configuration is layered (later overrides earlier):

1. **Defaults** — built-in `DbConfig` defaults  
2. **Config file** — TOML at `--config <path>` or, if omitted, `ugnosd.toml` in the current directory (if present)  
3. **Environment** — `UGNOS__*` variables (e.g. `UGNOS__DATA_DIR`, `UGNOS__HTTP_BIND`, `UGNOS__HTTP_WRITE_TOKEN`, `UGNOS__HTTP_READ_TOKEN`, `UGNOS__SEGMENT_STORE__COMPACTION_CHECK_INTERVAL_SECS`; use `__` for nested keys)  
4. **CLI** — `--config`, `--data-dir`, `--http-bind`, `--no-config`, `--validate-config`

**HTTP auth (deny-by-default):** When `http_write_token` or `http_read_token` is unset, the corresponding endpoints return 401. To enable auth: set `http_write_token` and/or `http_read_token` in TOML, or set `UGNOS__HTTP_WRITE_TOKEN` and/or `UGNOS__HTTP_READ_TOKEN` in the environment. Clients must send `Authorization: Bearer <token>`. `http_write_token` protects POST `/api/v1/write` (Remote Write); `http_read_token` protects the Prometheus read API (query, query_range, labels, series).

**Safe startup:** Before opening the database, the daemon checks that `data_dir` exists (creates it if missing) and is writable. If config is invalid, the data directory is unusable, or recovery fails, the process exits with a non-zero status and an error message.

**HTTP endpoints:** The daemon serves on the address given by `http_bind` (default `127.0.0.1:8080`; use `0.0.0.0:8080` for Docker/Kubernetes):

- **Ops:** **`GET /healthz`** (liveness), **`GET /readyz`** (readiness). Readiness is 200 after DB open and recovery; 503 otherwise.
- **Prometheus:** **`POST /api/v1/write`** (Remote Write), **`GET /api/v1/query`**, **`GET /api/v1/query_range`**, **`GET /api/v1/labels`**, **`GET /api/v1/label/<name>/values`**, **`GET /api/v1/series`**. Auth is deny-by-default when `http_write_token` (or gRPC auth) is configured.

**Graceful shutdown:** On SIGINT (Ctrl+C) or SIGTERM, the daemon stops accepting new HTTP connections, waits for in-flight requests to finish (up to 30s), flushes the database buffer, then sends shutdown to the background flush thread (which performs a final flush and closes the WAL). The segment store’s compaction loop is stopped when the process exits. This guarantees WAL flush and a clean compaction stop as per the acceptance criteria.

Example:

```bash
cargo build --release
./target/release/ugnosd --data-dir /var/lib/ugnos
# Bind health server to all interfaces (e.g. for containers):
./target/release/ugnosd --data-dir /var/lib/ugnos --http-bind 0.0.0.0:8080
# Or validate config without starting the DB:
./target/release/ugnosd --validate-config --config /etc/ugnosd.toml
```

See `ugnosd.toml.example` for a full TOML template.

### Deployment recipes (single artifact per platform)

The build produces a **single static-ish binary per platform** (no separate runtime or config artifacts required):

```bash
cargo build --release --bin ugnosd
# Artifact: target/release/ugnosd
```

- **systemd:** Copy `deploy/systemd/ugnosd.service` to `/etc/systemd/system/`, create user `ugnos`, install the binary to `/usr/local/bin/ugnosd`, set `data_dir` (e.g. `/var/lib/ugnos`) and optionally `--config /etc/ugnosd.toml`. Then `systemctl daemon-reload && systemctl enable --now ugnosd`. Use `TimeoutStopSec=35` so SIGTERM allows WAL flush before kill.
- **Docker:** See [Docker](#docker) below; image runs `ugnosd` as PID 1 with config and env overrides.
- **Kubernetes:** Use the manifests under `deploy/k8s/` (Deployment, Service, optional ConfigMap). Set `livenessProbe` to `GET /healthz` and `readinessProbe` to `GET /readyz` on port 8080; give the pod a `terminationGracePeriodSeconds` of at least 35 so graceful shutdown can flush WAL.

## Docker

Build and run `ugnosd` in a container for local evaluation or deployment. The image runs as a non-root user, exposes port 8080 for `/healthz` and `/readyz`, and uses exec-form entrypoint so the daemon is PID 1 and receives SIGTERM for graceful shutdown (WAL flush, compaction stop).

### Build

From the **ugnos** project root:

```bash
docker build -t ugnosd:latest .
```

The image uses Rust 1.93 by default (edition 2024 requires 1.85+). To pin a different version:

```bash
docker build --build-arg RUST_VERSION=1.93 -t ugnosd:latest .
```

### Run (standalone)

Default: data in a named volume, health server on `0.0.0.0:8080`:

```bash
docker run -d --name ugnosd -p 8080:8080 -v ugnos_data:/var/lib/ugnos ugnosd:latest
curl -s http://localhost:8080/healthz
curl -s http://localhost:8080/readyz
```

With a config file (mount TOML and optional env overrides):

```bash
docker run -d --name ugnosd -p 8080:8080 \
  -v ugnos_data:/var/lib/ugnos \
  -v /path/to/ugnosd.toml:/etc/ugnosd.toml:ro \
  -e UGNOS__HTTP_BIND=0.0.0.0:8080 \
  -e UGNOS__HTTP_WRITE_TOKEN=secret \
  -e UGNOS__HTTP_READ_TOKEN=secret \
  ugnosd:latest --config /etc/ugnosd.toml
```

Give the daemon time to shut down cleanly (default Docker stop timeout is 10s; the daemon may need up to 30s to drain connections and flush WAL):

```bash
docker stop -t 35 ugnosd
```

### Quickstart with Docker Compose

From the **ugnos** project root:

```bash
docker compose up -d
curl -s http://localhost:8080/healthz
curl -s http://localhost:8080/readyz
docker compose down
```

Compose defines a healthcheck and `stop_grace_period: 35s` so `docker compose down` sends SIGTERM and allows the daemon to flush before exit.

### Publish to a registry

Tag and push to your registry (e.g. GitHub Container Registry or Docker Hub):

```bash
# Example: GHCR
docker tag ugnosd:latest ghcr.io/YOUR_ORG/ugnosd:0.5.0
docker push ghcr.io/YOUR_ORG/ugnosd:0.5.0

# Example: Docker Hub
docker tag ugnosd:latest YOUR_USER/ugnosd:0.5.0
docker push YOUR_USER/ugnosd:0.5.0
```

### Verification (adversarial)

- **Liveness/readiness:** After `docker compose up -d`, `GET /healthz` and `GET /readyz` must return 200. If readiness returns 503, the DB may still be recovering; wait a few seconds.
- **Graceful shutdown:** Run the container, write data (when ingest APIs exist), then `docker compose down` or `docker stop -t 35 ugnosd`. Container should exit 0; on next start, data should still be present (persistence across restart).
- **Invalid config:** Override with invalid `UGNOS_HTTP_BIND` (e.g. `not-a-host`) and confirm the container exits non-zero and does not serve traffic.

From the project root you can run an automated verification script (builds image, brings up compose, checks health endpoints, asserts invalid config fails, then tears down):

```bash
./scripts/verify-docker.sh
```

## How to build and test (this workspace)

From the **ugnos** project root:

```bash
cargo build --release
cargo test
```

## Examples

The [`examples/`](examples/) folder contains runnable demos:

| Example | What it demonstrates |
|---------|----------------------|
| `persistence_demo` | DbConfig, insert, flush, snapshot, query, recover; restart flow |
| `encoding_compression_demo` | Segment encoding (GorillaXor, Zstd), TagSet, insert/query |
| `event_listener_demo` | DbEventListener, event stream; asserts on flush/snapshot events |
| `cardinality_demo` | max_series_cardinality, cardinality_scope_tag_key; limit exceeded error |
| `retention_demo` | retention_ttl; TTL expiry and compaction |
| `query_tag_filter_demo` | db.query with tag filter; multiple series, filtered counts |
| `gen_minimal_write_request` | Emits Snappy WriteRequest to stdout; pipe to curl for Remote Write |
| `prometheus_api_client_demo` | GET query/query_range/labels/series against ugnosd; Grafana datasource reference |

**Library examples** (persistence, encoding, event_listener, cardinality, retention, query_tag_filter) run standalone: `cargo run --example <name>`.

**Server examples** require a running ugnosd with data. Use the scripts:

```bash
./scripts/run-prometheus-api-client-demo.sh   # Prometheus read API demo
./scripts/verify-remote-write-query.sh         # Write → query assertion
```

## Benchmarks

```bash
cargo bench -p ugnos
NOWAL=1 cargo bench -p ugnos
```

Benchmark results are saved in `target/criterion/`. Benchmarks default to the in-memory engine (segments disabled) to keep IO minimal.

## License

This project is licensed under either of

* MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.
