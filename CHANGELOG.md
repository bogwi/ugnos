# Changelog

All notable changes to the Rust Time-Series Database Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# Milestone 2 — Query engine + APIs
Goal: become usable as a production database via a networked server binary and stable client surface.

## [Released]

### [0.5.0] - 2026-02-25 - Query Language

### Added
*Deliverables:*
- A stable query surface, **PromQL-like**, for metrics-focused deployments (Grafana-compatible over Prometheus HTTP API).
- Vectorized execution for scans/aggregations; parallelism controls.
- Home page with HTTP API documentation and version.
- Educational examples and scripts: `prometheus_api_client_demo.rs`, `gen_minimal_write_request.rs`, `verify-http-auth.sh`, `verify-remote-write-query.sh`, `run-prometheus-api-client-demo.sh`, `run-prometheus-api-client-demo.sh`, `verify-http-auth.sh`, `verify-remote-write-query.sh`.


*Acceptance criteria:*
- Query planner produces explain output; regression suite covers semantics.
- Window aggregates and downsampling work and are tested.
- PromQL-like regression suite covers a representative dashboard subset:
  - vector selector with label matchers (`=`, `!=`, `=~`, `!~`)
  - range selectors with core functions (`rate`, `increase`, `avg_over_time`, `max_over_time`, …)
  - aggregation with grouping (`sum by (...)`, `avg without (...)`)
- PromQL is defined to follow IEEE 754 for aggregation and operators (NaN propagates).
  - sum / avg / count: No NaN-skipping; any NaN propagates (sum “same as +”, avg “same as /”)
  - min / max: “NaN is only ever considered a minimum or maximum if all aggregated values are NaN” (minNum/maxNum). So we do not need NaN-skipping; the spec is “no filter, IEEE 754, min/max special case”.

## [Pass-through]

### [0.4.4] - 2026-02-19 - HTTP ops endpoints (liveness/readiness) + Prometheus Remote Write

### Added
*Deliverables:*

- Ops endpoints over HTTP (hyper): `GET /healthz` (liveness), `GET /readyz` (readiness); `HEAD` allowed; non-GET/HEAD on ops paths return 405; readiness set false on shutdown for drain; `Content-Type: text/plain; charset=utf-8`; library module `ugnos::http_ops` with `OpsState` and pure `handle_ops_request` for testability.
- Prometheus Remote Write ingest: `POST /api/v1/write` accepts Snappy-compressed protobuf `WriteRequest`; mapping: `__name__` → series name, remaining labels → `TagSet`, sample timestamp (ms) → internal nanoseconds; invalid payloads → 400 with actionable error; cardinality limit → 429 with explicit error body and `ugnos_remote_write_rejections` (reason label); AuthN/AuthZ deny-by-default via optional `http_write_token` (config/env `UGNOS__HTTP_WRITE_TOKEN`), Bearer token required when set; library module `ugnos::remote_write` and generated `ugnos::prometheus` (prompb types).
- Prometheus HTTP API v1 (Grafana Prometheus datasource compatibility): `GET /api/v1/query` (instant), `GET /api/v1/query_range` (range), `GET /api/v1/labels`, `GET /api/v1/label/<name>/values`, `GET /api/v1/series`; standard JSON envelope and result formats; library module `ugnos::prometheus_api`; metadata endpoints use `DbCore::list_series_keys` / `list_series_names` (cardinality tracker and segment manifest).
- **Stable query surface (PromQL-like, Grafana-compatible):** Module `ugnos::query_surface` defines the supported PromQL subset. Full evaluation expression tree (`EvalExpr`) covering instant vector selectors, range functions, and aggregations. Parsing uses `promql-parser` crate (not hand-rolled). Instant vector selectors with full label matchers: `=`, `!=`, `=~`, `!~` (exact, not-equal, regex, negated regex); optional `offset <duration>`; metric name via identifier or `{__name__=~"..."}`. Range functions: `rate()`, `increase()`, `avg_over_time()`, `max_over_time()`, `min_over_time()`, `sum_over_time()` with counter-reset handling. Aggregations: `sum`, `avg`, `min`, `max`, `count` with `by (label, ...)` / `without (label, ...)` grouping. Composable: `sum by (job) (rate(metric[5m]))`. Query planner produces `explain()` output for plan inspection and regression testing. Unsupported expressions (binary ops, subqueries) return 422 with actionable error. Semantics follow Prometheus (e.g. missing label matches `!=` and `!~`).
- **Vectorized execution and parallelism controls:** Multi-series scans for Prometheus API (`/api/v1/query`, `/api/v1/query_range`) run in parallel (Rayon). Optional `DbConfig::query_max_parallel_series` caps concurrency via a dedicated thread pool; daemon config `query_max_parallel_series` and env `UGNOS__QUERY_MAX_PARALLEL_SERIES` supported. Vectorized range functions in `ugnos::query`: `compute_rate`, `compute_increase`, `compute_avg_over_time`, `compute_max_over_time`, `compute_min_over_time`, `compute_sum_over_time` operate on `&[(Timestamp, Value)]` slices with NaN skipping and counter-reset correction. Vectorized aggregates: `aggregate_sum`, `aggregate_avg`, `aggregate_min`, `aggregate_max`, `aggregate_count` over value slices (NaN skipped, ±Inf preserved for sum); `reduce_last` for instant-vector last sample. Expression evaluation engine in `prometheus_api.rs`: `eval_vector` (instant) and `eval_matrix` (range) recursively walk `EvalExpr` tree, pre-fetching data windows for range functions to avoid redundant I/O. Existing in-chunk parallel filtering in `execute_query` unchanged.

*Acceptance criteria:*
- Invalid Remote Write payloads return 400 with actionable error.
- Backpressure returns 429 and emits metrics (request rejected, reason).
- Cardinality-limit rejections return explicit error semantics (documented) and emit metrics.
- Prometheus HTTP API returns correct JSON schema for success/error responses (Grafana-compatible).
- AuthN/AuthZ is deny-by-default and covers all **HTTP** endpoints shipped in Milestone 2 (ops + Prometheus compatibility).
- Query planner produces explain output (`query_surface::explain`); regression suite covers semantics for all supported expression types.
- Window aggregates (`avg_over_time`, `max_over_time`, `min_over_time`, `sum_over_time`) and downsampling (via `query_range` step interpolation) work and are tested.
- PromQL-like regression suite covers representative dashboard subset: vector selectors with all 4 label matchers; range selectors with `rate`, `increase`, `avg_over_time`, `max_over_time`; aggregation with grouping (`sum by (...)`, `avg without (...)`, `count`, `min by`, `max by`).

## [Pass-through]

### [0.4.3] - 2026-02-17 - External APIs (native)

### Added
*Deliverables:*

- gRPC (Tonic) API for ingest/query/administration.
- AuthN/AuthZ enforcement for the server API surface.

*Acceptance criteria:*

- End-to-end integration tests validate ingest → persist → restart → query.
- Backpressure behavior is well-defined (429/RESOURCE_EXHAUSTED) with metrics.
- AuthN/AuthZ is deny-by-default and covers all **gRPC** endpoints shipped in Milestone 2.

### Changed
- Change in `ci.yml`: On main we only save (no restore), so we never save the same key we restored. On branches/PRs we restore to compare against baseline.

## [Released]

### [0.4.2] - 2026-02-15 - Server binary: `ugnosd`

### Added
*Deliverables:*

- Production-grade daemon with config file, env overrides, and CLI flags.
- Health endpoints, readiness/liveness, graceful shutdown, safe startup checks.
- Docker image build + publish instructions; `docker compose` quickstart for local evaluation.
- Verification scripts in `scripts/`: `verify-docker.sh` (Docker/compose + health checks), `verify-k8s-local.sh` (kind + deploy + healthz/readyz), `verify-k8s-deploy.sh` (existing cluster), `verify-k8s-manifests.sh` (manifest validation).

*Acceptance criteria:*

- Runs as a single static-ish artifact (per platform), with documented deployment recipes (systemd, Docker, Kubernetes).
- Graceful shutdown guarantees WAL flush policy and clean compaction stop.
- Docker image can start `ugnosd` with a config and serves health endpoints (`/healthz`, `/readyz`).

*Assertions (implemented):*

- README "Deployment recipes", `deploy/systemd/ugnosd.service`, `deploy/k8s/*.yaml` 
- `tests/daemon_health_tests.rs`: `graceful_shutdown_prints_flush_and_complete` (exit 0 on Unix), `graceful_shutdown_then_restart_recovery_succeeds`; daemon calls `db.flush()` then `drop(db)`. 

### Changed
- Change in `src/core.rs`: The whole cardinality block (scope, register_and_was_new, append_scope_key, record_series_cardinality) runs only when `self.config.max_series_cardinality.is_some()`. When it’s `None`, that block is skipped.

## [Released]

### [0.4.1] - 2026-02-11

### Changed
- make CI to no trigger on tag pushes

### Fixed
- README code exmaples formatting and clarity

# Milestone 1 — Single-node durable storage engine
Goal: move from "in-memory with WAL + snapshots" toward a **real on-disk engine** with retention and compaction.


## [Released]

### [0.4.0] - 2026-02-10 - Indexing & cardinality controls

### Added

*Deliverables:*

- Time index per segment/block
  - time-range is executed via binary search (partition_point) over sorted timestamps in each series block.
- Tag index strategy suitable for high-cardinality tags
  * What is implemented: an inverted index using Roaring bitmaps:
    * Per-block: `(key_id,value_id)` - RoaringBitmap(rows), compressed (zstd) + CRC.
    * Per-segment postings: `(key_id,value_id)` - RoaringBitmap(series ordinals), compressed (zstd) + CRC.
- Cardinality estimation and hard limits per tenant/namespace
  - Hard limits per scope: enforced in `DbCore::insert` with a configurable scope key (`DbConfig::cardinality_scope_tag_key`) and configurable hard limit (`DbConfig::max_series_cardinality`).
  - Durable enforcement across restart: on-disk journal+checkpoint (`src/cardinality_store.rs`) and a “restart bypass” break-it test already existed (`tests/indexing_cardinality_tests.rs`).
  - Exact distinct series keys: implementation tracks exact distinct series keys (HashSet of canonicalized (series, sorted tags)), not an approximate estimator as was planned initially (e.g. HyperLogLog) and reflected in the Milestone 1. Exact is stronger for correctness.

*Acceptance criteria:*

- Tag filters avoid full scans for common workloads (measurable via query benchmark)
  - Avoid full scan: when tag index exists, the query path computes candidate rows via Roaring bitmap intersections and returns only those rows (no per-row tag checks over the full in-range window).
  - Proved by `breakit_corrupt_tag_index_returns_corruption`: corrupting the persisted tag index causes the query to return `DbError::Corruption` (meaning the query path does consult and validate the index, rather than silently scanning).
  - Measurable via benchmark: Criterion benches include `segments_query/*_range_with_tag_filter` (and an in-memory analog), which is exactly the “measurable” part of the AC.
- Configurable hard limit returns explicit error + metrics
  - Explicit error: `DbError::SeriesCardinalityLimitExceeded { current, limit, scope }`.
  - Metrics: counter `ugnos_cardinality_limit_rejections{scope=...}` and gauge `ugnos_series_cardinality{scope=...}`.
  - Proved by a test `cardinality_limit_rejection_emits_explicit_error_and_metrics` : inserting a series beyond the limit returns `DbError::SeriesCardinalityLimitExceeded`.

New test files: `tests/cardinality_metrics_tests.rs`, `tests/indexing_cardinality_tests.rs`, `tests/segment_postings_index_tests.rs`, `tests/tag_index_corruption_tests.rs`, `tests/test_manifest.rs`

Note: Modern practice alignment

- High-cardinality tag indexing: inverted index + bitmap postings (Roaring) is a mainstream approach for fast AND intersections and compact storage; see the Roaring bitmap performance/usage references and high-cardinality discussions in observability/data systems, e.g. ClickHouse engineering writeups ([High Cardinality: The slow observability challenge](https://clickhouse.com/resources/engineering/high-cardinality-slow-observability-challenge)) and Roaring bitmap docs ([Roaring Bitmap Benchmarks](https://biscuit.readthedocs.io/en/latest/benchmark_roaring.html)).
- Per-tenant cardinality management: documented best practice in metrics systems (tenant quotas + explicit limits + observability), e.g. [Grafana Enterprise Metrics cardinality management](https://grafana.com/docs/enterprise-metrics/latest/manage/tenant-management/cardinality-management) and [InfluxDB guidance](https://docs.influxdata.com/influxdb/cloud/write-data/best-practices/resolve-high-cardinality/).

## [Released]

### [0.3.1] - 2026-02-07 - Encoding & compression

### Added

- **Timestamp delta encoding** (varint) for series blocks; configurable float encoding strategies (**Raw64**, **GorillaXor**).
- **Tag dictionary encoding** for series blocks; optional per-block compression (**LZ4**, **Zstd** with configurable level).
- Series block storage format with **block-level checksums** and **versioning** (v2 header with payload CRC32 and version field).
- Bench suite reports segment size per encoding configuration; tests assert p99 query latency within target for encoded/compressed segments.
- Acceptance tests and break-it tests covering format layout validation, checksum/version enforcement, roundtrip correctness, and corruption detection.

## [Released]

## [0.3.0] - 2026-02-02 - Storage engine: segment files + compaction

### Added

- Segment-store compaction triggers based on **total L0 bytes** and **L0 max age** (in addition to segment-count).
- Retention **physical reclamation** via background rewrite/removal of expired-on-disk data (works for single segments and L1 segments).
- “Break it” tests covering retention physical reclamation and the new size/time compaction triggers.

*Note: this feature was implemented yet in 18be8bbdd5ff3753e4b5fb3635d02a29f2a4d579. Now it was hardened and tested additionally.*

### Changed

- Advancing the retention tombstone watermark now nudges the background compaction/reclaim loop for timely disk reclamation.
- Segment ingest applies the retention watermark up-front and treats “all data expired” as a no-op instead of an error.

### Fixed

- L0 compaction now handles “retention removed everything” by deleting input segments without producing an empty output segment.

### Removed

- N/A

# Milestone 0 — Hardening the existing core
Goal: make today’s single-node core reliable, testable, and ready to become a storage engine component.

## [0.2.3] - 2026-02-02 - Documentation correctness

### Added

- README is now the crate-level documentation, and all README Rust snippets are exercised as rustdoc doc-tests during `cargo test`.
- Clear “what this is / what this is not (yet)” positioning in the README.

### Changed

- README code examples are now deterministic and CI-safe (use temp dirs and `no_run` to avoid filesystem side-effects and flaky runtime behavior).

## [0.2.2] - 2026-02-02 - Performance and observability scaffolding

### Added

- Production metrics scaffolding via `metrics` with Prometheus in-process scraping (no server) through `telemetry::db_metrics::InProcessPrometheus`.
- Metrics for ingest throughput and flush latency (`ugnos_ingest_points`, `ugnos_flush_duration_seconds`, `ugnos_flush_end_to_end_duration_seconds`).
- Metrics for WAL fsync time and logical bytes written (`ugnos_wal_fsync_duration_seconds`, `ugnos_wal_bytes_written`).
- Metrics for snapshot size/time (`ugnos_snapshot_size_bytes`, `ugnos_snapshot_duration_seconds`).
- Deterministic dataset generators (fixed seeds) for stable, comparable benchmarks.
- Deterministic microbench suite using `iai-callgrind` (CI-friendly, instruction-count based).
- CI workflow that runs microbench suite and regression gates with configurable thresholds.

### Changed

- Criterion benchmarks are now deterministic and self-contained (fixed-seed datasets + temp dirs; no shared `./data`).
- Prometheus exporter is now feature-gated (`prometheus`, enabled by default) so consumers can disable it while retaining the `metrics` facade instrumentation.

## [Released]

## [0.2.1] - 2026-02-01 - Reliability & correctness baseline

### Added

- Structured observability hook via `DbConfig.event_listener` (no direct stdout logging in core).
- `DbError::Corruption` for explicit, actionable corruption surfaces.
- Deterministic crash/recovery tests covering: crash after WAL append, crash during flush, crash during snapshot write.
- WAL per-record CRC32 checksums with corruption errors including best-effort series/timestamp context.
- Snapshot format header/versioning + payload CRC32, and atomic snapshot install (temp + rename + fsync).

### Changed

- Segment/manifest corruption paths now return `DbError::Corruption` instead of generic internal errors.
- Snapshot creation is now acknowledged (blocking API semantics) to avoid nondeterministic tests/usage.
- WAL initialization upgrades older/non-versioned `wal.log` by rotating it aside for replay and starting a new versioned WAL.

### Fixed

- Eliminated `println!/eprintln!` from core hot paths; all telemetry is emitted via structured hooks.
- Prevented “partial snapshot” artifacts from breaking recovery (tmp snapshots are ignored unless atomically installed).

### Removed

- Direct stdout/stderr logging from core runtime paths.

## [0.1.1] - 2025-05-11

### Added

- Initial release of the time-series database core
- Concurrent write buffer with background flushing
- In-memory columnar storage format
- Write-Ahead Log (WAL) for durability
- Snapshot mechanism for point-in-time recovery
- Time range queries with tag filtering
- Thread-safe architecture with RwLock/Mutex
- Configuration API with sensible defaults
- Recovery process for WAL and snapshots
- Example code demonstrating database usage
- Persistence demonstration with recovery

### Fixed

- N/A (initial release)

### Changed

- N/A (initial release)

### Removed

- N/A (initial release)
