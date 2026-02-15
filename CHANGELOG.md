# Changelog

All notable changes to the Rust Time-Series Database Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Released]

### [0.4.2] - 2026-02-15

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

## [Released]

### [0.4.1] - 2026-02-11

### Changed
- make CI to no trigger on tag pushes

### Fixed
- README code exmaples formatting and clarity

## [Released]

### [0.4.0] - 2026-02-10

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

### [0.3.1] - 2026-02-07

### Added

- **Timestamp delta encoding** (varint) for series blocks; configurable float encoding strategies (**Raw64**, **GorillaXor**).
- **Tag dictionary encoding** for series blocks; optional per-block compression (**LZ4**, **Zstd** with configurable level).
- Series block storage format with **block-level checksums** and **versioning** (v2 header with payload CRC32 and version field).
- Bench suite reports segment size per encoding configuration; tests assert p99 query latency within target for encoded/compressed segments.
- Acceptance tests and break-it tests covering format layout validation, checksum/version enforcement, roundtrip correctness, and corruption detection.

## [Released]

## [0.3.0] - 2026-02-02

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

## [0.2.3] - 2026-02-02

### Added

- README is now the crate-level documentation, and all README Rust snippets are exercised as rustdoc doc-tests during `cargo test`.
- Clear “what this is / what this is not (yet)” positioning in the README.

### Changed

- README code examples are now deterministic and CI-safe (use temp dirs and `no_run` to avoid filesystem side-effects and flaky runtime behavior).

## [0.2.2] - 2026-02-02

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

## [0.2.1] - 2026-02-01

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
