# Changelog

All notable changes to the Rust Time-Series Database Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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