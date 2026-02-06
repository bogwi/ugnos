# Changelog

All notable changes to the Rust Time-Series Database Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
