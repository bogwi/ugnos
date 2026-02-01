# Changelog

All notable changes to the Rust Time-Series Database Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

## [Unreleased]

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