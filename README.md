# ugnos: Concurrent Time-Series Database Core in Rust

`ugnos` is a concurrent, embeddable **time-series storage + query engine** designed for durability and high-throughput ingest in Rust services.

For project goals and long-term architecture, see the [whitepaper](Ugnos_Concurrent_Time-Series_Database_Core_Whitepaper.md).

## Features (today)

- **Concurrent ingest**: sharded write buffering + background flush thread.
- **Durable persistence**:
  - **WAL** with **explicit format versioning** and **per-record CRC32** checksums.
  - **Snapshots** with **explicit format versioning**, **payload CRC32**, and **atomic install** (temp + rename + fsync).
- **On-disk segment engine (SST-like)**:
  - Immutable segment files with per-series columnar blocks + CRC.
  - Atomic manifest (`MANIFEST.bin`) tracking active segments and retention watermark.
  - Background compaction (L0 → L1 merge) with safe concurrent reads.
- **Retention/TTL**:
  - Immediate logical deletion via tombstone watermark.
  - Physical removal via compaction guarantees.
- **Observability hooks**:
  - No stdout logging in core hot paths.
  - Structured `DbEvent` stream via `DbConfig.event_listener`.

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

```rust
use std::path::PathBuf;
use std::time::Duration;

use ugnos::{DbConfig, DbCore};

let mut cfg = DbConfig::default();
cfg.data_dir = PathBuf::from("./data");

// Durability toggles
cfg.enable_wal = true;
cfg.enable_snapshots = true;
cfg.enable_segments = true; // segment engine + compaction + retention

// Tuning
cfg.wal_buffer_size = 1_000;
cfg.flush_interval = Duration::from_millis(250);
cfg.snapshot_interval = Duration::from_secs(60 * 15);

// Retention (optional): makes data older than now - ttl invisible, and compaction reclaims disk.
cfg.retention_ttl = Some(Duration::from_secs(60 * 60 * 24 * 7));
cfg.retention_check_interval = Duration::from_secs(1);

let mut db = DbCore::with_config(cfg).unwrap();
db.recover().unwrap();
```

## Observability (event hook)

Core emits structured events via `DbConfig.event_listener`:

```rust
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

```rust
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

    let mut cfg = DbConfig::default();
    cfg.data_dir = PathBuf::from("./data");
    cfg.enable_wal = true;
    cfg.enable_snapshots = true;
    cfg.enable_segments = true;
    cfg.retention_ttl = Some(Duration::from_secs(60 * 60 * 24 * 30));
    cfg.event_listener = Arc::new(MemoryEvents(events.clone()));

    let mut db = DbCore::with_config(cfg)?;
    db.recover()?;

    let mut tags = TagSet::new();
    tags.insert("host".to_string(), "server1".to_string());
    tags.insert("region".to_string(), "us-east".to_string());

    db.insert("cpu_usage", 100, 0.75, tags.clone())?;
    db.insert("cpu_usage", 200, 0.80, tags.clone())?;
    db.flush()?; // blocks until durable

    let mut results = db.query("cpu_usage", 0..u64::MAX, Some(&tags))?;
    results.sort_by_key(|(ts, _)| *ts);
    assert_eq!(results.len(), 2);

    Ok(())
}
```

## How to build and test (this workspace)

This repo is a Rust workspace. From the workspace root (the `papers/` folder):

```bash
cargo build --release -p ugnos
cargo test --release -p ugnos
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


