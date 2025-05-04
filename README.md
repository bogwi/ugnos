# ugnos: Concurrent Time-Series Database Core in Rust

A high-performance time-series database core implementation in Rust, designed for efficient storage and retrieval of time-series data. 

## Overview based on the whitepaper
A project like **ugnos** would be used in scenarios where you need to efficiently store, write, and query large volumes of time-stamped data, especially when high concurrency and performance are required. Here are some concrete use cases and domains where such a project would be valuable:

---

### 1. **IoT Data Ingestion and Analytics**
- **Why:** IoT devices generate massive streams of time-stamped sensor data.
- **How:** The database core can ingest concurrent writes from thousands of devices and allow fast, parallel analytics on the collected data.

### 2. **Financial Market Data Storage**
- **Why:** Financial systems need to store and analyze high-frequency trading data (tick data, order books, etc.).
- **How:** The columnar, concurrent design allows for rapid ingestion and real-time querying of market events.

### 3. **Monitoring and Observability Platforms**
- **Why:** Infrastructure and application monitoring tools (like Prometheus, InfluxDB) need to store metrics (CPU, memory, network) over time.
- **How:** The core can serve as the backend for storing and querying these metrics efficiently.

### 4. **Scientific Experimentation and Research**
- **Why:** Experiments often generate time-series data (e.g., environmental sensors, lab instruments).
- **How:** Researchers can use the database to store, tag, and analyze experiment results in parallel.

### 5. **Industrial Automation and SCADA Systems**
- **Why:** Industrial systems log time-stamped events and sensor readings for process control and diagnostics.
- **How:** The database can handle high-throughput writes from multiple sources and support fast queries for dashboards and alerts.

### 6. **Real-Time Analytics for Web and Mobile Apps**
- **Why:** Apps may track user events, interactions, or telemetry as time-series data.
- **How:** The core can power analytics dashboards or anomaly detection engines.

### 7. **Edge Computing and Local Data Aggregation**
- **Why:** Edge devices may need to locally store and process time-series data before syncing to the cloud.
- **How:** The lightweight, efficient Rust core is ideal for resource-constrained environments.

---
Read the [whitepaper](Ugnos_Concurrent_Time-Series_Database_Core_Whitepaper.md) for more information and future enhancements.
---

## Current Features

- Concurrent write buffer with background flushing
- Fast, columnar in-memory storage format
- Persistence mechanisms:
  - Write-Ahead Log (WAL) for durability
  - Periodic snapshotting for faster recovery
- Tag-based filtering
- Time range queries
- Thread-safe architecture

## Persistence Implementation

The database supports two persistence mechanisms:

### Write-Ahead Log (WAL)

The WAL logs all insert operations before they are applied to the in-memory database. This ensures that in case of a crash, no data is lost. Key features of the WAL:

- Logs are only flushed to disk in batches to improve performance
- During shutdown, all pending WAL entries are immediately flushed
- Write operations are first recorded in the WAL before being applied to the in-memory buffer
- Serialized using bincode for efficiency and space optimization

### Snapshotting

Snapshots provide point-in-time backups of the entire database state. Benefits:

- Faster recovery compared to replaying a large WAL
- Configurable snapshot interval
- Snapshots are stored in binary format for compact size and fast loading
- Each snapshot is timestamped for tracking and recovery

### Recovery Process

On startup, the database:
1. Loads the most recent snapshot if available
2. Applies any WAL entries that were created after the snapshot
3. This two-phase recovery ensures both durability and fast startup times

## Configuration

The database can be configured with the following options:

```rust
DbConfig {
    // Interval between automatic buffer flushes
    flush_interval: Duration::from_secs(1),
    // Directory for persistence files
    data_dir: PathBuf::from("./data"),
    // Maximum number of entries to buffer in WAL before writing to disk
    wal_buffer_size: 1000,
    // Whether to enable WAL
    enable_wal: true,
    // Whether to enable snapshots
    enable_snapshots: true,
    // Interval between automatic snapshots (if enabled)
    snapshot_interval: Duration::from_secs(60 * 15), // 15 minutes
}
```

## Future Enhancements

- Compression for WAL and snapshots
- Log compaction
- Distributed operation
- Multi-node replication

## Project Structure

- `src/`: Contains the core library code.
  - `lib.rs`: Main library entry point.
  - `core.rs`: `DbCore` struct, main API, background flush thread.
  - `storage.rs`: `InMemoryStorage` implementation (columnar, sorted).
  - `buffer.rs`: `WriteBuffer` implementation (sharded).
  - `query.rs`: Parallel query execution logic.
  - `types.rs`: Core data types (`Timestamp`, `Value`, `TagSet`, `DataPoint`, `TimeSeriesChunk`).
  - `error.rs`: Custom `DbError` enum.
  - `index.rs`: (Placeholder) Intended for indexing logic.
  - `utils.rs`: (Placeholder) Utility functions.
- `tests/`: Integration tests.
- `benches/`: Criterion performance benchmarks.
- `Cargo.toml`: Project manifest and dependencies.
- `README.md`: This file.

## How to Build and Test

1.  **Prerequisites:**
    *   Rust toolchain (latest stable recommended): Install via `rustup` (https://rustup.rs/).
    *   Build essentials (C compiler, linker, make): Install via your system's package manager (e.g., `sudo apt-get update && sudo apt-get install build-essential` on Debian/Ubuntu).

2.  **Build:**
    ```bash
    cargo build --release
    ```

3.  **Run Tests:**
    ```bash
    cargo test --release
    ```

4.  **Run Benchmarks (WAL enabled, default):**
    ```bash
    cargo bench
    ```

5.  **Run Benchmarks (WAL disabled, try it first!):**
    ```bash
    NOWAL=1 cargo bench
    ```

6. **Benchmark results:**
    ```zsh
    insert_single                         
    time:   [411.73 ns 413.21 ns 415.00 ns]
    insert_single_no_wal
    time:   [333.63 ns 340.32 ns 346.92 ns]
    query_operations/query_range_no_tags  
    time:   [349.10 µs 350.12 µs 351.24 µs]
    query_operations/query_range_with_tag 
    time:   [383.56 µs 385.10 µs 387.02 µs]
    ```
    > Benchmark results will be saved in `target/criterion/`. Benchmark with WAL (`cargo bench`) is hard on system resources! It will create ./data directory with snapshots(not use for benchmarks) and WAL(10GB) files. Cleans up after the work is done. Using **ugnos** you can specify the folder where do you want to store your persistent snapshots and WAL files. Check `DbConfig`, `examples/persistence_demo.rs`, and `tests/integration_tests.rs` for more details.

7. **Run examples (persistence_demo.rs):**
    ```bash
    cargo run --example persistence_demo
    ```
    > This example demonstrates how to create a database with persistence enabled, insert data, and query it. It also shows how to configure the database with different options. Check `examples/persistence_demo.rs` for more details. Creates ./demo_data directory with snapshots and WAL files, very small footprint, to illustrate the persistence mechanism.
8. **Doc-tests:**
    Doc-tests will be added after all advanced API is implemented and the project will get its version 1.0.0. Check the [whitepaper](Ugnos_Concurrent_Time-Series_Database_Core_Whitepaper.md) for more details how it will be looking in the future.


## Basic Usage (Example)

```rust
use rust_tsdb_core::{DbCore, TagSet};
use std::collections::HashMap;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new DB core with a 500ms flush interval
    let db = DbCore::new(Duration::from_millis(500));

    // Prepare tags
    let mut tags = TagSet::new();
    tags.insert("host".to_string(), "server1".to_string());
    tags.insert("region".to_string(), "us-east".to_string());

    // Insert data points
    db.insert("cpu_usage", 1700000000000, 0.75, tags.clone())?;
    db.insert("cpu_usage", 1700000001000, 0.80, tags.clone())?;
    db.insert("cpu_usage", 1700000002000, 0.78, tags.clone())?;

    // Trigger a manual flush (optional, background flush also runs)
    db.flush()?;
    // Wait for flush to likely complete
    std::thread::sleep(Duration::from_millis(100));

    // Query data
    let query_tags = tags.clone(); // Or a subset
    let results = db.query(
        "cpu_usage",
        1700000000000..1700000003000, // Time range (start inclusive, end exclusive)
        Some(&query_tags),
    )?;

    println!("Query Results:");
    for (timestamp, value) in results {
        println!("  Timestamp: {}, Value: {}", timestamp, value);
    }

    Ok(())
    // DbCore automatically handles shutdown of the flush thread when it goes out of scope
}

```

## License

This project is licensed under either of

* MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.


