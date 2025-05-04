# ugnos: Concurrent Time-Series Database Core Whitepaper

**Domain:** Databases / Time-Series Analytics  
**Toolchain:** Rust (cargo), rayon, criterion (for benchmarking), serde, clippy, rustfmt  
**Architecture outline:** Columnar time-series storage (timestamps, values, tags), concurrent write buffer, parallel query executor, and serialization layer.  
**Description:**
A minimal, high-performance time-series database core. Uses DOD to store timestamps, values, and tags in separate vectors, enabling fast concurrent writes and parallel queries. Designed for scalability and efficient memory usage. 

A project like **"ugnos: Concurrent Time-Series Database Core"** would be used in scenarios where you need to efficiently store, write, and query large volumes of time-stamped data, especially when high concurrency and performance are required. Here are some concrete use cases and domains where such a project would be valuable:

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

**In summary:**  
You would use this project as the storage and query engine wherever you need to handle large, concurrent, and high-performance time-series data workloads—especially when you want a modern, safe, and efficient implementation (Rust) that can scale with your needs. It could be the foundation for a custom time-series database, a component in a larger analytics system, or a backend for any application that needs to track and analyze time-stamped data.

---

## **1. Data Model & Storage Layout**

### **Columnar Storage (DOD - Data-Oriented Design)**
- **Timestamps:** Stored in a contiguous `Vec<Timestamp>`.
- **Values:** Stored in a contiguous `Vec<Value>`, where `Value` could be a float, integer, or even an enum for multiple types.
- **Tags/Labels:** Stored in a `Vec<TagSet>` or as separate vectors for each tag dimension (for fast filtering).
- **Why columnar?**  
  - Enables SIMD/vectorized operations.
  - Improves cache locality for analytical queries.
  - Allows efficient compression and serialization.

### **Example:**
```rust
struct TimeSeries {
    timestamps: Vec<u64>,
    values: Vec<f64>,
    tags: Vec<HashMap<String, String>>,
}
```

---

## **2. Write Path: Concurrent Write Buffer**

- **Write Buffer:**  
  - Incoming writes are staged in a lock-free or sharded buffer (e.g., using `crossbeam` or `rayon`).
  - Periodically flushed to the main storage (append-only for immutability and speed).
- **Concurrency:**  
  - Multiple threads can write to different shards/buffers in parallel.
  - Use atomic operations or fine-grained locks only when necessary.

---

## **3. Query Path: Parallel Query Executor**

- **Parallelism:**  
  - Queries (e.g., range scans, aggregations) are split into chunks and processed in parallel using `rayon`.
  - Each thread works on a slice of the data, then results are merged.
- **Predicate Pushdown:**  
  - Filters (e.g., on tags or time ranges) are applied as early as possible to minimize data scanned.

---

## **4. Serialization Layer**

- **Serde:**  
  - Use `serde` for (de)serialization to/from disk or network.
  - Enables snapshotting, replication, or exporting data.
- **Compression:**  
  - Optional: Integrate with `lz4`, `snappy`, or similar for compressed storage.

---

## **5. Indexing**

- **Time-based Index:**  
  - Simple binary search on the timestamp vector for fast range queries.
- **Tag-based Index:**  
  - Hash maps or inverted indexes for quick tag-based filtering.

---

## **6. Durability & Persistence**

- **Write-Ahead Log (WAL):**  
  - Optionally, use a WAL to ensure durability before flushing to main storage.
- **Snapshotting:**  
  - Periodically serialize the in-memory state to disk for recovery.

---

## **7. Scalability & Extensibility**

- **Sharding:**  
  - Partition data by time, series, or tag for horizontal scalability.
- **Pluggable Storage:**  
  - Abstract storage layer to allow swapping in-memory, file-based, or even cloud storage backends.

---

## **8. Example: Minimal Core API**

```rust
impl TimeSeries {
    pub fn insert(
        &self,
        series: &str,
        timestamp: Timestamp,
        value: Value,
        tags: TagSet,
    ) -> Result<(), DbError> { /* ... */ }
    pub fn query(
        &self,
        series: &str,
        time_range: Range<Timestamp>,
        tag_filter: Option<&TagSet>,
    ) -> Result<Vec<(Timestamp, Value)>, DbError> { /* ... */ }
}
```

---

## **Summary Table**

| Component         | Purpose                                 | Rust Tooling         |
|-------------------|-----------------------------------------|----------------------|
| Columnar Storage  | Fast analytics, cache efficiency        | `Vec`, custom structs|
| Write Buffer      | High-throughput concurrent ingestion    | `rayon`, `crossbeam` |
| Query Executor    | Parallel, scalable queries              | `rayon`              |
| Serialization     | Persistence, export/import              | `serde`              |
| Indexing          | Fast lookups                            | `HashMap`, custom    |
| Durability        | Crash recovery                          | File I/O, WAL        |

---

**In essence:**  
The foundation is a highly concurrent, columnar, append-only storage engine with parallel query execution, designed for time-series workloads. Rust’s safety and concurrency features make it ideal for building such a performant and reliable core. This foundation can be extended with more advanced features (retention policies, downsampling, clustering) as needed.

---

## **1. Retention Policies & Downsampling**

### **Retention Policies**
- **Purpose:** Automatically delete or archive old data to save space.
- **Implementation:**  
  - Periodic background task scans for data older than a configured threshold and removes it.
  - Can be implemented as a background thread or async task.
- **Rust Tools:** `tokio` (for async), `chrono` (for time calculations).

### **Downsampling**
- **Purpose:** Reduce storage by aggregating old data (e.g., average per hour/day).
- **Implementation:**  
  - Background job computes aggregates and replaces raw data with summaries.
  - Store aggregates in separate columnar structures.
- **Rust Tools:** `rayon` for parallel aggregation.

---

## **2. Compression & Encoding**

- **Purpose:** Reduce disk and memory usage.
- **Techniques:**  
  - Delta encoding for timestamps.
  - Gorilla or XOR encoding for float values.
  - Dictionary encoding for tags.
- **Rust Tools:** `lz4`, `snap`, or custom encoding crates.

---

## **3. Multi-Tenancy & Isolation**

- **Purpose:** Support multiple independent users or applications.
- **Implementation:**  
  - Namespace data by tenant ID.
  - Isolate buffers and storage per tenant.
- **Rust Tools:** Use enums/structs for tenant metadata, `HashMap<TenantId, TimeSeries>`.

---

## **4. Distributed Storage & Clustering**

- **Purpose:** Scale horizontally across multiple machines.
- **Implementation:**  
  - Shard data by time or series key.
  - Use a consensus protocol (e.g., Raft) for metadata and leader election.
  - Replicate data for fault tolerance.
- **Rust Tools:** `raft-rs`, `tonic` (gRPC), `tokio` for networking.

---

## **5. Advanced Query Engine**

- **Features:**  
  - Support for SQL-like or PromQL-like query languages.
  - Complex aggregations, joins, and window functions.
- **Implementation:**  
  - Build a parser (e.g., with `nom` or `pest`).
  - Query planner and optimizer.
  - Vectorized execution engine.
- **Rust Tools:** `datafusion` (Apache Arrow), `sqlparser-rs`.

---

## **6. Real-Time Alerting & Continuous Queries**

- **Purpose:** Trigger actions or notifications on data conditions (e.g., threshold breaches).
- **Implementation:**  
  - Register continuous queries or alert rules.
  - Evaluate rules on new data in real time.
- **Rust Tools:** Use async channels (`tokio::sync::mpsc`) for event-driven processing.

---

## **7. Security & Access Control**

- **Purpose:** Protect data and restrict access.
- **Implementation:**  
  - Authentication (API keys, OAuth).
  - Role-based access control (RBAC) for series/tags.
- **Rust Tools:** `jsonwebtoken`, `argon2` for password hashing.

---

## **8. API & Integration**

- **Purpose:** Expose data to clients and integrate with other systems.
- **Implementation:**  
  - REST or gRPC API for data ingestion and queries.
  - WebSocket for real-time streaming.
- **Rust Tools:** `axum`, `warp`, `tonic`.

---

## **9. Observability & Monitoring**

- **Purpose:** Monitor database health and performance.
- **Implementation:**  
  - Expose internal metrics (ingest rate, query latency, memory usage).
  - Integrate with Prometheus or similar.
- **Rust Tools:** `metrics`, `prometheus` crates.

---

## **10. Pluggable Storage Engines**

- **Purpose:** Support different backends (in-memory, file, cloud object storage).
- **Implementation:**  
  - Define a `StorageEngine` trait.
  - Implement for various backends.
- **Rust Tools:** Traits, dynamic dispatch, or generics.

---

## **Example: Advanced API Sketch**

```rust
trait StorageEngine {
    fn insert(&self, tenant: &str, series: &str, timestamp: u64, value: f64, tags: &TagSet);
    fn query(&self, tenant: &str, query: Query) -> QueryResult;
    fn compact(&self);
    fn snapshot(&self, path: &str);
}

struct Database {
    engines: HashMap<String, Box<dyn StorageEngine>>,
    // ... other fields ...
}
```

---

## **Summary Table**

| Feature                | Purpose/Benefit                        | Rust Tools/Crates         |
|------------------------|----------------------------------------|---------------------------|
| Retention/Downsampling | Save space, long-term trends           | `tokio`, `rayon`, `chrono`|
| Compression/Encoding   | Reduce storage/costs                   | `lz4`, `snap`             |
| Multi-Tenancy          | Serve multiple users/apps               | `HashMap`, custom structs |
| Distributed/Cluster    | Scale, fault tolerance                  | `raft-rs`, `tonic`        |
| Advanced Query Engine  | Powerful analytics                      | `datafusion`, `sqlparser` |
| Real-Time Alerting     | Proactive monitoring                    | `tokio`, async channels   |
| Security/Access Control| Protect data                            | `jsonwebtoken`, `argon2`  |
| API/Integration        | Client access, ecosystem integration    | `axum`, `tonic`, `warp`   |
| Observability          | Monitor DB health                       | `metrics`, `prometheus`   |
| Pluggable Storage      | Flexibility, extensibility              | Traits, generics          |

---


# Time-Series Database Core Advanced API

This document describes the advanced API functions for the Rust concurrent time-series database core. Each function includes its signature, arguments, return type, and documentation.

---

```rust
/// Insert a single data point into a time series.
///
/// # Arguments
/// * `tenant` - Tenant or namespace identifier.
/// * `series` - Name of the time series.
/// * `timestamp` - Timestamp of the data point (epoch, ns/ms/s).
/// * `value` - Value to insert (float/int).
/// * `tags` - Optional key-value tags for the data point.
///
/// # Returns
/// * `Result<(), DbError>` - Ok if successful, error otherwise.
fn insert(
    tenant: &str,
    series: &str,
    timestamp: u64,
    value: f64,
    tags: Option<&TagSet>,
) -> Result<(), DbError>;
```

---

```rust
/// Bulk insert multiple data points into a time series.
///
/// # Arguments
/// * `tenant` - Tenant or namespace identifier.
/// * `series` - Name of the time series.
/// * `data` - Vector of (timestamp, value, tags) tuples.
///
/// # Returns
/// * `Result<(), DbError>` - Ok if successful, error otherwise.
fn insert_batch(
    tenant: &str,
    series: &str,
    data: Vec<(u64, f64, Option<TagSet>)>,
) -> Result<(), DbError>;
```

---

```rust
/// Query data points from a time series.
///
/// # Arguments
/// * `tenant` - Tenant or namespace identifier.
/// * `series` - Name of the time series.
/// * `time_range` - (start, end) timestamps (inclusive).
/// * `tag_filter` - Optional tag filter.
/// * `aggregation` - Optional aggregation (e.g., "avg", "sum").
/// * `downsample` - Optional downsampling interval (seconds).
///
/// # Returns
/// * `Result<Vec<DataPoint>, DbError>` - Vector of matching data points or error.
fn query(
    tenant: &str,
    series: &str,
    time_range: (u64, u64),
    tag_filter: Option<&TagSet>,
    aggregation: Option<&str>,
    downsample: Option<u64>,
) -> Result<Vec<DataPoint>, DbError>;
```

---

```rust
/// List all series for a tenant, optionally filtered by tag.
///
/// # Arguments
/// * `tenant` - Tenant or namespace identifier.
/// * `tag_filter` - Optional tag filter.
///
/// # Returns
/// * `Result<Vec<String>, DbError>` - Vector of series names or error.
fn list_series(
    tenant: &str,
    tag_filter: Option<&TagSet>,
) -> Result<Vec<String>, DbError>;
```

---

```rust
/// Delete data points from a series within a time range and optional tag filter.
///
/// # Arguments
/// * `tenant` - Tenant or namespace identifier.
/// * `series` - Name of the time series.
/// * `time_range` - (start, end) timestamps (inclusive).
/// * `tag_filter` - Optional tag filter.
///
/// # Returns
/// * `Result<u64, DbError>` - Number of deleted points or error.
fn delete(
    tenant: &str,
    series: &str,
    time_range: (u64, u64),
    tag_filter: Option<&TagSet>,
) -> Result<u64, DbError>;
```

---

```rust
/// Register a continuous query or alert rule.
///
/// # Arguments
/// * `tenant` - Tenant or namespace identifier.
/// * `rule` - Rule definition (e.g., threshold, aggregation, action).
///
/// # Returns
/// * `Result<RuleId, DbError>` - Registered rule ID or error.
fn register_rule(
    tenant: &str,
    rule: RuleDefinition,
) -> Result<RuleId, DbError>;
```

---

```rust
/// Remove a registered rule by ID.
///
/// # Arguments
/// * `tenant` - Tenant or namespace identifier.
/// * `rule_id` - Rule identifier.
///
/// # Returns
/// * `Result<(), DbError>` - Ok if successful, error otherwise.
fn remove_rule(
    tenant: &str,
    rule_id: RuleId,
) -> Result<(), DbError>;
```

---

```rust
/// Take a snapshot of the database for backup or migration.
///
/// # Arguments
/// * `path` - Filesystem path to store the snapshot.
///
/// # Returns
/// * `Result<(), DbError>` - Ok if successful, error otherwise.
fn snapshot(
    path: &str,
) -> Result<(), DbError>;
```

---

```rust
/// Restore the database from a snapshot.
///
/// # Arguments
/// * `path` - Filesystem path of the snapshot.
///
/// # Returns
/// * `Result<(), DbError>` - Ok if successful, error otherwise.
fn restore(
    path: &str,
) -> Result<(), DbError>;
```

---

```rust
/// Get database health and metrics.
///
/// # Returns
/// * `DbMetrics` - Struct with health, usage, and performance metrics.
fn get_metrics() -> DbMetrics;
```

---

## Types Referenced

```rust
type TagSet = HashMap<String, String>;
type RuleId = u64;

struct DataPoint {
    timestamp: u64,
    value: f64,
    tags: Option<TagSet>,
}

struct RuleDefinition {
    // e.g., series, condition, aggregation, action, etc.
}

struct DbMetrics {
    uptime: u64,
    series_count: usize,
    data_points: usize,
    memory_usage: usize,
    query_latency_ms: f64,
    // ... other metrics ...
}

enum DbError {
    NotFound,
    InvalidInput,
    PermissionDenied,
    InternalError,
    // ... other errors ...
}
``` 

# USAGE.md

## Usage Scenarios for the Rust Concurrent Time-Series Database Core

This guide describes how an engineer, Bob, can use this project in real-world scenarios. Bob has just discovered the project on GitHub and wants to understand how to apply it to his needs. Each section addresses a specific use case, showing how Bob would integrate or use the database core.

---

### 1. IoT Data Ingestion and Analytics
**Scenario:** Bob is building a platform to collect sensor data from thousands of IoT devices.

**How Bob uses it:**
- Deploys the database core as a backend service.
- Devices send time-stamped sensor readings via REST/gRPC API.
- Bob uses `insert_batch` to efficiently ingest data from multiple devices.
- For analytics, he uses `query` with aggregation and downsampling to generate reports.

**Example:**
```rust
// Ingest data from devices
insert_batch("iot-tenant", "temperature", data_points)?;
// Query average temperature per hour
query("iot-tenant", "temperature", (start, end), None, Some("avg"), Some(3600))?;
```

---

### 2. Financial Market Data Storage
**Scenario:** Bob needs to store and analyze high-frequency trading data.

**How Bob uses it:**
- Streams tick data into the database using `insert_batch` for each trading symbol.
- Runs parallel queries for real-time analytics and backtesting using `query` with tag filters (e.g., by symbol or exchange).

**Example:**
```rust
// Store tick data
insert_batch("finance", "AAPL_ticks", ticks)?;
// Query for a specific symbol and time range
query("finance", "AAPL_ticks", (start, end), None, Some("max"), None)?;
```

---

### 3. Monitoring and Observability Platforms
**Scenario:** Bob is building a monitoring system for servers and applications.

**How Bob uses it:**
- Agents push metrics (CPU, memory, etc.) to the database using `insert` or `insert_batch`.
- Dashboards and alerting systems use `query` to fetch and visualize metrics.
- Bob registers alert rules with `register_rule` for real-time notifications.

**Example:**
```rust
// Ingest server metrics
insert("monitoring", "cpu_usage", timestamp, value, Some(tags))?;
// Register a CPU alert rule
register_rule("monitoring", cpu_alert_rule)?;
```

---

### 4. Scientific Experimentation and Research
**Scenario:** Bob is a researcher collecting time-series data from lab instruments.

**How Bob uses it:**
- Stores experiment results with rich tags (e.g., experiment ID, sample type) using `insert`.
- Uses `query` with tag filters to analyze subsets of data.
- Takes snapshots with `snapshot` for reproducibility and sharing.

**Example:**
```rust
// Store experiment data
insert("lab", "experiment1", timestamp, value, Some(tags))?;
// Query by sample type
query("lab", "experiment1", (start, end), Some(&sample_tag), None, None)?;
// Take a snapshot
snapshot("/backups/lab_experiment1.snap")?;
```

---

### 5. Industrial Automation and SCADA Systems
**Scenario:** Bob manages a factory with many sensors and control systems.

**How Bob uses it:**
- Collects sensor readings from PLCs and SCADA systems using `insert_batch`.
- Uses `query` for real-time dashboards and historical analysis.
- Sets up retention policies and deletes old data with `delete`.

**Example:**
```rust
// Ingest sensor data
insert_batch("factory", "pressure_sensors", readings)?;
// Delete data older than 90 days
let cutoff = now - 90*24*3600;
delete("factory", "pressure_sensors", (0, cutoff), None)?;
```

---

### 6. Real-Time Analytics for Web and Mobile Apps
**Scenario:** Bob is tracking user events and telemetry from a mobile app.

**How Bob uses it:**
- App clients send event data to the backend, which uses `insert`.
- Bob queries event rates and user behavior patterns with `query` and aggregation.
- Uses `list_series` to discover all tracked event types.

**Example:**
```rust
// Store user event
insert("app", "user_clicks", timestamp, 1.0, Some(user_tags))?;
// List all event series
list_series("app", None)?;
```

---

### 7. Edge Computing and Local Data Aggregation
**Scenario:** Bob is deploying the database on edge devices for local data collection.

**How Bob uses it:**
- Runs the database core on resource-constrained hardware (e.g., Raspberry Pi).
- Collects and stores data locally using `insert`.
- Periodically takes a `snapshot` and syncs to the cloud.
- Uses `restore` to recover from failures or migrate data.

**Example:**
```rust
// Local data collection
insert("edge", "humidity", timestamp, value, None)?;
// Sync snapshot to cloud
snapshot("/mnt/usb/edge_data.snap")?;
// Restore after device replacement
restore("/mnt/usb/edge_data.snap")?;
```

---

## General Workflow
1. **Integrate** the database core as a library or service in your stack.
2. **Ingest** data using `insert` or `insert_batch`.
3. **Query** data for analytics, monitoring, or visualization.
4. **Manage** data lifecycle with `delete`, `snapshot`, and `restore`.
5. **Extend** with alerting, retention, and custom rules as needed.