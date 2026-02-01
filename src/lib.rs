#![doc = r#"
ugnos: Concurrent Time-Series Database Core in Rust

This crate provides a high-performance, concurrent time-series database core for Rust applications. It is designed for efficient ingestion, storage, and querying of large volumes of time-stamped data, with support for:
- Concurrent write buffering and background flushing
- Columnar in-memory storage
- Write-Ahead Log (WAL) and snapshot-based persistence
- Tag-based filtering and time range queries
- Thread-safe architecture

See the README for usage examples and more details.
"#]
// Declare modules
pub mod buffer;
pub mod core;
pub mod error;
pub mod index;
pub mod persistence;
pub mod query;
pub mod segments;
pub mod storage;
pub mod telemetry;
pub mod types;
pub mod utils;

/// Main entry point for interacting with the time-series database core.
pub use crate::core::DbCore;
/// Configuration options for the database core.
pub use crate::core::DbConfig;
/// Error type for database operations.
pub use crate::error::DbError;
/// Write-Ahead Log for durability and recovery.
pub use crate::persistence::WriteAheadLog;
/// Snapshot manager for point-in-time backups and fast recovery.
pub use crate::persistence::Snapshotter;
/// Structured event hook for observability.
pub use crate::telemetry::{DbEvent, DbEventListener};
/// Represents a single time-series data point.
pub use crate::types::DataPoint;
/// Type alias for a set of tags (key-value pairs) associated with a data point.
pub use crate::types::TagSet;
/// Type alias for a timestamp (nanoseconds since epoch).
pub use crate::types::Timestamp;
/// Type alias for a value in a time series (f64).
pub use crate::types::Value;

// Default flush interval if not specified
use std::time::Duration;
/// The default interval between automatic buffer flushes (1 second).
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

