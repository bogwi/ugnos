#![doc = include_str!("../README.md")]
// Declare modules
pub mod buffer;
mod cardinality_store;
pub mod core;
pub mod encoding;
pub mod error;
pub mod index;
pub mod persistence;
pub mod query;
/// Stable PromQL-like query surface (label matchers =, !=, =~, !~) for Grafana compatibility.
pub mod query_surface;
pub mod segments;
pub mod storage;
pub mod telemetry;
pub mod types;
pub mod utils;

/// gRPC (Tonic) API for ingest, query, and administration.
pub mod grpc;

/// HTTP ops endpoints (liveness/readiness) for hyper.
pub mod http_ops;

/// Prometheus Remote Write ingest: decode Snappy-compressed WriteRequest and map to DbCore inserts.
pub mod remote_write;

/// Prometheus HTTP API v1 (query, query_range, labels, label values, series) for Grafana compatibility.
pub mod prometheus_api;

/// PromQL library API: run PromQL against a [`DbCore`] from library code (no HTTP).
pub mod promql;

/// Generated Prometheus prompb types (WriteRequest, TimeSeries, Sample, Label).
pub mod prometheus {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
}

/// Configuration options for the database core.
pub use crate::core::DbConfig;
/// Main entry point for interacting with the time-series database core.
pub use crate::core::DbCore;
/// Error type for database operations.
pub use crate::error::DbError;
/// Snapshot manager for point-in-time backups and fast recovery.
pub use crate::persistence::Snapshotter;
/// Write-Ahead Log for durability and recovery.
pub use crate::persistence::WriteAheadLog;
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

// PromQL library API: stable result types and error (programmatic contracts, not JSON-only).
pub use crate::promql::{MetricLabels, PromqlError};
pub use crate::promql::{InstantSample, RangeSeries};

// Default flush interval if not specified
use std::time::Duration;
/// The default interval between automatic buffer flushes (1 second).
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
