use thiserror::Error;

/// Custom error type for the database core operations.
#[derive(Error, Debug)]
pub enum DbError {
    #[error("Series not found: {0}")]
    SeriesNotFound(String),

    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    // TODO: Add specific serialization errors if needed (e.g., from serde)
    #[error("Serialization Error: {0}")]
    Serialization(String),

    #[error("Lock acquisition failed: {0}")]
    LockError(String),

    #[error("Invalid time range: start={start}, end={end}")]
    InvalidTimeRange { start: u64, end: u64 },

    #[error("Feature not implemented: {0}")]
    NotImplemented(String),

    #[error("Configuration Error: {0}")]
    ConfigError(String),

    #[error("Background task error: {0}")]
    BackgroundTaskError(String),

    #[error("Corruption detected: {details}")]
    Corruption {
        details: String,
        series: Option<String>,
        timestamp: Option<u64>,
    },

    #[error("Internal error: {0}")]
    Internal(String),
}

// Implement conversion from lock poison errors for convenience
impl<T> From<std::sync::PoisonError<T>> for DbError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        DbError::LockError(format!("Mutex/RwLock poisoned: {}", err))
    }
}
