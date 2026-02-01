use std::path::PathBuf;
use std::sync::Arc;

/// Structured, in-process event hook for observability.
///
/// This crate is a library; emitting logs directly (e.g. `println!`) is not acceptable for
/// production. Instead, callers can provide an implementation that forwards these events to
/// `tracing`, `log`, metrics, or custom sinks.
pub trait DbEventListener: std::fmt::Debug + Send + Sync + 'static {
    fn on_event(&self, event: DbEvent);
}

/// Structured events emitted by the core.
#[derive(Debug, Clone)]
pub enum DbEvent {
    FlushThreadStarted,
    FlushThreadStopping,
    FlushThreadPanicked,

    WalRotateFailed { error: String },
    WalCheckpointFailed { error: String },

    SegmentIngestFailed { error: String },
    SegmentCompactionFailed { error: String },

    SnapshotCreated { path: PathBuf, timestamp: u64 },
    SnapshotFailed { error: String },

    RetentionAdvanced { delete_before: u64 },
    RetentionAdvanceFailed { delete_before: u64, error: String },
}

#[derive(Debug)]
pub struct NoopEventListener;

impl DbEventListener for NoopEventListener {
    #[inline]
    fn on_event(&self, _event: DbEvent) {}
}

pub fn noop_event_listener() -> Arc<dyn DbEventListener> {
    Arc::new(NoopEventListener)
}

