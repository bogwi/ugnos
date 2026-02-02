use std::path::PathBuf;
use std::sync::Arc;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

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

/// Production-grade metrics instrumentation and in-process scraping.
///
/// ## Key properties
/// - Library-safe: emitting metrics is effectively a no-op until a recorder is installed.
/// - Serverless scraping: callers can render Prometheus exposition text in-process.
/// - Low operational overhead: a lightweight upkeep thread is spawned to keep histograms bounded.
pub mod db_metrics {
    use super::*;

    use ::metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

    #[cfg(feature = "prometheus")]
    use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

    // --- metric names ---
    //
    // Notes:
    // - Counters are exposed as `<name>_total` by the Prometheus exporter.
    // - Histograms are exposed as `<name>_bucket`, `<name>_sum`, `<name>_count`.

    pub const INGEST_POINTS: &str = "ugnos_ingest_points";
    pub const FLUSH_POINTS: &str = "ugnos_flush_points";
    pub const FLUSH_DURATION_SECONDS: &str = "ugnos_flush_duration_seconds";
    pub const FLUSH_E2E_DURATION_SECONDS: &str = "ugnos_flush_end_to_end_duration_seconds";

    pub const WAL_BYTES_WRITTEN: &str = "ugnos_wal_bytes_written";
    pub const WAL_FSYNC_DURATION_SECONDS: &str = "ugnos_wal_fsync_duration_seconds";

    pub const SNAPSHOT_DURATION_SECONDS: &str = "ugnos_snapshot_duration_seconds";
    pub const SNAPSHOT_SIZE_BYTES: &str = "ugnos_snapshot_size_bytes";

    /// Handle to the in-process Prometheus recorder/scrape renderer.
    ///
    /// This does **not** start an HTTP server. Call [`InProcessPrometheus::render`] to scrape.
    #[cfg(feature = "prometheus")]
    #[derive(Debug)]
    pub struct InProcessPrometheus {
        handle: PrometheusHandle,
        stop: Arc<AtomicBool>,
        upkeep_thread: Mutex<Option<JoinHandle<()>>>,
    }

    #[cfg(feature = "prometheus")]
    impl InProcessPrometheus {
        /// Installs a global Prometheus recorder (once per process) and starts a small upkeep thread.
        ///
        /// The upkeep thread periodically calls `run_upkeep()` to keep exporter internals healthy
        /// (e.g. histogram maintenance). This is required when using `install_recorder`.
        pub fn install(upkeep_interval: Duration) -> Result<Self, MetricsInitError> {
            describe_all();

            let builder = PrometheusBuilder::new();
            let handle = builder
                .install_recorder()
                .map_err(MetricsInitError::from_build_error)?;

            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();
            let handle_clone = handle.clone();
            let upkeep_thread = std::thread::Builder::new()
                .name("ugnos-metrics-upkeep".to_string())
                .spawn(move || {
                    while !stop_clone.load(Ordering::Relaxed) {
                        std::thread::sleep(upkeep_interval);
                        handle_clone.run_upkeep();
                    }
                })
                .map_err(|e| MetricsInitError::ThreadSpawn(e.to_string()))?;

            Ok(Self {
                handle,
                stop,
                upkeep_thread: Mutex::new(Some(upkeep_thread)),
            })
        }

        /// Renders the current metrics in the Prometheus text exposition format.
        pub fn render(&self) -> String {
            self.handle.render()
        }
    }

    #[cfg(feature = "prometheus")]
    impl Drop for InProcessPrometheus {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Ok(mut guard) = self.upkeep_thread.lock() {
                if let Some(t) = guard.take() {
                    let _ = t.join();
                }
            }
        }
    }

    #[cfg(feature = "prometheus")]
    #[derive(Debug, thiserror::Error)]
    pub enum MetricsInitError {
        #[error("metrics recorder already installed")]
        AlreadyInstalled,
        #[error("failed to install prometheus recorder: {0}")]
        Install(String),
        #[error("failed to spawn upkeep thread: {0}")]
        ThreadSpawn(String),
    }

    #[cfg(feature = "prometheus")]
    impl MetricsInitError {
        fn from_build_error(e: BuildError) -> Self {
            match e {
                BuildError::FailedToSetGlobalRecorder(_) => MetricsInitError::AlreadyInstalled,
                other => MetricsInitError::Install(other.to_string()),
            }
        }
    }

    #[inline]
    pub fn record_ingest_points(points: u64) {
        if points > 0 {
            ::metrics::counter!(INGEST_POINTS).increment(points);
        }
    }

    #[inline]
    pub fn record_flush(duration: Duration, points: u64) {
        ::metrics::histogram!(FLUSH_DURATION_SECONDS).record(duration.as_secs_f64());
        if points > 0 {
            ::metrics::counter!(FLUSH_POINTS).increment(points);
        }
    }

    #[inline]
    pub fn record_flush_end_to_end(duration: Duration) {
        ::metrics::histogram!(FLUSH_E2E_DURATION_SECONDS).record(duration.as_secs_f64());
    }

    #[inline]
    pub fn record_wal_bytes_written(bytes: u64) {
        if bytes > 0 {
            ::metrics::counter!(WAL_BYTES_WRITTEN).increment(bytes);
        }
    }

    #[inline]
    pub fn record_wal_fsync(duration: Duration) {
        ::metrics::histogram!(WAL_FSYNC_DURATION_SECONDS).record(duration.as_secs_f64());
    }

    #[inline]
    pub fn record_snapshot(duration: Duration, size_bytes: u64) {
        ::metrics::histogram!(SNAPSHOT_DURATION_SECONDS).record(duration.as_secs_f64());
        ::metrics::gauge!(SNAPSHOT_SIZE_BYTES).set(size_bytes as f64);
    }

    fn describe_all() {
        // Counters
        describe_counter!(
            INGEST_POINTS,
            Unit::Count,
            "Total number of points ingested via DbCore::insert."
        );
        describe_counter!(
            FLUSH_POINTS,
            Unit::Count,
            "Total number of points flushed from the write buffer."
        );
        describe_counter!(
            WAL_BYTES_WRITTEN,
            Unit::Bytes,
            "Total number of bytes written to the WAL (logical bytes, not including filesystem metadata)."
        );

        // Histograms
        describe_histogram!(
            FLUSH_DURATION_SECONDS,
            Unit::Seconds,
            "Flush processing time in the background flush thread."
        );
        describe_histogram!(
            FLUSH_E2E_DURATION_SECONDS,
            Unit::Seconds,
            "End-to-end latency observed by DbCore::flush (enqueue->ack)."
        );
        describe_histogram!(
            WAL_FSYNC_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of WAL fsync/sync_data calls."
        );
        describe_histogram!(
            SNAPSHOT_DURATION_SECONDS,
            Unit::Seconds,
            "Time to create and durably install a snapshot."
        );

        // Gauges
        describe_gauge!(
            SNAPSHOT_SIZE_BYTES,
            Unit::Bytes,
            "Size of the most recently created snapshot file."
        );
    }
}

