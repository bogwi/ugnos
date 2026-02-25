//! Demonstrates the observability event hook: a custom `DbEventListener` that collects
//! structured `DbEvent`s, attached via `DbConfig::event_listener`, with assertions on
//! the event stream after insert, flush, and snapshot.
//!
//! Run with: `cargo run --example event_listener_demo`

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, DbError, DbEvent, DbEventListener, TagSet};

/// Collects all `DbEvent`s into a shared `Vec` for later inspection.
#[derive(Debug)]
struct EventCollector(Arc<Mutex<Vec<DbEvent>>>);

impl DbEventListener for EventCollector {
    fn on_event(&self, event: DbEvent) {
        if let Ok(mut guard) = self.0.lock() {
            guard.push(event);
        }
        // If the mutex is poisoned (e.g. main panicked while holding the lock), skip pushing
        // to avoid panicking inside the listener during Drop or from the flush thread.
    }
}

fn main() -> Result<(), DbError> {
    let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let collector = EventCollector(events.clone());

    let dir = TempDir::new().expect("temp dir");
    let cfg = DbConfig {
        data_dir: PathBuf::from(dir.path()),
        flush_interval: Duration::from_millis(100),
        snapshot_interval: Duration::from_secs(60),
        enable_wal: true,
        enable_snapshots: true,
        event_listener: Arc::new(collector),
        ..DbConfig::default()
    };

    let db = DbCore::with_config(cfg)?;

    let mut tags = TagSet::new();
    tags.insert("host".to_string(), "web-01".to_string());
    tags.insert("region".to_string(), "us-east-1".to_string());
    tags.insert("env".to_string(), "production".to_string());
    let t0 = 1_700_000_000_000_000_000u64; // plausible ns since epoch
    db.insert("node_cpu_seconds_total", t0, 0.42, tags.clone())?;
    db.insert("node_cpu_seconds_total", t0 + 15_000_000_000, 0.48, tags)?;

    db.flush()?;
    // No dedicated "FlushCompleted" event is emitted; completion is observed via flush() return.

    db.snapshot()?;
    // Snapshot command is processed synchronously; SnapshotCreated or SnapshotFailed is emitted before ack.

    // By now the flush thread has run (flush + snapshot); we can assert on observed events without racy early checks.
    {
        let ev = events.lock().unwrap();
        assert!(
            ev.iter().any(|e| matches!(e, DbEvent::FlushThreadStarted)),
            "expected FlushThreadStarted from background flush thread"
        );
        assert!(
            ev.iter().any(|e| matches!(e, DbEvent::SnapshotCreated { .. })),
            "expected at least one SnapshotCreated after snapshot()"
        );
    }

    drop(db);
    // Shutdown is sent; flush thread emits FlushThreadStopping and exits; Drop joins the thread.

    {
        let ev = events.lock().unwrap();
        assert!(
            ev.iter().any(|e| matches!(e, DbEvent::FlushThreadStopping)),
            "expected FlushThreadStopping after DbCore drop"
        );
    }

    println!("event_listener_demo: observed FlushThreadStarted, SnapshotCreated, FlushThreadStopping");
    Ok(())
}
