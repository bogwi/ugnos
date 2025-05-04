//! Core database logic: main API, background flush thread, and orchestration of storage, buffer, and persistence.

use crate::buffer::WriteBuffer;
use crate::error::DbError;
use crate::persistence::{Snapshotter, WriteAheadLog};
use crate::query::execute_query;
use crate::storage::InMemoryStorage;
use crate::types::{DataPoint, TagSet, Timestamp, Value};

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Commands sent to the background flush thread to control flushing, shutdown, and snapshotting.
enum FlushCommand {
    Flush,
    Shutdown,
    Snapshot,
}

/// Configuration options for the DbCore
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// Interval between automatic buffer flushes
    pub flush_interval: Duration,
    /// Directory for persistence files (WAL and snapshots)
    pub data_dir: PathBuf,
    /// Maximum number of entries to buffer in WAL before writing to disk
    pub wal_buffer_size: usize,
    /// Whether to enable WAL (Write-Ahead Logging)
    pub enable_wal: bool,
    /// Whether to enable snapshots
    pub enable_snapshots: bool,
    /// Interval between automatic snapshots (if enabled)
    pub snapshot_interval: Duration,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            flush_interval: Duration::from_secs(1),
            data_dir: PathBuf::from("./data"),
            wal_buffer_size: 1000,
            enable_wal: true,
            enable_snapshots: true,
            snapshot_interval: Duration::from_secs(60 * 15), // 15 minutes
        }
    }
}

/// The main concurrent time-series database core struct.
#[derive(Debug)]
pub struct DbCore {
    /// In-memory storage for all time series data.
    storage: Arc<RwLock<InMemoryStorage>>,
    /// Buffer for staging writes before flush.
    write_buffer: Arc<Mutex<WriteBuffer>>,
    /// Channel sender for flush thread commands.
    flush_cmd_tx: mpsc::Sender<FlushCommand>,
    /// Handle for the background flush thread.
    flush_handle: Option<JoinHandle<()>>,
    /// Write-Ahead Log for durability (if enabled).
    wal: Option<Arc<Mutex<WriteAheadLog>>>,
    /// Snapshot manager (if enabled).
    snapshotter: Option<Arc<Snapshotter>>,
    /// Database configuration.
    config: DbConfig,
}

impl DbCore {
    /// Creates a new `DbCore` instance with the provided configuration.
    ///
    /// This sets up the in-memory storage, write buffer, and (if enabled) persistence mechanisms
    /// such as the Write-Ahead Log (WAL) and snapshotting. It also spawns the background flush thread,
    /// which periodically flushes staged writes to storage and handles snapshot creation.
    ///
    /// # Arguments
    /// * `config` - The database configuration to use.
    ///
    /// # Returns
    /// * `Ok(DbCore)` if initialization succeeds.
    /// * `Err(DbError)` if any component fails to initialize (e.g., WAL or snapshotter).
    ///
    /// # Errors
    /// Returns an error if persistence components cannot be initialized.
    pub fn with_config(config: DbConfig) -> Result<Self, DbError> {
        let storage = Arc::new(RwLock::new(InMemoryStorage::default()));
        let write_buffer = Arc::new(Mutex::new(WriteBuffer::default()));

        // Initialize persistence components if enabled
        let wal = if config.enable_wal {
            let wal_dir = config.data_dir.join("wal");
            let wal = WriteAheadLog::new(wal_dir, config.wal_buffer_size)?;
            Some(Arc::new(Mutex::new(wal)))
        } else {
            None
        };

        let snapshotter = if config.enable_snapshots {
            let snapshot_dir = config.data_dir.join("snapshots");
            let snapshotter = Snapshotter::new(snapshot_dir)?;
            Some(Arc::new(snapshotter))
        } else {
            None
        };

        // Create a channel for communication with the flush thread
        let (flush_cmd_tx, flush_cmd_rx) = mpsc::channel::<FlushCommand>();

        // Clone Arcs for the background thread
        let buffer_clone = Arc::clone(&write_buffer);
        let storage_clone = Arc::clone(&storage);
        let wal_clone = wal.clone();
        let snapshotter_clone = snapshotter.clone();
        let config_clone = config.clone();
        let flush_cmd_tx_clone = flush_cmd_tx.clone(); // Clone the sender for the thread

        // Time tracking for snapshots
        let mut last_snapshot_time = SystemTime::now();

        // The background flush thread periodically flushes the write buffer to storage,
        // handles snapshot creation, and responds to explicit flush/snapshot/shutdown commands.

        // Spawn the background flush thread
        let flush_handle = thread::spawn(move || {
            println!("Flush thread started.");
            loop {
                // Check if it's time for a snapshot
                if config_clone.enable_snapshots {
                    let now = SystemTime::now();
                    if now.duration_since(last_snapshot_time).unwrap_or_default() >= config_clone.snapshot_interval {
                        // It's time for a snapshot, but we'll do it in the next iteration to avoid blocking here
                        let _ = flush_cmd_tx_clone.send(FlushCommand::Snapshot);
                        last_snapshot_time = now;
                    }
                }
                
                // Wait for a command or timeout
                match flush_cmd_rx.recv_timeout(config_clone.flush_interval) {
                    // Received a command to flush or timed out
                    Ok(FlushCommand::Flush) | Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Acquire lock on the buffer
                        let mut buffer_guard = match buffer_clone.lock() {
                            Ok(guard) => guard,
                            Err(poisoned) => {
                                eprintln!(
                                    "Flush thread: Write buffer lock poisoned: {}. Shutting down.",
                                    poisoned
                                );
                                // If the lock is poisoned, we can't recover, so shut down.
                                break;
                            }
                        };

                        // Drain data from the buffer
                        let data_to_flush = buffer_guard.drain_all_buffers();
                        // Release buffer lock *before* acquiring storage lock
                        drop(buffer_guard);

                        if !data_to_flush.is_empty() {
                            println!(
                                "Flush thread: Drained data for {} series. Acquiring storage lock...",
                                data_to_flush.len()
                            );
                            // Acquire write lock on storage
                            let mut storage_guard = match storage_clone.write() {
                                Ok(guard) => guard,
                                Err(poisoned) => {
                                    eprintln!(
                                        "Flush thread: Storage lock poisoned: {}. Shutting down.",
                                        poisoned
                                    );
                                    // If the lock is poisoned, we can't recover, so shut down.
                                    break;
                                }
                            };

                            // Append data to storage
                            match storage_guard.append_batch(data_to_flush) {
                                Ok(_) => println!("Flush thread: Data flushed successfully."),
                                Err(e) => eprintln!("Flush thread: Error flushing data: {}", e),
                            }
                            // Storage lock released here
                        }
                    }
                    // Received command to create a snapshot
                    Ok(FlushCommand::Snapshot) => {
                        if let Some(snapshotter) = &snapshotter_clone {
                            // First flush any pending data
                            let mut buffer_guard = match buffer_clone.lock() {
                                Ok(guard) => guard,
                                Err(_) => continue, // Skip if poisoned
                            };
                            
                            let data_to_flush = buffer_guard.drain_all_buffers();
                            drop(buffer_guard);
                            
                            if !data_to_flush.is_empty() {
                                if let Ok(mut storage_guard) = storage_clone.write() {
                                    let _ = storage_guard.append_batch(data_to_flush);
                                }
                            }
                            
                            // Now create the snapshot
                            if let Ok(storage_guard) = storage_clone.read() {
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as u64;
                                
                                match snapshotter.create_snapshot(storage_guard.get_all_series(), now) {
                                    Ok(path) => println!("Flush thread: Created snapshot at {:?}", path),
                                    Err(e) => eprintln!("Flush thread: Error creating snapshot: {}", e),
                                }
                                
                                // Log the snapshot in WAL if enabled
                                if let Some(wal) = &wal_clone {
                                    if let Ok(mut wal_guard) = wal.lock() {
                                        if let Err(e) = wal_guard.log_flush(now) {
                                            eprintln!("Flush thread: Error logging snapshot to WAL: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Received shutdown command
                    Ok(FlushCommand::Shutdown) => {
                        println!("Flush thread: Received shutdown command. Flushing one last time...");
                        // Perform a final flush before shutting down
                        let mut buffer_guard = match buffer_clone.lock() {
                            Ok(guard) => guard,
                            Err(_) => break, // Already poisoned, just exit
                        };
                        let data_to_flush = buffer_guard.drain_all_buffers();
                        drop(buffer_guard);
                        
                        if !data_to_flush.is_empty() {
                            if let Ok(mut storage_guard) = storage_clone.write() {
                                let _ = storage_guard.append_batch(data_to_flush);
                            }
                        }
                        
                        // Flush and close WAL if enabled
                        if let Some(wal) = &wal_clone {
                            if let Ok(mut wal_guard) = wal.lock() {
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as u64;
                                
                                // Log the final flush
                                if let Err(e) = wal_guard.log_flush(now) {
                                    eprintln!("Flush thread: Error logging final flush to WAL: {}", e);
                                }
                                
                                // Close the WAL
                                if let Err(e) = wal_guard.close() {
                                    eprintln!("Flush thread: Error closing WAL: {}", e);
                                }
                            }
                        }
                        
                        println!("Flush thread: Final flush complete.");
                        println!("Flush thread: Exiting.");
                        break; // Exit the loop
                    }
                    // Channel disconnected (DbCore dropped)
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        println!("Flush thread: Command channel disconnected. Exiting.");
                        break; // Exit the loop
                    }
                }
            }
        });

        Ok(DbCore {
            storage,
            write_buffer,
            flush_cmd_tx,
            flush_handle: Some(flush_handle),
            wal,
            snapshotter,
            config,
        })
    }

    /// Creates a new `DbCore` instance with default configuration, but with a custom flush interval.
    ///
    /// This is a convenience constructor for quickly creating a database with a specific flush interval.
    /// All other configuration options use their default values.
    ///
    /// # Arguments
    /// * `flush_interval` - The interval between automatic buffer flushes.
    ///
    /// # Panics
    /// Panics if the database cannot be initialized with the default configuration.
    pub fn new(flush_interval: Duration) -> Self {
        let mut config = DbConfig::default();
        config.flush_interval = flush_interval;
        Self::with_config(config).expect("Failed to initialize DbCore with default configuration")
    }
    
    /// Recovers the database state from disk using the latest snapshot and any newer WAL entries.
    ///
    /// This method should be called after constructing the database if you want to restore
    /// persisted data. It loads the most recent snapshot (if enabled), then applies any
    /// WAL entries that occurred after the snapshot.
    ///
    /// # Returns
    /// * `Ok(())` if recovery succeeds or if persistence is not enabled.
    /// * `Err(DbError)` if recovery fails.
    ///
    /// # Errors
    /// Returns an error if loading the snapshot or WAL fails.
    pub fn recover(&mut self) -> Result<(), DbError> {
        if self.snapshotter.is_none() && self.wal.is_none() {
            // No persistence enabled, nothing to recover
            return Ok(());
        }
        
        // First try to load from the latest snapshot
        let mut latest_timestamp = 0;
        if let Some(snapshotter) = &self.snapshotter {
            if let Some(data) = snapshotter.load_latest_snapshot()? {
                // Get latest snapshot timestamp
                if let Ok(Some(ts)) = snapshotter.get_latest_snapshot_timestamp() {
                    latest_timestamp = ts;
                }
                
                // Load snapshot data into storage
                let mut storage_guard = self.storage.write()?;
                for (series, points) in data {
                    storage_guard.append_points(&series, points)?;
                }
                
                println!("Recovered from snapshot with timestamp {}", latest_timestamp);
            }
        }
        
        // Apply any WAL entries that are newer than the snapshot
        if let Some(wal) = &self.wal {
            let wal_entries = wal.lock()?.read_all_entries()?;
            
            let mut pending_inserts = std::collections::HashMap::new();
            
            for entry in wal_entries {
                match entry {
                    crate::persistence::WalEntry::Insert { series, timestamp, value, tags } => {
                        // Only apply if newer than snapshot
                        if timestamp > latest_timestamp {
                            let point = DataPoint { timestamp, value, tags };
                            pending_inserts
                                .entry(series)
                                .or_insert_with(Vec::new)
                                .push(point);
                        }
                    },
                    crate::persistence::WalEntry::Flush { timestamp } => {
                        // This was a flush or snapshot point
                        latest_timestamp = timestamp;
                        
                        // Apply all pending inserts
                        if !pending_inserts.is_empty() {
                            let mut storage_guard = self.storage.write()?;
                            for (series, points) in pending_inserts.drain() {
                                storage_guard.append_points(&series, points)?;
                            }
                        }
                    }
                }
            }
            
            // Apply any remaining pending inserts
            if !pending_inserts.is_empty() {
                let mut storage_guard = self.storage.write()?;
                for (series, points) in pending_inserts {
                    storage_guard.append_points(&series, points)?;
                }
            }
            
            println!("Applied WAL entries");
        }
        
        Ok(())
    }

    /// Inserts a data point into the specified time series.
    ///
    /// This method is thread-safe and can be called concurrently from multiple threads.
    /// The data point is first staged in the write buffer and will be flushed to storage
    /// either automatically (by the background thread) or manually (via `flush()`).
    /// If WAL is enabled, the insert is also logged for durability.
    ///
    /// # Arguments
    /// * `series` - Name of the time series.
    /// * `timestamp` - Timestamp of the data point.
    /// * `value` - Value to insert.
    /// * `tags` - Associated tags for the data point.
    ///
    /// # Returns
    /// * `Ok(())` if the data point is staged successfully.
    /// * `Err(DbError)` if staging or logging fails.
    ///
    /// # Errors
    /// Returns an error if the WAL or write buffer cannot be accessed.
    pub fn insert(
        &self,
        series: &str,
        timestamp: Timestamp,
        value: Value,
        tags: TagSet,
    ) -> Result<(), DbError> {
        let point = DataPoint { timestamp, value, tags: tags.clone() };
        
        // Log to WAL if enabled
        if let Some(wal) = &self.wal {
            let mut wal_guard = wal.lock()?;
            wal_guard.log_insert(series, timestamp, value, tags)?;
        }
        
        // Acquire lock on the write buffer
        let mut buffer_guard = self.write_buffer.lock()?; // Propagate PoisonError
        // Stage the data point
        buffer_guard.stage(series, point)
    }

    /// Queries data points from a specific time series within a given time range,
    /// optionally filtering by a set of tags.
    ///
    /// This method is thread-safe and allows concurrent queries. It acquires a read lock
    /// on the storage and the relevant series chunk, then executes the query in parallel.
    ///
    /// # Arguments
    /// * `series` - The name of the time series to query.
    /// * `time_range` - The time range for the query (start inclusive, end exclusive).
    /// * `tag_filter` - An optional set of tags to filter by. Only points matching all tags are returned.
    ///
    /// # Returns
    /// * `Ok(Vec<(Timestamp, Value)>)` with all matching data points.
    /// * `Err(DbError)` if the series does not exist or a lock cannot be acquired.
    ///
    /// # Errors
    /// Returns an error if the series is not found or if a lock is poisoned.
    pub fn query(
        &self,
        series: &str,
        time_range: Range<Timestamp>,
        tag_filter: Option<&TagSet>,
    ) -> Result<Vec<(Timestamp, Value)>, DbError> {
        // Acquire read lock on the storage
        let storage_guard = self.storage.read()?; // Propagate PoisonError

        // Get the specific series chunk (as an Arc<RwLock<TimeSeriesChunk>>)
        let chunk_arc = storage_guard
            .get_chunk_for_query(series)
            .ok_or_else(|| DbError::SeriesNotFound(series.to_string()))?;

        // Acquire read lock on the specific chunk
        // This allows concurrent queries on the same series
        let chunk_guard = chunk_arc.read()?; // Propagate PoisonError

        // Execute the query using the query module function
        execute_query(chunk_guard, time_range, tag_filter)
    }

    /// Triggers an immediate flush of the write buffer to storage.
    ///
    /// This sends a command to the background flush thread to flush all staged data points
    /// to the in-memory storage. Useful for testing or ensuring data is persisted before shutdown.
    ///
    /// # Returns
    /// * `Ok(())` if the flush command is sent successfully.
    /// * `Err(DbError)` if the command cannot be sent.
    ///
    /// # Errors
    /// Returns an error if the background thread cannot be reached.
    pub fn flush(&self) -> Result<(), DbError> {
        self.flush_cmd_tx.send(FlushCommand::Flush).map_err(|e| {
            DbError::BackgroundTaskError(format!("Failed to send flush command: {}", e))
        })
    }
    
    /// Triggers an immediate snapshot of the current database state.
    ///
    /// This sends a command to the background flush thread to create a snapshot of all
    /// in-memory data. Snapshots are only available if enabled in the configuration.
    ///
    /// # Returns
    /// * `Ok(())` if the snapshot command is sent successfully.
    /// * `Err(DbError)` if snapshots are not enabled or the command cannot be sent.
    ///
    /// # Errors
    /// Returns an error if snapshots are disabled or if the background thread cannot be reached.
    pub fn snapshot(&self) -> Result<(), DbError> {
        if self.snapshotter.is_none() {
            return Err(DbError::ConfigError("Snapshots are not enabled".to_string()));
        }
        
        self.flush_cmd_tx.send(FlushCommand::Snapshot).map_err(|e| {
            DbError::BackgroundTaskError(format!("Failed to send snapshot command: {}", e))
        })
    }
    
    /// Returns a reference to the current database configuration.
    ///
    /// This allows inspection of the configuration used to initialize the database.
    ///
    /// # Returns
    /// * A reference to the `DbConfig` struct.
    pub fn get_config(&self) -> &DbConfig {
        &self.config
    }
}

/// Default implementation uses a 1-second flush interval.
impl Default for DbCore {
    fn default() -> Self {
        Self::with_config(DbConfig::default()).expect("Failed to initialize DbCore with default configuration")
    }
}

/// Implement Drop to gracefully shut down the background flush thread.
impl Drop for DbCore {
    fn drop(&mut self) {
        println!("DbCore dropping. Sending shutdown command to flush thread...");
        // Send the shutdown command, ignoring potential errors if the thread already panicked
        let _ = self.flush_cmd_tx.send(FlushCommand::Shutdown);

        // Wait for the flush thread to finish
        if let Some(handle) = self.flush_handle.take() {
            println!("Waiting for flush thread to exit...");
            if let Err(e) = handle.join() {
                eprintln!("Flush thread panicked: {:?}", e);
            }
            println!("Flush thread joined.");
        } else {
            println!("Flush thread handle already taken or thread never started.");
        }
        println!("DbCore dropped.");
    }
}

