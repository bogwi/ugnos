use crate::error::DbError;
use crate::types::{DataPoint, TagSet, Timestamp, Value};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use serde::{Deserialize, Serialize};

/// Represents an entry in the Write-Ahead Log
#[derive(Serialize, Deserialize, Debug)]
pub enum WalEntry {
    Insert {
        series: String,
        timestamp: Timestamp,
        value: Value,
        tags: TagSet,
    },
    Flush {
        timestamp: Timestamp,
    },
}

/// Handles Write-Ahead Logging operations
#[derive(Debug)]
pub struct WriteAheadLog {
    log_file: Option<BufWriter<File>>,
    log_path: PathBuf,
    pending_entries: Vec<WalEntry>,
    max_pending_entries: usize,
}

impl WriteAheadLog {
    /// Creates a new WAL instance with the specified log file path
    pub fn new<P: AsRef<Path>>(log_dir: P, max_pending_entries: usize) -> Result<Self, DbError> {
        fs::create_dir_all(&log_dir)?;
        
        let log_path = log_dir.as_ref().join("wal.log");
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map(BufWriter::new)
            .map_err(|e| DbError::Io(e))?;

        Ok(WriteAheadLog {
            log_file: Some(log_file),
            log_path,
            pending_entries: Vec::with_capacity(max_pending_entries),
            max_pending_entries,
        })
    }

    /// Logs an insert operation to the WAL
    pub fn log_insert(
        &mut self, 
        series: &str, 
        timestamp: Timestamp, 
        value: Value, 
        tags: TagSet
    ) -> Result<(), DbError> {
        let entry = WalEntry::Insert {
            series: series.to_string(),
            timestamp,
            value,
            tags,
        };
        
        self.pending_entries.push(entry);
        
        // If we've reached the maximum number of pending entries, flush to disk
        if self.pending_entries.len() >= self.max_pending_entries {
            self.flush_to_disk()?;
        }
        
        Ok(())
    }

    /// Flushes pending entries to disk
    pub fn flush_to_disk(&mut self) -> Result<(), DbError> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }
        
        if let Some(log_file) = &mut self.log_file {
            for entry in &self.pending_entries {
                let serialized = bincode::serialize(entry)
                    .map_err(|e| DbError::Serialization(e.to_string()))?;
                
                // Write length of entry first (for easier reading)
                let len = serialized.len() as u32;
                log_file.write_all(&len.to_le_bytes())?;
                
                // Write the serialized entry
                log_file.write_all(&serialized)?;
            }
            
            // Ensure it's written to disk
            log_file.flush()?;
            
            // Clear pending entries
            self.pending_entries.clear();
            
            Ok(())
        } else {
            Err(DbError::Internal("WAL file not initialized".to_string()))
        }
    }
    
    /// Records a flush operation in the WAL
    pub fn log_flush(&mut self, timestamp: Timestamp) -> Result<(), DbError> {
        let entry = WalEntry::Flush { timestamp };
        self.pending_entries.push(entry);
        self.flush_to_disk()
    }
    
    /// Closes the WAL file
    pub fn close(&mut self) -> Result<(), DbError> {
        // Flush any pending entries
        self.flush_to_disk()?;
        
        // Close the file
        self.log_file = None;
        
        Ok(())
    }
    
    /// Reads all entries from the WAL file
    pub fn read_all_entries(&self) -> Result<Vec<WalEntry>, DbError> {
        let file = File::open(&self.log_path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        
        loop {
            // Read length of next entry
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(DbError::Io(e)),
            }
            
            let len = u32::from_le_bytes(len_bytes) as usize;
            
            // Read the serialized entry
            let mut entry_bytes = vec![0u8; len];
            reader.read_exact(&mut entry_bytes)?;
            
            // Deserialize the entry
            let entry = bincode::deserialize(&entry_bytes)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            
            entries.push(entry);
        }
        
        Ok(entries)
    }
}

/// Handles snapshot operations
#[derive(Debug)]
pub struct Snapshotter {
    snapshot_dir: PathBuf,
}

impl Snapshotter {
    /// Creates a new Snapshotter instance
    pub fn new<P: AsRef<Path>>(snapshot_dir: P) -> Result<Self, DbError> {
        let dir_path = snapshot_dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir_path)?;
        
        Ok(Snapshotter {
            snapshot_dir: dir_path,
        })
    }
    
    /// Creates a snapshot of the database state
    pub fn create_snapshot(
        &self,
        series_data: &HashMap<String, Arc<RwLock<crate::types::TimeSeriesChunk>>>,
        timestamp: Timestamp,
    ) -> Result<PathBuf, DbError> {
        let snapshot_path = self.snapshot_dir.join(format!("snapshot_{}.bin", timestamp));
        let file = File::create(&snapshot_path)?;
        let mut writer = BufWriter::new(file);
        
        // Write the number of series
        let series_count = series_data.len() as u32;
        writer.write_all(&series_count.to_le_bytes())?;
        
        // Serialize each series data
        for (series_name, chunk_arc) in series_data {
            // Write series name length and name
            let name_bytes = series_name.as_bytes();
            let name_len = name_bytes.len() as u32;
            writer.write_all(&name_len.to_le_bytes())?;
            writer.write_all(name_bytes)?;
            
            // Get read lock on the chunk
            let chunk = chunk_arc.read().map_err(|e| 
                DbError::LockError(format!("Failed to acquire read lock on chunk: {}", e)))?;
            
            // Write number of data points
            let point_count = chunk.len() as u32;
            writer.write_all(&point_count.to_le_bytes())?;
            
            // Write all data points
            for i in 0..chunk.len() {
                // Write timestamp
                writer.write_all(&chunk.timestamps[i].to_le_bytes())?;
                
                // Write value
                writer.write_all(&chunk.values[i].to_le_bytes())?;
                
                // Write tags
                let tags = &chunk.tags[i];
                let tags_count = tags.len() as u32;
                writer.write_all(&tags_count.to_le_bytes())?;
                
                for (key, value) in tags {
                    // Write key length and key
                    let key_bytes = key.as_bytes();
                    let key_len = key_bytes.len() as u32;
                    writer.write_all(&key_len.to_le_bytes())?;
                    writer.write_all(key_bytes)?;
                    
                    // Write value length and value
                    let value_bytes = value.as_bytes();
                    let value_len = value_bytes.len() as u32;
                    writer.write_all(&value_len.to_le_bytes())?;
                    writer.write_all(value_bytes)?;
                }
            }
        }
        
        writer.flush()?;
        
        Ok(snapshot_path)
    }
    
    /// Loads the latest snapshot
    pub fn load_latest_snapshot(&self) -> Result<Option<HashMap<String, Vec<DataPoint>>>, DbError> {
        let mut entries = fs::read_dir(&self.snapshot_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_name()
                    .to_string_lossy()
                    .starts_with("snapshot_")
            })
            .collect::<Vec<_>>();
        
        if entries.is_empty() {
            return Ok(None);
        }
        
        // Sort by filename to get the latest snapshot
        entries.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
        
        let latest = &entries[0];
        let file = File::open(latest.path())?;
        let mut reader = BufReader::new(file);
        
        // Read number of series
        let mut series_count_bytes = [0u8; 4];
        reader.read_exact(&mut series_count_bytes)?;
        let series_count = u32::from_le_bytes(series_count_bytes) as usize;
        
        let mut result = HashMap::with_capacity(series_count);
        
        // Read each series
        for _ in 0..series_count {
            // Read series name
            let mut name_len_bytes = [0u8; 4];
            reader.read_exact(&mut name_len_bytes)?;
            let name_len = u32::from_le_bytes(name_len_bytes) as usize;
            
            let mut name_bytes = vec![0u8; name_len];
            reader.read_exact(&mut name_bytes)?;
            let series_name = String::from_utf8(name_bytes)
                .map_err(|e| DbError::Internal(format!("Invalid UTF-8 in series name: {}", e)))?;
            
            // Read point count
            let mut point_count_bytes = [0u8; 4];
            reader.read_exact(&mut point_count_bytes)?;
            let point_count = u32::from_le_bytes(point_count_bytes) as usize;
            
            let mut points = Vec::with_capacity(point_count);
            
            // Read all data points
            for _ in 0..point_count {
                // Read timestamp
                let mut ts_bytes = [0u8; 8];
                reader.read_exact(&mut ts_bytes)?;
                let timestamp = u64::from_le_bytes(ts_bytes);
                
                // Read value
                let mut val_bytes = [0u8; 8];
                reader.read_exact(&mut val_bytes)?;
                let value = f64::from_le_bytes(val_bytes);
                
                // Read tags
                let mut tags_count_bytes = [0u8; 4];
                reader.read_exact(&mut tags_count_bytes)?;
                let tags_count = u32::from_le_bytes(tags_count_bytes) as usize;
                
                let mut tags = HashMap::with_capacity(tags_count);
                
                for _ in 0..tags_count {
                    // Read key
                    let mut key_len_bytes = [0u8; 4];
                    reader.read_exact(&mut key_len_bytes)?;
                    let key_len = u32::from_le_bytes(key_len_bytes) as usize;
                    
                    let mut key_bytes = vec![0u8; key_len];
                    reader.read_exact(&mut key_bytes)?;
                    let key = String::from_utf8(key_bytes)
                        .map_err(|e| DbError::Internal(format!("Invalid UTF-8 in tag key: {}", e)))?;
                    
                    // Read value
                    let mut value_len_bytes = [0u8; 4];
                    reader.read_exact(&mut value_len_bytes)?;
                    let value_len = u32::from_le_bytes(value_len_bytes) as usize;
                    
                    let mut value_bytes = vec![0u8; value_len];
                    reader.read_exact(&mut value_bytes)?;
                    let value = String::from_utf8(value_bytes)
                        .map_err(|e| DbError::Internal(format!("Invalid UTF-8 in tag value: {}", e)))?;
                    
                    tags.insert(key, value);
                }
                
                points.push(DataPoint {
                    timestamp,
                    value,
                    tags,
                });
            }
            
            result.insert(series_name, points);
        }
        
        Ok(Some(result))
    }
    
    /// Finds the latest snapshot timestamp
    pub fn get_latest_snapshot_timestamp(&self) -> Result<Option<Timestamp>, DbError> {
        let entries = fs::read_dir(&self.snapshot_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_name()
                    .to_string_lossy()
                    .starts_with("snapshot_")
            })
            .collect::<Vec<_>>();
        
        if entries.is_empty() {
            return Ok(None);
        }
        
        // Parse timestamps from filenames (format: snapshot_<timestamp>.bin)
        let mut timestamps = Vec::new();
        
        for entry in &entries {
            let filename = entry.file_name().to_string_lossy().into_owned();
            let parts: Vec<&str> = filename.split('_').collect();
            if parts.len() < 2 {
                continue;
            }
            
            let ts_part_with_ext = parts[1];
            let ts_part = ts_part_with_ext.split('.').next();
            
            if let Some(ts_str) = ts_part {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    timestamps.push(ts);
                }
            }
        }
        
        if timestamps.is_empty() {
            return Ok(None);
        }
        
        timestamps.sort_by(|a, b| b.cmp(a));
        Ok(Some(timestamps[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::fs;
    use std::thread;
    use tempfile::tempdir;
    use crate::types::TimeSeriesChunk;

    // Helper function to get current timestamp in nanoseconds
    fn get_current_timestamp() -> Timestamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    // Helper function to create a TagSet
    fn create_tags(pairs: &[(&str, &str)]) -> TagSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // Helper function to create a TimeSeriesChunk with test data
    fn create_test_chunk(points: Vec<DataPoint>) -> TimeSeriesChunk {
        let mut chunk = TimeSeriesChunk::default();
        for point in points {
            chunk.timestamps.push(point.timestamp);
            chunk.values.push(point.value);
            chunk.tags.push(point.tags);
        }
        chunk
    }

    #[test]
    fn test_wal_basic_operations() {
        // Create a temporary directory for the WAL
        let temp_dir = tempdir().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        
        // Create WAL with small buffer
        let mut wal = WriteAheadLog::new(&wal_dir, 3).unwrap();
        
        // Insert test data with real timestamps
        let ts1 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts3 = get_current_timestamp();
        
        let tags1 = create_tags(&[("host", "server1"), ("region", "us-east")]);
        let tags2 = create_tags(&[("host", "server2"), ("region", "us-west")]);
        
        // Log inserts
        wal.log_insert("series1", ts1, 1.0, tags1.clone()).unwrap();
        wal.log_insert("series1", ts2, 2.0, tags1.clone()).unwrap();
        
        // Verify pending entries (not yet flushed)
        assert_eq!(wal.pending_entries.len(), 2);
        
        // This should trigger auto-flush (max_pending_entries = 3)
        wal.log_insert("series2", ts3, 3.0, tags2.clone()).unwrap();
        
        // Verify flush occurred (pending entries cleared)
        assert_eq!(wal.pending_entries.len(), 0);
        
        // Log flush event
        let flush_ts = get_current_timestamp();
        wal.log_flush(flush_ts).unwrap();
        
        // Explicitly close the WAL
        wal.close().unwrap();
        
        // Create a new WAL instance to read the log
        let wal2 = WriteAheadLog::new(&wal_dir, 3).unwrap();
        let entries = wal2.read_all_entries().unwrap();
        
        // Verify all entries are present
        assert_eq!(entries.len(), 4); // 3 inserts + 1 flush
        
        // Verify specific entries
        match &entries[0] {
            WalEntry::Insert { series, timestamp, value, tags } => {
                assert_eq!(series, "series1");
                assert_eq!(*timestamp, ts1);
                assert_eq!(*value, 1.0);
                assert_eq!(*tags, tags1);
            },
            _ => panic!("Expected Insert entry"),
        }
        
        match &entries[3] {
            WalEntry::Flush { timestamp } => {
                assert_eq!(*timestamp, flush_ts);
            },
            _ => panic!("Expected Flush entry"),
        }
    }
    
    #[test]
    fn test_wal_empty_flush() {
        let temp_dir = tempdir().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        
        let mut wal = WriteAheadLog::new(&wal_dir, 10).unwrap();
        
        // Flush with no pending entries
        wal.flush_to_disk().unwrap();
        
        // Should not create any entries
        let entries = wal.read_all_entries().unwrap();
        assert_eq!(entries.len(), 0);
    }
    
    #[test]
    fn test_snapshotter_create_and_load() {
        // Create a temporary directory for snapshots
        let temp_dir = tempdir().unwrap();
        let snapshot_dir = temp_dir.path().join("snapshots");
        
        // Create a Snapshotter
        let snapshotter = Snapshotter::new(&snapshot_dir).unwrap();
        
        // Create test data with real timestamps
        let ts1 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = get_current_timestamp();
        
        let tags1 = create_tags(&[("host", "server1")]);
        let tags2 = create_tags(&[("host", "server2")]);
        
        let points1 = vec![
            DataPoint { timestamp: ts1, value: 1.0, tags: tags1.clone() },
            DataPoint { timestamp: ts2, value: 2.0, tags: tags2.clone() },
        ];
        
        // Create a TimeSeriesChunk with our test data
        let chunk = create_test_chunk(points1);
        
        // Create a HashMap to simulate storage
        let mut data = HashMap::new();
        data.insert("test_series".to_string(), Arc::new(RwLock::new(chunk)));
        
        // Create a snapshot
        let snapshot_ts = get_current_timestamp();
        let snapshot_path = snapshotter.create_snapshot(&data, snapshot_ts).unwrap();
        
        // Verify snapshot file was created
        assert!(snapshot_path.exists());
        
        // Load the snapshot
        let loaded_data = snapshotter.load_latest_snapshot().unwrap().unwrap();
        
        // Verify loaded data
        assert_eq!(loaded_data.len(), 1);
        assert!(loaded_data.contains_key("test_series"));
        
        let loaded_points = &loaded_data["test_series"];
        assert_eq!(loaded_points.len(), 2);
        
        // Verify point details
        assert_eq!(loaded_points[0].timestamp, ts1);
        assert_eq!(loaded_points[0].value, 1.0);
        assert_eq!(loaded_points[0].tags, tags1);
        
        assert_eq!(loaded_points[1].timestamp, ts2);
        assert_eq!(loaded_points[1].value, 2.0);
        assert_eq!(loaded_points[1].tags, tags2);
        
        // Verify timestamp extraction
        let extracted_ts = snapshotter.get_latest_snapshot_timestamp().unwrap().unwrap();
        assert_eq!(extracted_ts, snapshot_ts);
    }
    
    #[test]
    fn test_snapshotter_no_snapshots() {
        // Create a temporary directory for snapshots
        let temp_dir = tempdir().unwrap();
        let snapshot_dir = temp_dir.path().join("empty_snapshots");
        
        // Create directory
        fs::create_dir_all(&snapshot_dir).unwrap();
        
        // Create a Snapshotter
        let snapshotter = Snapshotter::new(&snapshot_dir).unwrap();
        
        // Try to load from empty directory
        let loaded = snapshotter.load_latest_snapshot().unwrap();
        assert!(loaded.is_none());
        
        // Try to get latest timestamp
        let ts = snapshotter.get_latest_snapshot_timestamp().unwrap();
        assert!(ts.is_none());
    }
    
    #[test]
    fn test_snapshotter_multiple_snapshots() {
        // Create a temporary directory for snapshots
        let temp_dir = tempdir().unwrap();
        let snapshot_dir = temp_dir.path().join("multi_snapshots");
        
        // Create a Snapshotter
        let snapshotter = Snapshotter::new(&snapshot_dir).unwrap();
        
        // Create simple test data
        let ts = get_current_timestamp();
        let point = DataPoint { 
            timestamp: ts, 
            value: 1.0, 
            tags: create_tags(&[("test", "tag")]) 
        };
        
        let chunk = create_test_chunk(vec![point]);
        
        // Create storage with one series
        let mut data = HashMap::new();
        data.insert("series".to_string(), Arc::new(RwLock::new(chunk)));
        
        // Create first snapshot
        let snapshot_ts1 = get_current_timestamp();
        snapshotter.create_snapshot(&data, snapshot_ts1).unwrap();
        
        // Wait to ensure different timestamp
        thread::sleep(std::time::Duration::from_millis(10));
        
        // Create second snapshot
        let snapshot_ts2 = get_current_timestamp();
        snapshotter.create_snapshot(&data, snapshot_ts2).unwrap();
        
        // Verify latest timestamp is the second one
        let latest_ts = snapshotter.get_latest_snapshot_timestamp().unwrap().unwrap();
        assert_eq!(latest_ts, snapshot_ts2);
        
        // Verify we load the latest snapshot
        let loaded = snapshotter.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(loaded.len(), 1);
    }
} 