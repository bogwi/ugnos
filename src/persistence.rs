use crate::error::DbError;
use crate::types::{DataPoint, TagSet, Timestamp, Value};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};

const WAL_MAGIC: &[u8; 8] = b"UGNWAL01";
const WAL_VERSION: u32 = 2;

const WAL_REC_INSERT: u8 = 1;
const WAL_REC_FLUSH: u8 = 2;

const SNAP_MAGIC: &[u8; 8] = b"UGNSNP01";
const SNAP_VERSION: u32 = 1;

/// Represents an entry in the Write-Ahead Log
#[derive(Serialize, Deserialize, Debug)]
pub enum WalEntry {
    Insert {
        seq: u64,
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
    log_dir: PathBuf,
    log_path: PathBuf,
    pending_entries: Vec<WalEntry>,
    max_pending_entries: usize,
    format_version: u32,
}

impl WriteAheadLog {
    /// Creates a new WAL instance with the specified log file path
    pub fn new<P: AsRef<Path>>(log_dir: P, max_pending_entries: usize) -> Result<Self, DbError> {
        fs::create_dir_all(&log_dir)?;
        let log_dir = log_dir.as_ref().to_path_buf();
        
        let log_path = log_dir.join("wal.log");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)
            .map_err(DbError::Io)?;

        // If file is empty, write header (magic + version).
        // If file is non-empty but not at the current format version, rotate it aside and start fresh.
        let len = file.metadata().map_err(DbError::Io)?.len();
        let format_version = if len == 0 {
            write_wal_header(&mut file, WAL_VERSION)?;
            WAL_VERSION
        } else {
            match detect_wal_version(&log_path)? {
                Some(v) if v == WAL_VERSION => WAL_VERSION,
                _ => {
                    // Upgrade path: preserve old file for replay and start a new WAL in the current format.
                    drop(file);
                    let upgraded = log_dir.join(format!("wal_upgrade_{:020}.log", now_ns()));
                    if log_path.exists() {
                        fs::rename(&log_path, &upgraded)?;
                        sync_dir(&log_dir)?;
                    }
                    let mut file2 = OpenOptions::new()
                        .create(true)
                        .read(true)
                        .append(true)
                        .open(&log_path)
                        .map_err(DbError::Io)?;
                    write_wal_header(&mut file2, WAL_VERSION)?;
                    file = file2;
                    WAL_VERSION
                }
            }
        };

        let log_file = BufWriter::new(file);

        Ok(WriteAheadLog {
            log_file: Some(log_file),
            log_dir,
            log_path,
            pending_entries: Vec::with_capacity(max_pending_entries),
            max_pending_entries,
            format_version,
        })
    }

    /// Logs an insert operation to the WAL
    pub fn log_insert(
        &mut self, 
        seq: u64,
        series: &str, 
        timestamp: Timestamp, 
        value: Value, 
        tags: TagSet
    ) -> Result<(), DbError> {
        let entry = WalEntry::Insert {
            seq,
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
                match self.format_version {
                    2 => {
                        let payload = encode_wal_record_v2(entry)?;
                        let mut hasher = Crc32::new();
                        hasher.update(&payload);
                        let crc = hasher.finalize();
                        let len = payload.len() as u32;
                        log_file.write_all(&len.to_le_bytes())?;
                        log_file.write_all(&crc.to_le_bytes())?;
                        log_file.write_all(&payload)?;
                    }
                    // Legacy: length-prefixed bincode without checksum.
                    _ => {
                        let serialized = bincode::serialize(entry)
                            .map_err(|e| DbError::Serialization(e.to_string()))?;
                        let len = serialized.len() as u32;
                        log_file.write_all(&len.to_le_bytes())?;
                        log_file.write_all(&serialized)?;
                    }
                }
            }
            
            // Ensure it's written to disk
            log_file.flush()?;
            log_file.get_ref().sync_data()?;
            
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

    /// Truncates the WAL back to just the header (checkpoint).
    ///
    /// This is safe to call after all data up to this point has been made durable elsewhere
    /// (e.g., persisted into segment files and the manifest fsync'd).
    pub fn checkpoint_truncate(&mut self) -> Result<(), DbError> {
        self.flush_to_disk()?;
        self.log_file = None;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.log_path)
            .map_err(DbError::Io)?;

        write_wal_header(&mut file, WAL_VERSION)?;

        // Reopen for append.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.log_path)
            .map_err(DbError::Io)?;
        self.log_file = Some(BufWriter::new(file));
        self.format_version = WAL_VERSION;
        Ok(())
    }

    /// Rotates `wal.log` to a timestamped file and starts a new `wal.log`.
    ///
    /// This is used to keep WAL replay bounded when segment files exist. The returned path
    /// can be deleted after the corresponding segment commit is durable.
    pub fn rotate(&mut self, now_ns: u64) -> Result<PathBuf, DbError> {
        self.flush_to_disk()?;
        self.log_file = None;

        let rotated = self.log_dir.join(format!("wal_{:020}.log", now_ns));
        // If wal.log doesn't exist (shouldn't happen), just create a new one.
        if self.log_path.exists() {
            fs::rename(&self.log_path, &rotated)?;
            sync_dir(&self.log_dir)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.log_path)
            .map_err(DbError::Io)?;
        write_wal_header(&mut file, WAL_VERSION)?;
        self.log_file = Some(BufWriter::new(file));
        self.format_version = WAL_VERSION;
        Ok(rotated)
    }

    /// Reads entries from all WAL log files (`wal_*.log` then `wal.log`).
    pub fn read_all_entries_all_logs(&self) -> Result<Vec<WalEntry>, DbError> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(&self.log_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("wal_") && name.ends_with(".log") {
                paths.push(entry.path());
            }
        }
        paths.sort();
        paths.push(self.log_path.clone());

        let mut out = Vec::new();
        for p in paths {
            if p.exists() {
                out.extend(read_entries_from_path(&p)?);
            }
        }
        Ok(out)
    }
    
    /// Reads all entries from the current WAL file (`wal.log`).
    pub fn read_all_entries(&self) -> Result<Vec<WalEntry>, DbError> {
        read_entries_from_path(&self.log_path)
    }
}

fn write_wal_header(file: &mut File, version: u32) -> Result<(), DbError> {
    file.write_all(WAL_MAGIC).map_err(DbError::Io)?;
    file.write_all(&version.to_le_bytes()).map_err(DbError::Io)?;
    file.flush().map_err(DbError::Io)?;
    file.sync_data().map_err(DbError::Io)?;
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<(), DbError> {
    let d = File::open(dir).map_err(DbError::Io)?;
    d.sync_data().map_err(DbError::Io)?;
    Ok(())
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn detect_wal_version(path: &Path) -> Result<Option<u32>, DbError> {
    let mut f = File::open(path)?;
    let mut magic = [0u8; 8];
    if let Err(e) = f.read_exact(&mut magic) {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(DbError::Io(e));
    }
    if &magic != WAL_MAGIC {
        return Ok(None);
    }
    let mut ver = [0u8; 4];
    f.read_exact(&mut ver)?;
    Ok(Some(u32::from_le_bytes(ver)))
}

fn encode_wal_record_v2(entry: &WalEntry) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    match entry {
        WalEntry::Insert { seq, series, timestamp, value, tags } => {
            buf.push(WAL_REC_INSERT);
            buf.extend_from_slice(&seq.to_le_bytes());
            buf.extend_from_slice(&timestamp.to_le_bytes());
            let s = series.as_bytes();
            let slen = u32::try_from(s.len()).map_err(|_| DbError::Internal("Series name too large".to_string()))?;
            buf.extend_from_slice(&slen.to_le_bytes());
            buf.extend_from_slice(s);
            buf.extend_from_slice(&value.to_le_bytes());
            let tags_bytes = bincode::serialize(tags).map_err(|e| DbError::Serialization(e.to_string()))?;
            let tlen = u32::try_from(tags_bytes.len()).map_err(|_| DbError::Internal("Tags blob too large".to_string()))?;
            buf.extend_from_slice(&tlen.to_le_bytes());
            buf.extend_from_slice(&tags_bytes);
        }
        WalEntry::Flush { timestamp } => {
            buf.push(WAL_REC_FLUSH);
            buf.extend_from_slice(&timestamp.to_le_bytes());
        }
    }
    Ok(buf)
}

fn wal_v2_extract_context_best_effort(payload: &[u8]) -> (Option<String>, Option<u64>) {
    use std::io::Cursor;
    let mut cur = Cursor::new(payload);
    let mut typ = [0u8; 1];
    if cur.read_exact(&mut typ).is_err() {
        return (None, None);
    }
    if typ[0] != WAL_REC_INSERT {
        return (None, None);
    }
    let mut b8 = [0u8; 8];
    if cur.read_exact(&mut b8).is_err() {
        return (None, None);
    }
    if cur.read_exact(&mut b8).is_err() {
        return (None, None);
    }
    let ts = u64::from_le_bytes(b8);
    let mut b4 = [0u8; 4];
    if cur.read_exact(&mut b4).is_err() {
        return (None, Some(ts));
    }
    let slen = u32::from_le_bytes(b4) as usize;
    let mut sb = vec![0u8; slen];
    if cur.read_exact(&mut sb).is_err() {
        return (None, Some(ts));
    }
    let series = String::from_utf8(sb).ok();
    (series, Some(ts))
}

fn decode_wal_record_v2(payload: &[u8]) -> Result<WalEntry, String> {
    use std::io::Cursor;
    let mut cur = Cursor::new(payload);
    let mut typ = [0u8; 1];
    cur.read_exact(&mut typ).map_err(|e| e.to_string())?;
    match typ[0] {
        WAL_REC_INSERT => {
            let mut b8 = [0u8; 8];
            cur.read_exact(&mut b8).map_err(|e| e.to_string())?;
            let seq = u64::from_le_bytes(b8);
            cur.read_exact(&mut b8).map_err(|e| e.to_string())?;
            let timestamp = u64::from_le_bytes(b8);

            let mut b4 = [0u8; 4];
            cur.read_exact(&mut b4).map_err(|e| e.to_string())?;
            let slen = u32::from_le_bytes(b4) as usize;
            let mut sb = vec![0u8; slen];
            cur.read_exact(&mut sb).map_err(|e| e.to_string())?;
            let series = String::from_utf8(sb).map_err(|e| e.to_string())?;

            cur.read_exact(&mut b8).map_err(|e| e.to_string())?;
            let value = f64::from_le_bytes(b8);

            cur.read_exact(&mut b4).map_err(|e| e.to_string())?;
            let tlen = u32::from_le_bytes(b4) as usize;
            let mut tb = vec![0u8; tlen];
            cur.read_exact(&mut tb).map_err(|e| e.to_string())?;
            let tags: TagSet = bincode::deserialize(&tb).map_err(|e| e.to_string())?;

            Ok(WalEntry::Insert { seq, series, timestamp, value, tags })
        }
        WAL_REC_FLUSH => {
            let mut b8 = [0u8; 8];
            cur.read_exact(&mut b8).map_err(|e| e.to_string())?;
            let timestamp = u64::from_le_bytes(b8);
            Ok(WalEntry::Flush { timestamp })
        }
        other => Err(format!("Unknown WAL record type {}", other)),
    }
}

fn read_entries_from_path(path: &Path) -> Result<Vec<WalEntry>, DbError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut entries = Vec::new();

    // Detect optional header.
    let mut magic = [0u8; 8];
    let mut version: Option<u32> = None;
    if let Ok(()) = reader.read_exact(&mut magic) {
        if &magic == WAL_MAGIC {
            let mut ver = [0u8; 4];
            reader.read_exact(&mut ver)?;
            version = Some(u32::from_le_bytes(ver));
        } else {
            // Pre-header WAL: rewind to start so legacy loop works.
            reader = BufReader::new(File::open(path)?);
        }
    }

    match version {
        Some(2) => {
            // Checksummed records.
            loop {
                let rec_start_off = reader.stream_position().unwrap_or(0);

                let mut len_bytes = [0u8; 4];
                match reader.read_exact(&mut len_bytes) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(DbError::Io(e)),
                }
                let len = u32::from_le_bytes(len_bytes) as usize;

                let mut crc_bytes = [0u8; 4];
                reader.read_exact(&mut crc_bytes)?;
                let expected_crc = u32::from_le_bytes(crc_bytes);

                let mut payload = vec![0u8; len];
                reader.read_exact(&mut payload)?;

                let mut hasher = Crc32::new();
                hasher.update(&payload);
                let actual_crc = hasher.finalize();
                if actual_crc != expected_crc {
                    let (series, timestamp) = wal_v2_extract_context_best_effort(&payload);
                    return Err(DbError::Corruption {
                        details: format!(
                            "WAL record CRC mismatch in {:?} at offset {}",
                            path, rec_start_off
                        ),
                        series,
                        timestamp,
                    });
                }

                let entry = decode_wal_record_v2(&payload).map_err(|e| DbError::Corruption {
                    details: format!(
                        "WAL record decode failed in {:?} at offset {}: {}",
                        path, rec_start_off, e
                    ),
                    series: None,
                    timestamp: None,
                })?;
                entries.push(entry);
            }
        }
        // Legacy formats (v1 or pre-header): length-prefixed bincode without checksums.
        _ => loop {
            // Read length of next entry
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {}
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
        },
    }

    Ok(entries)
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
        // Build payload in-memory so we can checksum it.
        let mut series_names: Vec<&String> = series_data.keys().collect();
        series_names.sort();

        let mut payload: Vec<u8> = Vec::new();
        let series_count = series_names.len() as u32;
        payload.extend_from_slice(&series_count.to_le_bytes());

        for series_name in series_names {
            let name_bytes = series_name.as_bytes();
            let name_len = name_bytes.len() as u32;
            payload.extend_from_slice(&name_len.to_le_bytes());
            payload.extend_from_slice(name_bytes);

            let chunk_arc = series_data
                .get(series_name)
                .ok_or_else(|| DbError::Internal("Snapshot series map mutated during snapshot".to_string()))?;
            let chunk = chunk_arc.read().map_err(|e| {
                DbError::LockError(format!("Failed to acquire read lock on chunk: {}", e))
            })?;

            let point_count = chunk.len() as u32;
            payload.extend_from_slice(&point_count.to_le_bytes());

            for i in 0..chunk.len() {
                payload.extend_from_slice(&chunk.timestamps[i].to_le_bytes());
                payload.extend_from_slice(&chunk.values[i].to_le_bytes());

                let tags = &chunk.tags[i];
                let tags_count = tags.len() as u32;
                payload.extend_from_slice(&tags_count.to_le_bytes());

                // Deterministic tag ordering.
                let mut kv: Vec<(&String, &String)> = tags.iter().collect();
                kv.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
                for (key, value) in kv {
                    let key_bytes = key.as_bytes();
                    let key_len = key_bytes.len() as u32;
                    payload.extend_from_slice(&key_len.to_le_bytes());
                    payload.extend_from_slice(key_bytes);

                    let value_bytes = value.as_bytes();
                    let value_len = value_bytes.len() as u32;
                    payload.extend_from_slice(&value_len.to_le_bytes());
                    payload.extend_from_slice(value_bytes);
                }
            }
        }

        let mut hasher = Crc32::new();
        hasher.update(&payload);
        let crc = hasher.finalize();

        // Atomic install: write to temp, fsync, rename, fsync dir.
        let final_path = self.snapshot_dir.join(format!("snapshot_{}.bin", timestamp));
        let tmp_path = self.snapshot_dir.join(format!(".tmp_snapshot_{}.bin", timestamp));

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut w = BufWriter::new(file);
        w.write_all(SNAP_MAGIC)?;
        w.write_all(&SNAP_VERSION.to_le_bytes())?;
        w.write_all(&timestamp.to_le_bytes())?;
        w.write_all(&(payload.len() as u64).to_le_bytes())?;
        w.write_all(&crc.to_le_bytes())?;
        w.write_all(&payload)?;
        w.flush()?;
        w.get_ref().sync_data()?;
        drop(w);

        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.snapshot_dir)?;
        Ok(final_path)
    }
    
    /// Loads the latest snapshot
    pub fn load_latest_snapshot(&self) -> Result<Option<HashMap<String, Vec<DataPoint>>>, DbError> {
        let mut snaps = list_snapshots(&self.snapshot_dir)?;
        snaps.sort_by(|a, b| b.0.cmp(&a.0));

        let Some((_, latest_path)) = snaps.first() else {
            return Ok(None);
        };

        let file = File::open(latest_path)?;
        let mut reader = BufReader::new(file);

        // Header
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != SNAP_MAGIC {
            return Err(DbError::Corruption {
                details: format!("Bad snapshot magic in {:?}", latest_path),
                series: None,
                timestamp: None,
            });
        }
        let mut v = [0u8; 4];
        reader.read_exact(&mut v)?;
        let version = u32::from_le_bytes(v);
        if version != SNAP_VERSION {
            return Err(DbError::Corruption {
                details: format!("Unsupported snapshot version {} in {:?}", version, latest_path),
                series: None,
                timestamp: None,
            });
        }
        let mut b8 = [0u8; 8];
        reader.read_exact(&mut b8)?;
        let _timestamp = u64::from_le_bytes(b8);
        reader.read_exact(&mut b8)?;
        let payload_len = u64::from_le_bytes(b8) as usize;
        let mut c4 = [0u8; 4];
        reader.read_exact(&mut c4)?;
        let expected_crc = u32::from_le_bytes(c4);

        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;
        let mut hasher = Crc32::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            return Err(DbError::Corruption {
                details: format!("Snapshot payload CRC mismatch in {:?}", latest_path),
                series: None,
                timestamp: None,
            });
        }

        let mut cur = std::io::Cursor::new(payload);
        let mut series_count_bytes = [0u8; 4];
        cur.read_exact(&mut series_count_bytes)?;
        let series_count = u32::from_le_bytes(series_count_bytes) as usize;
        
        let mut result = HashMap::with_capacity(series_count);
        
        // Read each series
        for _ in 0..series_count {
            // Read series name
            let mut name_len_bytes = [0u8; 4];
            cur.read_exact(&mut name_len_bytes)?;
            let name_len = u32::from_le_bytes(name_len_bytes) as usize;
            
            let mut name_bytes = vec![0u8; name_len];
            cur.read_exact(&mut name_bytes)?;
            let series_name = String::from_utf8(name_bytes)
                .map_err(|e| DbError::Internal(format!("Invalid UTF-8 in series name: {}", e)))?;
            
            // Read point count
            let mut point_count_bytes = [0u8; 4];
            cur.read_exact(&mut point_count_bytes)?;
            let point_count = u32::from_le_bytes(point_count_bytes) as usize;
            
            let mut points = Vec::with_capacity(point_count);
            
            // Read all data points
            for _ in 0..point_count {
                // Read timestamp
                let mut ts_bytes = [0u8; 8];
                cur.read_exact(&mut ts_bytes)?;
                let timestamp = u64::from_le_bytes(ts_bytes);
                
                // Read value
                let mut val_bytes = [0u8; 8];
                cur.read_exact(&mut val_bytes)?;
                let value = f64::from_le_bytes(val_bytes);
                
                // Read tags
                let mut tags_count_bytes = [0u8; 4];
                cur.read_exact(&mut tags_count_bytes)?;
                let tags_count = u32::from_le_bytes(tags_count_bytes) as usize;
                
                let mut tags = HashMap::with_capacity(tags_count);
                
                for _ in 0..tags_count {
                    // Read key
                    let mut key_len_bytes = [0u8; 4];
                    cur.read_exact(&mut key_len_bytes)?;
                    let key_len = u32::from_le_bytes(key_len_bytes) as usize;
                    
                    let mut key_bytes = vec![0u8; key_len];
                    cur.read_exact(&mut key_bytes)?;
                    let key = String::from_utf8(key_bytes)
                        .map_err(|e| DbError::Internal(format!("Invalid UTF-8 in tag key: {}", e)))?;
                    
                    // Read value
                    let mut value_len_bytes = [0u8; 4];
                    cur.read_exact(&mut value_len_bytes)?;
                    let value_len = u32::from_le_bytes(value_len_bytes) as usize;
                    
                    let mut value_bytes = vec![0u8; value_len];
                    cur.read_exact(&mut value_bytes)?;
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
        let mut snaps = list_snapshots(&self.snapshot_dir)?;
        snaps.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(snaps.first().map(|(ts, _)| *ts))
    }
}

fn list_snapshots(dir: &Path) -> Result<Vec<(Timestamp, PathBuf)>, DbError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("snapshot_") || !name.ends_with(".bin") {
            continue;
        }
        let ts_part = name
            .strip_prefix("snapshot_")
            .and_then(|s| s.strip_suffix(".bin"));
        let Some(ts_str) = ts_part else { continue };
        let Ok(ts) = ts_str.parse::<u64>() else { continue };
        out.push((ts, entry.path()));
    }
    Ok(out)
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
        wal.log_insert(1, "series1", ts1, 1.0, tags1.clone()).unwrap();
        wal.log_insert(2, "series1", ts2, 2.0, tags1.clone()).unwrap();
        
        // Verify pending entries (not yet flushed)
        assert_eq!(wal.pending_entries.len(), 2);
        
        // This should trigger auto-flush (max_pending_entries = 3)
        wal.log_insert(3, "series2", ts3, 3.0, tags2.clone()).unwrap();
        
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
            WalEntry::Insert { seq, series, timestamp, value, tags } => {
                assert_eq!(*seq, 1);
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