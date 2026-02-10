//! Durable cardinality state for series keys.
//!
//! Enterprise production requirement: cardinality hard limits must remain enforceable across
//! restarts. Purely in-memory tracking is bypassable and therefore incorrect.
//!
//! This module implements a small, crash-tolerant, append-only journal per scope, plus an optional
//! checkpoint file to bound replay cost.

use crate::error::DbError;
use crate::index::SeriesKey;

use crc32fast::Hasher as Crc32;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const CHECKPOINT_FILE: &str = "checkpoint.bin";
const JOURNAL_FILE: &str = "journal.log";
const SCOPE_META_FILE: &str = "scope.txt";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug)]
pub(crate) struct CardinalityStore {
    root: PathBuf,
    // Serialize appends per process/thread.
    append_lock: Mutex<()>,
}

impl CardinalityStore {
    pub(crate) fn open(root: PathBuf) -> Result<Self, DbError> {
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            append_lock: Mutex::new(()),
        })
    }

    pub(crate) fn scope_dir(&self, scope: &str) -> PathBuf {
        self.root.join(escape_scope(scope))
    }

    /// Loads all scopes found on disk.
    pub(crate) fn load_all_scopes(&self) -> Result<Vec<(String, Vec<SeriesKey>)>, DbError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for e in fs::read_dir(&self.root)?.flatten() {
            let p = e.path();
            if !p.is_dir() {
                continue;
            }
            let scope = read_scope_meta(&p)
                .or_else(|| p.file_name().and_then(|n| n.to_str()).map(unescape_scope))
                .unwrap_or_else(|| "default".to_string());
            let keys = load_scope_dir(&p)?;
            out.push((scope, keys));
        }
        Ok(out)
    }

    /// Appends a newly observed series key to the scope journal.
    ///
    /// Durability:
    /// - Calls `sync_data()` to ensure restart-enforcement even after process crash.
    pub(crate) fn append_scope_key(&self, scope: &str, key: &SeriesKey) -> Result<(), DbError> {
        let _g = self
            .append_lock
            .lock()
            .map_err(|e| DbError::LockError(format!("cardinality store poisoned: {e}")))?;

        let dir = self.scope_dir(scope);
        fs::create_dir_all(&dir)?;
        ensure_scope_meta(&dir, scope)?;
        let journal_path = dir.join(JOURNAL_FILE);

        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)?;

        write_record(&mut f, key)?;
        f.sync_data()?;
        Ok(())
    }

    /// Writes a compact checkpoint (deduped keys) and truncates the journal.
    pub(crate) fn checkpoint_scope(&self, scope: &str, keys: &[SeriesKey]) -> Result<(), DbError> {
        let _g = self
            .append_lock
            .lock()
            .map_err(|e| DbError::LockError(format!("cardinality store poisoned: {e}")))?;

        let dir = self.scope_dir(scope);
        fs::create_dir_all(&dir)?;
        ensure_scope_meta(&dir, scope)?;

        // Deduplicate in-memory (checkpoint is authoritative).
        let mut uniq = std::collections::HashSet::with_capacity(keys.len());
        let mut deduped = Vec::new();
        for k in keys {
            if uniq.insert(k.clone()) {
                deduped.push(k.clone());
            }
        }

        let checkpoint_path = dir.join(CHECKPOINT_FILE);
        let tmp_path = dir.join("checkpoint.bin.tmp");
        let payload = bincode::serialize(&(FORMAT_VERSION, deduped))
            .map_err(|e| DbError::Serialization(e.to_string()))?;

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        write_payload_with_crc(&mut f, &payload)?;
        f.sync_data()?;
        drop(f);
        fs::rename(&tmp_path, &checkpoint_path)?;
        sync_parent_dir(&checkpoint_path)?;

        // Truncate journal after checkpoint install.
        let journal_path = dir.join(JOURNAL_FILE);
        if journal_path.exists() {
            let jf = OpenOptions::new().write(true).open(&journal_path)?;
            jf.set_len(0)?;
            jf.sync_data()?;
        }

        Ok(())
    }
}

fn escape_scope(scope: &str) -> String {
    // Percent-encode any non [A-Za-z0-9._-] bytes.
    let mut out = String::with_capacity(scope.len());
    for b in scope.as_bytes() {
        let c = *b as char;
        if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
            out.push(c);
        } else {
            out.push('%');
            out.push_str(&format!("{:02X}", b));
        }
    }
    if out.is_empty() {
        "default".to_string()
    } else {
        out
    }
}

fn unescape_scope(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |c: u8| -> Option<u8> {
                match c {
                    b'0'..=b'9' => Some(c - b'0'),
                    b'a'..=b'f' => Some(c - b'a' + 10),
                    b'A'..=b'F' => Some(c - b'A' + 10),
                    _ => None,
                }
            };
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| "default".to_string())
}

fn ensure_scope_meta(dir: &Path, scope: &str) -> Result<(), DbError> {
    let p = dir.join(SCOPE_META_FILE);
    if p.exists() {
        return Ok(());
    }
    match OpenOptions::new().create_new(true).write(true).open(&p) {
        Ok(mut f) => {
            f.write_all(scope.as_bytes())?;
            f.write_all(b"\n")?;
            f.sync_data()?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(DbError::Io(e)),
    }
    Ok(())
}

fn read_scope_meta(dir: &Path) -> Option<String> {
    let p = dir.join(SCOPE_META_FILE);
    let mut f = File::open(p).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    let s = s.trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

fn load_scope_dir(dir: &Path) -> Result<Vec<SeriesKey>, DbError> {
    let mut out: Vec<SeriesKey> = Vec::new();

    let checkpoint_path = dir.join(CHECKPOINT_FILE);
    if checkpoint_path.exists() {
        let mut f = File::open(&checkpoint_path)?;
        let keys = read_payload_with_crc(&mut f, &checkpoint_path)?;
        let (version, keys): (u32, Vec<SeriesKey>) =
            bincode::deserialize(&keys).map_err(|e| DbError::Serialization(e.to_string()))?;
        if version != FORMAT_VERSION {
            return Err(DbError::Corruption {
                details: format!(
                    "Unsupported cardinality checkpoint version {} in {:?}",
                    version, checkpoint_path
                ),
                series: None,
                timestamp: None,
            });
        }
        out.extend(keys);
    }

    let journal_path = dir.join(JOURNAL_FILE);
    if journal_path.exists() {
        let mut f = File::open(&journal_path)?;
        loop {
            match read_record(&mut f, &journal_path) {
                Ok(Some(key)) => out.push(key),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }
    }

    Ok(out)
}

fn write_record(w: &mut File, key: &SeriesKey) -> Result<(), DbError> {
    let payload = bincode::serialize(key).map_err(|e| DbError::Serialization(e.to_string()))?;
    let len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("cardinality record too large".to_string()))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&payload)?;
    let mut h = Crc32::new();
    h.update(&len.to_le_bytes());
    h.update(&payload);
    let crc = h.finalize();
    w.write_all(&crc.to_le_bytes())?;
    Ok(())
}

fn read_record(r: &mut File, path: &Path) -> Result<Option<SeriesKey>, DbError> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(DbError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    if let Err(e) = r.read_exact(&mut payload) {
        // Incomplete trailing record (crash) => ignore tail.
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(DbError::Io(e));
    }
    let mut crc_buf = [0u8; 4];
    if let Err(e) = r.read_exact(&mut crc_buf) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(DbError::Io(e));
    }
    let crc = u32::from_le_bytes(crc_buf);
    let mut h = Crc32::new();
    h.update(&len_buf);
    h.update(&payload);
    if h.finalize() != crc {
        return Err(DbError::Corruption {
            details: format!("Cardinality journal checksum mismatch in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let key: SeriesKey =
        bincode::deserialize(&payload).map_err(|e| DbError::Serialization(e.to_string()))?;
    Ok(Some(key))
}

fn write_payload_with_crc(w: &mut File, payload: &[u8]) -> Result<(), DbError> {
    let len: u64 = payload
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("checkpoint too large".to_string()))?;
    w.write_all(&len.to_le_bytes())?;
    let mut h = Crc32::new();
    h.update(payload);
    let crc = h.finalize();
    w.write_all(&crc.to_le_bytes())?;
    w.write_all(payload)?;
    Ok(())
}

fn read_payload_with_crc(r: &mut File, path: &Path) -> Result<Vec<u8>, DbError> {
    let mut len_buf = [0u8; 8];
    r.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf) as usize;
    let mut crc_buf = [0u8; 4];
    r.read_exact(&mut crc_buf)?;
    let expected_crc = u32::from_le_bytes(crc_buf);
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    let mut h = Crc32::new();
    h.update(&payload);
    if h.finalize() != expected_crc {
        return Err(DbError::Corruption {
            details: format!("Cardinality checkpoint checksum mismatch in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    Ok(payload)
}

fn sync_parent_dir(path: &Path) -> Result<(), DbError> {
    let parent = path
        .parent()
        .ok_or_else(|| DbError::Internal("Missing parent dir".to_string()))?;
    let dir = File::open(parent)?;
    dir.sync_data()?;
    Ok(())
}

