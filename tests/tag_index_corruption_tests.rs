use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use ugnos::{DbConfig, DbCore, DbError, TagSet};

// --- black-box manifest reader (copied from segments_engine_tests.rs pattern) ---

#[derive(serde::Deserialize)]
struct TestManifest {
    version: u32,
    next_segment_id: u64,
    delete_before: Option<u64>,
    segments: Vec<TestSegmentRecord>,
}

#[derive(serde::Deserialize)]
struct TestSegmentRecord {
    id: u64,
    level: u8,
    created_at: u64,
    max_seq: u64,
    min_ts: u64,
    max_ts: u64,
    file_name: String,
    series: BTreeMap<String, TestSeriesBlockMeta>,
    #[serde(default)]
    tag_postings_offset: u64,
    #[serde(default)]
    tag_postings_len: u32,
}

#[derive(serde::Deserialize)]
struct TestSeriesBlockMeta {
    offset: u64,
    len: u64,
    row_count: u32,
    min_ts: u64,
    max_ts: u64,
    crc32: u32,
    #[serde(default)]
    tag_index_offset: u64,
    #[serde(default)]
    tag_index_len: u32,
}

fn assert_manifest_invariants(m: &TestManifest) {
    assert_eq!(m.version, 1, "manifest version mismatch");
    if let Some(db) = m.delete_before {
        assert!(db > 0, "delete_before must be > 0 when present");
    }

    let max_id = m.segments.iter().map(|s| s.id).max().unwrap_or(0);
    assert!(
        m.next_segment_id >= max_id.saturating_add(1),
        "next_segment_id must be >= max(segment_id)+1 (max_id={}, next={})",
        max_id,
        m.next_segment_id
    );

    for s in &m.segments {
        assert!(s.id > 0, "segment id must be > 0");
        assert!(!s.file_name.is_empty(), "segment file_name must be non-empty");
        assert!(s.created_at > 0, "segment created_at must be > 0");
        assert!(s.max_seq > 0, "segment max_seq must be > 0");
        assert!(s.min_ts <= s.max_ts, "segment min_ts must be <= max_ts");
        let _ = s.level; // may vary; we only need it to be well-formed

        assert!(
            (s.tag_postings_offset == 0 && s.tag_postings_len == 0)
                || (s.tag_postings_offset > 0 && s.tag_postings_len > 0),
            "tag postings offset/len must be both zero or both non-zero (off={}, len={})",
            s.tag_postings_offset,
            s.tag_postings_len
        );

        for (name, meta) in &s.series {
            assert!(!name.is_empty(), "series name must be non-empty");
            assert!(meta.offset > 0, "series block offset must be > 0");
            assert!(meta.len > 0, "series block len must be > 0");
            assert!(meta.row_count > 0, "series block row_count must be > 0");
            assert!(meta.min_ts <= meta.max_ts, "series meta min_ts <= max_ts");
            assert!(meta.crc32 != 0, "series block crc32 must be non-zero");
            assert!(
                (meta.tag_index_offset == 0 && meta.tag_index_len == 0)
                    || (meta.tag_index_offset > 0 && meta.tag_index_len > 0),
                "tag index offset/len must be both zero or both non-zero (off={}, len={})",
                meta.tag_index_offset,
                meta.tag_index_len
            );
        }
    }
}

fn read_manifest(data_dir: &Path) -> TestManifest {
    use crc32fast::Hasher as Crc32;
    use std::fs::File;
    use std::io::Read;

    // `SegmentStore::open` stores manifest under `data_dir/engine/segments/MANIFEST.bin`
    let manifest_path = data_dir.join("engine").join("segments").join("MANIFEST.bin");
    let mut f = File::open(&manifest_path).expect("manifest exists");

    let mut magic = [0u8; 8];
    f.read_exact(&mut magic).unwrap();
    assert_eq!(&magic, b"UGNMAN01");

    let mut b4 = [0u8; 4];
    f.read_exact(&mut b4).unwrap();
    let _version = u32::from_le_bytes(b4);

    let mut b8 = [0u8; 8];
    f.read_exact(&mut b8).unwrap();
    let len = u64::from_le_bytes(b8) as usize;

    f.read_exact(&mut b4).unwrap();
    let expected_crc = u32::from_le_bytes(b4);

    let mut payload = vec![0u8; len];
    f.read_exact(&mut payload).unwrap();

    let mut hasher = Crc32::new();
    hasher.update(&payload);
    let actual_crc = hasher.finalize();
    assert_eq!(actual_crc, expected_crc, "manifest CRC mismatch");

    let m = bincode::deserialize::<TestManifest>(&payload).unwrap();
    assert_manifest_invariants(&m);
    m
}

fn tags(pairs: &[(&str, &str)]) -> TagSet {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn segment_path(data_dir: &Path, file_name: &str) -> PathBuf {
    data_dir
        .join("engine")
        .join("segments")
        .join(file_name)
}

/// Adversarial test: prove the query path *consults* the on-disk per-block tag index and fails fast
/// on corruption (instead of silently scanning or returning wrong results).
#[test]
fn breakit_corrupt_tag_index_is_detected_and_query_errors() {
    let dir = TempDir::new().unwrap();
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.enable_segments = true;
    cfg.enable_wal = false;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(60 * 60);
    cfg.segment_store.enable_tag_index = true;

    let db = DbCore::with_config(cfg).unwrap();

    // Ensure a segment is created with a non-empty tag index.
    for i in 0..200u64 {
        let host = if i % 20 == 0 { "needle" } else { "haystack" };
        db.insert("m", i, i as f64, tags(&[("host", host), ("region", "r1")]))
            .unwrap();
    }
    db.flush().unwrap();

    let manifest = read_manifest(dir.path());
    assert!(
        !manifest.segments.is_empty(),
        "expected at least one segment record in manifest"
    );

    // Pick the latest segment record.
    let seg = manifest
        .segments
        .iter()
        .max_by_key(|s| s.id)
        .expect("segment record");

    let meta = seg
        .series
        .get("m")
        .expect("series meta for m exists");

    assert!(
        meta.tag_index_len > 0 && meta.tag_index_offset > 0,
        "expected persisted per-block tag index for series block; got offset={}, len={}",
        meta.tag_index_offset,
        meta.tag_index_len
    );

    // Corrupt a byte inside the tag index region (not the trailing crc) so CRC verification fails.
    let seg_path = segment_path(dir.path(), &seg.file_name);
    let mut bytes = std::fs::read(&seg_path).expect("read segment");
    let off = meta.tag_index_offset as usize;
    let len = meta.tag_index_len as usize;
    assert!(off + len <= bytes.len(), "tag index region out of bounds");
    assert!(len > 16, "tag index too small to safely corrupt");
    let corrupt_pos = off + 12;
    bytes[corrupt_pos] ^= 0b0101_1010;
    std::fs::write(&seg_path, bytes).expect("write corrupted segment");

    // Query that would rely on the tag index: should return a corruption error.
    let filter = tags(&[("host", "needle")]);
    let r = db.query("m", 0..500, Some(&filter));
    assert!(
        matches!(r, Err(DbError::Corruption { .. })),
        "expected corruption error after tag index corruption; got: {r:?}"
    );
}

