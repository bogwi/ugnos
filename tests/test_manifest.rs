use std::path::Path;

/// Test-only manifest reader (black-box): validates CRC and deserializes payload.
///
/// `SegmentStore::open` stores manifest under `data_dir/engine/segments/MANIFEST.bin`.
pub fn read_manifest(dir: &Path) -> TestManifest {
    use crc32fast::Hasher as Crc32;
    use std::fs::File;
    use std::io::Read;

    let manifest_path = dir.join("engine").join("segments").join("MANIFEST.bin");
    let mut f = File::open(&manifest_path).unwrap();

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

#[derive(serde::Deserialize)]
pub struct TestManifest {
    pub version: u32,
    pub next_segment_id: u64,
    pub delete_before: Option<u64>,
    pub segments: Vec<TestSegmentRecord>,
}

#[derive(serde::Deserialize)]
pub struct TestSegmentRecord {
    pub id: u64,
    pub level: u8,
    pub created_at: u64,
    pub max_seq: u64,
    pub min_ts: u64,
    pub max_ts: u64,
    pub file_name: String,
    pub series: std::collections::BTreeMap<String, TestSeriesBlockMeta>,
    #[serde(default)]
    pub tag_postings_offset: u64,
    #[serde(default)]
    pub tag_postings_len: u32,
}

#[derive(serde::Deserialize)]
pub struct TestSeriesBlockMeta {
    pub offset: u64,
    pub len: u64,
    pub row_count: u32,
    pub min_ts: u64,
    pub max_ts: u64,
    pub crc32: u32,
    #[serde(default)]
    pub tag_index_offset: u64,
    #[serde(default)]
    pub tag_index_len: u32,
}

fn assert_manifest_invariants(m: &TestManifest) {
    assert_eq!(m.version, 1, "manifest version mismatch");
    assert!(
        !m.segments.is_empty(),
        "expected manifest to contain at least one segment"
    );
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
        assert!(
            !s.file_name.is_empty(),
            "segment file_name must be non-empty"
        );
        assert!(
            s.level <= 1,
            "segment level must be 0 or 1 in tests (got {})",
            s.level
        );
        assert!(s.created_at > 0, "segment created_at must be > 0");
        assert!(s.max_seq > 0, "segment max_seq must be > 0");
        assert!(s.min_ts <= s.max_ts, "segment min_ts must be <= max_ts");

        // Postings index fields should be consistent (both present or both absent).
        assert!(
            (s.tag_postings_offset == 0 && s.tag_postings_len == 0)
                || (s.tag_postings_offset > 0 && s.tag_postings_len > 0),
            "tag postings offset/len must be both zero or both non-zero (off={}, len={})",
            s.tag_postings_offset,
            s.tag_postings_len
        );

        // Series block metadata should be self-consistent.
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
