use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crc32fast::Hasher as Crc32;
use tempfile::tempdir;

use ugnos::encoding::{
    BlockCompression, FloatEncoding, SegmentEncodingConfig, TagEncoding, SER_BLOCK_MAGIC,
};
use ugnos::{DbConfig, DbCore, DbError, TagSet};

mod test_manifest;

const SEG_MAGIC: &[u8; 8] = b"UGNSEG01";

// V2 series block header layout (encoding.rs): magic[0..8], version[8..12], row_count[12..16],
// codec[16..20], compression_param[20..24], uncompressed_len[24..28], 
// uncompressed_crc32[28..32], stored_len[32..36], payload[36..].
// Acceptance criterion: storage format includes block-level checksums and versioning.
const SERIES_BLOCK_V2_VERSION_OFFSET: usize = 8;
const SERIES_BLOCK_V2_CRC_OFFSET: usize = 28;
const SERIES_BLOCK_V2_HEADER_LEN: usize = 36;
const SERIES_BLOCK_V2_VERSION: u32 = 2;
const SEG_FOOTER_MAGIC: &[u8; 8] = b"UGNSEGF1";
const FOOTER_LEN: usize = 8 + 8 + 8 + 4; // magic + index_off + index_len + crc32

fn make_cfg(dir: &Path, encoding: SegmentEncodingConfig) -> DbConfig {
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.to_path_buf();
    cfg.enable_segments = true;
    cfg.enable_wal = false;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(3600); // reduce background noise
    cfg.segment_store.encoding = encoding;
    cfg
}

fn seg_total_bytes(dir: &Path) -> u64 {
    let seg_dir = dir.join("engine").join("segments");
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(seg_dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with("seg_") && name.ends_with(".seg") {
                if let Ok(m) = e.metadata() {
                    total = total.saturating_add(m.len());
                }
            }
        }
    }
    total
}

fn first_segment_path(cfg: &DbConfig) -> PathBuf {
    let seg_dir = cfg.data_dir.join("engine").join("segments");
    for e in std::fs::read_dir(&seg_dir).expect("read segment dir") {
        let e = e.expect("dir entry");
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with("seg_") && name.ends_with(".seg") {
            return e.path();
        }
    }
    panic!("expected a segment file under {:?}", seg_dir);
}

fn read_u32_le(bytes: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(bytes[off..off + 4].try_into().expect("u32 slice"))
}

fn read_u64_le(bytes: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(bytes[off..off + 8].try_into().expect("u64 slice"))
}

fn read_string(bytes: &[u8], off: &mut usize) -> String {
    let len = read_u32_le(bytes, *off) as usize;
    *off += 4;
    let s = std::str::from_utf8(&bytes[*off..*off + len]).expect("utf8 string");
    *off += len;
    s.to_string()
}

#[derive(Debug, Clone, Copy)]
struct SeriesIndexEntry {
    offset: u64,
    len: u64,
}

fn parse_segment_index(seg_bytes: &[u8]) -> BTreeMap<String, SeriesIndexEntry> {
    assert!(seg_bytes.len() >= FOOTER_LEN, "segment file too small");

    // Footer fields are little-endian and fixed-size.
    let footer_start = seg_bytes.len() - FOOTER_LEN;
    let footer_magic: &[u8] = &seg_bytes[footer_start..footer_start + 8];
    assert_eq!(footer_magic, SEG_FOOTER_MAGIC, "bad segment footer magic");

    let index_offset = read_u64_le(seg_bytes, footer_start + 8);
    let index_len = read_u64_le(seg_bytes, footer_start + 16);
    let footer_crc = read_u32_le(seg_bytes, footer_start + 24);

    // Validate footer CRC (protects against accidental index offset/len corruption).
    let mut footer_hasher = Crc32::new();
    footer_hasher.update(&index_offset.to_le_bytes());
    footer_hasher.update(&index_len.to_le_bytes());
    assert_eq!(footer_hasher.finalize(), footer_crc, "footer CRC mismatch");

    let index_offset_usz: usize = index_offset.try_into().expect("index_offset usize");
    let index_len_usz: usize = index_len.try_into().expect("index_len usize");
    assert!(
        index_offset_usz
            .checked_add(index_len_usz)
            .unwrap_or(usize::MAX)
            <= seg_bytes.len(),
        "index range out of bounds"
    );
    let index_bytes = &seg_bytes[index_offset_usz..index_offset_usz + index_len_usz];

    const INDEX_VERSION_TAG_INDEX: u8 = 2;
    let mut off = 0usize;
    let (series_count, entry_extra) = if index_bytes.len() >= 5 && index_bytes[0] == INDEX_VERSION_TAG_INDEX {
        let count = u32::from_le_bytes(index_bytes[1..5].try_into().expect("count bytes")) as usize;
        off = 5;
        // version 2: after offset/len we have row_count, min_ts, max_ts, crc32, tag_index_offset, tag_index_len
        (count, 4 + 8 + 8 + 4 + 8 + 4)
    } else {
        let count = read_u32_le(index_bytes, off) as usize;
        off = 4;
        (count, 4 + 8 + 8 + 4)
    };

    let mut out: BTreeMap<String, SeriesIndexEntry> = BTreeMap::new();
    for _ in 0..series_count {
        let name = read_string(index_bytes, &mut off);
        let offset = read_u64_le(index_bytes, off);
        off += 8;
        let len = read_u64_le(index_bytes, off);
        off += 8;
        off += entry_extra;
        out.insert(name, SeriesIndexEntry { offset, len });
    }
    out
}

/// Acceptance criterion: 
/// "Storage format includes block-level checksums and versioning."
/// This test would FAIL if the format were changed to
/// remove the version field or the payload checksum field.
#[test]
fn test_ac_storage_format_includes_block_level_checksum_and_version() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::Raw64,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::None,
    };
    let cfg = make_cfg(dir.path(), encoding);

    {
        let db = DbCore::with_config(cfg.clone()).unwrap();
        db.insert("s", 1, 1.0, TagSet::new()).unwrap();
        db.insert("s", 2, 2.0, TagSet::new()).unwrap();
        db.flush().unwrap();
    }

    let seg_path = first_segment_path(&cfg);
    let bytes = std::fs::read(&seg_path).unwrap();
    let index = parse_segment_index(&bytes);
    let ent = index.get("s").expect("series s in index");
    let off: usize = ent.offset.try_into().expect("offset");
    let len: usize = ent.len.try_into().expect("len");
    assert!(off + len <= bytes.len(), "block range in bounds");
    let block = &bytes[off..off + len];

    assert!(
        block.len() >= SERIES_BLOCK_V2_HEADER_LEN,
        "block must have full v2 header including checksum and version; len={}",
        block.len()
    );
    assert_eq!(
        &block[0..8],
        SER_BLOCK_MAGIC,
        "storage format must include series block magic"
    );
    let version = read_u32_le(block, SERIES_BLOCK_V2_VERSION_OFFSET);
    assert_eq!(
        version, SERIES_BLOCK_V2_VERSION,
        "storage format must include block version (v2) at offset {}",
        SERIES_BLOCK_V2_VERSION_OFFSET
    );
    let payload_checksum = read_u32_le(block, SERIES_BLOCK_V2_CRC_OFFSET);
    assert_ne!(
        payload_checksum, 0,
        "block-level payload checksum must be present (non-zero for non-empty payload) at offset {}",
        SERIES_BLOCK_V2_CRC_OFFSET
    );
}

#[test]
fn test_encoding_and_compression_roundtrip_with_weird_floats_and_tags() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::GorillaXor,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::Zstd { level: 1 },
    };
    let cfg = make_cfg(dir.path(), encoding);
    let db = DbCore::with_config(cfg.clone()).unwrap();

    let series = "s";
    let mut tags0: TagSet = TagSet::new();
    tags0.insert("host".to_string(), "a".to_string());
    tags0.insert("region".to_string(), "us-east-1".to_string());

    let mut tags1: TagSet = TagSet::new();
    tags1.insert("host".to_string(), "b".to_string());
    tags1.insert("region".to_string(), "us-east-1".to_string());

    // Exercise:
    // - out-of-order timestamps (forces sort + delta encoding correctness)
    // - float bit patterns (NaN payload, infinities, -0.0)
    // - varying tagsets and tag filter matching
    let values = [
        f64::from_bits(0x7ff8_0000_0000_0001), // a specific quiet NaN payload
        f64::INFINITY,
        -0.0,
        1.25,
        f64::NEG_INFINITY,
        1.25,
    ];
    let timestamps = [100u64, 80, 90, 90, 10_000_000_000, 91];
    let tags = [
        tags0.clone(),
        tags0.clone(),
        tags1.clone(),
        tags0.clone(),
        tags1.clone(),
        tags0.clone(),
    ];

    for i in 0..timestamps.len() {
        db.insert(series, timestamps[i], values[i], tags[i].clone())
            .unwrap();
    }
    db.flush().unwrap();

    // This test asserts ordering because it is a single-segment query.
    // Global ordering across *multiple* segments is explicitly not guaranteed (see README).
    let m = test_manifest::read_manifest(&cfg.data_dir);
    assert_eq!(
        m.segments.len(),
        1,
        "expected exactly one segment; if this becomes multi-segment, do not assume query order"
    );

    // Query full range (no tags) and verify ordering + float bits preserved.
    let got = db.query(series, 0..u64::MAX, None).unwrap();
    assert_eq!(got.len(), timestamps.len());
    // Actually verify ordering: results must be in non-decreasing timestamp order.
    assert!(
        got.windows(2).all(|w| w[1].0 >= w[0].0),
        "query must return points in timestamp order, got: {:?}",
        got.iter().map(|(ts, _)| ts).collect::<Vec<_>>()
    );

    // Spot-check that at least one NaN payload survived bit-exactly.
    let want_nan_bits = values[0].to_bits();
    let got_nan_bits = got
        .iter()
        .find_map(|(_, v)| if v.is_nan() { Some(v.to_bits()) } else { None })
        .expect("expected at least one NaN to survive");
    assert_eq!(
        got_nan_bits, want_nan_bits,
        "NaN payload must roundtrip exactly"
    );

    // Tag-filtered query
    let mut filter: TagSet = TagSet::new();
    filter.insert("region".to_string(), "us-east-1".to_string());
    filter.insert("host".to_string(), "a".to_string());
    let got2 = db.query(series, 0..u64::MAX, Some(&filter)).unwrap();
    assert!(!got2.is_empty());
}

#[test]
fn test_block_checksums_detect_corruption() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::Raw64,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::Lz4,
    };
    let cfg = make_cfg(dir.path(), encoding);
    let db = DbCore::with_config(cfg.clone()).unwrap();

    let series = "corrupt_me";
    let tags: TagSet = TagSet::new();
    for ts in 0u64..200u64 {
        db.insert(series, ts, (ts as f64) * 0.125, tags.clone())
            .unwrap();
    }
    db.flush().unwrap();

    // Pick the series block deterministically and flip a byte inside it.
    // This guards against false negatives where we corrupt unrelated regions.
    let seg_path = first_segment_path(&cfg);
    let mut bytes = std::fs::read(&seg_path).unwrap();
    assert!(bytes.starts_with(SEG_MAGIC), "bad segment header magic");

    let index = parse_segment_index(&bytes);
    let ent = *index.get(series).expect("expected target series in index");
    let off: usize = ent.offset.try_into().expect("block offset usize");
    let len: usize = ent.len.try_into().expect("block len usize");
    assert!(off + len <= bytes.len(), "block range out of bounds");
    assert!(len > 32, "block too small for corruption test");

    let idx = off + (len / 2);
    bytes[idx] ^= 0x5A;
    std::fs::write(&seg_path, bytes).unwrap();

    // Next query must fail with corruption rather than silently returning wrong results.
    let err = db.query(series, 0..u64::MAX, None).unwrap_err();
    match err {
        DbError::Corruption { .. } => {}
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[test]
fn test_segment_header_magic_is_validated_on_startup() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::Raw64,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::None,
    };
    let cfg = make_cfg(dir.path(), encoding);

    {
        let db = DbCore::with_config(cfg.clone()).unwrap();
        db.insert("s", 1, 1.0, TagSet::new()).unwrap();
        db.flush().unwrap();
    }

    let seg_path = first_segment_path(&cfg);
    let mut bytes = std::fs::read(&seg_path).unwrap();
    assert!(bytes.len() >= 12);
    bytes[0] ^= 0xFF; // break magic
    std::fs::write(&seg_path, bytes).unwrap();

    let err = DbCore::with_config(cfg).unwrap_err();
    match err {
        DbError::Corruption { .. } => {}
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[test]
fn test_segment_version_is_validated_on_startup() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::Raw64,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::None,
    };
    let cfg = make_cfg(dir.path(), encoding);

    {
        let db = DbCore::with_config(cfg.clone()).unwrap();
        db.insert("s", 1, 1.0, TagSet::new()).unwrap();
        db.flush().unwrap();
    }

    let seg_path = first_segment_path(&cfg);
    let mut bytes = std::fs::read(&seg_path).unwrap();
    assert!(bytes.starts_with(SEG_MAGIC));
    // Header: [magic:8][version:u32 little-endian]...
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes()); // unsupported
    std::fs::write(&seg_path, bytes).unwrap();

    let err = DbCore::with_config(cfg).unwrap_err();
    match err {
        DbError::Corruption { .. } => {}
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[test]
fn test_space_reduction_with_compression_is_measurable() {
    let dir0 = tempdir().unwrap();
    let dir1 = tempdir().unwrap();

    // Baseline: no compression.
    let base_enc = SegmentEncodingConfig {
        float_encoding: FloatEncoding::Raw64,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::None,
    };
    // Optimized: Gorilla + Zstd(1).
    let opt_enc = SegmentEncodingConfig {
        float_encoding: FloatEncoding::GorillaXor,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::Zstd { level: 1 },
    };

    let db0 = DbCore::with_config(make_cfg(dir0.path(), base_enc)).unwrap();
    let db1 = DbCore::with_config(make_cfg(dir1.path(), opt_enc)).unwrap();

    // Make data intentionally compressible:
    // - monotonic timestamps (good for delta varint)
    // - slowly changing values (good for Gorilla + Zstd)
    // - repeated tag keys/values (good for dictionary + Zstd)
    let series = "space";
    for i in 0u64..20_000u64 {
        let mut tags: TagSet = TagSet::new();
        tags.insert("host".to_string(), "api-1".to_string());
        tags.insert("region".to_string(), "us-east-1".to_string());
        tags.insert("shard".to_string(), format!("s{}", i % 4));
        let v = (i as f64) * 0.001;
        db0.insert(series, i, v, tags.clone()).unwrap();
        db1.insert(series, i, v, tags).unwrap();
    }
    db0.flush().unwrap();
    db1.flush().unwrap();

    let b0 = seg_total_bytes(dir0.path());
    let b1 = seg_total_bytes(dir1.path());
    assert!(b0 > 0 && b1 > 0);

    // We expect a real reduction, not noise.
    // Keep the threshold conservative but meaningful.
    assert!(
        b1 < (b0 * 2 / 10),
        "expected >=80% size reduction; baseline={}B optimized={}B",
        b0,
        b1
    );
}

/// Acceptance criterion: 
/// "measurable space reduction without breaking p99 latency targets." 
/// This test asserts p99 query latency stays within a target when using encoded/compressed segments.
const P99_QUERY_LATENCY_TARGET_MS: u64 = 200;

#[test]
fn test_ac_query_latency_with_encoded_segments_within_target() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::GorillaXor,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::Zstd { level: 1 },
    };
    let cfg = make_cfg(dir.path(), encoding);
    let db = DbCore::with_config(cfg).unwrap();

    let series = "latency_test";
    let tags: TagSet = TagSet::new();
    for ts in 0u64..5_000u64 {
        db.insert(series, ts, (ts as f64) * 0.001, tags.clone())
            .unwrap();
    }
    db.flush().unwrap();

    let range = 0u64..u64::MAX;
    let mut latencies_ms: Vec<u64> = Vec::with_capacity(500);
    for _ in 0..500 {
        let start = std::time::Instant::now();
        let _ = db.query(series, range.clone(), None).unwrap();
        latencies_ms.push(start.elapsed().as_millis() as u64);
    }
    latencies_ms.sort_unstable();
    let p99_idx = (latencies_ms.len() * 99) / 100;
    let p99_ms = latencies_ms[p99_idx.min(latencies_ms.len().saturating_sub(1))];

    assert!(
        p99_ms <= P99_QUERY_LATENCY_TARGET_MS,
        "p99 query latency {} ms must not exceed target {} ms (encoding & compression must not break latency)",
        p99_ms,
        P99_QUERY_LATENCY_TARGET_MS
    );
}


/// If delta encoding or decoder is wrong, 
/// reordered timestamps could produce wrong or silent corruption.
#[test]
fn test_breakit_roundtrip_preserves_timestamp_order_and_values_bit_exact() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::GorillaXor,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::Zstd { level: 3 },
    };
    let cfg = make_cfg(dir.path(), encoding);
    let db = DbCore::with_config(cfg).unwrap();

    let series = "order";
    let mut tags: TagSet = TagSet::new();
    tags.insert("k".to_string(), "v".to_string());

    let timestamps: Vec<u64> = (0..500).map(|i| 1_000_000 + i * 100).collect();
    let values: Vec<f64> = (0..500)
        .map(|i| f64::from_bits((i as u64).wrapping_mul(0x0008_0000_0000_0001)))
        .collect();
    for i in 0..timestamps.len() {
        db.insert(series, timestamps[i], values[i], tags.clone())
            .unwrap();
    }
    db.flush().unwrap();

    let got = db.query(series, 0..u64::MAX, None).unwrap();
    assert_eq!(
        got.len(),
        timestamps.len(),
        "row count must roundtrip exactly"
    );
    for (i, (ts, val)) in got.iter().enumerate() {
        assert_eq!(
            *ts, timestamps[i],
            "timestamp at index {} must roundtrip",
            i
        );
        assert_eq!(
            val.to_bits(),
            values[i].to_bits(),
            "value at index {} must roundtrip bit-exact",
            i
        );
    }
}

/// If checksum were skipped, 
/// corrupting only the payload (and not the header) could return wrong data.
/// We corrupt a byte in the block and expect a corruption error (segment block CRC or payload CRC).
#[test]
fn test_breakit_corrupt_any_byte_in_block_yields_corruption_error() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::Raw64,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::None,
    };
    let cfg = make_cfg(dir.path(), encoding);
    let db = DbCore::with_config(cfg.clone()).unwrap();

    db.insert("x", 1, 1.0, TagSet::new()).unwrap();
    db.insert("x", 2, 2.0, TagSet::new()).unwrap();
    db.flush().unwrap();

    let seg_path = first_segment_path(&cfg);
    let mut bytes = std::fs::read(&seg_path).unwrap();
    let index = parse_segment_index(&bytes);
    let ent = *index.get("x").expect("series x");
    let off: usize = ent.offset.try_into().unwrap();
    let len: usize = ent.len.try_into().unwrap();
    assert!(
        len >= SERIES_BLOCK_V2_HEADER_LEN + 1,
        "block has payload to corrupt"
    );
    let corrupt_at = off + SERIES_BLOCK_V2_HEADER_LEN;
    bytes[corrupt_at] ^= 0xFF;
    std::fs::write(&seg_path, bytes).unwrap();

    let err = db.query("x", 0..u64::MAX, None).unwrap_err();
    match err {
        DbError::Corruption { .. } => {}
        other => panic!("corrupt payload must yield Corruption, got {other:?}"),
    }
}

/// Adversarial: 
/// all NaN payloads; if Gorilla or decoder mishandles NaN, this would produce wrong or panic.
#[test]
fn test_breakit_all_nan_roundtrip() {
    let dir = tempdir().unwrap();
    let encoding = SegmentEncodingConfig {
        float_encoding: FloatEncoding::GorillaXor,
        tag_encoding: TagEncoding::Dictionary,
        compression: BlockCompression::None,
    };
    let cfg = make_cfg(dir.path(), encoding);
    let db = DbCore::with_config(cfg).unwrap();

    let series = "nan";
    let nan_bits = [0x7ff8_0000_0000_0001u64, 0xfff8_0000_0000_0002];
    for (i, &bits) in nan_bits.iter().enumerate() {
        db.insert(series, i as u64, f64::from_bits(bits), TagSet::new())
            .unwrap();
    }
    db.flush().unwrap();

    let got = db.query(series, 0..u64::MAX, None).unwrap();
    assert_eq!(got.len(), nan_bits.len());
    for (i, (_, v)) in got.iter().enumerate() {
        assert!(v.is_nan(), "value {} must be NaN", i);
        assert_eq!(
            v.to_bits(),
            nan_bits[i],
            "NaN payload must roundtrip bit-exact"
        );
    }
}
