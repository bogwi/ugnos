use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crc32fast::Hasher as Crc32;
use tempfile::tempdir;

use ugnos::{DbConfig, DbCore, DbError, TagSet};
use ugnos::segments::{BlockCompression, FloatEncoding, SegmentEncodingConfig, TagEncoding};

const SEG_MAGIC: &[u8; 8] = b"UGNSEG01";
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
        index_offset_usz.checked_add(index_len_usz).unwrap_or(usize::MAX) <= seg_bytes.len(),
        "index range out of bounds"
    );
    let index_bytes = &seg_bytes[index_offset_usz..index_offset_usz + index_len_usz];

    let mut off = 0usize;
    let series_count = read_u32_le(index_bytes, off) as usize;
    off += 4;

    let mut out: BTreeMap<String, SeriesIndexEntry> = BTreeMap::new();
    for _ in 0..series_count {
        let name = read_string(index_bytes, &mut off);
        let offset = read_u64_le(index_bytes, off);
        off += 8;
        let len = read_u64_le(index_bytes, off);
        off += 8;
        // row_count (u32), min_ts (u64), max_ts (u64), crc32 (u32) — not needed here.
        off += 4 + 8 + 8 + 4;
        out.insert(name, SeriesIndexEntry { offset, len });
    }
    out
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
    let db = DbCore::with_config(cfg).unwrap();

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
    let tags = [tags0.clone(), tags0.clone(), tags1.clone(), tags0.clone(), tags1.clone(), tags0.clone()];

    for i in 0..timestamps.len() {
        db.insert(series, timestamps[i], values[i], tags[i].clone()).unwrap();
    }
    db.flush().unwrap();

    // Query full range (no tags) and verify ordering + float bits preserved.
    let mut got = db.query(series, 0..u64::MAX, None).unwrap();
    got.sort_by_key(|(ts, _)| *ts);
    assert_eq!(got.len(), timestamps.len());

    // Spot-check that at least one NaN payload survived bit-exactly.
    let want_nan_bits = values[0].to_bits();
    let got_nan_bits = got.iter().find_map(|(_, v)| {
        if v.is_nan() { Some(v.to_bits()) } else { None }
    }).expect("expected at least one NaN to survive");
    assert_eq!(got_nan_bits, want_nan_bits, "NaN payload must roundtrip exactly");

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
        db.insert(series, ts, (ts as f64) * 0.125, tags.clone()).unwrap();
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

