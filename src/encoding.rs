//! Series block encoding and compression: timestamp deltas, float (Raw64/Gorilla), tag dictionary, LZ4/Zstd.

use crate::error::DbError;
use crate::types::{Row, TagSet, Timestamp, Value};

use crc32fast::Hasher as Crc32;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::path::Path;

// --- Public API ---

/// Storage encoding configuration for series blocks within segment files.
///
/// Note: This is persisted into each series block header so readers do not require
/// out-of-band configuration. Configuration only affects *newly written* blocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentEncodingConfig {
    pub float_encoding: FloatEncoding,
    pub tag_encoding: TagEncoding,
    pub compression: BlockCompression,
}

impl Default for SegmentEncodingConfig {
    fn default() -> Self {
        Self {
            float_encoding: FloatEncoding::Raw64,
            tag_encoding: TagEncoding::Dictionary,
            compression: BlockCompression::None,
        }
    }
}

/// Float encoding strategy for series blocks. Serde: lowercase string (e.g. `"raw64"`, `"gorillaxor"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FloatEncoding {
    /// Store IEEE-754 bits verbatim (8 bytes/value).
    Raw64,
    /// Gorilla-style XOR + leading/trailing-zero bitpacking over consecutive values.
    GorillaXor,
}

/// Tag encoding strategy. Serde: lowercase string (e.g. `"dictionary"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TagEncoding {
    /// Dictionary encode all tag keys/values within the block and store per-row ids.
    Dictionary,
}

/// Per-block compression. Serde: adjacently tagged table `type` + optional `level` (e.g. `type = "zstd", level = 3`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "level", rename_all = "lowercase")]
pub enum BlockCompression {
    None,
    Lz4,
    Zstd { level: i32 },
}

/// Series block magic bytes (v1 and v2).
pub const SER_BLOCK_MAGIC: &[u8; 8] = b"UGNSER01";

/// Tag block index magic (inverted index per block).
pub(crate) const TAG_INDEX_MAGIC: &[u8; 8] = b"UGNTAGIX";
const TAG_INDEX_VERSION: u32 = 4;

const SER_BLOCK_V2: u32 = 2;
const TS_CODEC_DELTA_VARINT: u8 = 1;
const FLOAT_CODEC_RAW64: u8 = 1;
const FLOAT_CODEC_GORILLA_XOR: u8 = 2;
const TAG_CODEC_DICTIONARY: u8 = 1;
const COMPRESS_NONE: u8 = 0;
const COMPRESS_LZ4: u8 = 1;
const COMPRESS_ZSTD: u8 = 2;

/// Encode a series block with the given encoding config.
pub(crate) fn encode_series_block(
    rows: &[Row],
    encoding: &SegmentEncodingConfig,
) -> Result<Vec<u8>, DbError> {
    let row_count = rows.len();
    if row_count == 0 {
        return Err(DbError::Internal(
            "Refusing to encode an empty series block".to_string(),
        ));
    }
    if row_count > (u32::MAX as usize) {
        return Err(DbError::Internal("Series block too large".to_string()));
    }

    let mut payload: Vec<u8> = Vec::new();

    for r in rows {
        write_u64(&mut payload, r.seq);
    }

    let base_ts = rows[0].timestamp;
    write_u64(&mut payload, base_ts);
    let mut prev = base_ts;
    for r in &rows[1..] {
        let ts = r.timestamp;
        if ts < prev {
            return Err(DbError::Internal(
                "Rows must be sorted by timestamp for delta encoding".to_string(),
            ));
        }
        let d = ts - prev;
        write_var_u64(&mut payload, d);
        prev = ts;
    }

    match encoding.float_encoding {
        FloatEncoding::Raw64 => {
            for r in rows {
                payload.extend_from_slice(&r.value.to_bits().to_le_bytes());
            }
        }
        FloatEncoding::GorillaXor => {
            let values: Vec<u64> = rows.iter().map(|r| r.value.to_bits()).collect();
            encode_gorilla_xor_u64(&values, &mut payload)?;
        }
    }

    match encoding.tag_encoding {
        TagEncoding::Dictionary => {
            encode_tags_dictionary(rows, &mut payload)?;
        }
    }

    let uncompressed_len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Series block payload too large".to_string()))?;
    let uncompressed_crc32 = crc32(&payload);

    let (compression_codec, compression_param, stored_payload) =
        compress_block_payload(encoding.compression, &payload)?;

    let stored_len: u32 = stored_payload
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Compressed series block too large".to_string()))?;

    let mut buf = Vec::with_capacity(64 + stored_payload.len());
    buf.extend_from_slice(SER_BLOCK_MAGIC);
    write_u32(&mut buf, SER_BLOCK_V2);
    write_u32(&mut buf, row_count as u32);
    buf.push(TS_CODEC_DELTA_VARINT);
    buf.push(match encoding.float_encoding {
        FloatEncoding::Raw64 => FLOAT_CODEC_RAW64,
        FloatEncoding::GorillaXor => FLOAT_CODEC_GORILLA_XOR,
    });
    buf.push(match encoding.tag_encoding {
        TagEncoding::Dictionary => TAG_CODEC_DICTIONARY,
    });
    buf.push(compression_codec);
    write_u32(&mut buf, compression_param);
    write_u32(&mut buf, uncompressed_len);
    write_u32(&mut buf, uncompressed_crc32);
    write_u32(&mut buf, stored_len);
    buf.extend_from_slice(&stored_payload);
    Ok(buf)
}

/// Parse v2 series block container and return (row_count, float_codec, tag_codec, compression, compression_param, decompressed_payload).
pub(crate) fn decode_series_block_v2_container(
    block: &[u8],
    path: &Path,
) -> Result<(usize, u8, u8, u8, u32, Vec<u8>), DbError> {
    let mut cur = std::io::Cursor::new(block);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic)?;
    if &magic != SER_BLOCK_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad series block magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let block_version = read_u32(&mut cur)?;
    if block_version != SER_BLOCK_V2 {
        return Err(DbError::Corruption {
            details: format!(
                "Unexpected series block version {} in {:?}",
                block_version, path
            ),
            series: None,
            timestamp: None,
        });
    }
    let row_count = read_u32(&mut cur)? as usize;
    if row_count == 0 {
        return Err(DbError::Corruption {
            details: format!("Empty series block in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let mut codec = [0u8; 4];
    cur.read_exact(&mut codec)?;
    let ts_codec = codec[0];
    let float_codec = codec[1];
    let tag_codec = codec[2];
    let compression = codec[3];
    if ts_codec != TS_CODEC_DELTA_VARINT {
        return Err(DbError::Corruption {
            details: format!("Unknown timestamp codec {} in {:?}", ts_codec, path),
            series: None,
            timestamp: None,
        });
    }
    let compression_param = read_u32(&mut cur)?;
    let uncompressed_len = read_u32(&mut cur)? as usize;
    let expected_crc = read_u32(&mut cur)?;
    let stored_len = read_u32(&mut cur)? as usize;
    if uncompressed_len > 512 * 1024 * 1024 {
        return Err(DbError::Corruption {
            details: "Refusing to allocate oversized series block".to_string(),
            series: None,
            timestamp: None,
        });
    }

    let hdr_len = cur.position() as usize;
    if hdr_len.checked_add(stored_len).unwrap_or(usize::MAX) > block.len() {
        return Err(DbError::Corruption {
            details: "Truncated series block payload".to_string(),
            series: None,
            timestamp: None,
        });
    }
    let stored = &block[hdr_len..hdr_len + stored_len];

    let payload: Vec<u8> = match compression {
        COMPRESS_NONE => stored.to_vec(),
        COMPRESS_LZ4 => {
            lz4_flex::decompress_size_prepended(stored).map_err(|e| DbError::Corruption {
                details: format!("LZ4 decompress failed: {}", e),
                series: None,
                timestamp: None,
            })?
        }
        COMPRESS_ZSTD => {
            let _level = i32::from_le_bytes(compression_param.to_le_bytes());
            zstd::bulk::decompress(stored, uncompressed_len).map_err(|e| DbError::Corruption {
                details: format!("Zstd decompress failed: {}", e),
                series: None,
                timestamp: None,
            })?
        }
        other => {
            return Err(DbError::Corruption {
                details: format!("Unknown compression codec {} in {:?}", other, path),
                series: None,
                timestamp: None,
            })
        }
    };

    if payload.len() != uncompressed_len {
        return Err(DbError::Corruption {
            details: "Series block decompressed length mismatch".to_string(),
            series: None,
            timestamp: None,
        });
    }
    let actual_crc = crc32(&payload);
    if actual_crc != expected_crc {
        return Err(DbError::Corruption {
            details: "Series block payload CRC mismatch".to_string(),
            series: None,
            timestamp: None,
        });
    }

    Ok((
        row_count,
        float_codec,
        tag_codec,
        compression,
        compression_param,
        payload,
    ))
}

// --- Decoded block types (used by segments for query path) ---

pub(crate) struct DecodedBlockV1Query {
    pub(crate) timestamps: Vec<Timestamp>,
    pub(crate) values: Vec<Value>,
    offsets: Vec<u32>,
    tags_blob: Vec<u8>,
}

impl DecodedBlockV1Query {
    pub(crate) fn row_matches_filter_v1(&self, i: usize, filter: &TagSet) -> Result<bool, DbError> {
        let s = self.offsets[i] as usize;
        let e = self.offsets[i + 1] as usize;
        let tags: TagSet = bincode::deserialize(&self.tags_blob[s..e])
            .map_err(|e| DbError::Serialization(e.to_string()))?;
        Ok(check_tags(&tags, filter))
    }
}

pub(crate) struct TagFilterMatcherV2 {
    pub(crate) pairs: Vec<(u32, u32)>,
}

pub(crate) struct DecodedBlockV2Query {
    pub(crate) timestamps: Vec<Timestamp>,
    pub(crate) values: Vec<Value>,
    dict: Vec<String>,
    offsets: Vec<u32>,
    tags_blob: Vec<u8>,
}

impl DecodedBlockV2Query {
    pub(crate) fn build_tag_filter_matcher(&self, filter: &TagSet) -> Option<TagFilterMatcherV2> {
        if filter.is_empty() {
            return Some(TagFilterMatcherV2 { pairs: Vec::new() });
        }
        let mut map: HashMap<&str, u32> = HashMap::with_capacity(self.dict.len());
        for (i, s) in self.dict.iter().enumerate() {
            map.entry(s.as_str()).or_insert(i as u32);
        }
        let mut pairs = Vec::with_capacity(filter.len());
        for (k, v) in filter {
            let kid = *map.get(k.as_str())?;
            let vid = *map.get(v.as_str())?;
            pairs.push((kid, vid));
        }
        Some(TagFilterMatcherV2 { pairs })
    }

    pub(crate) fn row_matches_filter_v2(
        &self,
        i: usize,
        matcher: &TagFilterMatcherV2,
    ) -> Result<bool, DbError> {
        if matcher.pairs.is_empty() {
            return Ok(true);
        }
        let s = self.offsets[i] as usize;
        let e = self.offsets[i + 1] as usize;
        if s > e || e > self.tags_blob.len() {
            return Err(DbError::Corruption {
                details: "Tag offsets out of bounds".to_string(),
                series: None,
                timestamp: None,
            });
        }
        let mut cur = std::io::Cursor::new(&self.tags_blob[s..e]);
        let pair_count = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
            details,
            series: None,
            timestamp: None,
        })? as usize;

        let mut found = vec![false; matcher.pairs.len()];
        for _ in 0..pair_count {
            let kid = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })?;
            let vid = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })?;
            for (idx, (fk, fv)) in matcher.pairs.iter().copied().enumerate() {
                if !found[idx] && kid == fk && vid == fv {
                    found[idx] = true;
                }
            }
        }
        Ok(found.into_iter().all(|b| b))
    }
}

/// Decode v1 block for query path (timestamps + values + tag filter support).
pub(crate) fn decode_series_block_v1_for_query(
    block: &[u8],
    path: &Path,
) -> Result<DecodedBlockV1Query, DbError> {
    let mut cur = std::io::Cursor::new(block);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic)?;
    if &magic != SER_BLOCK_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad series block magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let row_count = read_u32(&mut cur)? as usize;
    if row_count == 0 {
        return Err(DbError::Corruption {
            details: format!("Empty series block in {:?}", path),
            series: None,
            timestamp: None,
        });
    }

    for _ in 0..row_count {
        let _ = read_u64(&mut cur)?;
    }

    let mut timestamps = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        timestamps.push(read_u64(&mut cur)?);
    }
    let mut values = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        values.push(read_f64(&mut cur)?);
    }

    let mut offsets = Vec::with_capacity(row_count + 1);
    for _ in 0..(row_count + 1) {
        offsets.push(read_u32(&mut cur)?);
    }
    let tags_len = read_u32(&mut cur)? as usize;
    let mut tags_blob = vec![0u8; tags_len];
    cur.read_exact(&mut tags_blob)?;

    Ok(DecodedBlockV1Query {
        timestamps,
        values,
        offsets,
        tags_blob,
    })
}

/// Decode v1 block to full rows.
pub(crate) fn decode_series_block_v1_all_rows(
    block: &[u8],
    path: &Path,
) -> Result<Vec<Row>, DbError> {
    let mut cur = std::io::Cursor::new(block);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic)?;
    if &magic != SER_BLOCK_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad series block magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let row_count = read_u32(&mut cur)? as usize;
    if row_count == 0 {
        return Err(DbError::Corruption {
            details: format!("Empty series block in {:?}", path),
            series: None,
            timestamp: None,
        });
    }

    let mut seqs = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        seqs.push(read_u64(&mut cur)?);
    }
    let mut timestamps = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        timestamps.push(read_u64(&mut cur)?);
    }
    let mut values = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        values.push(read_f64(&mut cur)?);
    }

    let mut offsets = Vec::with_capacity(row_count + 1);
    for _ in 0..(row_count + 1) {
        offsets.push(read_u32(&mut cur)?);
    }
    let tags_len = read_u32(&mut cur)? as usize;
    let mut tags_blob = vec![0u8; tags_len];
    cur.read_exact(&mut tags_blob)?;

    let mut out = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let s = offsets[i] as usize;
        let e = offsets[i + 1] as usize;
        let tags: TagSet = bincode::deserialize(&tags_blob[s..e])
            .map_err(|e| DbError::Serialization(e.to_string()))?;
        out.push(Row {
            seq: seqs[i],
            timestamp: timestamps[i],
            value: values[i],
            tags,
        });
    }
    Ok(out)
}

/// Decode v2 block for query path.
pub(crate) fn decode_series_block_v2_for_query(
    block: &[u8],
    path: &Path,
) -> Result<DecodedBlockV2Query, DbError> {
    let (row_count, float_codec, tag_codec, _compression, _compression_param, payload) =
        decode_series_block_v2_container(block, path)?;

    let mut cur = std::io::Cursor::new(payload);

    for _ in 0..row_count {
        let _ = read_u64(&mut cur)?;
    }

    let base = read_u64(&mut cur)?;
    let mut timestamps = Vec::with_capacity(row_count);
    timestamps.push(base);
    let mut prev = base;
    for _ in 1..row_count {
        let d = read_var_u64(&mut cur).map_err(|details| DbError::Corruption {
            details,
            series: None,
            timestamp: None,
        })?;
        prev = prev.checked_add(d).ok_or_else(|| DbError::Corruption {
            details: "Timestamp delta overflow".to_string(),
            series: None,
            timestamp: None,
        })?;
        timestamps.push(prev);
    }

    let mut values = Vec::with_capacity(row_count);
    match float_codec {
        FLOAT_CODEC_RAW64 => {
            for _ in 0..row_count {
                let bits = read_u64(&mut cur)?;
                values.push(f64::from_bits(bits));
            }
        }
        FLOAT_CODEC_GORILLA_XOR => {
            let bits = decode_gorilla_xor_u64(&mut cur, row_count)?;
            for b in bits {
                values.push(f64::from_bits(b));
            }
        }
        other => {
            return Err(DbError::Corruption {
                details: format!("Unknown float codec {} in {:?}", other, path),
                series: None,
                timestamp: None,
            });
        }
    }

    match tag_codec {
        TAG_CODEC_DICTIONARY => {
            let dict_count = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut dict = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                let n = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })? as usize;
                let mut b = vec![0u8; n];
                cur.read_exact(&mut b)?;
                let s = String::from_utf8(b).map_err(|e| {
                    DbError::Internal(format!("Invalid UTF-8 in dictionary: {}", e))
                })?;
                dict.push(s);
            }

            let mut offsets: Vec<u32> = Vec::with_capacity(row_count + 1);
            offsets.push(0);
            let mut total: u32 = 0;
            for _ in 0..row_count {
                let len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })?;
                total = total.checked_add(len).ok_or_else(|| DbError::Corruption {
                    details: "Tag blob overflow".to_string(),
                    series: None,
                    timestamp: None,
                })?;
                offsets.push(total);
            }
            let tags_len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut tags_blob = vec![0u8; tags_len];
            cur.read_exact(&mut tags_blob)?;

            Ok(DecodedBlockV2Query {
                timestamps,
                values,
                dict,
                offsets,
                tags_blob,
            })
        }
        other => Err(DbError::Corruption {
            details: format!("Unknown tag codec {} in {:?}", other, path),
            series: None,
            timestamp: None,
        }),
    }
}

/// Decode v2 block to full rows.
pub(crate) fn decode_series_block_v2_all_rows(
    block: &[u8],
    path: &Path,
) -> Result<Vec<Row>, DbError> {
    let (row_count, float_codec, tag_codec, _compression, _compression_param, payload) =
        decode_series_block_v2_container(block, path)?;
    let mut cur = std::io::Cursor::new(payload);

    let mut seqs = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        seqs.push(read_u64(&mut cur)?);
    }

    let base = read_u64(&mut cur)?;
    let mut timestamps = Vec::with_capacity(row_count);
    timestamps.push(base);
    let mut prev = base;
    for _ in 1..row_count {
        let d = read_var_u64(&mut cur).map_err(|details| DbError::Corruption {
            details,
            series: None,
            timestamp: None,
        })?;
        prev = prev.checked_add(d).ok_or_else(|| DbError::Corruption {
            details: "Timestamp delta overflow".to_string(),
            series: None,
            timestamp: None,
        })?;
        timestamps.push(prev);
    }

    let mut values = Vec::with_capacity(row_count);
    match float_codec {
        FLOAT_CODEC_RAW64 => {
            for _ in 0..row_count {
                values.push(f64::from_bits(read_u64(&mut cur)?));
            }
        }
        FLOAT_CODEC_GORILLA_XOR => {
            let bits = decode_gorilla_xor_u64(&mut cur, row_count)?;
            for b in bits {
                values.push(f64::from_bits(b));
            }
        }
        other => {
            return Err(DbError::Corruption {
                details: format!("Unknown float codec {} in {:?}", other, path),
                series: None,
                timestamp: None,
            });
        }
    }

    match tag_codec {
        TAG_CODEC_DICTIONARY => {
            let dict_count = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut dict = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                let n = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })? as usize;
                let mut b = vec![0u8; n];
                cur.read_exact(&mut b)?;
                let s = String::from_utf8(b).map_err(|e| {
                    DbError::Internal(format!("Invalid UTF-8 in dictionary: {}", e))
                })?;
                dict.push(s);
            }

            let mut offsets: Vec<u32> = Vec::with_capacity(row_count + 1);
            offsets.push(0);
            let mut total: u32 = 0;
            for _ in 0..row_count {
                let len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })?;
                total = total.checked_add(len).ok_or_else(|| DbError::Corruption {
                    details: "Tag blob overflow".to_string(),
                    series: None,
                    timestamp: None,
                })?;
                offsets.push(total);
            }
            let tags_len = read_var_u32(&mut cur).map_err(|details| DbError::Corruption {
                details,
                series: None,
                timestamp: None,
            })? as usize;
            let mut tags_blob = vec![0u8; tags_len];
            cur.read_exact(&mut tags_blob)?;

            let mut out = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let s = offsets[i] as usize;
                let e = offsets[i + 1] as usize;
                if s > e || e > tags_blob.len() {
                    return Err(DbError::Corruption {
                        details: "Tag offsets out of bounds".to_string(),
                        series: None,
                        timestamp: None,
                    });
                }
                let mut tcur = std::io::Cursor::new(&tags_blob[s..e]);
                let pair_count = read_var_u32(&mut tcur).map_err(|details| DbError::Corruption {
                    details,
                    series: None,
                    timestamp: None,
                })? as usize;
                let mut tags: TagSet = TagSet::with_capacity(pair_count);
                for _ in 0..pair_count {
                    let kid = read_var_u32(&mut tcur).map_err(|details| DbError::Corruption {
                        details,
                        series: None,
                        timestamp: None,
                    })? as usize;
                    let vid = read_var_u32(&mut tcur).map_err(|details| DbError::Corruption {
                        details,
                        series: None,
                        timestamp: None,
                    })? as usize;
                    let k = dict.get(kid).ok_or_else(|| DbError::Corruption {
                        details: "Dictionary key id out of range".to_string(),
                        series: None,
                        timestamp: None,
                    })?;
                    let v = dict.get(vid).ok_or_else(|| DbError::Corruption {
                        details: "Dictionary value id out of range".to_string(),
                        series: None,
                        timestamp: None,
                    })?;
                    tags.insert(k.clone(), v.clone());
                }
                out.push(Row {
                    seq: seqs[i],
                    timestamp: timestamps[i],
                    value: values[i],
                    tags,
                });
            }
            Ok(out)
        }
        other => Err(DbError::Corruption {
            details: format!("Unknown tag codec {} in {:?}", other, path),
            series: None,
            timestamp: None,
        }),
    }
}

#[cfg(test)]
pub(crate) fn encode_series_block_v1(rows: &[Row]) -> Result<Vec<u8>, DbError> {
    let row_count = rows.len();
    if row_count == 0 {
        return Err(DbError::Internal(
            "Refusing to encode an empty series block".to_string(),
        ));
    }
    if row_count > (u32::MAX as usize) {
        return Err(DbError::Internal("Series block too large".to_string()));
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(SER_BLOCK_MAGIC);
    write_u32(&mut buf, row_count as u32);

    for r in rows {
        write_u64(&mut buf, r.seq);
    }
    for r in rows {
        write_u64(&mut buf, r.timestamp);
    }
    for r in rows {
        write_f64(&mut buf, r.value);
    }

    let mut offsets: Vec<u32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut tags_blob: Vec<u8> = Vec::new();
    for r in rows {
        let enc = bincode::serialize(&r.tags).map_err(|e| DbError::Serialization(e.to_string()))?;
        let next = offsets
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(enc.len() as u32)
            .ok_or_else(|| DbError::Internal("Tags blob overflow".to_string()))?;
        tags_blob.extend_from_slice(&enc);
        offsets.push(next);
    }

    for off in offsets {
        write_u32(&mut buf, off);
    }
    write_u32(&mut buf, tags_blob.len() as u32);
    buf.extend_from_slice(&tags_blob);
    Ok(buf)
}

// --- Helpers (binary, varint, compression, gorilla, tags) ---

#[inline]
pub(crate) fn check_tags(point_tags: &TagSet, filter_tags: &TagSet) -> bool {
    if point_tags.len() < filter_tags.len() {
        return false;
    }
    filter_tags
        .iter()
        .all(|(key, value)| point_tags.get(key) == Some(value))
}

pub fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub fn write_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn read_u32<R: Read>(r: &mut R) -> Result<u32, DbError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
pub fn read_u64<R: Read>(r: &mut R) -> Result<u64, DbError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
pub fn read_f64<R: Read>(r: &mut R) -> Result<f64, DbError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(f64::from_le_bytes(b))
}

#[inline]
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut h = Crc32::new();
    h.update(bytes);
    h.finalize()
}

fn write_var_u64(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

pub(crate) fn write_var_u32(buf: &mut Vec<u8>, v: u32) {
    write_var_u64(buf, v as u64);
}

fn read_var_u64<R: Read>(r: &mut R) -> Result<u64, String> {
    let mut out: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)
            .map_err(|e| format!("Truncated varint: {}", e))?;
        let byte = b[0];
        out |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Ok(out);
        }
        shift = shift.saturating_add(7);
    }
    Err("Varint too long".to_string())
}

pub(crate) fn read_var_u32<R: Read>(r: &mut R) -> Result<u32, String> {
    let v = read_var_u64(r)?;
    if v > u32::MAX as u64 {
        return Err("Varint does not fit in u32".to_string());
    }
    Ok(v as u32)
}

fn compress_block_payload(
    compression: BlockCompression,
    payload: &[u8],
) -> Result<(u8, u32, Vec<u8>), DbError> {
    match compression {
        BlockCompression::None => Ok((COMPRESS_NONE, 0, payload.to_vec())),
        BlockCompression::Lz4 => Ok((COMPRESS_LZ4, 0, lz4_flex::compress_prepend_size(payload))),
        BlockCompression::Zstd { level } => {
            let compressed = zstd::bulk::compress(payload, level)
                .map_err(|e| DbError::Internal(format!("Zstd compress failed: {}", e)))?;
            let param = u32::from_le_bytes(level.to_le_bytes());
            Ok((COMPRESS_ZSTD, param, compressed))
        }
    }
}

struct BitWriter<'a> {
    out: &'a mut Vec<u8>,
    cur: u8,
    used: u8,
}

impl<'a> BitWriter<'a> {
    fn new(out: &'a mut Vec<u8>) -> Self {
        Self {
            out,
            cur: 0,
            used: 0,
        }
    }

    fn push_bit(&mut self, bit: bool) {
        let b = if bit { 1u8 } else { 0u8 };
        self.cur |= b << (7 - self.used);
        self.used += 1;
        if self.used == 8 {
            self.out.push(self.cur);
            self.cur = 0;
            self.used = 0;
        }
    }

    fn push_bits(&mut self, mut v: u64, count: u8) {
        if count == 0 {
            return;
        }
        if count < 64 {
            v &= (1u64 << count) - 1;
        }
        for i in (0..count).rev() {
            self.push_bit(((v >> i) & 1) == 1);
        }
    }

    fn finish(self) {
        if self.used > 0 {
            self.out.push(self.cur);
        }
    }
}

struct BitReader<'a, R: Read> {
    r: &'a mut R,
    cur: u8,
    left: u8,
}

impl<'a, R: Read> BitReader<'a, R> {
    fn new(r: &'a mut R) -> Self {
        Self { r, cur: 0, left: 0 }
    }

    fn read_bit(&mut self) -> Result<bool, DbError> {
        if self.left == 0 {
            let mut b = [0u8; 1];
            self.r.read_exact(&mut b).map_err(DbError::Io)?;
            self.cur = b[0];
            self.left = 8;
        }
        let bit = (self.cur & (1u8 << (self.left - 1))) != 0;
        self.left -= 1;
        Ok(bit)
    }

    fn read_bits(&mut self, count: u8) -> Result<u64, DbError> {
        let mut out = 0u64;
        for _ in 0..count {
            out <<= 1;
            out |= if self.read_bit()? { 1 } else { 0 };
        }
        Ok(out)
    }
}

fn encode_gorilla_xor_u64(values: &[u64], out: &mut Vec<u8>) -> Result<(), DbError> {
    if values.is_empty() {
        return Err(DbError::Internal(
            "Cannot Gorilla-encode empty values".to_string(),
        ));
    }
    out.extend_from_slice(&values[0].to_le_bytes());
    let mut bw = BitWriter::new(out);

    let mut prev = values[0];
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;
    let mut prev_sigbits: u8 = 0;

    for &cur in &values[1..] {
        let x = prev ^ cur;
        if x == 0 {
            bw.push_bit(false);
        } else {
            bw.push_bit(true);
            let leading = x.leading_zeros() as u8;
            let trailing = x.trailing_zeros() as u8;
            let sigbits_u32 = 64u32
                .saturating_sub(leading as u32)
                .saturating_sub(trailing as u32);
            let sigbits: u8 = sigbits_u32
                .try_into()
                .map_err(|_| DbError::Internal("Invalid significant bit width".to_string()))?;

            if prev_sigbits != 0 && leading >= prev_leading && trailing >= prev_trailing {
                bw.push_bit(false);
                let significant = x >> prev_trailing;
                bw.push_bits(significant, prev_sigbits);
            } else {
                bw.push_bit(true);
                bw.push_bits(leading as u64, 6);
                bw.push_bits((sigbits - 1) as u64, 6);
                let significant = x >> trailing;
                bw.push_bits(significant, sigbits);
                prev_leading = leading;
                prev_trailing = trailing;
                prev_sigbits = sigbits;
            }
        }
        prev = cur;
    }
    bw.finish();
    Ok(())
}

fn decode_gorilla_xor_u64<R: Read>(r: &mut R, count: usize) -> Result<Vec<u64>, DbError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let first = read_u64(r)?;
    let mut out = Vec::with_capacity(count);
    out.push(first);
    let mut br = BitReader::new(r);

    let mut prev = first;
    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;
    let mut prev_sigbits: u8 = 0;

    for _ in 1..count {
        let control = br.read_bit()?;
        if !control {
            out.push(prev);
            continue;
        }
        let mode = br.read_bit()?;
        let (_leading, sigbits, trailing) = if !mode {
            if prev_sigbits == 0 {
                return Err(DbError::Corruption {
                    details: "Gorilla reuse window before initialization".to_string(),
                    series: None,
                    timestamp: None,
                });
            }
            (prev_leading, prev_sigbits, prev_trailing)
        } else {
            let leading = br.read_bits(6)? as u8;
            let sigbits = (br.read_bits(6)? as u8).saturating_add(1);
            if leading > 63 || sigbits == 0 || sigbits > 64 {
                return Err(DbError::Corruption {
                    details: "Invalid Gorilla bit widths".to_string(),
                    series: None,
                    timestamp: None,
                });
            }
            let trailing = 64u8.saturating_sub(leading).saturating_sub(sigbits);
            prev_leading = leading;
            prev_trailing = trailing;
            prev_sigbits = sigbits;
            (leading, sigbits, trailing)
        };

        let significant = br.read_bits(sigbits)?;
        let x = significant << trailing;
        let cur = prev ^ x;
        out.push(cur);
        prev = cur;
    }
    Ok(out)
}

fn encode_tags_dictionary(rows: &[Row], out: &mut Vec<u8>) -> Result<(), DbError> {
    let mut uniq: BTreeSet<String> = BTreeSet::new();
    for r in rows {
        for (k, v) in &r.tags {
            uniq.insert(k.clone());
            uniq.insert(v.clone());
        }
    }
    let dict: Vec<String> = uniq.into_iter().collect();

    let mut map: HashMap<&str, u32> = HashMap::with_capacity(dict.len());
    for (i, s) in dict.iter().enumerate() {
        map.insert(s.as_str(), i as u32);
    }

    write_var_u32(out, dict.len() as u32);
    for s in &dict {
        let b = s.as_bytes();
        let n: u32 = b
            .len()
            .try_into()
            .map_err(|_| DbError::Internal("Dictionary string too large".to_string()))?;
        write_var_u32(out, n);
        out.extend_from_slice(b);
    }

    let mut tags_blob: Vec<u8> = Vec::new();
    let mut lengths: Vec<u32> = Vec::with_capacity(rows.len());
    for r in rows {
        let mut kv: Vec<(&String, &String)> = r.tags.iter().collect();
        kv.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));

        let mut row_enc: Vec<u8> = Vec::new();
        write_var_u32(&mut row_enc, kv.len() as u32);
        for (k, v) in kv {
            let kid = *map
                .get(k.as_str())
                .ok_or_else(|| DbError::Internal("Missing dictionary key".to_string()))?;
            let vid = *map
                .get(v.as_str())
                .ok_or_else(|| DbError::Internal("Missing dictionary value".to_string()))?;
            write_var_u32(&mut row_enc, kid);
            write_var_u32(&mut row_enc, vid);
        }

        let len_u32: u32 = row_enc
            .len()
            .try_into()
            .map_err(|_| DbError::Internal("Tags blob too large".to_string()))?;
        lengths.push(len_u32);
        tags_blob.extend_from_slice(&row_enc);
    }

    for l in lengths {
        write_var_u32(out, l);
    }

    let tags_len_u32: u32 = tags_blob
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Tags blob too large".to_string()))?;
    write_var_u32(out, tags_len_u32);
    out.extend_from_slice(&tags_blob);
    Ok(())
}

// --- Tag block index (inverted index: (key_id, value_id) -> row bitmap) ---

/// Builds the same dictionary as `encode_tags_dictionary` (sorted order) and returns
/// (dict, map from (k,v) string pair to (key_id, value_id)) for (k,v) that appear in rows.
fn build_tag_dict(rows: &[Row]) -> (Vec<String>, HashMap<(String, String), (u32, u32)>) {
    let mut uniq: BTreeSet<String> = BTreeSet::new();
    for r in rows {
        for (k, v) in &r.tags {
            uniq.insert(k.clone());
            uniq.insert(v.clone());
        }
    }
    let dict: Vec<String> = uniq.into_iter().collect();
    let str_to_id: HashMap<&str, u32> = dict
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i as u32))
        .collect();
    let mut kv_to_id: HashMap<(String, String), (u32, u32)> = HashMap::new();
    for r in rows {
        for (k, v) in &r.tags {
            if let (Some(&kid), Some(&vid)) = (str_to_id.get(k.as_str()), str_to_id.get(v.as_str()))
            {
                kv_to_id.insert((k.clone(), v.clone()), (kid, vid));
            }
        }
    }
    (dict, kv_to_id)
}

/// Builds a tag block index from rows.
///
/// Version 4 stores a classic inverted index `(key_id, value_id) -> set(row_idx)` as
/// **Roaring compressed bitmaps**. This is the enterprise-standard approach for high-cardinality
/// tag filtering because it supports fast AND intersections while remaining compact for both
/// sparse and dense sets (the representation adapts internally).
///
/// Important: the index stores only `(kid,vid)` ids; it does **not** duplicate the tag dictionary
/// strings already stored in the series block.
pub(crate) fn build_tag_block_index(rows: &[Row]) -> Result<Vec<u8>, DbError> {
    let row_count = rows.len();
    if row_count == 0 {
        return Err(DbError::Internal(
            "Refusing to build tag index for empty block".to_string(),
        ));
    }
    let (_dict, str_to_id) = build_tag_dict(rows);
    // (kid, vid) -> roaring bitmap (row indices where this (k,v) appears)
    let mut by_kv: BTreeMap<(u32, u32), RoaringBitmap> = BTreeMap::new();
    for (i, r) in rows.iter().enumerate() {
        let i: u32 = i
            .try_into()
            .map_err(|_| DbError::Internal("Too many rows for tag index".to_string()))?;
        for (k, v) in &r.tags {
            if let Some(&(kid, vid)) = str_to_id.get(&(k.clone(), v.clone())) {
                by_kv.entry((kid, vid)).or_default().insert(i);
            }
        }
    }

    // On-disk v4 encoding:
    // magic(8) + version(u32) + row_count(var_u32) + entries(var_u32)
    // raw_len(var_u32) + zstd_len(var_u32) + zstd(raw_payload) + crc32(u32)
    //
    // raw_payload layout:
    //   [ kid(var_u32) vid(var_u32) bitmap_len(var_u32) bitmap_bytes ]
    let mut out = Vec::new();
    out.extend_from_slice(TAG_INDEX_MAGIC);
    write_u32(&mut out, TAG_INDEX_VERSION);
    write_var_u32(&mut out, row_count as u32);
    write_var_u32(&mut out, by_kv.len() as u32);
    let mut raw = Vec::new();
    for ((kid, vid), bm) in &by_kv {
        write_var_u32(&mut raw, *kid);
        write_var_u32(&mut raw, *vid);
        let mut bm_bytes = Vec::new();
        bm.serialize_into(&mut bm_bytes)
            .map_err(|e| DbError::Serialization(e.to_string()))?;
        let n: u32 = bm_bytes
            .len()
            .try_into()
            .map_err(|_| DbError::Internal("Tag index bitmap too large".to_string()))?;
        write_var_u32(&mut raw, n);
        raw.extend_from_slice(&bm_bytes);
    }

    let raw_len: u32 = raw
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Tag index payload too large".to_string()))?;
    let zstd =
        zstd::stream::encode_all(std::io::Cursor::new(&raw), 3).map_err(|e| DbError::Io(e))?;
    let zstd_len: u32 = zstd
        .len()
        .try_into()
        .map_err(|_| DbError::Internal("Tag index compressed payload too large".to_string()))?;

    write_var_u32(&mut out, raw_len);
    write_var_u32(&mut out, zstd_len);
    out.extend_from_slice(&zstd);

    let crc = crc32(&out);
    write_u32(&mut out, crc);
    Ok(out)
}

/// Parsed tag block index for use in query path.
pub(crate) enum TagBlockIndex {
    /// Version 1: (kid,vid) -> bitmap(row_count bits).
    BitmapV1 {
        row_count: usize,
        dict: Vec<String>,
        bitmaps: HashMap<(u32, u32), Vec<u8>>,
    },
    /// Version 2: (kid,vid) -> postings (sorted row indices).
    PostingsV2 {
        row_count: usize,
        dict: Vec<String>,
        postings: HashMap<(u32, u32), Vec<u32>>,
    },
    /// Version 3: (kid,vid) -> roaring compressed bitmap(row indices).
    RoaringV3 {
        row_count: usize,
        bitmaps: HashMap<(u32, u32), RoaringBitmap>,
    },
}

pub(crate) enum TagIndexCandidates {
    /// Filter is empty; treat as no tag filtering.
    All,
    /// Candidate bitmap (bit i => row i matches all filter terms).
    Bitmap(Vec<u8>),
    /// Candidate row indices that match all filter terms.
    Rows(Vec<u32>),
}

impl TagBlockIndex {
    /// Returns candidates for the filter (AND semantics).
    ///
    /// Returns `None` if no rows can match (missing dictionary term or missing posting/bitmap).
    pub fn candidates_for_filter(&self, filter: &TagSet) -> Option<TagIndexCandidates> {
        if filter.is_empty() {
            return Some(TagIndexCandidates::All);
        }
        let (row_count, dict) = match self {
            TagBlockIndex::RoaringV3 { .. } => return None,
            TagBlockIndex::BitmapV1 {
                row_count, dict, ..
            } => (*row_count, dict),
            TagBlockIndex::PostingsV2 {
                row_count, dict, ..
            } => (*row_count, dict),
        };
        let _ = row_count;

        let mut map: HashMap<&str, u32> = HashMap::new();
        for (i, s) in dict.iter().enumerate() {
            map.insert(s.as_str(), i as u32);
        }

        match self {
            TagBlockIndex::BitmapV1 {
                row_count, bitmaps, ..
            } => {
                let bytes = (*row_count + 7) / 8;
                let mut result: Option<Vec<u8>> = None;
                for (k, v) in filter {
                    let kid = *map.get(k.as_str())?;
                    let vid = *map.get(v.as_str())?;
                    let bm = bitmaps.get(&(kid, vid))?;
                    result = Some(match result.take() {
                        None => bm.clone(),
                        Some(acc) => {
                            debug_assert_eq!(acc.len(), bm.len());
                            acc.iter().zip(bm.iter()).map(|(a, b)| a & b).collect()
                        }
                    });
                }
                // Defensive: ensure correct length.
                result
                    .filter(|bm| bm.len() == bytes)
                    .map(TagIndexCandidates::Bitmap)
            }
            TagBlockIndex::PostingsV2 { postings, .. } => {
                let mut acc: Option<Vec<u32>> = None;
                for (k, v) in filter {
                    let kid = *map.get(k.as_str())?;
                    let vid = *map.get(v.as_str())?;
                    let p = postings.get(&(kid, vid))?;
                    acc = Some(match acc.take() {
                        None => p.clone(),
                        Some(cur) => intersect_sorted_u32(&cur, p),
                    });
                    if let Some(ref a) = acc {
                        if a.is_empty() {
                            return None;
                        }
                    }
                }
                acc.map(TagIndexCandidates::Rows)
            }
            TagBlockIndex::RoaringV3 { .. } => None,
        }
    }

    /// Preferred candidate computation for v3: use pre-resolved `(kid,vid)` pairs.
    ///
    /// Returns:
    /// - `Some(bitmap)` if there are candidates (bitmap may be empty only when matcher is empty)
    /// - `None` if any term is missing or intersection is empty.
    pub fn candidates_for_matcher_v2(&self, matcher: &TagFilterMatcherV2) -> Option<RoaringBitmap> {
        let TagBlockIndex::RoaringV3 { bitmaps, .. } = self else {
            return None;
        };
        if matcher.pairs.is_empty() {
            // Caller treats this as "no tag filtering". Keep explicit and cheap.
            return Some(RoaringBitmap::new());
        }
        let mut it = matcher.pairs.iter().copied();
        let first = it.next()?;
        let mut acc = bitmaps.get(&first)?.clone();
        for pair in it {
            let bm = bitmaps.get(&pair)?;
            acc &= bm;
            if acc.is_empty() {
                return None;
            }
        }
        Some(acc)
    }

    /// Returns true if row index `i` is set in the bitmap.
    #[inline]
    pub fn bitmap_get(bitmap: &[u8], row_count: usize, i: usize) -> bool {
        if i >= row_count {
            return false;
        }
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        bitmap
            .get(byte_idx)
            .map_or(false, |b| (b & (1 << bit_idx)) != 0)
    }
}

fn intersect_sorted_u32(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// Parses a tag block index blob.
pub(crate) fn parse_tag_block_index(bytes: &[u8], path: &Path) -> Result<TagBlockIndex, DbError> {
    let mut cur = std::io::Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic)?;
    if &magic != TAG_INDEX_MAGIC {
        return Err(DbError::Corruption {
            details: format!("Bad tag index magic in {:?}", path),
            series: None,
            timestamp: None,
        });
    }
    let version = read_u32(&mut cur)?;
    if version != 1 && version != 2 && version != 3 && version != TAG_INDEX_VERSION {
        return Err(DbError::Corruption {
            details: format!("Unsupported tag index version {} in {:?}", version, path),
            series: None,
            timestamp: None,
        });
    }
    let row_count = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
        details: d,
        series: None,
        timestamp: None,
    })? as usize;

    if version == TAG_INDEX_VERSION {
        // v4: roaring bitmaps in a zstd-compressed payload + trailing crc32, no embedded dictionary.
        let num_entries = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
            details: d,
            series: None,
            timestamp: None,
        })? as usize;
        let raw_len = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
            details: d,
            series: None,
            timestamp: None,
        })? as usize;
        let zstd_len = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
            details: d,
            series: None,
            timestamp: None,
        })? as usize;
        let mut zstd = vec![0u8; zstd_len];
        cur.read_exact(&mut zstd)?;

        let raw = zstd::stream::decode_all(std::io::Cursor::new(&zstd)).map_err(DbError::Io)?;
        if raw.len() != raw_len {
            return Err(DbError::Corruption {
                details: format!("Tag index payload length mismatch in {:?}", path),
                series: None,
                timestamp: None,
            });
        }

        let mut payload = std::io::Cursor::new(&raw);
        let mut bitmaps: HashMap<(u32, u32), RoaringBitmap> = HashMap::with_capacity(num_entries);
        for _ in 0..num_entries {
            let kid = read_var_u32(&mut payload).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let vid = read_var_u32(&mut payload).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let n = read_var_u32(&mut payload).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })? as usize;
            let mut bm_bytes = vec![0u8; n];
            payload.read_exact(&mut bm_bytes)?;
            let mut rdr = std::io::Cursor::new(&bm_bytes);
            let bm = RoaringBitmap::deserialize_from(&mut rdr)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            bitmaps.insert((kid, vid), bm);
        }
        // trailing crc32
        let expected_crc = read_u32(&mut cur)?;
        if bytes.len() < 4 {
            return Err(DbError::Corruption {
                details: format!("Truncated tag index in {:?}", path),
                series: None,
                timestamp: None,
            });
        }
        let actual_crc = crc32(&bytes[..bytes.len() - 4]);
        if expected_crc != actual_crc {
            return Err(DbError::Corruption {
                details: format!("Tag index CRC mismatch in {:?}", path),
                series: None,
                timestamp: None,
            });
        }
        return Ok(TagBlockIndex::RoaringV3 { row_count, bitmaps });
    }

    if version == 3 {
        // v3: roaring bitmaps without compression + trailing crc32, no embedded dictionary.
        let num_entries = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
            details: d,
            series: None,
            timestamp: None,
        })? as usize;
        let mut bitmaps: HashMap<(u32, u32), RoaringBitmap> = HashMap::with_capacity(num_entries);
        for _ in 0..num_entries {
            let kid = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let vid = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let n = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })? as usize;
            let mut bm_bytes = vec![0u8; n];
            cur.read_exact(&mut bm_bytes)?;
            let mut rdr = std::io::Cursor::new(&bm_bytes);
            let bm = RoaringBitmap::deserialize_from(&mut rdr)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            bitmaps.insert((kid, vid), bm);
        }
        let expected_crc = read_u32(&mut cur)?;
        if bytes.len() < 4 {
            return Err(DbError::Corruption {
                details: format!("Truncated tag index in {:?}", path),
                series: None,
                timestamp: None,
            });
        }
        let actual_crc = crc32(&bytes[..bytes.len() - 4]);
        if expected_crc != actual_crc {
            return Err(DbError::Corruption {
                details: format!("Tag index CRC mismatch in {:?}", path),
                series: None,
                timestamp: None,
            });
        }
        return Ok(TagBlockIndex::RoaringV3 { row_count, bitmaps });
    }

    let dict_count = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
        details: d,
        series: None,
        timestamp: None,
    })? as usize;
    let mut dict = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        let n = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
            details: d,
            series: None,
            timestamp: None,
        })? as usize;
        let mut b = vec![0u8; n];
        cur.read_exact(&mut b)?;
        dict.push(
            String::from_utf8(b).map_err(|e| DbError::Internal(format!("Invalid UTF-8: {}", e)))?,
        );
    }
    let num_entries = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
        details: d,
        series: None,
        timestamp: None,
    })? as usize;
    if version == 1 {
        // Bitmap of row_count bits = ceil(row_count/8) bytes.
        let bitmap_bytes = (row_count + 7) / 8;
        let mut bitmaps = HashMap::new();
        for _ in 0..num_entries {
            let kid = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let vid = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let mut bm = vec![0u8; bitmap_bytes];
            cur.read_exact(&mut bm)?;
            bitmaps.insert((kid, vid), bm);
        }
        Ok(TagBlockIndex::BitmapV1 {
            row_count,
            dict,
            bitmaps,
        })
    } else {
        let mut postings: HashMap<(u32, u32), Vec<u32>> = HashMap::new();
        for _ in 0..num_entries {
            let kid = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let vid = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })?;
            let n = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                details: d,
                series: None,
                timestamp: None,
            })? as usize;
            let mut out = Vec::with_capacity(n);
            let mut cur_idx = 0u32;
            for j in 0..n {
                let v = read_var_u32(&mut cur).map_err(|d| DbError::Corruption {
                    details: d,
                    series: None,
                    timestamp: None,
                })?;
                if j == 0 {
                    cur_idx = v;
                } else {
                    cur_idx = cur_idx.saturating_add(v);
                }
                out.push(cur_idx);
            }
            postings.insert((kid, vid), out);
        }
        Ok(TagBlockIndex::PostingsV2 {
            row_count,
            dict,
            postings,
        })
    }
}

#[cfg(test)]
mod encoding_compression_acceptance_tests {
    use super::*;

    fn make_rows() -> Vec<Row> {
        let mut t0: TagSet = TagSet::new();
        t0.insert("host".to_string(), "a".to_string());
        t0.insert("region".to_string(), "us-east-1".to_string());

        vec![
            Row {
                seq: 1,
                timestamp: 100,
                value: 1.25,
                tags: t0.clone(),
            },
            Row {
                seq: 2,
                timestamp: 105,
                value: 1.5,
                tags: t0,
            },
        ]
    }

    #[test]
    fn series_block_container_has_and_enforces_versioning() {
        let rows = make_rows();
        let enc = SegmentEncodingConfig {
            float_encoding: FloatEncoding::Raw64,
            tag_encoding: TagEncoding::Dictionary,
            compression: BlockCompression::None,
        };
        let mut block = encode_series_block(&rows, &enc).expect("encode");

        block[8..12].copy_from_slice(&u32::MAX.to_le_bytes());

        let err = decode_series_block_v2_container(&block, std::path::Path::new("dummy.seg"))
            .unwrap_err();
        match err {
            DbError::Corruption { details, .. } => {
                assert!(
                    details.contains("Unexpected series block version"),
                    "unexpected details: {details}"
                );
            }
            other => panic!("expected corruption error, got {other:?}"),
        }
    }

    #[test]
    fn series_block_container_has_and_enforces_payload_checksum() {
        let rows = make_rows();
        let enc = SegmentEncodingConfig {
            float_encoding: FloatEncoding::GorillaXor,
            tag_encoding: TagEncoding::Dictionary,
            compression: BlockCompression::Zstd { level: 1 },
        };
        let mut block = encode_series_block(&rows, &enc).expect("encode");

        let expected_crc_off = 8 + 4 + 4 + 4 + 4 + 4;
        block[expected_crc_off..expected_crc_off + 4].copy_from_slice(&0u32.to_le_bytes());

        let err = decode_series_block_v2_container(&block, std::path::Path::new("dummy.seg"))
            .unwrap_err();
        match err {
            DbError::Corruption { details, .. } => {
                assert!(
                    details.contains("payload CRC mismatch"),
                    "unexpected details: {details}"
                );
            }
            other => panic!("expected corruption error, got {other:?}"),
        }
    }

    #[test]
    fn v1_block_encode_decode_roundtrip_is_stable() {
        let rows = make_rows();
        let bytes = encode_series_block_v1(&rows).expect("v1 encode");
        let path = Path::new("v1_roundtrip_test");

        let decoded = decode_series_block_v1_all_rows(&bytes, path).expect("v1 decode all rows");
        assert_eq!(decoded.len(), rows.len());

        for (a, b) in decoded.iter().zip(rows.iter()) {
            assert_eq!(a.seq, b.seq);
            assert_eq!(a.timestamp, b.timestamp);
            assert!(
                a.value.to_bits() == b.value.to_bits(),
                "value must be bit-exact"
            );
            assert_eq!(a.tags, b.tags);
        }

        // Also validate the query-path decoder + tag matching logic is coherent for v1 blocks.
        let q = decode_series_block_v1_for_query(&bytes, path).expect("v1 decode for query");
        let mut filter = TagSet::new();
        filter.insert("host".to_string(), "a".to_string());
        for i in 0..rows.len() {
            let expected = check_tags(&rows[i].tags, &filter);
            let got = q.row_matches_filter_v1(i, &filter).expect("tag match");
            assert_eq!(got, expected, "row_matches_filter_v1 mismatch at i={}", i);
        }
    }
}
