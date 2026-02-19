//! Prometheus Remote Write ingest: `POST /api/v1/write`.
//!
//! Accepts Snappy-compressed protobuf `WriteRequest` payloads, maps `__name__` → series name,
//! remaining labels → `TagSet`, and sample timestamps (ms) → internal resolution (nanoseconds).
//! Exemplars and histograms in the payload are ignored; only `samples` are ingested.

use std::sync::Arc;

use crate::error::DbError;
use crate::prometheus;
use crate::telemetry::db_metrics;
use crate::types::{TagSet, Timestamp};
use crate::DbCore;
use http::StatusCode;
use prost::Message;

/// Prometheus metric name label; its value becomes the UGNOS series name.
const LABEL_NAME: &str = "__name__";

/// Converts Prometheus remote-write timestamp (milliseconds) to internal resolution (nanoseconds).
#[inline]
fn ms_to_ns(ts_ms: i64) -> Option<Timestamp> {
    u64::try_from(ts_ms).ok().map(|ms| ms * 1_000_000)
}

/// Extracts series name from `__name__` and builds tag set from remaining labels.
/// Returns `None` if `__name__` is missing (invalid series).
fn series_and_tags(labels: &[prometheus::Label]) -> Option<(String, TagSet)> {
    let mut name: Option<String> = None;
    let mut tags = TagSet::new();
    for l in labels {
        if l.name == LABEL_NAME {
            name = Some(l.value.clone());
        } else {
            tags.insert(l.name.clone(), l.value.clone());
        }
    }
    name.map(|n| (n, tags))
}

/// Result of handling a remote write request: status and body for the HTTP response.
#[derive(Debug)]
pub struct RemoteWriteResponse {
    /// HTTP status code.
    pub status: StatusCode,
    /// Response body (plain text, actionable for errors).
    pub body: Vec<u8>,
}

/// Decompresses Snappy-compressed body (raw Snappy format as used by Prometheus).
fn decompress_snappy(encoded: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoder = snap::raw::Decoder::new();
    decoder
        .decompress_vec(encoded)
        .map_err(|e| format!("snappy decompress failed: {}", e))
}

/// Handles a Prometheus Remote Write request: decompresses body, decodes WriteRequest,
/// maps each time series and sample to `DbCore::insert`, and returns the appropriate
/// HTTP status and body.
///
/// **Mapping:** `__name__` → series name; all other labels → `TagSet`; sample timestamp (ms) →
/// internal timestamp (nanoseconds). Exemplars and histograms in the payload are ignored.
///
/// **Errors:** Invalid or empty payload → 400 with actionable message; cardinality limit →
/// 429 with explicit error (metrics emitted in both cases).
pub fn handle_remote_write(body: &[u8], db: &Arc<DbCore>) -> RemoteWriteResponse {
    let decoded = match decompress_snappy(body) {
        Ok(d) => d,
        Err(e) => {
            db_metrics::record_remote_write_rejected("invalid_payload");
            return RemoteWriteResponse {
                status: StatusCode::BAD_REQUEST,
                body: format!("invalid remote write payload: {}", e).into_bytes(),
            };
        }
    };

    let write_req = match prometheus::WriteRequest::decode(decoded.as_slice()) {
        Ok(r) => r,
        Err(e) => {
            db_metrics::record_remote_write_rejected("invalid_payload");
            return RemoteWriteResponse {
                status: StatusCode::BAD_REQUEST,
                body: format!("invalid remote write protobuf: {}", e).into_bytes(),
            };
        }
    };

    let mut points_written: u64 = 0;

    for ts in &write_req.timeseries {
        let (series, tags) = match series_and_tags(&ts.labels) {
            Some(p) => p,
            None => {
                db_metrics::record_remote_write_rejected("invalid_payload");
                return RemoteWriteResponse {
                    status: StatusCode::BAD_REQUEST,
                    body: b"remote write: timeseries missing __name__ label".to_vec(),
                };
            }
        };

        for sample in &ts.samples {
            let timestamp_ns = match ms_to_ns(sample.timestamp) {
                Some(ns) => ns,
                None => {
                    db_metrics::record_remote_write_rejected("invalid_payload");
                    return RemoteWriteResponse {
                        status: StatusCode::BAD_REQUEST,
                        body: format!(
                            "remote write: invalid timestamp ms={} (must be non-negative)",
                            sample.timestamp
                        )
                        .into_bytes(),
                    };
                }
            };

            match db.insert(&series, timestamp_ns, sample.value, tags.clone()) {
                Ok(()) => points_written = points_written.saturating_add(1),
                Err(DbError::SeriesCardinalityLimitExceeded {
                    scope,
                    limit,
                    current,
                }) => {
                    db_metrics::record_cardinality_limit_rejected(&scope);
                    db_metrics::record_remote_write_rejected("cardinality_limit");
                    return RemoteWriteResponse {
                        status: StatusCode::TOO_MANY_REQUESTS,
                        body: format!(
                            "series cardinality limit exceeded: scope={}, current={}, limit={}; points_written={}",
                            scope, current, limit, points_written
                        )
                        .into_bytes(),
                    };
                }
                Err(e) => {
                    db_metrics::record_remote_write_rejected("internal");
                    return RemoteWriteResponse {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        body: format!("remote write ingest error: {}", e).into_bytes(),
                    };
                }
            }
        }
    }

    RemoteWriteResponse {
        status: StatusCode::OK,
        body: format!("ok points={}", points_written).into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::{handle_remote_write, series_and_tags, ms_to_ns};
    use crate::prometheus::{Label, Sample, TimeSeries, WriteRequest};
    use crate::DbCore;
    use prost::Message;
    use std::sync::Arc;

    fn encode_snappy(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = snap::raw::Encoder::new();
        encoder.compress_vec(bytes).unwrap()
    }

    #[test]
    fn ms_to_ns_positive() {
        assert_eq!(ms_to_ns(0), Some(0));
        assert_eq!(ms_to_ns(1), Some(1_000_000));
        assert_eq!(ms_to_ns(1000), Some(1_000_000_000));
    }

    #[test]
    fn ms_to_ns_negative_rejected() {
        assert_eq!(ms_to_ns(-1), None);
    }

    #[test]
    fn series_and_tags_extracts_name_and_tags() {
        let labels = [
            Label {
                name: "__name__".to_string(),
                value: "http_requests_total".to_string(),
            },
            Label {
                name: "job".to_string(),
                value: "api".to_string(),
            },
            Label {
                name: "method".to_string(),
                value: "GET".to_string(),
            },
        ];
        let (name, tags) = series_and_tags(&labels).unwrap();
        assert_eq!(name, "http_requests_total");
        assert_eq!(tags.get("job"), Some(&"api".to_string()));
        assert_eq!(tags.get("method"), Some(&"GET".to_string()));
        assert!(tags.get("__name__").is_none());
    }

    #[test]
    fn series_and_tags_missing_name_returns_none() {
        let labels = [Label {
            name: "job".to_string(),
            value: "api".to_string(),
        }];
        assert!(series_and_tags(&labels).is_none());
    }

    #[test]
    fn roundtrip_decode_snappy_write_request() {
        let wr = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_string(),
                        value: "x".to_string(),
                    },
                    Label {
                        name: "a".to_string(),
                        value: "b".to_string(),
                    },
                ],
                samples: vec![
                    Sample {
                        value: 1.0,
                        timestamp: 1000,
                    },
                    Sample {
                        value: 2.0,
                        timestamp: 2000,
                    },
                ],
            }],
        };
        let mut buf = Vec::new();
        wr.encode(&mut buf).unwrap();
        let compressed = encode_snappy(&buf);
        let decoded = super::decompress_snappy(&compressed).unwrap();
        let parsed = crate::prometheus::WriteRequest::decode(decoded.as_slice()).unwrap();
        assert_eq!(parsed.timeseries.len(), 1);
        assert_eq!(parsed.timeseries[0].labels.len(), 2);
        assert_eq!(parsed.timeseries[0].samples.len(), 2);
    }

    #[test]
    fn handle_remote_write_invalid_snappy_returns_400() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::DbConfig::default();
        config.data_dir = dir.path().to_path_buf();
        let db = Arc::new(DbCore::with_config(config).unwrap());
        let r = handle_remote_write(b"not snappy", &db);
        assert_eq!(r.status, http::StatusCode::BAD_REQUEST);
        assert!(r.body.starts_with(b"invalid remote write payload"));
    }

    #[test]
    fn handle_remote_write_valid_ingests_points() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::DbConfig::default();
        config.data_dir = dir.path().to_path_buf();
        let mut db = DbCore::with_config(config).unwrap();
        db.recover().unwrap();
        let db = Arc::new(db);

        let wr = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_string(),
                        value: "metric_a".to_string(),
                    },
                    Label {
                        name: "env".to_string(),
                        value: "test".to_string(),
                    },
                ],
                samples: vec![
                    Sample {
                        value: 42.5,
                        timestamp: 1_000,
                    },
                    Sample {
                        value: 43.0,
                        timestamp: 2_000,
                    },
                ],
            }],
        };
        let mut buf = Vec::new();
        wr.encode(&mut buf).unwrap();
        let body = encode_snappy(&buf);

        let r = handle_remote_write(&body, &db);
        assert_eq!(r.status, http::StatusCode::OK);
        assert!(r.body.starts_with(b"ok points=2"));

        db.flush().unwrap();
        let points = db
            .query("metric_a", 0..u64::MAX, None)
            .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0], (1_000_000_000, 42.5));
        assert_eq!(points[1], (2_000_000_000, 43.0));
    }

    #[test]
    fn handle_remote_write_missing_name_returns_400() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::DbConfig::default();
        config.data_dir = dir.path().to_path_buf();
        let db = Arc::new(DbCore::with_config(config).unwrap());
        let wr = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "job".to_string(),
                    value: "x".to_string(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 0,
                }],
            }],
        };
        let mut buf = Vec::new();
        wr.encode(&mut buf).unwrap();
        let body = encode_snappy(&buf);
        let r = handle_remote_write(&body, &db);
        assert_eq!(r.status, http::StatusCode::BAD_REQUEST);
        assert!(
            std::str::from_utf8(&r.body).unwrap().contains("__name__"),
            "body should mention __name__: {:?}",
            std::str::from_utf8(&r.body)
        );
    }

    /// Adversarial: cardinality limit causes 429 and explicit error body.
    #[test]
    fn handle_remote_write_cardinality_limit_returns_429() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::DbConfig::default();
        config.data_dir = dir.path().to_path_buf();
        config.max_series_cardinality = Some(1);
        let mut db = DbCore::with_config(config).unwrap();
        db.recover().unwrap();
        let db = Arc::new(db);

        // First series succeeds; second series hits cardinality limit.
        let wr = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![
                        Label {
                            name: "__name__".to_string(),
                            value: "first".to_string(),
                        },
                        Label {
                            name: "a".to_string(),
                            value: "1".to_string(),
                        },
                    ],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 1000,
                    }],
                },
                TimeSeries {
                    labels: vec![
                        Label {
                            name: "__name__".to_string(),
                            value: "second".to_string(),
                        },
                        Label {
                            name: "a".to_string(),
                            value: "2".to_string(),
                        },
                    ],
                    samples: vec![Sample {
                        value: 2.0,
                        timestamp: 2000,
                    }],
                },
            ],
        };
        let mut buf = Vec::new();
        wr.encode(&mut buf).unwrap();
        let body = encode_snappy(&buf);
        let r = handle_remote_write(&body, &db);
        assert_eq!(r.status, http::StatusCode::TOO_MANY_REQUESTS);
        let body_str = std::str::from_utf8(&r.body).unwrap();
        assert!(
            body_str.contains("cardinality limit"),
            "body should mention cardinality limit: {}",
            body_str
        );
        assert!(
            body_str.contains("points_written=1"),
            "body should report points_written before rejection: {}",
            body_str
        );
    }
}
