//! Emits a minimal Snappy-compressed Prometheus WriteRequest to stdout.
//!
//! Used by `scripts/verify-http-auth.sh` to produce a valid remote-write payload
//! for testing auth (401 vs 200). The payload contains one series with one sample.
//!
//! When ugnosd has auth enabled, configure `UGNOS__HTTP_WRITE_TOKEN` or `http_write_token`
//! in TOML (see README). POST with `Authorization: Bearer <token>`.
//!
//! Run with: `cargo run --example gen_minimal_write_request`
//!
//! Output is raw binary (Snappy-compressed protobuf). Running directly will show
//! garbled bytes in the terminal; pipe the output to curl or another consumer.
//!
//! Simplest way: run `./scripts/verify-http-auth.sh`. To do it manually:
//!
//! Terminal 1 (start daemon, leave running):
//! ```ignore
//! DATA=$(mktemp -d)
//! UGNOS__HTTP_WRITE_TOKEN=verify-auth-token ./target/release/ugnosd \
//!   --no-config --data-dir "$DATA" --http-bind 127.0.0.1:18180
//! ```
//!
//! Terminal 2 (pipe payload to curl):
//! ```ignore
//! TOKEN=verify-auth-token cargo run -q --example gen_minimal_write_request 2>/dev/null | \
//!   curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/x-protobuf" \
//!   --data-binary @- http://127.0.0.1:18180/api/v1/write
//! ```
//!
//! Stop daemon with Ctrl+C in terminal 1. Wait for it to exit fully before `rm -rf "$DATA"`.

use prost::Message;
use std::io::{self, Write};

fn main() {
    let wr = ugnos::prometheus::WriteRequest {
        timeseries: vec![ugnos::prometheus::TimeSeries {
            labels: vec![
                ugnos::prometheus::Label {
                    name: "__name__".to_string(),
                    value: "auth_verify_metric".to_string(),
                },
                ugnos::prometheus::Label {
                    name: "script".to_string(),
                    value: "verify-http-auth".to_string(),
                },
            ],
            samples: vec![ugnos::prometheus::Sample {
                value: 1.0,
                timestamp: 1000,
            }],
        }],
    };
    let mut buf = Vec::new();
    wr.encode(&mut buf).expect("encode WriteRequest");
    let mut encoder = snap::raw::Encoder::new();
    let compressed = encoder.compress_vec(&buf).expect("snappy compress");
    io::stdout().write_all(&compressed).expect("write stdout");
}
