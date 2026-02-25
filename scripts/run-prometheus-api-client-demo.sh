#!/usr/bin/env bash
# Runs prometheus_api_client_demo with all prerequisites: starts ugnosd, writes
# data, waits for flush, then runs the example. Run from ugnos project root:
#   ./scripts/run-prometheus-api-client-demo.sh
# Requires: cargo, curl.

set -e

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

err() { echo "ERR: $*" >&2; exit 1; }
log() { echo "[run-prometheus-api-client-demo] $*"; }

command -v cargo >/dev/null || err "cargo not found"
command -v curl >/dev/null || err "curl not found"

# --- Build ---
log "Building ugnosd and prometheus_api_client_demo..."
cargo build --release -q 2>/dev/null || cargo build --release
cargo build --release --example prometheus_api_client_demo -q 2>/dev/null || cargo build --release --example prometheus_api_client_demo

DATA_DIR="$(mktemp -d)"
BIND="127.0.0.1:18182"
BASE="http://$BIND"
TOKEN="demo-read-token"

cleanup() {
  if [ -n "${DAEMON_PID:-}" ]; then
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# --- Start daemon ---
log "Starting ugnosd (auth + max_series_cardinality for labels/series)..."
UGNOS__HTTP_WRITE_TOKEN="$TOKEN" \
UGNOS__HTTP_READ_TOKEN="$TOKEN" \
UGNOS__MAX_SERIES_CARDINALITY=10000 \
  "$ROOT/target/release/ugnosd" \
  --no-config \
  --data-dir "$DATA_DIR" \
  --http-bind "$BIND" \
  >/dev/null 2>&1 &
DAEMON_PID=$!

log "Waiting for daemon (up to 15s)..."
for i in $(seq 1 15); do
  if curl -sf "http://$BIND/healthz" >/dev/null 2>&1; then
    log "Daemon ready"
    break
  fi
  if [ "$i" -eq 15 ]; then
    err "Daemon did not become ready within 15s"
  fi
  sleep 1
done

# --- Write sample data ---
log "Writing sample data (gen_minimal_write_request → POST /api/v1/write)..."
HTTP_STATUS="$(cargo run --release --example gen_minimal_write_request -q 2>/dev/null | \
  curl -s -o /dev/null -w '%{http_code}' \
  -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @- \
  "http://$BIND/api/v1/write")"
if [ "$HTTP_STATUS" != "200" ]; then
  err "Write failed with HTTP $HTTP_STATUS"
fi
log "Write OK"

# --- Wait for flush ---
log "Waiting 2s for flush..."
sleep 2

# --- Run the Prometheus API client demo ---
log "Running prometheus_api_client_demo..."
echo ""
UGNOS_API_BASE="$BASE" UGNOS__HTTP_READ_TOKEN="$TOKEN" \
  cargo run --release --example prometheus_api_client_demo
echo ""
log "Done."
