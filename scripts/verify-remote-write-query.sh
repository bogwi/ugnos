#!/usr/bin/env bash
# Verifies HTTP write→query path: POST /api/v1/write, then GET /api/v1/query, assert data matches.
# Fills the gap: no script previously validated the HTTP Remote Write → Prometheus API read flow.
# Run from ugnos project root: ./scripts/verify-remote-write-query.sh
# Requires: cargo, curl.
#
# With auth enabled (both write and read tokens), asserts:
# - Write without token → 401
# - Query without token → 401
# - Write with token → 200
# - Query with token (after flush) → 200, non-empty result, metric and value correct

set -e

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

err() { echo "ERR: $*" >&2; exit 1; }
log() { echo "[verify-remote-write-query] $*"; }

command -v cargo >/dev/null || err "cargo not found"
command -v curl >/dev/null || err "curl not found"

# --- Build ---
log "Building ugnosd..."
cargo build --release -q 2>/dev/null || cargo build --release

DATA_DIR="$(mktemp -d)"
BIND="127.0.0.1:18181"
TOKEN="verify-rwq-token"

# list_series_keys (used by Prometheus query) is only populated when max_series_cardinality is set
# gen_minimal_write_request writes auth_verify_metric at timestamp 1000ms (= 1s Unix)
METRIC="auth_verify_metric"
QUERY_TIME="2"

cleanup() {
  if [ -n "${DAEMON_PID:-}" ]; then
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# --- Start daemon with both write and read tokens ---
log "Starting ugnosd with UGNOS__HTTP_WRITE_TOKEN and UGNOS__HTTP_READ_TOKEN..."
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

# --- 401 without token: write ---
log "Assert: POST /api/v1/write without token → 401"
HTTP_STATUS="$(curl -s -o /tmp/ugnos_rwq_body -w '%{http_code}' \
  -X POST \
  -H "Content-Type: application/x-protobuf" \
  --data-binary "x" \
  "http://$BIND/api/v1/write")"
if [ "$HTTP_STATUS" != "401" ]; then
  err "Expected 401 without token (write), got HTTP $HTTP_STATUS"
fi
log "401 write without token: OK"

# --- 401 without token: query ---
log "Assert: GET /api/v1/query without token → 401"
HTTP_STATUS="$(curl -s -o /tmp/ugnos_rwq_body -w '%{http_code}' \
  "http://$BIND/api/v1/query?query=${METRIC}&time=${QUERY_TIME}")"
if [ "$HTTP_STATUS" != "401" ]; then
  err "Expected 401 without token (query), got HTTP $HTTP_STATUS"
fi
log "401 query without token: OK"

# --- POST write with token ---
log "Assert: POST /api/v1/write with token + valid payload → 200"
HTTP_STATUS="$(cargo run --release --example gen_minimal_write_request -q 2>/dev/null | \
  curl -s -o /tmp/ugnos_rwq_body -w '%{http_code}' \
  -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @- \
  "http://$BIND/api/v1/write")"
if [ "$HTTP_STATUS" != "200" ]; then
  BODY="$(cat /tmp/ugnos_rwq_body 2>/dev/null || true)"
  err "Expected 200 with token (write), got HTTP $HTTP_STATUS; body: $BODY"
fi
log "200 write with token: OK"

# --- Wait for flush (default flush_interval is 1s) ---
log "Waiting 2s for flush..."
sleep 2

# --- GET query with token, assert result ---
log "Assert: GET /api/v1/query with token → 200, non-empty, metric/value correct"
HTTP_STATUS="$(curl -s -o /tmp/ugnos_rwq_body -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://$BIND/api/v1/query?query=${METRIC}&time=${QUERY_TIME}")"
if [ "$HTTP_STATUS" != "200" ]; then
  BODY="$(cat /tmp/ugnos_rwq_body 2>/dev/null || true)"
  err "Expected 200 with token (query), got HTTP $HTTP_STATUS; body: $BODY"
fi

BODY="$(cat /tmp/ugnos_rwq_body 2>/dev/null || true)"
if ! echo "$BODY" | grep -q '"status":"success"'; then
  err "Expected status success in query response, got: ${BODY:0:200}"
fi
if ! echo "$BODY" | grep -q "$METRIC"; then
  err "Expected metric $METRIC in query result, got: ${BODY:0:400}"
fi
if ! echo "$BODY" | grep -qE '"1"|"1\.0"'; then
  err "Expected value 1 or 1.0 in query result (gen_minimal_write_request writes 1.0), got: ${BODY:0:400}"
fi
if ! echo "$BODY" | grep -q '"resultType":"vector"'; then
  err "Expected resultType vector, got: ${BODY:0:200}"
fi
log "Query result OK: status success, metric $METRIC, value 1"

# --- Optional: query_range ---
log "Assert: GET /api/v1/query_range with token → 200, non-empty"
HTTP_STATUS="$(curl -s -o /tmp/ugnos_rwq_body -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://$BIND/api/v1/query_range?query=${METRIC}&start=0&end=10&step=1")"
if [ "$HTTP_STATUS" != "200" ]; then
  BODY="$(cat /tmp/ugnos_rwq_body 2>/dev/null || true)"
  err "Expected 200 for query_range, got HTTP $HTTP_STATUS; body: $BODY"
fi
BODY="$(cat /tmp/ugnos_rwq_body 2>/dev/null || true)"
if ! echo "$BODY" | grep -q '"resultType":"matrix"'; then
  err "Expected resultType matrix in query_range, got: ${BODY:0:200}"
fi
if ! echo "$BODY" | grep -q "$METRIC"; then
  err "Expected metric $METRIC in query_range result, got: ${BODY:0:400}"
fi
log "Query range result OK"

log "All write→query checks passed."
