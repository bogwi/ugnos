#!/usr/bin/env bash
# Verifies HTTP auth: without token → 401, with token → 200 (POST /api/v1/write).
# Run from ugnos project root: ./scripts/verify-http-auth.sh
# Requires: cargo, curl.

set -e

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

err() { echo "ERR: $*" >&2; exit 1; }
log() { echo "[verify-http-auth] $*"; }

command -v cargo >/dev/null || err "cargo not found"
command -v curl >/dev/null || err "curl not found"

# --- Build ---
log "Building ugnosd..."
cargo build --release -q 2>/dev/null || cargo build --release

DATA_DIR="$(mktemp -d)"
BIND="127.0.0.1:18180"
TOKEN="verify-auth-token"

cleanup() {
  if [ -n "${DAEMON_PID:-}" ]; then
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# --- Start daemon with http_write_token ---
log "Starting ugnosd with UGNOS__HTTP_WRITE_TOKEN..."
UGNOS__HTTP_WRITE_TOKEN="$TOKEN" "$ROOT/target/release/ugnosd" \
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

# --- 401 without token ---
log "Assert: POST /api/v1/write without token → 401"
HTTP_STATUS="$(curl -s -o /tmp/ugnos_auth_body -w '%{http_code}' \
  -X POST \
  -H "Content-Type: application/x-protobuf" \
  --data-binary "x" \
  "http://$BIND/api/v1/write")"
if [ "$HTTP_STATUS" != "401" ]; then
  err "Expected 401 without token, got HTTP $HTTP_STATUS"
fi
BODY="$(cat /tmp/ugnos_auth_body 2>/dev/null || true)"
if ! echo "$BODY" | grep -qE "auth|unauthorized"; then
  err "Expected auth-related body, got: $BODY"
fi
log "401 without token: OK"

# --- 200 with valid token and payload ---
log "Assert: POST /api/v1/write with token + valid payload → 200"
HTTP_STATUS="$(cargo run --release --example gen_minimal_write_request -q 2>/dev/null | \
  curl -s -o /tmp/ugnos_auth_body -w '%{http_code}' \
  -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @- \
  "http://$BIND/api/v1/write")"
if [ "$HTTP_STATUS" != "200" ]; then
  BODY="$(cat /tmp/ugnos_auth_body 2>/dev/null || true)"
  err "Expected 200 with token, got HTTP $HTTP_STATUS; body: $BODY"
fi
log "200 with token: OK"

log "All HTTP auth checks passed."
