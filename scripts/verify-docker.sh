#!/usr/bin/env bash
# Adversarial verification for ugnosd Docker image and docker-compose quickstart.
# Run from ugnos project root: ./scripts/verify-docker.sh
# Requires: docker, docker compose (v2), curl.

set -e

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

err() { echo "ERR: $*" >&2; exit 1; }
log() { echo "[verify] $*"; }

command -v docker >/dev/null || err "docker not found"
docker compose version >/dev/null 2>&1 || err "docker compose not found"
command -v curl >/dev/null || err "curl not found"

# --- Build ---
log "Building image..."
docker build -t ugnosd:verify .

# --- Compose up and health checks ---
log "Starting stack..."
docker compose up -d

cleanup() {
  log "Stopping stack..."
  docker compose down
}
trap cleanup EXIT

log "Waiting for /healthz and /readyz (up to 30s)..."
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:8080/healthz" >/dev/null && curl -sf "http://127.0.0.1:8080/readyz" >/dev/null; then
    log "healthz and readyz returned 200"
    break
  fi
  if [ "$i" -eq 30 ]; then
    err "healthz/readyz did not return 200 within 30s"
  fi
  sleep 1
done

# --- Acceptance: Docker with config file serves /healthz and /readyz ---
log "Acceptance: run container with mounted config, assert /healthz and /readyz 200..."
CONFIG_DIR="${ROOT}/.verify-docker-config"
mkdir -p "$CONFIG_DIR"
cat > "${CONFIG_DIR}/ugnosd.toml" << 'TOML'
data_dir = "/var/lib/ugnos"
flush_interval_secs = 1
enable_wal = true
enable_segments = true
http_bind = "0.0.0.0:8080"
[segment_store]
compaction_check_interval_secs = 1
TOML
CONTAINER_NAME="ugnosd-verify-config"
docker run -d --name "$CONTAINER_NAME" -p 9080:8080 \
  -v "${CONFIG_DIR}/ugnosd.toml:/etc/ugnosd.toml:ro" \
  -e UGNOS_DATA_DIR=/var/lib/ugnos \
  ugnosd:verify --config /etc/ugnosd.toml --http-bind 0.0.0.0:8080
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:9080/healthz" >/dev/null && curl -sf "http://127.0.0.1:9080/readyz" >/dev/null; then
    log "With config: healthz and readyz returned 200"
    break
  fi
  if [ "$i" -eq 30 ]; then
    docker rm -f "$CONTAINER_NAME" 2>/dev/null
    rm -rf "$CONFIG_DIR"
    err "With config: healthz/readyz did not return 200 within 30s"
  fi
  sleep 1
done
docker rm -f "$CONTAINER_NAME" 2>/dev/null
rm -rf "$CONFIG_DIR"

# --- Adversarial: invalid config should fail startup ---
log "Adversarial: container with invalid UGNOS_HTTP_BIND should exit non-zero..."
RUN_RESULT=0
docker run --rm -e UGNOS_HTTP_BIND=not-a-valid-address -e UGNOS_DATA_DIR=/var/lib/ugnos ugnosd:verify --no-config --data-dir /var/lib/ugnos 2>/dev/null || RUN_RESULT=$?
if [ "$RUN_RESULT" -eq 0 ]; then
  err "Expected container with invalid UGNOS_HTTP_BIND to exit non-zero, got 0"
fi
log "Invalid config correctly exited with non-zero"

log "All checks passed."
