#!/usr/bin/env bash
# Validates Kubernetes manifests for ugnosd. Exit 0 only when validation was actually run and passed.
# Requires either: kubeconform (offline), or kubectl + a reachable cluster (dry-run).
# Run from ugnos project root: ./scripts/verify-k8s-manifests.sh

set -e

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
K8S_DIR="${ROOT}/deploy/k8s"

err() { echo "ERR: $*" >&2; exit 1; }
log() { echo "[verify-k8s] $*"; }

# Offline validation when kubeconform is available (no cluster needed).
if command -v kubeconform >/dev/null 2>&1; then
  log "Validating with kubeconform (offline)..."
  kubeconform -summary -output text "${K8S_DIR}"/*.yaml || err "kubeconform validation failed"
  log "All manifests valid."
  exit 0
fi

# Otherwise require kubectl and a reachable cluster. Do not exit 0 when we did not validate.
if ! command -v kubectl >/dev/null 2>&1; then
  err "kubectl not found. Install kubeconform for offline validation, or kubectl + a reachable cluster. No validation was performed."
fi

log "Validating with kubectl apply --dry-run=client (cluster must be reachable)..."
set +e
out=$(kubectl apply --dry-run=client -f "${K8S_DIR}/" 2>&1)
r=$?
set -e
if [ $r -ne 0 ]; then
  set +e
  echo "$out" | grep -q "connection refused"
  c1=$?
  echo "$out" | grep -q "dial tcp"
  c2=$?
  set -e
  if [ $c1 -eq 0 ] || [ $c2 -eq 0 ]; then
    err "No cluster reachable; validation was NOT performed. To pass: use a reachable cluster or install kubeconform for offline validation. Do not treat this as success."
  fi
  echo "$out" >&2
  err "kubectl dry-run failed."
fi
log "All manifests valid."
