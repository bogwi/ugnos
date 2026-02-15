#!/usr/bin/env bash
# Runtime verification: deploy ugnosd to a Kubernetes cluster, wait for Ready, assert /healthz and /readyz.
# Exit 0 only when the workload actually runs and probes succeed.
#
# Prerequisites:
#   - kubectl, reachable cluster, curl
#   - Image ugnosd:latest available to the cluster (e.g. docker build -t ugnosd:latest .
#     then kind load docker-image ugnosd:latest, or minikube image load ugnosd:latest)
#
# Run from ugnos project root: ./scripts/verify-k8s-deploy.sh

set -e

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
K8S_DIR="${ROOT}/deploy/k8s"
NS="ugnos-verify-${RANDOM}"

err() { echo "ERR: $*" >&2; exit 1; }
log() { echo "[verify-k8s-deploy] $*"; }

command -v kubectl >/dev/null || err "kubectl not found"
command -v curl >/dev/null || err "curl not found"

cleanup() {
  log "Cleaning up namespace ${NS}..."
  kubectl delete namespace "${NS}" --ignore-not-found --timeout=30s 2>/dev/null || true
  if [ -n "${PF_PID}" ] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

log "Creating namespace ${NS}..."
kubectl create namespace "${NS}"

log "Applying deploy/k8s/..."
kubectl apply -f "${K8S_DIR}/" -n "${NS}"

log "Waiting for deployment/ugnosd to be available (up to 120s)..."
if ! kubectl wait --for=condition=available deployment/ugnosd -n "${NS}" --timeout=120s 2>&1; then
  log "Deployment failed to become available. Pod status:"
  kubectl get pods -n "${NS}" -o wide 2>/dev/null || true
  kubectl describe deployment ugnosd -n "${NS}" 2>/dev/null | tail -30
  err "Deployment did not become available. Image ugnosd:latest may be missing; for kind: kind load docker-image ugnosd:latest"
fi

log "Pod is Ready; asserting /healthz and /readyz..."
kubectl port-forward -n "${NS}" "svc/ugnosd" 18080:8080 &
PF_PID=$!
sleep 3
if ! kill -0 "${PF_PID}" 2>/dev/null; then
  err "port-forward exited unexpectedly"
fi

if ! curl -sf "http://127.0.0.1:18080/healthz" >/dev/null; then
  err "GET /healthz did not return 200"
fi
if ! curl -sf "http://127.0.0.1:18080/readyz" >/dev/null; then
  err "GET /readyz did not return 200"
fi
log "healthz and readyz returned 200"

log "Kubernetes deployment verification passed (workload ran and probes succeeded)."
trap - EXIT
cleanup
exit 0
