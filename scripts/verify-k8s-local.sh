#!/usr/bin/env bash
# One-shot proof that the Kubernetes deployment works: create a local kind cluster,
# build the image, load it, deploy manifests, assert /healthz and /readyz, then tear down.
# No manual cluster or image setup. Exit 0 only when the workload ran and probes passed.
#
# Requires: docker, kind, kubectl, curl.
# Run from ugnos project root: ./scripts/verify-k8s-local.sh

set -e

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
K8S_DIR="${ROOT}/deploy/k8s"
KIND_NAME="ugnos-verify-${RANDOM}"
NS="ugnos-verify-${RANDOM}"
PF_PID=""

err() { echo "ERR: $*" >&2; exit 1; }
log() { echo "[verify-k8s-local] $*"; }

command -v docker >/dev/null || err "docker not found"
command -v kind >/dev/null || err "kind not found (install: https://kind.sigs.k8s.io/docs/user/quick-start/)"
command -v kubectl >/dev/null || err "kubectl not found"
command -v curl >/dev/null || err "curl not found"

# Remember current context so we can restore it after deleting our cluster.
SAVED_CONTEXT=""
if kubectl config current-context >/dev/null 2>&1; then
  SAVED_CONTEXT=$(kubectl config current-context)
fi

cleanup() {
  if [ -n "${PF_PID}" ] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
  log "Deleting namespace ${NS}..."
  kubectl delete namespace "${NS}" --ignore-not-found --timeout=30s 2>/dev/null || true
  log "Deleting kind cluster ${KIND_NAME}..."
  kind delete cluster --name "${KIND_NAME}" 2>/dev/null || true
  if [ -n "${SAVED_CONTEXT}" ]; then
    kubectl config use-context "${SAVED_CONTEXT}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

log "Creating kind cluster ${KIND_NAME}..."
kind create cluster --name "${KIND_NAME}" --wait 2m

log "Building ugnosd image..."
docker build -t ugnosd:latest .

log "Loading ugnosd:latest into kind cluster..."
kind load docker-image ugnosd:latest --name "${KIND_NAME}"

log "Creating namespace ${NS}..."
kubectl create namespace "${NS}"

log "Applying deploy/k8s/..."
kubectl apply -f "${K8S_DIR}/" -n "${NS}"

log "Waiting for deployment/ugnosd to be available (up to 120s)..."
if ! kubectl wait --for=condition=available deployment/ugnosd -n "${NS}" --timeout=120s 2>&1; then
  log "Deployment failed to become available. Pod status:"
  kubectl get pods -n "${NS}" -o wide 2>/dev/null || true
  kubectl describe deployment ugnosd -n "${NS}" 2>/dev/null | tail -40
  err "Deployment did not become available."
fi

log "Asserting /healthz and /readyz..."
kubectl port-forward -n "${NS}" "svc/ugnosd" 18080:8080 &
PF_PID=$!
for i in $(seq 1 15); do
  if curl -sf "http://127.0.0.1:18080/healthz" >/dev/null && curl -sf "http://127.0.0.1:18080/readyz" >/dev/null; then
    break
  fi
  if [ "$i" -eq 15 ]; then
    err "GET /healthz or /readyz did not return 200 within 15s"
  fi
  sleep 1
done
log "healthz and readyz returned 200"

log "Kubernetes deployment verified (workload ran, probes passed). Tearing down."
trap - EXIT
cleanup
exit 0
