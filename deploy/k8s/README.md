# Kubernetes manifests for ugnosd

## Prove the deployment works (one run, no manual setup)

On a machine with **Docker**, **kind**, **kubectl**, and **curl** installed, a single command creates a local cluster, builds the image, deploys, verifies `/healthz` and `/readyz`, then tears down:

```bash
./scripts/verify-k8s-local.sh
```

Exit 0 only when the workload actually ran and both probes returned 200. No pre-created cluster or pre-loaded image; the script does everything. (Scripts live under `scripts/`; see main README for Docker verification.)

## Prerequisites (for manual apply)

- **Image**: The deployment uses `ugnosd:latest` by default. Either:
  - **Production**: Push your image to a registry and set `spec.template.spec.containers[0].image` (e.g. `ghcr.io/your-org/ugnosd:0.4.2`) and `imagePullPolicy: Always` (or omit).
  - **Local (kind/minikube)**: Build and load the image, then apply with `imagePullPolicy: IfNotPresent` (already set).

## Verify against an existing cluster

If you already have a cluster and have built/loaded `ugnosd:latest`:

```bash
docker build -t ugnosd:latest . && kind load docker-image ugnosd:latest   # if using kind
./scripts/verify-k8s-deploy.sh
```

That script deploys to the current `kubectl` context, waits for Ready, then asserts `/healthz` and `/readyz`. Requires: `kubectl`, reachable cluster, `curl`, image available to the cluster.

## Validate manifests (syntax/schema only)

**Manifest validation** (valid YAML and API schema) does **not** prove the deployment will work at runtime. Use it for quick checks or CI without a cluster.

**Script:** `./scripts/verify-k8s-manifests.sh`  
Exit 0 only when kubeconform or `kubectl apply --dry-run=client` actually ran and passed. Otherwise exits 1 (no false success).

**Manual (requires reachable cluster):**

```bash
kubectl apply --dry-run=client -f deploy/k8s/
```

## Apply

```bash
kubectl apply -f deploy/k8s/
```

Apply ConfigMap only if you want to run with a config file; then mount it in the Deployment and add `--config /etc/ugnosd/ugnosd.toml` to args.

## Assertions (why this deployment does not fail)

| Requirement | Implementation |
|-------------|----------------|
| Daemon runs as non-root (uid 10001) | `securityContext.runAsUser/runAsGroup: 10001`, `runAsNonRoot: true` |
| Data dir writable | `fsGroup: 10001` so emptyDir is group-writable by the container process |
| Graceful shutdown (WAL flush, compaction stop) | `terminationGracePeriodSeconds: 35` (daemon needs up to 30s) |
| Liveness/readiness | `GET /healthz` and `GET /readyz` on port 8080; daemon serves both |
| Service targets pods | `selector.app: ugnosd` matches pod template labels |
