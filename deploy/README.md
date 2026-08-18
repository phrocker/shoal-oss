# Shoal platform deployment

This directory contains platform artifacts for running Shoal as two Kubernetes tiers:

- **Write tier**: one `shoal-embed` StatefulSet per shard. Each pod has stable identity and a persistent local data directory for WAL/tablet state. Today the binary is started as `shoal-embed serve --data <dir> --address 0.0.0.0:<port> --metrics-address 0.0.0.0:<port>`.
- **Read fleet**: stateless `shoal` pods behind a Service. They serve the Accumulo-compatible Thrift scan surface and open immutable RFiles through the configured storage backend.

For local development these collapse into a single `shoal-embed` process. The intended platform shape is: writes land in a shard-local `shoal-embed`, flush/compaction emits RFiles to a shared object-store prefix, and read-fleet pods open those same RFiles for hedged scans.

## Artifacts

- `../Dockerfile`: multi-stage Linux/amd64 image. It builds the two platform runtime binaries, `/shoal-embed` and `/shoal`, into a distroless static image. The image has no fixed entrypoint; choose a binary with Kubernetes `command`.
- `k8s/`: plain Kubernetes YAML for the write tier, read fleet, shared ConfigMap, and placeholder Secret.
- `helm/shoal/`: minimal Helm chart wrapping the same resources.

## Build and push

```bash
docker build -t ghcr.io/YOUR_ORG/shoal:TAG .
docker push ghcr.io/YOUR_ORG/shoal:TAG
```

If you cannot run Docker locally, validate the same Go code path with:

```bash
go build ./...
```

## Deploy with plain manifests

Edit `deploy/k8s/configmap.yaml` and image names in `write-tier.yaml` / `read-fleet.yaml`, then apply:

```bash
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/write-tier.yaml
kubectl apply -f deploy/k8s/read-fleet.yaml
```

The Secret is a template only. Replace `key.json` with a real GCS service-account key if not using Workload Identity / node Application Default Credentials, and replace `accumulo-password` with the metadata-walk password used by `cmd/shoal`.

## Deploy with Helm

```bash
helm upgrade --install shoal deploy/helm/shoal \
  --set image.repository=ghcr.io/YOUR_ORG/shoal \
  --set image.tag=TAG \
  --set objectStorage.bucket=YOUR_BUCKET \
  --set objectStorage.prefix=shards/shard-0000 \
  --set readFleet.zk='zk-0.zk:2181,zk-1.zk:2181,zk-2.zk:2181' \
  --set readFleet.accumuloPassword='REPLACE_ME'
```

## Wired binary flags and environment

Confirmed from source:

- `cmd/shoal-embed/main.go`: `serve` supports `--data`, `--address` (gRPC bind address, e.g. `0.0.0.0:9876`; defaults to `127.0.0.1:<port>` when only the legacy `--port` is set), `--metrics-port` / `--metrics-address` (HTTP health/readiness/metrics bind, default `127.0.0.1:9877`), and `--drain-timeout` (default `30s`, bounds graceful shutdown; see below).
- `cmd/shoal/main.go`: read fleet supports `-listen`, `-zk`, `-instance`, `-accumulo-version`, `-user`, `-password` or `SHOAL_PASSWORD`, `-zk-timeout`, `-storage`, `-cache-bytes`, `-log-level`, `-prewarm-tables`, and `-prewarm-parallelism`.
- `internal/storage/gcs/gcs.go`: the GCS backend uses Application Default Credentials and accepts paths like `gs://bucket/object` or `bucket/object`.

The manifests pass only supported process flags to the containers. `GOOGLE_APPLICATION_CREDENTIALS` is set for the GCS client when a key Secret is mounted.

## Health, metrics, and graceful drain

`shoal-embed serve` exposes an HTTP surface (default `0.0.0.0:9877` in the manifests, via `--metrics-address`) alongside the gRPC write port:

- `/healthz`: unconditional liveness — 200 once the process has started serving. Used for `livenessProbe` in both manifests.
- `/readyz`: readiness — 200 once startup finishes, 503 during startup and during shutdown drain. Used for `readinessProbe`, so a draining pod is removed from Service endpoints before its gRPC port stops accepting new work.
- `/metrics`: Prometheus text-format metrics (via `internal/obs`). Not yet wired to a `ServiceMonitor`/scrape annotation in these manifests; add one for your Prometheus setup if needed.

On `SIGINT`/`SIGTERM`, `cmdServe` flips `/readyz` to not-ready immediately, then waits up to `--drain-timeout` (default `30s`) for in-flight gRPC calls to finish via `grpc.Server.GracefulStop()`. If that deadline passes, it force-stops remaining RPCs (`grpc.Server.Stop()`) rather than hanging indefinitely, then closes the engine and exits. This bounds shutdown end-to-end, but the final engine-close step itself is not time-boxed (forcibly interrupting it risks leaving WAL/tablet state in a torn condition, which is worse than a slightly longer shutdown). The write-tier `terminationGracePeriodSeconds` (`40s`) is set above the `30s` drain default to leave margin for that close step before Kubernetes sends `SIGKILL`; if you raise `--drain-timeout`, raise `terminationGracePeriodSeconds` to match.

## Current platform gaps / TODOs

These are deliberately documented, not papered over with invented flags:

1. `shoal-embed serve` has no CLI/env for `engine.Options.Backend`, storage backend, bucket, or prefix. The engine/tablet layers support a backend, but the CLI does not wire it yet; therefore the write-tier manifest keeps WAL/RFiles on the PVC today.
2. `cmd/shoal` reads RFile paths from Accumulo/Shoal metadata and has `-storage=gs|local`, but no bucket/prefix override flag. The shared bucket/prefix ConfigMap values are operator intent/future wiring.
3. S3 is mentioned in high-level docs, but this repository currently exposes local, memory, and GCS storage packages; no S3 backend package or binary flag is available.
4. No rolling-upgrade/rollback runbook, dashboards, or alerting rules are provided yet; `/metrics` is exposed but not wired to a scrape config or alert thresholds. Tablet migration and data-loss-safe coordination during drain remain the Accumulo manager/coordinator's responsibility; this platform layer only stops accepting new gRPC work and waits for in-flight calls to finish.
