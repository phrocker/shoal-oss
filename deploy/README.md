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

- `cmd/shoal-embed/main.go`: `serve` supports `--data`, `--address` (gRPC bind address, e.g. `0.0.0.0:9876`; defaults to `127.0.0.1:<port>` when only the legacy `--port` is set), `--metrics-port` / `--metrics-address` (HTTP health/readiness/metrics bind, default `127.0.0.1:9877`), `--quiesce-delay` (default `5s`, a pause after flipping `/readyz` to not-ready and before anything stops accepting work; see below), and `--drain-timeout` (default `30s`, bounds graceful shutdown; see below).
- `cmd/shoal/main.go`: read fleet supports `-listen`, `-zk`, `-instance`, `-accumulo-version`, `-user`, `-password` or `SHOAL_PASSWORD`, `-zk-timeout`, `-storage`, `-cache-bytes`, `-log-level`, `-prewarm-tables`, and `-prewarm-parallelism`.
- `internal/storage/gcs/gcs.go`: the GCS backend uses Application Default Credentials and accepts paths like `gs://bucket/object` or `bucket/object`.

The manifests pass only supported process flags to the containers. `GOOGLE_APPLICATION_CREDENTIALS` is set for the GCS client when a key Secret is mounted.

## Health, metrics, and graceful drain

`shoal-embed serve` exposes an HTTP surface (default `0.0.0.0:9877` in the manifests, via `--metrics-address`) alongside the gRPC write port:

- `/healthz`: unconditional liveness — 200 once the process has started serving. Used for `livenessProbe` in both manifests.
- `/readyz`: readiness — 200 once startup finishes, 503 during startup and during shutdown drain. Used for `readinessProbe`. What this actually gates depends on the Service: the write-tier headless Service sets `publishNotReadyAddresses: true` (required so StatefulSet peers can resolve each other's stable DNS names during cluster bootstrap/formation), which means Kubernetes keeps publishing a pod's address in that Service's Endpoints/EndpointSlices regardless of readiness — so this probe does **not** remove a draining write-tier pod from Service endpoints. It still gates StatefulSet rollout ordering (a not-ready pod blocks the next pod's rolling update) and gives operators/monitoring an accurate not-ready signal. Per-shard write routing during drain — sending new writes to a different, ready shard-owner — is the responsibility of whatever owns shard-to-writer assignment (the Accumulo-model manager/coordinator), not this Service; a generic ready-only Service would incorrectly load-balance shard-targeted writes across shards that cannot serve them.
- `/metrics`: Prometheus text-format metrics (via `internal/obs`). Not yet wired to a `ServiceMonitor`/scrape annotation in these manifests; add one for your Prometheus setup if needed.

On `SIGINT`/`SIGTERM`, `cmdServe` (via `serveHandle.RunUntilSignal`) flips `/readyz` to not-ready immediately, waits `--quiesce-delay` (default `5s`) so a readiness-polling consumer has a chance to react, then waits up to `--drain-timeout` (default `30s`) for in-flight gRPC calls to finish — HTTP and gRPC shut down concurrently under that same deadline, so a slow `/stats`/`/metrics` request can't eat into the budget meant for draining writes. If the deadline passes, it force-stops remaining RPCs (`grpc.Server.Stop()`) rather than hanging indefinitely, then closes the engine and exits; the whole sequence is guaranteed to finish before the process exits. This bounds most shutdown work end-to-end, with one known residual gap: `Write`/`Flush`/`Compact` each run a single synchronous, non-cancellable engine call today, so force-stopping the gRPC transport cannot interrupt one already in progress — shutdown still waits for it to return (closing the engine underneath an in-flight call would risk torn WAL/tablet state, which is worse than a slower shutdown), logging a warning naming the number of such calls still outstanding if the drain deadline passes first. Making those engine calls themselves cancellation-aware would close this gap; that's a larger engine-layer change and remains open (see gaps below).

The write-tier `terminationGracePeriodSeconds` (`45s`) covers `--quiesce-delay` (`5s`) + `--drain-timeout` (`30s`) plus a `10s` margin for engine close and process exit before Kubernetes sends `SIGKILL`; if you raise either flag, raise `terminationGracePeriodSeconds` to match.

## Current platform gaps / TODOs

These are deliberately documented, not papered over with invented flags:

1. `shoal-embed serve` has no CLI/env for `engine.Options.Backend`, storage backend, bucket, or prefix. The engine/tablet layers support a backend, but the CLI does not wire it yet; therefore the write-tier manifest keeps WAL/RFiles on the PVC today.
2. `cmd/shoal` reads RFile paths from Accumulo/Shoal metadata and has `-storage=gs|local`, but no bucket/prefix override flag. The shared bucket/prefix ConfigMap values are operator intent/future wiring.
3. S3 is mentioned in high-level docs, but this repository currently exposes local, memory, and GCS storage packages; no S3 backend package or binary flag is available.
4. No rolling-upgrade/rollback runbook, dashboards, or alerting rules are provided yet; `/metrics` is exposed but not wired to a scrape config or alert thresholds. Tablet migration and data-loss-safe coordination during drain remain the Accumulo manager/coordinator's responsibility; this platform layer only stops accepting new gRPC work and waits for in-flight calls to finish.
5. `Write`/`Flush`/`Compact`/`CreateTable` discard the request context in `internal/embedstore`, so an in-flight call cannot be cancelled once dispatched; a pathologically slow or stuck one can extend shutdown past `--drain-timeout` (shutdown logs a warning with the outstanding count when this happens, but does not force it to abort). Only the streaming `Scan` RPC observes transport cancellation today.
