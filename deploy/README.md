# Shoal platform deployment

This directory contains platform artifacts for running Shoal as two Kubernetes tiers:

- **Write tier**: one `shoal-embed` StatefulSet per shard. Each pod has stable identity and a persistent local data directory for WAL/tablet state. The deployment manifests explicitly opt into the HTTP observability listener with `shoal-embed serve --data <dir> --address 0.0.0.0:<port> --metrics-address 0.0.0.0:<port> --quiesce-delay 10s`.
- **Read fleet**: stateless `shoal` pods behind a Service. They serve the Accumulo-compatible Thrift scan surface and open immutable RFiles through the configured storage backend.

For local development these collapse into a single `shoal-embed` process. The intended platform shape is: writes land in a shard-local `shoal-embed`, flush/compaction emits RFiles to a shared object-store prefix, and read-fleet pods open those same RFiles for hedged scans.

Shoal's accepted
[coordination authority model](../docs/coordination-authority.md) deliberately
separates this Shoal-only pod topology from Accumulo-connected ownership:
Kubernetes Leases may coordinate a Shoal-only writer group, while
Accumulo-connected roles use ZooKeeper ServiceLocks and manager/coordinator
authority. The same logical table must never be writable under both domains.
The current manifests do not yet implement the Kubernetes Lease adapter; that
work is ordered under #128 rather than implied by the StatefulSet alone.

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

- `cmd/shoal-embed/main.go`: `serve` supports `--data`, `--address` (gRPC bind address, e.g. `0.0.0.0:9876`; defaults to `127.0.0.1:<port>` when only the legacy `--port` is set), `--metrics-port` / `--metrics-address` (HTTP health/readiness/metrics bind; **opt-in** — the HTTP surface only starts if one of these is explicitly passed, so a bare `shoal-embed serve --port N` keeps behaving as a single-port process; when enabled, defaults to `127.0.0.1:9877`), `--quiesce-delay` (default `5s`, but only applied when the HTTP readiness surface is enabled; the manifests override it to `10s`), and `--drain-timeout` (default `30s`, bounds graceful shutdown; see below).
- `cmd/shoal/main.go`: read fleet supports `-listen`, `-zk`, `-instance`, `-accumulo-version`, `-user`, `-password` or `SHOAL_PASSWORD`, `-zk-timeout`, `-storage`, `-cache-bytes`, `-log-level`, `-prewarm-tables`, and `-prewarm-parallelism`.
- `internal/storage/gcs/gcs.go`: the GCS backend uses Application Default Credentials and accepts paths like `gs://bucket/object` or `bucket/object`.

The manifests pass only supported process flags to the containers. `GOOGLE_APPLICATION_CREDENTIALS` is set for the GCS client when a key Secret is mounted.

## Health, metrics, and graceful drain

`shoal-embed serve` can expose an HTTP surface alongside the gRPC write port, but only when asked: it is off by default and only starts if `--metrics-port` or `--metrics-address` is explicitly passed — a bare `shoal-embed serve --data ... --port 9876` (the top-level README's quick-start) stays a single-port process rather than silently gaining a second listening port. These manifests explicitly opt in via `--metrics-address 0.0.0.0:<port>` (default `9877`), so this section describes what's actually running in the write tier:

- `/healthz`: unconditional liveness — 200 once the process has started serving. Used for `livenessProbe` in both manifests.
- `/readyz`: readiness — 200 once startup finishes, 503 during startup and during shutdown drain. Used for `readinessProbe`, so a draining pod can be removed from Service endpoints before its gRPC port stops accepting new work.
- `/metrics`: Prometheus text-format metrics (via `internal/obs`). Not yet wired to a `ServiceMonitor`/scrape annotation in these manifests; add one for your Prometheus setup if needed.

The write-tier headless Service no longer sets `publishNotReadyAddresses: true`, so readiness now gates which pod IPs are published to clients.

On `SIGINT`/`SIGTERM`, `cmdServe` (via `serveHandle.RunUntilSignal`) flips `/readyz` to not-ready immediately. If the HTTP readiness surface is enabled, it then waits `--quiesce-delay` so a readiness-polling consumer has a chance to react before anything stops accepting work; if the HTTP surface is disabled, legacy single-port `serve` skips that quiesce and begins shutdown immediately. The manifests set `--quiesce-delay=10s`, `readinessProbe.periodSeconds=5`, and `readinessProbe.failureThreshold=1`, so a draining pod gets a real ready-to-not-ready withdrawal window before gRPC shutdown starts. After quiesce, HTTP and gRPC shut down concurrently under the same `--drain-timeout` budget (default `30s`), so a slow `/stats`/`/metrics` request can't eat into the budget meant for draining writes.

If that deadline passes, the server force-stops transport (`grpc.Server.Stop()`). Transport-cancellable RPCs (for example a streaming `Scan`) unwind quickly after that, so shutdown can still close the engine and exit cleanly. A unary RPC already inside a synchronous engine call (`Write`/`Flush`/`Compact`/`CreateTable`) is different: it ignores request cancellation today, so force-stopping transport cannot interrupt it safely. In that case shutdown returns an explicit timeout/in-flight error, skips engine close rather than racing a close against still-running engine work, and exits without claiming a completed close. Making those engine calls themselves cancellation-aware would close this gap fully; that's a larger engine-layer change and remains open (see gaps below).

The write-tier `terminationGracePeriodSeconds` (`50s`) covers the manifest's `--quiesce-delay=10s` + `--drain-timeout=30s` plus a `10s` margin for engine close and process exit before Kubernetes sends `SIGKILL`; if you raise either flag, raise `terminationGracePeriodSeconds` to match.

## Current platform gaps / TODOs

These are deliberately documented, not papered over with invented flags:

1. `shoal-embed serve` has no CLI/env for `engine.Options.Backend`, storage backend, bucket, or prefix. The engine/tablet layers support a backend, but the CLI does not wire it yet; therefore the write-tier manifest keeps WAL/RFiles on the PVC today.
2. `cmd/shoal` reads RFile paths from Accumulo/Shoal metadata and has `-storage=gs|local`, but no bucket/prefix override flag. The shared bucket/prefix ConfigMap values are operator intent/future wiring.
3. S3 is mentioned in high-level docs, but this repository currently exposes local, memory, and GCS storage packages; no S3 backend package or binary flag is available.
4. No rolling-upgrade/rollback runbook, dashboards, or alerting rules are provided yet; `/metrics` is exposed but not wired to a scrape config or alert thresholds. Tablet migration and data-loss-safe coordination during drain remain the Accumulo manager/coordinator's responsibility; this platform layer only stops accepting new gRPC work and waits for in-flight calls to finish.
5. `Write`/`Flush`/`Compact`/`CreateTable` discard the request context in `internal/embedstore`, so an in-flight call cannot be cancelled once dispatched. Shutdown now stays bounded by force-stopping transport at `--drain-timeout`, but if one of those unary engine calls is still running at that point the process returns a timeout/in-flight error and intentionally skips engine close rather than closing the engine unsafely underneath it. Only the streaming `Scan` RPC observes transport cancellation today.
