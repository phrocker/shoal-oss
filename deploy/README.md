# Shoal platform deployment

This directory contains platform artifacts for running Shoal as two Kubernetes tiers:

- **Write tier**: one `shoal-embed` StatefulSet per shard. Each pod has stable identity and a persistent local data directory for WAL/tablet state. The deployment manifests explicitly opt into the HTTP observability listener with `shoal-embed serve --data <dir> --address 0.0.0.0:<port> --metrics-address 0.0.0.0:<port> --quiesce-delay 10s`.
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

- `cmd/shoal-embed/main.go`: `serve` supports `--data`, `--address` (gRPC bind address, e.g. `0.0.0.0:9876`; defaults to `127.0.0.1:<port>` when only the legacy `--port` is set), `--metrics-port` / `--metrics-address` (HTTP health/readiness/metrics bind; **opt-in** — the HTTP surface only starts if one of these is explicitly passed, so a bare `shoal-embed serve --port N` keeps behaving as a single-port process; when enabled, defaults to `127.0.0.1:9877`), `--quiesce-delay` (default `5s`, but only applied when the HTTP readiness surface is enabled; the manifests override it to `10s`), `--drain-timeout` (default `30s`, bounds graceful shutdown; see below), and `--tls-cert` / `--tls-key` / `--tls-client-ca` (optional TLS material for both listeners together, each with a `SHOAL_EMBED_TLS_*` environment fallback; see the TLS section below).
- `cmd/shoal/main.go`: read fleet supports `-listen`, `-zk`, `-instance`, `-accumulo-version`, `-user`, `-password` or `SHOAL_PASSWORD`, `-zk-timeout`, `-storage`, `-cache-bytes`, `-log-level`, `-prewarm-tables`, and `-prewarm-parallelism`.
- `internal/storage/gcs/gcs.go`: the GCS backend uses Application Default Credentials and accepts paths like `gs://bucket/object` or `bucket/object`.

The manifests pass only supported process flags to the containers. `GOOGLE_APPLICATION_CREDENTIALS` is set for the GCS client when a key Secret is mounted.

## Health, metrics, and graceful drain

`shoal-embed serve` can expose an HTTP surface alongside the gRPC write port, but only when asked: it is off by default and only starts if `--metrics-port` or `--metrics-address` is explicitly passed — a bare `shoal-embed serve --data ... --port 9876` (the top-level README's quick-start) stays a single-port process rather than silently gaining a second listening port. These manifests explicitly opt in via `--metrics-address 0.0.0.0:<port>` (default `9877`), so this section describes what's actually running in the write tier:

- `/healthz`: unconditional liveness — 200 once the process has started serving. Used for `livenessProbe` in both manifests.
- `/readyz`: readiness — 200 once startup finishes, 503 during startup and during shutdown drain. Used for `readinessProbe`, so a draining pod can be removed from Service endpoints before its gRPC port stops accepting new work.
- `/metrics`: Prometheus text-format metrics (via `internal/obs`). Not yet wired to a `ServiceMonitor`/scrape annotation in these manifests; add one for your Prometheus setup if needed.

`cmd/shoal` (the read fleet) has no equivalent HTTP surface, so `deploy/k8s/read-fleet.yaml` and the Helm read-fleet template use `tcpSocket` liveness/readiness probes instead — they prove the Thrift port accepts TCP connections, not that the process is otherwise healthy. Do not assume readiness-contract parity between the two tiers; see the gaps list below.

The write-tier headless Service no longer sets `publishNotReadyAddresses: true`, so readiness now gates which pod IPs are published to clients.

On `SIGINT`/`SIGTERM`, `cmdServe` (via `serveHandle.RunUntilSignal`) flips `/readyz` to not-ready immediately. If the HTTP readiness surface is enabled, it then waits `--quiesce-delay` so a readiness-polling consumer has a chance to react before anything stops accepting work; if the HTTP surface is disabled, legacy single-port `serve` skips that quiesce and begins shutdown immediately. The manifests set `--quiesce-delay=10s`, `readinessProbe.periodSeconds=5`, and `readinessProbe.failureThreshold=1`, so a draining pod gets a real ready-to-not-ready withdrawal window before gRPC shutdown starts. After quiesce, HTTP and gRPC shut down concurrently under the same `--drain-timeout` budget (default `30s`), so a slow `/stats`/`/metrics` request can't eat into the budget meant for draining writes.

If that deadline passes, the server force-stops transport (`grpc.Server.Stop()`). Transport-cancellable RPCs (for example a streaming `Scan`) unwind quickly after that, so shutdown can still close the engine and exit cleanly. A unary RPC already inside a synchronous engine call (`Write`/`Flush`/`Compact`/`CreateTable`) is different: it ignores request cancellation today, so force-stopping transport cannot interrupt it safely. In that case shutdown returns an explicit timeout/in-flight error, skips engine close rather than racing a close against still-running engine work, and exits without claiming a completed close. Making those engine calls themselves cancellation-aware would close this gap fully; that's a larger engine-layer change and remains open (see gaps below).

The write-tier `terminationGracePeriodSeconds` (`50s`) covers the manifest's `--quiesce-delay=10s` + `--drain-timeout=30s` plus a `10s` margin for engine close and process exit before Kubernetes sends `SIGKILL`; if you raise either flag, raise `terminationGracePeriodSeconds` to match.

## TLS for the write-tier listeners

`shoal-embed serve` can terminate TLS on both its listeners — the gRPC write port and the HTTP `/healthz`/`/readyz`/`/stats`/`/metrics` surface — but it is **off by default**: existing plaintext deployments keep working unchanged.

Enable it by supplying a PEM certificate and private key, via flag or environment variable. A flag always wins over its same-purpose environment variable (see `flagOrEnv` in `cmd/shoal-embed/main.go`); there is no other precedence and no implicit fallback (for example, TLS is never auto-enabled, and a partially-specified configuration is a startup error, not a silent switch to plaintext):

| Setting | Flag | Environment variable |
| --- | --- | --- |
| Server certificate | `--tls-cert` | `SHOAL_EMBED_TLS_CERT` |
| Server private key | `--tls-key` | `SHOAL_EMBED_TLS_KEY` |
| Client CA bundle (enables mutual TLS) | `--tls-client-ca` | `SHOAL_EMBED_TLS_CLIENT_CA` |

`--tls-cert` and `--tls-key` (or their environment equivalents) must be set together — a cert without a key, or vice versa, fails startup with a descriptive error rather than falling back to plaintext. Adding `--tls-client-ca` turns on mutual TLS: both listeners then require and verify a client certificate against that CA bundle before completing the handshake. One certificate/key pair covers both listeners; they cannot be given independent TLS material. The resulting `tls.Config` pins `MinVersion` to TLS 1.2.

To enable TLS in the plain write-tier manifest: mount a Secret containing `tls.crt` / `tls.key` (and, for mutual TLS, `ca.crt`) at `/var/run/secrets/shoal/tls`, then uncomment the matching `--tls-*` args, volumeMount, and volume already spelled out as comments in `deploy/k8s/write-tier.yaml`. With Helm, set `writeTier.tls.enabled=true` and `writeTier.tls.secretName=<your secret>` (add `writeTier.tls.requireClientCert=true` for mutual TLS); the chart wires the args/volume for you and fails the render with a clear error if `secretName` is left unset while `enabled=true`.

Certificate/key rotation is out of scope for this slice: `shoal-embed` loads the key pair (and CA bundle) once at startup, so rotating any of them requires a pod restart today — see the gaps list below.

## Rolling upgrades, rollback, and voluntary disruption

Both tiers now carry an explicit rollout contract and a `PodDisruptionBudget`, instead of relying on whatever the `apps/v1`/`policy/v1` defaults happen to be:

- **Read fleet** (`shoal-read` Deployment): `strategy.rollingUpdate` sets `maxUnavailable: 1`, `maxSurge: 1` — a rollout keeps at least 2 of its 3 replicas serving, with one extra pod allowed briefly so the new revision's readiness is proven before an old replica is removed. Its `PodDisruptionBudget` allows the same `maxUnavailable: 1` for voluntary disruptions (node drains, cluster-autoscaler consolidation) outside of a rollout.
- **Write tier** (`shoal-embed` StatefulSet, one replica per shard today): `updateStrategy` is pinned to the StatefulSet default (`RollingUpdate`, `partition: 0`) explicitly rather than implicitly, leaving `partition: N` available as a lever for a staged/canary rollout later. Its `PodDisruptionBudget` sets `maxUnavailable: 0` — a **deliberate, restrictive** policy: it blocks the Kubernetes Eviction API (`kubectl drain`, cluster-autoscaler consolidation, and similar voluntary-disruption paths) from evicting the sole write-tier replica for a shard, because there is no live tablet hand-off/migration path yet and evicting it today would simply be an unplanned outage for that shard with no failover. This PDB does **not** block operator-initiated rolling updates: `kubectl rollout restart` / `helm upgrade` act through the StatefulSet controller directly, which replaces pods without going through the Eviction API, so PDBs never apply to them.

Recommended operator commands:

```bash
# Roll a new image/config to the read fleet (respects the strategy above)
kubectl set image deployment/shoal-read shoal=ghcr.io/YOUR_ORG/shoal:NEW_TAG
kubectl rollout status deployment/shoal-read
kubectl rollout undo deployment/shoal-read   # rollback to the previous revision

# Roll a new image/config to a write-tier shard (StatefulSet; bypasses the PDB, as above)
kubectl set image statefulset/shoal-embed shoal-embed=ghcr.io/YOUR_ORG/shoal:NEW_TAG
kubectl rollout status statefulset/shoal-embed
kubectl rollout undo statefulset/shoal-embed

# Same via Helm, plus its own rollback path
helm upgrade shoal deploy/helm/shoal --reuse-values --set image.tag=NEW_TAG
helm rollback shoal   # back to the previous release revision
```

A StatefulSet rolling update replaces the container but reuses the same PVC, so on-disk data for that shard is preserved across the restart; there is still no live migration of in-flight writes to another shard during the restart window itself. `terminationGracePeriodSeconds` (`50s`) and the drain sequence described above apply the same way during a rollout as during any other pod termination.

## Current platform gaps / TODOs

These are deliberately documented, not papered over with invented flags:

1. `shoal-embed serve` has no CLI/env for `engine.Options.Backend`, storage backend, bucket, or prefix. The engine/tablet layers support a backend, but the CLI does not wire it yet; therefore the write-tier manifest keeps WAL/RFiles on the PVC today.
2. `cmd/shoal` reads RFile paths from Accumulo/Shoal metadata and has `-storage=gs|local`, but no bucket/prefix override flag. The shared bucket/prefix ConfigMap values are operator intent/future wiring.
3. S3 is mentioned in high-level docs, but this repository currently exposes local, memory, and GCS storage packages; no S3 backend package or binary flag is available.
4. No dashboards or alerting rules are provided yet; `/metrics` is exposed but not wired to a scrape config or alert thresholds (a rolling-upgrade/rollback runbook is now above). Tablet migration and data-loss-safe coordination during drain remain the Accumulo manager/coordinator's responsibility; this platform layer only stops accepting new gRPC work and waits for in-flight calls to finish.
5. `Write`/`Flush`/`Compact`/`CreateTable` discard the request context in `internal/embedstore`, so an in-flight call cannot be cancelled once dispatched. Shutdown now stays bounded by force-stopping transport at `--drain-timeout`, but if one of those unary engine calls is still running at that point the process returns a timeout/in-flight error and intentionally skips engine close rather than closing the engine unsafely underneath it. Only the streaming `Scan` RPC observes transport cancellation today.
6. TLS material has no rotation story: `shoal-embed serve` loads the certificate/key/client-CA once at startup, so rotating any of them requires a pod restart (a rolling one, using the mechanics above) rather than a live reload/SIGHUP-style refresh.
7. The write-tier `PodDisruptionBudget` (`maxUnavailable: 0`) is a deliberate policy, not an oversight — see "Rolling upgrades, rollback, and voluntary disruption" above. It blocks voluntary eviction of the sole write-tier replica for a shard because there is no live tablet hand-off path yet; it does not block operator-driven rollouts.
8. `cmd/shoal` (read fleet) has no HTTP health/readiness/metrics surface, unlike `shoal-embed`; its manifests therefore use `tcpSocket` liveness/readiness probes only (able to prove the Thrift port accepts connections, not that it is otherwise healthy), and there is no `/metrics` to scrape for the read fleet.
9. Neither tier sets `readOnlyRootFilesystem: true` yet. Both already run as a non-root UID/GID with `seccompProfile: RuntimeDefault` and drop all Linux capabilities, but flipping the root filesystem read-only needs verification against a live cluster (to confirm no runtime writes land outside the mounted data/credential volumes) that isn't available in this environment, so it is left as a documented follow-up rather than an unverified change.
