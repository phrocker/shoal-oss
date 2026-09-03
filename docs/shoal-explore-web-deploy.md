# Deploying the single-instance Shoal Explorer workspace

`shoal-explore-web` is the optional Explorer workspace: **one binary plus a data
directory**. The UI is compiled in with `go:embed`, and the corpus is served
straight from Shoal's own storage engine through the embedded backend. There is
**no ZooKeeper, no tablet server, no Accumulo, and no external datastore** in
this deployment — Shoal's storage engine is the durable backend.

The goal is a single artifact that runs on a laptop and (once the shared-instance
prerequisites below land) in a shared instance, where the two deployments differ
**only in configuration**, never in code or image.

- [Image](#image)
- [Local: one command](#local-one-command)
- [The configuration seam: local vs shared](#the-configuration-seam-local-vs-shared)
- [The unsafe-configuration guard](#the-unsafe-configuration-guard)
- [Persistence and the #284 caveat](#persistence-and-the-284-caveat)
- [Shared / cloud instance: shape and open gaps](#shared--cloud-instance-shape-and-open-gaps)
- [Azure hosting (one option, not a decision)](#azure-hosting-one-option-not-a-decision)

## Image

`Dockerfile.shoal-explore-web` is a multi-stage build:

- **build stage** `golang:1.25-bookworm`, `CGO_ENABLED=0`, `GOWORK=off` (the
  repo's `go.work` pins a newer toolchain than the base image; the command
  itself builds on 1.25).
- **runtime stage** `gcr.io/distroless/static-debian12:nonroot` — runs as uid/gid
  `65532`, contains only the stripped binary and an empty, `65532`-owned corpus
  directory. `/var/lib/shoal/explorer` is a declared `VOLUME`.

```console
$ docker build -f Dockerfile.shoal-explore-web -t shoal-explore-web:test --build-arg VERSION=deploy-test .
...
 => naming to docker.io/library/shoal-explore-web:test

$ docker image inspect shoal-explore-web:test \
    --format 'Size={{.Size}} User={{.Config.User}} Volumes={{json .Config.Volumes}}'
Size=7097222 User=65532:65532 Volumes={"/var/lib/shoal/explorer":{}}
```

The image is ~7 MB, runs as non-root, and embeds no secrets: `Config.Env` is only
`PATH` and `SSL_CERT_FILE`, and the exported filesystem contains no keys, tokens,
or `.env` files (only the binary and the distroless `etc/passwd` that defines the
`nonroot` account).

## Local: one command

```console
$ docker compose -f deploy/shoal-explore-web/docker-compose.yml up --build
```

That starts the workspace with the loopback-gated **development authenticator**
(`-dev-auth`), which authenticates every request as a fixed, clearly-named
development principal (`development-principal@localhost`). It is safe only because
the listener is proven loopback-only.

### Why host networking

`-dev-auth` refuses any listener that another host can reach, so the app binds
`127.0.0.1` **inside the container**. Docker port publishing (`-p`) forwards to
the container's *external* interface, which a loopback-only listener never
answers — so publishing a port would leave you with a refused connection. The
compose file therefore uses `network_mode: host`:

- **Linux (and WSL2):** the app is on the host's `127.0.0.1:8098`. Open
  <http://127.0.0.1:8098> in a browser.
- **Docker Desktop (macOS / Windows):** "host" is the Docker Linux VM, so
  `127.0.0.1:8098` is the VM's loopback, not the desktop OS's. Reach it from a
  helper that shares that namespace, e.g.:

  ```console
  $ docker run --rm --network host curlimages/curl -s http://127.0.0.1:8098/api/v1/meta
  ```

  For a browser on Docker Desktop, run the native binary instead
  (`go run ./cmd/shoal-explore-web -dev-auth`), or enable Docker Desktop host
  networking.

The corpus lives on the named volume `explorer-data`, so `docker compose down`
followed by `up`, or any container restart, preserves it. `docker compose down -v`
deletes it.

## The configuration seam: local vs shared

The same image serves both deployments. Everything that varies is a flag or
mounted volume — never code:

| Concern            | Local (laptop)                     | Shared instance                              |
| ------------------ | ---------------------------------- | -------------------------------------------- |
| Bind address       | `-listen 127.0.0.1:8098` (loopback)| `-listen 0.0.0.0:<port>` (see gaps below)    |
| Data directory     | `-data /var/lib/shoal/explorer` (declared volume) | same flag, backed by durable storage |
| Authenticator      | `-dev-auth` (loopback-only)        | a real authenticator (**not yet wired**)     |
| TLS termination    | none (loopback)                    | terminated at an ingress/reverse proxy       |
| Log format         | Go `log` text to stderr            | same today (no structured-log flag yet)      |

`deploy/shoal-explore-web/.env.example` documents the one value the local compose
profile parameterises (`SHOAL_EXPLORE_LISTEN`). The remaining columns are the
seam for a shared instance and are covered under [gaps](#shared--cloud-instance-shape-and-open-gaps).

## The unsafe-configuration guard

The dangerous combination — a shared/public bind with the development
authenticator — **fails closed at startup** before any socket is bound. This is
enforced by `selectAuthenticator` / `listenAddressIsLoopback` in
`cmd/shoal-explore-web/authn.go`; it is verified here, not merely asserted.

Public bind + `-dev-auth` is refused:

```console
$ docker run --rm --network host shoal-explore-web:test \
    -data /var/lib/shoal/explorer -listen 0.0.0.0:8099 -dev-auth
shoal-explore-web: refusing to serve 0.0.0.0:8099 with -dev-auth: the
development-principal@localhost development principal is granted the whole
workspace corpus and is only safe on a loopback listener; bind 127.0.0.1 or
[::1], or supply a real authenticator
# exit code 1
```

No authenticator at all is also refused (the workspace never serves anonymously):

```console
$ docker run --rm --network host shoal-explore-web:test \
    -data /var/lib/shoal/explorer -listen 127.0.0.1:8099
shoal-explore-web: refusing to serve 127.0.0.1:8099 without authentication: no
authenticator is configured; pass -dev-auth to mint the
development-principal@localhost development principal on a loopback listener, or
supply a real authenticator before exposing the Explorer API
# exit code 1
```

The check runs twice: once on the requested flag value before anything binds,
and again on the listener's *resolved* address (which may be wider than
requested), closing the listener before the corpus is opened.

## Persistence and the #284 caveat

The restart test proves the volume works, but you must be precise about what it
proves. Two facts are distinct:

1. **The volume preserved the corpus on disk.**
2. **The workspace served that corpus after restart.**

On `main` today, document data is durable (Shoal storage engine) but the
*authorization* policy catalog is an in-process map (issue #284). A startup
**backfill**, gated to `-dev-auth` on a loopback listener, re-registers the
on-disk documents for the development principal at every start — so under the
local profile, restart serves the corpus, but only because the backfill rebuilds
the in-memory authorization each time.

Observed end-to-end (Docker Desktop, host-network curl helper):

```console
# seed one document
$ docker run --rm --network host -v "$PWD/seed:/seed:ro" curlimages/curl -s \
    -X POST http://127.0.0.1:8098/api/v1/ingest \
    -H "X-Shoal-Workspace-Request: 1" \
    -F "file=@/seed/seed.md;type=text/markdown"
{"snapshot":{"id":"2ab74122b4f13cb59c6dc13e8b41c5a856649374741fb7f12a19911f6047d036", ...}}

# before restart: 1 document served
$ docker run --rm --network host -v "$PWD/seed:/seed:ro" curlimages/curl -s \
    -X POST http://127.0.0.1:8098/api/v1/documents \
    -H "X-Shoal-Workspace-Request: 1" -H "Content-Type: application/json" \
    -d '{"page":{"limit":10}}'
{"snapshot":{"id":"2ab74122...","...":"..."},"documents":[{"document":{"title":"seed.md", ...}}]}

$ docker restart shoal-explore-test
# logs after restart:
#   Granted 1 pre-existing document(s) in /var/lib/shoal/explorer to development-principal@localhost ...
#   Shoal Explorer listening at http://127.0.0.1:8098

# after restart: SAME snapshot id, document still served
$ docker run --rm --network host -v "$PWD/seed:/seed:ro" curlimages/curl -s \
    -X POST http://127.0.0.1:8098/api/v1/documents \
    -H "X-Shoal-Workspace-Request: 1" -H "Content-Type: application/json" \
    -d '{"page":{"limit":10}}'
{"snapshot":{"id":"2ab74122...","...":"..."},"documents":[{"document":{"title":"seed.md", ...}}]}
```

The snapshot id `2ab74122…` is identical before and after restart, and the
post-restart log reports `Granted 1 pre-existing document(s)` — the corpus was on
disk and the dev-auth backfill re-authorized it.

**What this proves:** the named volume preserved the corpus across a restart, and
the local `-dev-auth` profile serves it again.
**What it does not prove:** that authorization survives on its own. It does not —
the policy catalog is in-memory and is reconstructed by the backfill each start
(#284). In a deployment **without** the backfill (any non-dev authenticator), a
restart would preserve the corpus on disk but serve an **empty** result set until
each document is ingested again. Do not read the green restart test as "policy
persistence works."

## Shared / cloud instance: shape and open gaps

The image is shared-instance ready; the *runtime prerequisites* are not all in
place on `main`. Honest gaps, none of which this deployment work should paper
over:

1. **No non-development authenticator is wired.** Without `-dev-auth`, startup
   fails closed demanding a real authenticator (shown above). A shared instance
   needs one minted at the edge — blocked on **edge identity, issue #278**.
2. **`-backend remote` is deliberately refused** because `auth.Decision` has no
   on-the-wire representation (issue #278): forwarding would authenticate at the
   edge and then call upstream with no identity. This means **multi-node scaling
   is out of scope**; the target is single instance, many users.
3. **Host-authority binding.** The handler requires the request `Host` to equal
   the listener's resolved address (`allowedAuthority`). A public bind of
   `0.0.0.0:<port>` makes the expected authority `0.0.0.0:<port>`, which real
   client `Host` headers won't match. A shared instance needs a configurable
   external authority — a config seam that does not exist yet.
4. **Durable policy store (#284)** must land before restarts serve a shared
   corpus without a dev-only backfill.

Until (1) and (3) are addressed, a shared bind either fails closed (no
authenticator) or rejects requests (host mismatch) — safe, but not yet
serviceable. This is a deliberate fail-closed posture, not a packaging defect.

## Azure hosting (one option, not a decision)

The hosting shape is **not decided**. The image is portable; the deciding factor
is **persistent-volume semantics**, because the corpus is a local directory that
must survive restarts and redeploys.

- **Azure Container Apps** — closest to the compose model. Mount an **Azure Files**
  volume at `/var/lib/shoal/explorer`. Caveat: Container Apps runs multiple
  replicas by default and load-balances across them; this workload is
  **single-instance** (no shared coordination — see gap #2), so pin
  `minReplicas: 1` / `maxReplicas: 1`. SMB/Azure Files latency under the storage
  engine should be validated before committing.
- **App Service (custom container)** — persistent storage via `WEBSITES_ENABLE_APP_SERVICE_STORAGE`
  or a mounted Azure Files share. Also single-instance (disable scale-out).
- **AKS** — a single-replica `Deployment` (or `StatefulSet`) with a
  `ReadWriteOnce` PVC. The most control, the most operational overhead.

In all three, TLS terminates at the platform ingress and the app still needs a
real authenticator (gap #1) before it is exposed. Treat the above as one
sketch, not a recommendation.
