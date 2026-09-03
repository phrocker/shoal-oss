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
- [Volume layout: a single state root](#volume-layout-a-single-state-root)
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
  `65532`, contains only the stripped binary and an empty, `65532`-owned state
  root. `/var/lib/shoal` (the whole state root) is a declared `VOLUME`.

```console
$ docker build -f Dockerfile.shoal-explore-web -t shoal-explore-web:test --build-arg VERSION=deploy-test .
...
 => naming to docker.io/library/shoal-explore-web:test

$ docker image inspect shoal-explore-web:test \
    --format 'Size={{.Size}} User={{.Config.User}} Cmd={{json .Config.Cmd}} Volumes={{json .Config.Volumes}}'
Size=7097160 User=65532:65532 Cmd=["-data","/var/lib/shoal/corpus","-listen","127.0.0.1:8098","-dev-auth"] Volumes={"/var/lib/shoal":{}}
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

The persistent state lives on the named volume `explorer-state` mounted at the
state root `/var/lib/shoal`, so `docker compose down` followed by `up`, or any
container restart, preserves it. `docker compose down -v` deletes it.

## Volume layout: a single state root

Mount the **state root** `/var/lib/shoal`, never the corpus directory. All
persistent state lives under one mount:

```
/var/lib/shoal/                 <- the declared VOLUME (mount THIS)
├── corpus/                     <- the document corpus (-data)
│   └── _shoal_explorer/        <- the corpus engine's table
└── corpus-policy/              <- the authorization catalog (once #284/#288 lands)
```

The authorization catalog is a **sibling** of the corpus, not a child of it: the
durable policy store (issue #284, PR #288) derives its directory as
`filepath.Clean(-data) + "-policy"`. It cannot nest inside `-data` because the
corpus engine treats every subdirectory of the data directory as a table and
would misread the catalog as table data.

The consequence is the whole point of this layout: **mounting only the corpus
directory would preserve documents but drop every authorization registration on
restart** — the workspace would then serve an empty or under-populated result
set, which the temporary startup backfill can partially mask, making the failure
look like something else. Mounting the state root keeps both. It is also robust
to the pending change to a `<state-root>/corpus` + `<state-root>/policy` layout
with an explicit policy-directory flag: both still fall under `/var/lib/shoal`.

The default `-data` is `/var/lib/shoal/corpus`; the image declares `/var/lib/shoal`
(not `/var/lib/shoal/corpus`) as the volume, and the state root is owned by
`65532` so the non-root process creates `corpus/` and the sibling `corpus-policy/`
at runtime.

## The configuration seam: local vs shared

The same image serves both deployments. Everything that varies is a flag or
mounted volume — never code:

| Concern            | Local (laptop)                     | Shared instance                              |
| ------------------ | ---------------------------------- | -------------------------------------------- |
| Bind address       | `-listen 127.0.0.1:8098` (loopback)| `-listen 0.0.0.0:<port>` (see gaps below)    |
| Data directory     | `-data /var/lib/shoal/corpus` (under the state-root volume) | same flag, backed by durable storage |
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
    -data /var/lib/shoal/corpus -listen 127.0.0.1:8099
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

The restart test proves the volume works, but you must be precise about *which*
directories you mounted and *what* survived. Three facts are distinct:

1. **The volume preserved the corpus bytes on disk.**
2. **The workspace served that corpus after restart.**
3. **The authorization state survived on its own.**

The layout above is designed so a single mount (`/var/lib/shoal`) covers both the
corpus and the authorization catalog. On `main` today, document data is durable
(Shoal storage engine) but the *authorization* policy catalog is an in-process
map (issue #284) — there is **no on-disk policy directory yet**. A startup
**backfill**, gated to `-dev-auth` on a loopback listener, re-registers the
on-disk documents for the development principal at every start, so under the
local profile a restart serves the corpus, but only because the backfill rebuilds
the in-memory authorization each time.

Observed end-to-end (Docker Desktop, host-network curl helper). **Mount:** the
named volume `shoal-explore-state` at `/var/lib/shoal` (the state root) — the
corpus directory itself was *not* mounted directly.

```console
$ docker run -d --name shoal-explore-test --network host \
    -v shoal-explore-state:/var/lib/shoal shoal-explore-web:test

# seed one document
$ docker run --rm --network host -v "$PWD/seed:/seed:ro" curlimages/curl -s \
    -X POST http://127.0.0.1:8098/api/v1/ingest \
    -H "X-Shoal-Workspace-Request: 1" \
    -F "file=@/seed/seed.md;type=text/markdown"
{"snapshot":{"id":"b7ece0950ac4662adfa51e2e119b61c334f4278951cf1010b859aac86a702176", ...}}

# before restart: seed.md served

$ docker restart shoal-explore-test
# logs after restart:
#   Granted 1 pre-existing document(s) in /var/lib/shoal/corpus to development-principal@localhost ...
#   Shoal Explorer listening at http://127.0.0.1:8098

# after restart: SAME snapshot id b7ece095..., seed.md still served
$ docker run --rm --network host -v "$PWD/seed:/seed:ro" curlimages/curl -s \
    -X POST http://127.0.0.1:8098/api/v1/documents \
    -H "X-Shoal-Workspace-Request: 1" -H "Content-Type: application/json" \
    -d '{"page":{"limit":10}}'
{"snapshot":{"id":"b7ece095...","...":"..."},"documents":[{"document":{"title":"seed.md", ...}}]}

# what is actually on the mounted state-root volume:
$ docker run --rm -v shoal-explore-state:/state --entrypoint sh curlimages/curl \
    -c "ls -1 /state; echo ---; ls -1 /state/corpus; echo ---; \
        test -d /state/corpus-policy && echo policy:YES || echo policy:NO"
corpus
---
_shoal_explorer
---
policy:NO
```

The snapshot id `b7ece095…` is identical before and after restart, and the
post-restart log reports `Granted 1 pre-existing document(s)` — the corpus was on
disk under `/var/lib/shoal/corpus` and the dev-auth backfill re-authorized it.

**What this proves (facts 1 and 2):** the single state-root mount preserved the
corpus on disk and the local `-dev-auth` profile served it again.

**What is unverified (fact 3), honestly:** the authorization half. This branch
does **not** include the durable policy store (#284 / PR #288), so no
`corpus-policy/` directory exists yet — the on-disk listing above shows only
`corpus/`, `policy:NO`. The policy catalog is in-memory and is reconstructed by
the dev-only backfill each start. I could not exercise authorization-state
persistence here and did not fake it. The volume layout is nevertheless correct
in advance: when #288 lands, `corpus-policy/` is created as a sibling of
`corpus/` **inside** the already-mounted `/var/lib/shoal`, so the same single
mount will carry it across restarts.

Do not read the green restart test as "policy persistence works." Without the
backfill (any non-dev authenticator), a restart on today's `main` would preserve
the corpus on disk but serve an **empty** result set until each document is
ingested again.

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
  volume at the state root `/var/lib/shoal` (covering both the corpus and the
  sibling policy catalog). Caveat: Container Apps runs multiple
  replicas by default and load-balances across them; this workload is
  **single-instance** (no shared coordination — see gap #2), so pin
  `minReplicas: 1` / `maxReplicas: 1`. SMB/Azure Files latency under the storage
  engine should be validated before committing.
- **App Service (custom container)** — persistent storage via `WEBSITES_ENABLE_APP_SERVICE_STORAGE`
  or a mounted Azure Files share at `/var/lib/shoal`. Also single-instance
  (disable scale-out).
- **AKS** — a single-replica `Deployment` (or `StatefulSet`) with a
  `ReadWriteOnce` PVC mounted at the state root `/var/lib/shoal`. The most
  control, the most operational overhead.

In all three, TLS terminates at the platform ingress and the app still needs a
real authenticator (gap #1) before it is exposed. Treat the above as one
sketch, not a recommendation.
