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
- [Real authentication with Microsoft Entra ID](#real-authentication-with-microsoft-entra-id)
- [The unsafe-configuration guard](#the-unsafe-configuration-guard)
- [Persistence: corpus and authorization both survive](#persistence-corpus-and-authorization-both-survive)
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
Size=7127891 User=65532:65532 Cmd=["-state-dir","/var/lib/shoal","-listen","127.0.0.1:8098","-dev-auth"] Volumes={"/var/lib/shoal":{}}
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

Configure the workspace with **`-state-dir /var/lib/shoal`** (the recommended
flag from PR #288) and mount that one directory as the volume. The command places
both persistent directories inside it:

```
/var/lib/shoal/                 <- the declared VOLUME (mount THIS, persist THIS)
├── corpus/                      <- the document corpus
│   └── _shoal_explorer/         <- the corpus engine's table
└── policy/                      <- the durable authorization catalog (#284/#288)
    └── _shoal_policy/           <- the policy store's table
```

**One sentence: persist `/var/lib/shoal` — the whole state root — and both the
corpus and the authorization catalog survive a restart.**

The authorization catalog is a **sibling** of the corpus, never a child of it:
the corpus engine treats every subdirectory of the corpus directory as a table,
so nesting the catalog there would corrupt table discovery. `-state-dir` keeps
them as siblings under one mount, which removes the earlier string-suffix
coupling entirely.

Flag precedence (see `resolveWorkspacePaths` and `TestResolveWorkspacePathsPrecedence`):

- **`-state-dir <root>`** (recommended) → corpus `<root>/corpus`, policy `<root>/policy`.
- **`-data <dir>`** (legacy, backwards compatible) → corpus `<dir>`, policy the
  sibling `filepath.Clean(<dir>)+"-policy"`. Both must be persisted.
- **`-policy-dir <dir>`** overrides the policy location and always wins.

**Mounting only the corpus directory** would preserve documents but drop every
authorization registration on restart. On this build that is not a silent
failure: the split-brain guard (below) refuses to start. Mounting the state root
keeps both. The image declares `/var/lib/shoal` (not a subdirectory) as the
volume, owned by `65532`, so the non-root process creates `corpus/` and `policy/`
at runtime.

## The configuration seam: local vs shared

The same image serves both deployments. Everything that varies is a flag or
mounted volume — never code:

| Concern            | Local (laptop)                     | Shared instance                              |
| ------------------ | ---------------------------------- | -------------------------------------------- |
| Bind address       | `-listen 127.0.0.1:8098` (loopback)| `-listen 0.0.0.0:<port>` (see gaps below)    |
| Host authority     | default (the resolved listen address) | `-allowed-host <external-name[:port]>` (see below) |
| Data directory     | `-state-dir /var/lib/shoal` (one mounted volume) | same flag, backed by durable storage |
| Authenticator      | `-dev-auth` (loopback-only)        | Microsoft Entra ID (`-entra-*`, see below)   |
| TLS termination    | none (loopback)                    | terminated at an ingress/reverse proxy       |
| Log format         | Go `log` text to stderr            | same today (no structured-log flag yet)      |

`deploy/shoal-explore-web/.env.example` documents the one value the local compose
profile parameterises (`SHOAL_EXPLORE_LISTEN`). The remaining columns are the
seam for a shared instance and are covered under [gaps](#shared--cloud-instance-shape-and-open-gaps).

## Real authentication with Microsoft Entra ID

A shared, non-loopback instance runs the **Microsoft Entra ID (Azure AD)
authenticator** instead of `-dev-auth`. It validates the OIDC bearer token on
each request — signature against the tenant's JWKS (fetched, cached, and
refreshed on rotation), an asymmetric-algorithm allowlist that rejects `alg:
none` and every symmetric algorithm, exact issuer, exact audience, and
expiry/not-before with a configurable clock-skew tolerance — and mints a trusted
per-request decision. `-dev-auth` and the Entra authenticator are **mutually
exclusive**; supplying both is refused. A validated token unlocks a non-loopback
listener, which `-dev-auth` never could.

### Required flags

| Flag / environment fallback                        | Purpose                                                             |
| -------------------------------------------------- | ------------------------------------------------------------------- |
| `-entra-tenant` / `SHOAL_ENTRA_TENANT`             | Tenant (directory) ID; derives the expected issuer and OIDC discovery. Required unless `-entra-issuer` is set. |
| `-entra-client-id` / `SHOAL_ENTRA_CLIENT_ID`       | Application (client) ID the token audience must match exactly. **Required.** |

A **client secret is never accepted**: this validates inbound bearer tokens, it
does not perform any token-issuing flow.

### Authority mapping (required to grant any corpus access)

A validated token proves **identity only**. It confers no corpus access by
itself. Authority is granted by mapping Entra **app roles** to workspace
operations, and the mapping is fail-closed:

| Flag / environment fallback                                      | Grant                                       |
| --------------------------------------------------------------- | ------------------------------------------- |
| `-entra-reader-roles` / `SHOAL_ENTRA_READER_ROLES`              | list, read, connect, neighborhood, retrieve |
| `-entra-contributor-roles` / `SHOAL_ENTRA_CONTRIBUTOR_ROLES`    | the reader set **plus** ingest              |

Both take a comma-separated list of app-role values. An authenticated caller
whose token carries **no configured role** — including a missing `roles` claim —
is granted **no corpus access at all**: they authenticate, but every registered
document is invisible to them. An absent or unrecognised role never widens
access. If neither list is configured, every caller is unmapped and the corpus
is invisible to everyone, so you must assign at least one role to grant access.

### Optional flags

| Flag / environment fallback                        | Default                                                          |
| -------------------------------------------------- | --------------------------------------------------------------- |
| `-entra-issuer` / `SHOAL_ENTRA_ISSUER`             | `https://login.microsoftonline.com/<tenant>/v2.0`               |
| `-entra-jwks-uri` / `SHOAL_ENTRA_JWKS_URI`         | resolved via OIDC discovery from the issuer                     |
| `-entra-allowed-algs`                              | `RS256` (any of RS/PS/ES 256/384/512; `HS*` and `none` refused) |
| `-entra-clock-skew`                                | `60s` (capped at `5m`)                                          |

### Example

```console
$ docker run --rm --network host -v shoal-explore-state:/var/lib/shoal \
    shoal-explore-web:test \
    -state-dir /var/lib/shoal -listen 0.0.0.0:8098 \
    -allowed-host explorer.example.test \
    -entra-tenant <tenant-guid> \
    -entra-client-id <application-client-id> \
    -entra-reader-roles Shoal.Reader \
    -entra-contributor-roles Shoal.Contributor
Validating Microsoft Entra ID bearer tokens for audience <application-client-id>; unmapped callers receive no corpus access
Shoal Explorer listening at http://0.0.0.0:8098
```

The IDs above are placeholders — substitute your tenant and application (client)
IDs. Clients call the API with `Authorization: Bearer <token>`, where the token
is a v2 Entra token addressed to `<application-client-id>`. Set `-allowed-host`
before exposing a public bind behind a reverse proxy (see below).

## Host authority (required for a public bind)

Every request must present a `Host` header (HTTP/1.1) or `:authority` (HTTP/2)
that matches an allow-list, enforced centrally before routing or handling. The
match is exact: the hostname compares **case-insensitively** and the port
**exactly**. There is **no wildcard, no suffix, and no `X-Forwarded-Host` or
`Forwarded` matching** — each of those is a classic host-authority bypass. A
mismatch is refused with `421 Misdirected Request` and a fixed body that never
echoes the submitted host. This bounds cache poisoning, absolute-URL/redirect
poisoning, virtual-host confusion, and DNS rebinding against a private-network
listener.

| Flag / environment fallback                | Purpose                                                                   |
| ------------------------------------------ | ------------------------------------------------------------------------- |
| `-allowed-host` / `SHOAL_ALLOWED_HOST`     | Comma-separated exact-match list of external authorities (`host` or `host:port`). |

**Default when unset — the resolved listen address (fail-closed).** A loopback
bind (`127.0.0.1:8098`) therefore serves only requests whose `Host` is that
loopback authority, preserving the local-first posture with no configuration.
A **non-loopback or wildcard** bind (`0.0.0.0:<port>`) resolves to a socket
address real client `Host` headers never carry, so such a deployment **refuses
every request until `-allowed-host` names the external authority** the proxy or
client actually sends. That is deliberate: a public bind fails closed rather
than silently accepting any `Host`.

A deployment behind a reverse proxy that legitimately answers to more than one
name lists each explicitly, comma-separated (for example
`-allowed-host explorer.example.test,explorer-internal.example.test`). Because
TLS terminates at the proxy, the value is usually the port-less external name
(for example `explorer.example.test`), matching the `Host` the browser sends on
the default port; include a port only when the client sends one.

`X-Forwarded-Host` is **never** trusted. A proxy story that requires honouring
a forwarded host is out of scope here (it needs an authenticated hop and an
explicit trust boundary) and is not implemented.

A `Host` in FQDN-root form with a single trailing dot (`explorer.example.test.`)
is treated as equal to its non-rooted spelling — the dot is normalised away on
both the configured and the request side, so either form matches either. Without
that normalisation the rooted form would fail closed (a `421`, not a security
hole); it is normalised only to avoid a confusing outage if a client sends it.

When `-listen` binds a non-loopback or wildcard address and `-allowed-host` is
unset, the command prints a one-time startup **WARNING** naming the bound
address and the `421` consequence, before any request is served. This is the
most common way to trip over the gate; refusals themselves are not logged
per-request, because the `Host` is attacker-controlled and would invite a log
flood.

## The unsafe-configuration guard

The dangerous combination — a shared/public bind with the development
authenticator — **fails closed at startup** before any socket is bound. This is
enforced by `selectAuthenticator` / `listenAddressIsLoopback` in
`cmd/shoal-explore-web/authn.go`; it is verified here, not merely asserted.

Public bind + `-dev-auth` is refused:

```console
$ docker run --rm --network host shoal-explore-web:test \
    -state-dir /var/lib/shoal -listen 0.0.0.0:8099 -dev-auth
shoal-explore-web: refusing to serve 0.0.0.0:8099 with -dev-auth: the
development-principal@localhost development principal is granted the whole
workspace corpus and is only safe on a loopback listener; bind 127.0.0.1 or
[::1], or supply a real authenticator
# exit code 1
```

No authenticator at all is also refused (the workspace never serves anonymously):

```console
$ docker run --rm --network host shoal-explore-web:test \
    -state-dir /var/lib/shoal -listen 127.0.0.1:8099
shoal-explore-web: refusing to serve 127.0.0.1:8099 without authentication: no
authenticator is configured; pass -dev-auth to mint the
development-principal@localhost development principal on a loopback listener, or
supply a real authenticator before exposing the Explorer API
# exit code 1
```

The check runs twice: once on the requested flag value before anything binds,
and again on the listener's *resolved* address (which may be wider than
requested), closing the listener before the corpus is opened.

## Persistence: corpus and authorization both survive

Be precise about which directories were mounted and what survived. Three facts
are distinct:

1. **The volume preserved the corpus bytes on disk.**
2. **The workspace served that corpus after restart.**
3. **The authorization registrations survived — not rebuilt from scratch.**

With PR #288 merged the policy catalog is durable, and `-state-dir /var/lib/shoal`
places `corpus/` and `policy/` under one mount, so all three hold.

### Observed under `-dev-auth` (the container / compose profile)

**Mount:** the named volume `shoal-explore-state` at `/var/lib/shoal` (the state
root); `-state-dir /var/lib/shoal`.

```console
$ docker run -d --name shoal-explore-test --network host \
    -v shoal-explore-state:/var/lib/shoal shoal-explore-web:test
# first-start log:
#   Granted 0 pre-existing document(s) in /var/lib/shoal/corpus to development-principal@localhost ...
#   Shoal Explorer listening at http://127.0.0.1:8098

# seed one document
$ docker run --rm --network host -v "$PWD/seed:/seed:ro" curlimages/curl -s \
    -X POST http://127.0.0.1:8098/api/v1/ingest \
    -H "X-Shoal-Workspace-Request: 1" \
    -F "file=@/seed/seed.md;type=text/markdown"
{"snapshot":{"id":"23f7a8e4cc666a53...", ...}}
# before restart: seed.md served

$ docker restart shoal-explore-test
# post-restart log:
#   Granted 0 pre-existing document(s) in /var/lib/shoal/corpus to development-principal@localhost ...
#   Shoal Explorer listening at http://127.0.0.1:8098

# after restart: seed.md still served
$ docker run --rm --network host -v "$PWD/seed:/seed:ro" curlimages/curl -s \
    -X POST http://127.0.0.1:8098/api/v1/documents \
    -H "X-Shoal-Workspace-Request: 1" -H "Content-Type: application/json" \
    -d '{"page":{"limit":10}}'
{ ... "documents":[{"document":{"title":"seed.md", ...}}]}

# both directories are present on the mounted state root:
$ docker run --rm -v shoal-explore-state:/state --entrypoint sh curlimages/curl \
    -c "ls -1 /state; echo ---; ls -1 /state/corpus; echo ---; ls -1 /state/policy"
corpus
policy
---
_shoal_explorer
---
_shoal_policy
```

Note the post-restart log: **`Granted 0 pre-existing document(s)`**. On the
pre-#288 build this line read `Granted 1` — the in-memory catalog had lost the
registration and the backfill re-created it. Now it is `0`: the durable policy
store already held the registration, so the backfill had nothing to migrate. Both
`corpus/` and `policy/` are present on the single mounted volume.

**Caveat, stated plainly:** this profile runs `-dev-auth`, and the split-brain
guard is **bypassed whenever the dev backfill is active**. A green restart test
*under `-dev-auth`* does not by itself demonstrate that a production container
survives a restart, because the backfill would re-register a lost corpus and mask
the loss. The `Granted 0` line is good evidence the durable store carried the
registration, but the authoritative proof of production behaviour is the guard
plus the integration tests below.

### Production-mode proof (backfill disabled)

Production mode uses the Entra authenticator; running it **without** either
`-dev-auth` or an `-entra-*` configuration still fails closed before the corpus
is opened, because no authenticator is configured. Observed:

```console
$ docker run --rm --network host -v shoal-explore-state:/var/lib/shoal \
    shoal-explore-web:test -state-dir /var/lib/shoal -listen 127.0.0.1:8098
shoal-explore-web: refusing to serve 127.0.0.1:8098 without authentication: no
authenticator is configured; pass -dev-auth ...
# exit code 1
```

So the durable-persistence and split-brain behaviours are proven where they are
reachable — the package's integration tests, which drive `openService` with
`backfill: nil` (production):

- **`TestStateDirLayoutSharesOneMountPoint`** — ingests under a `-state-dir` root,
  reopens with the backfill disabled, and serves the document after the reopen;
  the guard does not fire because `corpus/` and `policy/` shared the mount. This
  is fact 3 in production mode: **the authorization registrations survived on
  their own.**
- **`TestOpenServiceRefusesSplitBrainStateDirectory`** — ingests, removes the
  policy directory (a lost/unmounted policy volume), reopens in production, and
  asserts the guard refuses.

```console
$ go test ./cmd/shoal-explore-web/ -run 'SplitBrain|StateDir' -v
--- PASS: TestOpenServiceRefusesSplitBrainStateDirectory (0.06s)
--- PASS: TestStateDirLayoutSharesOneMountPoint (0.06s)
```

### The guard message (what a lost policy volume produces)

Captured by driving the real `openService` guard path (ingest under a state root,
delete `policy/`, reopen with `backfill: nil`):

```text
refusing to serve a split-brain workspace: corpus <root>/corpus holds 1
document(s) but the durable policy catalog <root>/policy holds no authorization
registrations. This is the signature of a lost or unmounted policy volume; every
registration was dropped and the workspace would serve an empty or
under-populated corpus. Restore the policy directory from the same volume as the
corpus (use -state-dir so both persist under one mount), or, for a corpus
ingested before the catalog was durable, run once with -dev-auth on a loopback
listener to re-register it (issue #284)
```

### Did the guard fire in the restart test?

**No — and that is expected.** The container restart test runs `-dev-auth`, which
activates the dev backfill and bypasses the guard by design. The guard protects
the production path (`backfill == nil`), which is currently unreachable from the
container CLI (it refuses earlier, above) and is exercised by the two tests. If a
future production deployment mounts only the corpus directory, the guard turns
that silent misconfiguration into a hard startup refusal with the message above.

## Shared / cloud instance: shape and open gaps

The image is shared-instance ready; the *runtime prerequisites* are not all in
place on `main`. Honest gaps, none of which this deployment work should paper
over:

1. **The Microsoft Entra ID authenticator is wired (issue #278).** Supply the
   `-entra-*` flags (see [Real authentication with Microsoft Entra ID](#real-authentication-with-microsoft-entra-id))
   and a non-loopback listener is allowed. Without either `-dev-auth` or an
   Entra configuration, startup still fails closed demanding an authenticator.
2. **`-backend remote` is deliberately refused** because `auth.Decision` has no
   on-the-wire representation (issue #278): forwarding would authenticate at the
   edge and then call upstream with no identity. This means **multi-node scaling
   is out of scope**; the target is single instance, many users.
3. **Host-authority binding is configurable (this change).** The handler
   requires the request `Host`/`:authority` to match an exact allow-list. It
   defaults to the listener's resolved address — so a loopback bind is served
   with no configuration — and a shared instance behind a reverse proxy sets
   `-allowed-host` / `SHOAL_ALLOWED_HOST` to the external name(s), decoupling the
   served authority from the wildcard socket address. See
   [Host authority](#host-authority-required-for-a-public-bind). Until it is set,
   a public bind of `0.0.0.0:<port>` fails closed (every request `421`s),
   because the resolved authority `0.0.0.0:<port>` never matches a real client
   `Host`.
4. **Durable policy store (#284 / PR #288) has landed.** The policy catalog now
   persists, and a split-brain guard refuses to start when the corpus holds
   documents but the policy catalog is empty (a lost policy volume). Use
   `-state-dir` so the corpus and policy always persist under one mount.

A public bind is now end-to-end serviceable behind a proxy: run the Entra
authenticator and set `-allowed-host` to the external name the proxy forwards.
Omitting `-allowed-host` on a public bind is a deliberate fail-closed posture —
requests are refused until the external authority is declared — not a packaging
defect.

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

In all three, TLS terminates at the platform ingress and the app runs the Entra
authenticator (`-entra-*`) and sets `-allowed-host` to the external name the
ingress forwards, so a public bind behind the ingress is end-to-end serviceable.
Treat the above as one sketch, not a recommendation.
