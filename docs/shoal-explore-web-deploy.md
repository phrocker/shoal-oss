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
- [Provider-neutral OIDC authentication](#provider-neutral-oidc-authentication)
- [Browser login (Authorization Code + PKCE)](#browser-login-authorization-code--pkce)
- [The unsafe-configuration guard](#the-unsafe-configuration-guard)
- [Persistence: corpus and authorization both survive](#persistence-corpus-and-authorization-both-survive)
- [Shared / cloud instance: shape and open gaps](#shared--cloud-instance-shape-and-open-gaps)
- [Azure hosting: App Service for Containers, single instance](#azure-hosting-app-service-for-containers-single-instance)

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
| Authenticator      | `-dev-auth` (loopback-only)        | OpenID Connect (`-oidc-*`, see below)        |
| TLS termination    | none (loopback)                    | terminated at an ingress/reverse proxy       |
| Log format         | Go `log` text to stderr            | same today (no structured-log flag yet)      |

`deploy/shoal-explore-web/.env.example` documents the one value the local compose
profile parameterises (`SHOAL_EXPLORE_LISTEN`). The remaining columns are the
seam for a shared instance and are covered under [gaps](#shared--cloud-instance-shape-and-open-gaps).

## Provider-neutral OIDC authentication

A shared, non-loopback instance runs the standards-based OIDC authenticator
instead of `-dev-auth`. It validates every bearer token before minting an
`auth.Decision`: asymmetric signature and key identifier, configured algorithm
allow-list, exact issuer, one of the configured audiences, expiry, and
not-before. Signing keys are fetched from `jwks_uri`, cached, and refreshed on a
bounded cadence when an unknown key identifier appears. `alg: none` and all
HMAC algorithms are always refused.

`-dev-auth` and OIDC are mutually exclusive. Missing or partial OIDC
configuration, unavailable or inconsistent discovery metadata, unavailable or
malformed JWKS, malformed claims, and unmapped authorization values all fail
closed. Authentication never falls back to anonymous or development authority.

### Required token validation and claim mapping

| Flag / environment fallback | Purpose |
| --- | --- |
| `-oidc-issuer` / `SHOAL_OIDC_ISSUER` | Exact issuer required in both the discovery document and token. |
| `-oidc-audience` / `SHOAL_OIDC_AUDIENCE` | Comma-separated accepted token audiences; at least one exact match is required. |
| `-oidc-authorization-claim` / `SHOAL_OIDC_AUTHORIZATION_CLAIM` | Exact top-level string or string-array claim whose values are mapped to authority. |
| `-oidc-reader-values` / `SHOAL_OIDC_READER_VALUES` | Comma-separated claim values granting list, read, connect, neighborhood, retrieve, workspace-settings read, and agent resolve. |
| `-oidc-contributor-values` / `SHOAL_OIDC_CONTRIBUTOR_VALUES` | Comma-separated claim values granting the reader operations plus ingest, workspace-settings write, agent register/heartbeat/revoke, and delegation for child-agent registration. |

At least one reader or contributor value is required. A missing, malformed, or
unmapped authorization claim is denied before a service operation runs.
Authentication alone never grants corpus access.

The subject defaults to the standard `sub` claim. Optional exact top-level
claim mappings preserve richer decision identity and delegation:

| Flag / environment fallback | Decision field |
| --- | --- |
| `-oidc-subject-claim` / `SHOAL_OIDC_SUBJECT_CLAIM` | `Subject` (default `sub`) |
| `-oidc-actor-claim` / `SHOAL_OIDC_ACTOR_CLAIM` | `Actor` |
| `-oidc-client-id-claim` / `SHOAL_OIDC_CLIENT_ID_CLAIM` | `ClientID` |
| `-oidc-delegation-claim` / `SHOAL_OIDC_DELEGATION_CLAIM` | ordered `OnBehalfOf` chain; accepts a string or string array |

When an optional mapping is configured, that claim becomes required and must
have the expected string shape. Token-derived identities are namespaced by the
validated issuer so subjects from different issuers cannot collide.

### Discovery, keys, and validation options

| Flag / environment fallback | Default |
| --- | --- |
| `-oidc-discovery-url` / `SHOAL_OIDC_DISCOVERY_URL` | `<issuer>/.well-known/openid-configuration` |
| `-oidc-jwks-uri` / `SHOAL_OIDC_JWKS_URI` | `jwks_uri` from discovery |
| `-oidc-allowed-algs` / `SHOAL_OIDC_ALLOWED_ALGS` | `RS256`; RS/PS/ES 256/384/512 are supported |
| `-oidc-clock-skew` | `60s`, capped at `5m` |

Issuer and endpoint URLs require HTTPS. Loopback HTTP is available only to the
in-process test seam, not to production command configuration. A client secret
is never accepted.

### Migration from the former provider-specific configuration

The former `-entra-*` flags, `SHOAL_ENTRA_*` settings, Azure Bicep parameters,
and browser `tenant_id` / `authority` response fields remain available as
**deprecated compatibility aliases**. They are translated into the OIDC
implementation rather than selecting a separate authenticator. New `-oidc-*`
values take precedence field-by-field, so configuration can be migrated
incrementally:

| Former setting | Provider-neutral replacement |
| --- | --- |
| tenant | `-oidc-issuer` / `SHOAL_OIDC_ISSUER`, set to the discovery document's exact issuer |
| client ID | `-oidc-audience` / `SHOAL_OIDC_AUDIENCE`; also set `-oidc-browser-client-id` for browser login |
| reader/contributor roles | `-oidc-authorization-claim roles` plus the corresponding reader/contributor values |
| issuer, JWKS URI, allowed algorithms, clock skew | the same-named `-oidc-*` options |
| browser scope | `-oidc-browser-scope` / `SHOAL_OIDC_BROWSER_SCOPE` |

Compatibility mode preserves the old issuer derivation
(`https://login.microsoftonline.com/<tenant>/v2.0`), `oid`-then-`sub` subject
selection, `roles` claim mapping, `entra:` identity namespace, unmapped
list-only/no-corpus decision, browser scope
`openid profile <client-id>/.default`, and v2 authorization/token endpoint
defaults. The deprecated exported `BrowserAuthConfig.TenantID` and `Authority`
fields and their JSON wire fields are retained; generic endpoint fields are
published alongside them.

This changes configuration names, not the authorization path: both modern and
compatibility inputs use the OIDC validator and mint an `auth.Decision` that
must pass the matching binder and resolver before any service operation runs.

### Example

```console
$ docker run --rm --network host -v shoal-explore-state:/var/lib/shoal \
    shoal-explore-web:test \
    -state-dir /var/lib/shoal -listen 0.0.0.0:8098 \
    -allowed-host explorer.example.test \
    -oidc-issuer https://identity.example.test \
    -oidc-audience shoal-api \
    -oidc-authorization-claim access \
    -oidc-reader-values reader \
    -oidc-contributor-values contributor
Validating OIDC bearer tokens for audience(s) shoal-api; unmapped authorization claims are denied
Shoal Explorer listening at http://0.0.0.0:8098
```

Clients call the API with a bearer token issued by the configured issuer and
addressed to one of the configured audiences. Set `-allowed-host` before
exposing a public bind behind a reverse proxy (see below).

## Browser login (Authorization Code + PKCE)

Browser login is optional. Set `-oidc-browser-client-id` and
`-oidc-browser-scope` to enable the embedded UI's OAuth 2.0 Authorization Code
flow with PKCE (S256 only). The access token is held in memory only; it is never
written to `localStorage`, `sessionStorage`, or a cookie.

The UI bootstraps by reading `GET /api/v1/auth-config`, which is unauthenticated
and returns only the non-secret client ID, scope, authorization endpoint, and
token endpoint. Endpoints come from OIDC discovery unless overridden with
`-oidc-authorization-endpoint` / `SHOAL_OIDC_AUTHORIZATION_ENDPOINT` and
`-oidc-token-endpoint` / `SHOAL_OIDC_TOKEN_ENDPOINT`. Under `-dev-auth`, or an
API-only OIDC configuration without a browser client ID, it reports
`{"configured":false}` and renders no login control.

Register the deployed origin's root (for example
`https://explorer.example.test/`) as an allowed redirect URI for a public client
at the chosen identity provider, permit CORS for the token endpoint, and grant
the scopes named by `-oidc-browser-scope`. The browser sends no client secret.
The server validates the resulting access token independently against its
issuer, audience, signature, time, and claim-mapping configuration.

## Streamable HTTP MCP

The same authenticated workspace exposes MCP `2025-11-25` at `/mcp`. It uses
the documented Streamable HTTP lifecycle: `POST initialize`, the returned
`MCP-Session-Id`, `POST notifications/initialized`, and a
`MCP-Protocol-Version: 2025-11-25` header on every subsequent request. Clients
must advertise both `application/json` and `text/event-stream` in `Accept`;
this implementation returns synchronous JSON responses and answers `GET /mcp`
with `405 Method Not Allowed` because it emits no unsolicited SSE messages.

`/mcp` is mounted inside the existing `webapi.Handler`, not beside it. Host
authority validation and the configured development/OIDC authenticator
therefore run before every MCP request. Every request must also carry exactly
one `Shoal-Workspace-ID` header containing the unpadded base64url encoding of
the owned workspace ID. Workspace settings narrow the issuer decision before
MCP dispatch and lower retrieval, graph, output, and context limits. Session
IDs retain lifecycle state only, are cryptographically random, and are bound to
the caller's authorization fingerprint plus the effective workspace ID,
settings ID, revision, cache dimensions, and limits. Another principal or
workspace cannot reuse one; a policy-generation or settings change makes the
old session absent. The initialize result reports the applied values in
`_meta["shoal.workspace"]`. An `Origin`, when present, must exactly match an
HTTP or HTTPS origin derived from the configured allowed Host authorities.

Generic MCP tool calls are durably recorded as `OperationToolCall` through the
authorized Explorer client. Recording is mandatory and fail-closed. Mutations
receive a durable admission record before dispatch; if their post-effect
outcome record fails, the response is explicitly indeterminate and directs the
caller to inspect current state before considering a retry. The first-party command advertises `shoal.ask`,
`shoal.provenance.{list,inspect,fold,unfold}`, and, when the embedded Fleet
providers are available, `shoal.agent_dispatch` and `shoal.agent_invoke`.
They use the same chat, interaction, registry, and dispatch providers as the
HTTP API; no second reasoning or action pipeline is constructed.
Ask observations preserve the exact complete evidence accepted with the durable
interaction, including citations, nodes, edges, assertions, visibility, and
embedding-space identities. Direct `shoal.retrieve` results also receive an
independent retrieval capture in addition to the generic tool-call record.
Provenance listing is bounded to 100 records by default (1,000 maximum) and
returns an opaque `next_cursor` for both HTTP and MCP callers.
`OperationToolCall` is only a provenance discriminator; the canonical
authorized operation is stored separately, actor/delegation metadata comes only
from the bound decision, and the authorized client derives the hashed
audit-purpose reason. Grounded evidence remains reauthorized with the legacy
`retrieve` operation, while source-free actions need only their exact authorized
action. Fleet action tools must separately enforce that exact `auth.Operation`
before effects.

## Durable Fleet registry, dispatch, and events

The embedded command composes the durable registry and dispatch services on
the same transaction runtime, mounts their combined handler once at
`/api/v1/fleet/`, and mounts the more-specific event subtree at
`/api/v1/fleet/events/` through the same authenticated workspace handler.
Register, heartbeat, revoke, resolve, list, enqueue, claim, cancel, status,
pull, invoke, subscription, publication, and delivery operations use the same
bound authorization resolver. Missing recorder, snapshot, generation, cursor,
registry, dispatch, or event dependencies fail startup rather than advertising
partial functionality. Every
privileged mutation first records an `OperationToolCall` interaction carrying
the exact `agent_*` authorization operation, decision fingerprint/expiry, and a
fresh trusted interaction snapshot. The adapter supplies no actor or reason;
the authorized interaction client derives trusted identity fields and validates
the exact persisted receipt.

Registry list requests accept an optional `limit` (default 25, maximum 32) and
opaque `cursor`. Responses return at most that many authorized descriptors and
an optional `next_cursor`; filtering advances through a bounded number of
stored descriptors per request.

Executor references are host-owned opaque capabilities. Configure their
allowlist with `-fleet-executor-refs` or `SHOAL_FLEET_EXECUTOR_REFS` as a
comma-separated list. An empty list is valid but fail-closed: registry reads
remain available while new registrations are rejected because no executor is
known to the host.

Dispatch transitions publish stable `action.enqueued`, `action.claimed`,
`action.completed`, `action.canceled`, and `action.failed` events. Raw
idempotency, claim, executor, and cancellation keys remain private; envelopes
carry the producer generation and an opaque hashed transition identity.
Lifecycle publication reauthorizes the original exact `dispatch` or `invoke`
operation and retains complete representable evidence. Subscription delivery
uses the dedicated `subscription_deliver` operation and rechecks the shared
policy-generation authority during long polls.

Fleet event cursors are AES-GCM-protected, restart-stable, and scoped to the
subscription and authorization identity. Event and subscription storage use
4,096 bounded logical slots with an event-local durable retention floor.
Expired publication retries fail closed. Historical physical cell-version
cleanup remains the responsibility of normal Explorer storage compaction; the
event subsystem does not advance a shared allocator history floor.

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

Production mode uses the OIDC authenticator; running it **without** either
`-dev-auth` or an `-oidc-*` configuration still fails closed before the corpus
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

1. **The provider-neutral OIDC authenticator is wired (issue #315).** Supply the
   `-oidc-*` flags (see [Provider-neutral OIDC authentication](#provider-neutral-oidc-authentication))
   and a non-loopback listener is allowed. Without either `-dev-auth` or an
   OIDC configuration, startup still fails closed demanding an authenticator.
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

A public bind is now end-to-end serviceable behind a proxy: run the OIDC
authenticator and set `-allowed-host` to the external name the proxy forwards.
Omitting `-allowed-host` on a public bind is a deliberate fail-closed posture —
requests are refused until the external authority is declared — not a packaging
defect.

## Azure hosting: App Service for Containers, single instance

The hosting shape **is** decided, and the deciding factor is exactly the one this
guide has been building toward: a **persistent, single-writer volume** that
survives restarts and redeploys without ever running two writers against the
embedded store. The full decision, evidence (tagged verified / documented /
inferred), and the deployable Bicep live under
[`deploy/shoal-explore-web/azure/`](../deploy/shoal-explore-web/azure/README.md).

**Chosen: Azure App Service for Containers, one instance, deploy stop-first.**
The short version of why:

- **Container Apps is rejected.** In single-revision mode it uses zero-downtime
  deployment, so a new revision is brought up healthy **before** the old one is
  torn down — the two overlap — and its only persistent storage is Azure Files,
  which "multiple containers can mount … in another replica, revision, or
  container app." For a single-writer local store that overlap is data
  corruption you cannot switch off. (Microsoft Learn: Container Apps application
  lifecycle; storage mounts.)
- **App Service is chosen.** It also warm-swaps a new container in before
  stopping the old, **but** it has a first-class `az webapp stop` / `start`, so a
  deploy can force the old writer fully down before the new one starts. Its
  bring-your-own Azure Files mount attaches at the state root `/var/lib/shoal`
  (App Service forbids mounting at `/` or `/home`, but `/var/lib/shoal` is fine).
  Single instance is `capacity: 1` + `numberOfWorkers: 1` + no autoscale.
- **AKS is rejected as overkill.** A `replicas: 1` StatefulSet on a
  ReadWriteOnce Azure Disk is the *only* option that enforces one writer at the
  storage layer, and it is the documented escalation path if the Azure Files
  risks below bite — but running a Kubernetes cluster to host one binary is not
  justified for this workload.

Two Azure Files risks are sized honestly in the artifact README. The genuine
gate is the non-root uid `65532` writing an SMB mount (a first-boot check settles
it). The SMB-semantics worry is **largely retired by evidence**: this binary has
no memory-mapped I/O anywhere in the tree and never reaches the only `flock` code
(`internal/promotion`, absent from `go list -deps ./cmd/shoal-explore-web` on both
Windows and Linux) — the two mechanisms that make embedded stores unsafe on SMB —
so Azure Files SMB is a reasonable default, with NFS or AKS + Azure Disk kept as
escalation paths only if a specific problem shows up. TLS terminates at the App Service front end and the
app runs the OIDC authenticator (`-oidc-*` / `SHOAL_OIDC_*`). The
host-authority gate (gap #3) is closed by **PR #295** (merged to `main` at
`3670e00`): the template sets `SHOAL_ALLOWED_HOST` to the App Service hostname
(or your custom domains) so the public bind is serviceable, not refused with 421.
See the artifact README's host-authority section for the wiring and the honest
defence-in-depth caveat.
