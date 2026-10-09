# Effects gateway: deployment

> **Not yet runnable.** There is no `cmd/shoal-gateway` and no chart entry.
> What exists is the library in `internal/effectsgateway`: the core, the
> dispatch client and the worker loop with its unrecorded-report log. The
> command (PR6) composes them; nothing in the package starts on its own.

This is the HTTP Path A worker from #391: it pulls an action off the fleet
dispatch queue, claims it under a fence, performs one HTTP request against a
configured operational surface, classifies what happened, and completes the
action. The reference design is `docs/gateway-proxy-design.md`; the slice-1
design this implements is the last comment on #391. `docs/gateways.md` places
it among the three gateways.

## Blockers

| | what | why the gateway needs it |
|---|---|---|
| **#480** | the lifecycle auditor, action recorder and reconciler refuse `OperationExecute`, so the grant cannot be turned on | a worker claims work it did not enqueue; until a principal can hold `OperationExecute` on one descriptor's scope, the only claimant is the enqueuer |
| **#430** | claim renewal (`POST actions/{id}/extend`) | without it the fenced window is the claim lease, at most five minutes; `-renew` is parsed and refused |
| **#484** | the lost-fence ambiguity route (`POST actions/{id}/ambiguity`) | a worker whose claim lapsed mid-effect has nowhere to record what it attempted |
| **#486** | a heartbeat moves the descriptor generation, so `/complete` and `/ambiguity` answer 404 after one heartbeat | until fixed, the gateway must never register or heartbeat while holding claims |

Both routes now exist, and the dispatch client speaks them (`Extend`,
`ReportAmbiguity`). It has no heartbeat method, and will not get one: a worker
cannot truthfully assert a descriptor's liveness, so the gateway never
heartbeats and carries no registrar credential (#391).

## What exists

| file | what it is |
|---|---|
| `config.go` | flags, validation, lease arithmetic, credential sources |
| `routes.go` | the route table, the request binder, derived effects, the startup descriptor check |
| `classify.go` | the pure response classifier and the closed `ErrorCode` vocabulary |
| `timing.go` | clock anchoring, PRECHECK predicates, the send gate, the grace-period formula |
| `executorkey.go` | `ExecutorKey` decoding in both platform spellings |
| `dialer.go` | the egress-restricted target transport and the separate explorer client |
| `client.go`, `client_ops.go` | the internal dispatch client: pull, claim, extend, complete (bound on the claim fence), ambiguity, resolve (its own ref only), attestation presentation; every claim-scoped request carries the record's `Shoal-Correlation-ID` |
| `logging.go` | the one logging function, and the policy it enforces |
| `worker.go` | the worker loop: pull, filter, precheck, attest, claim, bind, send, classify, complete; renewal and FENCE_LOST; slots, backoff and drain |
| `unrecorded.go` | the unrecorded-report log (#514) and the directory lock that keeps the gateway to one replica |

The dispatch client is tested against the real explorer composition — the
embedded store, the real recorder and publisher, the authenticated webapi
handler, over a socket — in
`cmd/shoal-explore-web/effects_gateway_client_test.go`. Its principal is the
enqueuer's. A foreign claimant now has a credential: the executor mapping
below (#391). `cmd/shoal-explore-web/oidc_executor_e2e_test.go` drives a mapped
ServiceAccount token from a second issuer through pull, claim, extend and
complete on the real routes.

## Configuration

| flag | default | rule |
|---|---|---|
| `-dispatch-url` | | required; https, or http to loopback |
| `-allow-plaintext-dispatch` | false | admits a remote `http://` explorer behind a mesh that authenticates the hop |
| `-dispatch-token-env` / `-dispatch-token-file` | `SHOAL_DISPATCH_TOKEN` / | exactly one; the file form is read per call, which is how a projected token rotates |
| `-agent-id` | | required; the descriptor ID as unpadded base64url, not a display name |
| `-capability` | `effects.http` | fleet name grammar |
| `-surface-name` | | required; the ambiguity report's `target` |
| `-target-base-url` | | required; https, or http to loopback; **no plaintext opt-out**; no userinfo, query or fragment |
| `-target-credential-env` / `-target-credential-file` | `SHOAL_TARGET_CREDENTIAL` / | exactly one; sent verbatim as the value of `-target-auth-header` |
| `-target-auth-header` | `Authorization` | must not be a header the gateway writes itself |
| `-idempotency-header` | | required with any `key` route, refused without one; distinct from the auth header |
| `-idempotency-retention` | | required with any `key` route, refused without one; must exceed `T + 5s` |
| `-target-allow-private` | false | admits private and loopback resolution; link-local and metadata stay refused |
| `-routes` | | required; the route table as a JSON array |
| `-claim-lease` (L) | 4m | `0 < L ≤ 5m` |
| `-operation-timeout` (T) | 3m | positive |
| `-plane-timeout` | 10s | positive, `≤ L/4` |
| `-max-response-bytes` | 64 KiB | 1 to 1 MiB |
| `-pull-limit` | 32 | 1 to 256 |
| `-pull-interval` | 2s | at least 100ms |
| `-health-address` | | optional `host:port` |
| `-renew` | false | refused until #430 |

Two credential rules carried over from the LLM gateway: passing the env flag
beside the file flag is refused even when the env flag is typed as its default,
and an unset env variable is *absence* while an unreadable file is *breakage*.
Absence of a target credential is accepted only for a loopback target.

The executor reference the gateway's descriptor registers, and that an
executor-bound dispatch credential names, follows one rule:

An executor reference must pass `executorref.ValidExecutorRef`
(`pkg/executorref`, #391). Every place that accepts one applies this same rule,
and parity tests hold them all to the same verdict:
- fleet registration;
- the ATPL compiler;
- attestation presentation, on the server and in `pkg/attestation/api`;
- an action-execution decision's executor binding;
- the explorer's `-fleet-executor-refs`, `-fleet-external-executor-refs`,
  `-fleet-external-egress-executor-refs` and `-fleet-ask-executor-ref` flags,
  the `-fleet-executor-attestation` trust file, and the chart's
  `explorer.fleet.*ExecutorRef(s)` values.

The rule is a fixed ASCII charset, `^[A-Za-z0-9][A-Za-z0-9._:/@-]*$`, from 1
to 1024 bytes. A reference starts with a letter or digit and continues with
letters, digits and `. _ : / @ -`. It contains no spaces and no non-ASCII
characters. References are compared byte for byte, and inside this charset
two references that look alike are equal.

**Migration.** The rule tightens *registration*, as #544's floor did.
- Stored descriptors keep resolving.
- A descriptor whose reference falls outside the charset is refused with
  `invalid_argument` at its next `Register`, or at the next ATPL
  `plan`/`apply` that declares it.
- A new attestation presentation for such a reference is refused the same way,
  and no executor binding can name it.
- Host configuration naming one is refused at startup. The error names the
  flag and the entry's position.

If this happens to a descriptor that has worked for a long time, the control
is working: rename the executor reference, and its host binding, into the
charset and re-register.

### Lease arithmetic

```
0 < L ≤ MaxActionClaimTTL (5m)
planeTimeout ≤ L/4
without renewal:   L > T + 5s + planeTimeout
with key routes:   retention > T + 5s
```

Without renewal the lease bounds the claim, the operation and the report
together, which limits an operation to about 4.5 minutes. The retention rule
exists because the two claim-time rules `T + 5s < deadline − now` and
`deadline − created_at ≤ retention` cannot both hold when the retention is
shorter: nothing would ever be claimable, and the gateway would look idle.

### Grace period

`terminationGracePeriodSeconds ≥ T + 2×5s + 5s`: the in-flight request, its
completion, the fallback ambiguity report, and five seconds to exit.
`GracePeriodSeconds` rounds up. For `T = 10m` that is 615s; for the default
`T = 3m`, 195s.

## Issuing executor credentials

A worker claims work it did not enqueue, so it needs `OperationExecute`. In
the explorer exactly one thing grants it: the executor mapping,
`-oidc-executor-mapping-file` (`SHOAL_OIDC_EXECUTOR_MAPPING_FILE`; chart value
`explorer.auth.oidc.executorMapping`). No workspace, Fleet, development or
approver mapping holds execute, and `oidc_execute_grant_test.go` asserts both
halves. Without the file, no token can pull, claim, extend or complete queued
work.

The intended credential is a **projected ServiceAccount token**. The kubelet
mints it for the gateway's pod, signs it with the cluster's service-account
issuer, and rotates it. The gateway reads it from `-dispatch-token-file` on
every call, so a rotated token takes effect without a restart.

### The mapping file

```json
{
  "version": "shoal.executors/v1",
  "issuer": "https://oidc.eks.eu-west-1.amazonaws.com/id/0123456789ABCDEF",
  "audience": "shoal-executors",
  "service_assertion": {
    "claim": ["kubernetes.io", "namespace"],
    "equals": "shoal-gateways"
  },
  "executors": [
    {"subject": "system:serviceaccount:shoal-gateways:stripe", "executor_ref": "stripe"},
    {"subject": "system:serviceaccount:shoal-gateways:ledger", "executor_ref": "ledger"}
  ]
}
```

The file is decoded strictly. Unknown fields, duplicate keys, keys that match
a field only up to case, and trailing data are all refused, at any depth. Any
refusal stops the explorer from starting. A refusal names the entry's
position, never its value.

| field | rule |
|---|---|
| `version` | `shoal.executors/v1` |
| `issuer` | The executor credentials' own issuer, which may differ from `-oidc-issuer`. It is held to the OIDC issuer rule (#553): an absolute `https` URL with a valid host, and no user info, query, or `#` anywhere (an empty fragment, `https://x/#`, included). It must also be canonical: exactly as `url.Parse` renders it, a lower-case host, no default port, no dot segments. A token's `iss` must equal it byte for byte. |
| `jwks_uri` | Optional. Overrides discovery for this issuer, as `-oidc-jwks-uri` does for the human issuer. `https` only. |
| `audience` | Required, and disjoint from `-oidc-audience` and the approver mapping's audience. |
| `service_assertion` | Required, and positive: a claim path (a list of segments; a dotted string is one key, never a path) that must equal `equals` exactly. For projected ServiceAccount tokens, use the namespace claim `["kubernetes.io", "namespace"]`. For an IdP client-credentials token, use a marker such as Auth0's `["gty"]` = `client-credentials`. "Absent" is not an accepted form, because absence proves nothing about who a token was issued to. |
| `executors` | 1 to 256 `{subject, executor_ref}` entries. A subject is compared byte for byte with the token's `sub`: no trimming, case folding or normalization. `executor_ref` must pass `executorref.ValidExecutorRef`. Subjects are unique and references are unique: one credential maps to one surface, and one surface has one credential. |

**Choose a service assertion no human token can ever carry.** Good choices:
- Kubernetes: the ServiceAccount claim `["kubernetes.io", "namespace"]`, or
  `["kubernetes.io", "serviceaccount", "name"]`.
- Entra: `["idtyp"]` = `app`. Entra sets it only on app-only tokens.

Never assert a claim a user can be given, such as a group, a role or a
scope. The assertion separates the two kinds of principal in both
directions.

**One principal is never both a human and an executor.** While the mapping is
configured, the workspace and approver branches refuse any token that either:
- (a) satisfies the service assertion, whichever issuer signed it; or
- (b) is from the executor issuer and has a `sub` the mapping names.

Both refusals are the generic `401`. The rule matters most when the mapping
names `-oidc-issuer` itself, which is allowed on purpose: Entra workload
identities share the tenant issuer with humans. Without the rule, a mapped
service principal's token sent to the workspace audience would be minted as
a reader, with any label grant its claims match. With it, that token is
refused there and works only on the executor audience. A human of a
*different* issuer whose `sub` happens to spell a mapped subject is
unaffected by (b), but is still refused by (a) if their token carries the
assertion.

The second issuer gets its own discovery and JWKS cache, separate from the
human issuer's. Both issuers may publish a key under the same `kid`. A key
from one issuer still never verifies a token the other issuer's parser
accepts. The mapping may also name `-oidc-issuer` itself; the audiences still
keep the two branches apart. The executor branch accepts the same signing
algorithms as `-oidc-allowed-algs`. Kubernetes signs with RS256, the default.

The explorer must be able to fetch the issuer's discovery document
(`<issuer>/.well-known/openid-configuration`) or the `jwks_uri` override over
TLS that it trusts:
- On a managed cluster, the public OIDC issuer URL works (EKS, GKE, AKS).
- On a self-managed cluster, the API server serves discovery only to callers
  the `system:service-account-issuer-discovery` ClusterRole is bound to, and
  its certificate is usually signed by the cluster CA. Publish the issuer's
  discovery and keys somewhere the explorer can reach and trust instead.

### What a mapped token is minted as

| | |
|---|---|
| operations | `execute` and `agent_resolve`, and nothing else. `agent_resolve` is confined to the descriptor the binding names: resolving another reference's descriptor answers `not_found`, and a list holds only the bound descriptor (`resolvableUnderBinding`, #573). The gateway resolves its own descriptor at startup. |
| service role | `action_execution`. It cannot heartbeat or register: a worker cannot truthfully assert a descriptor's liveness. |
| executor binding | the entry's `executor_ref`. The fleet narrows every execute route to it (#573). Another reference's work answers `not_found`. |
| subject, actor, client ID | `oidcexec:<issuer>#<sub>`, one identity. It is outside the human identity family (`oidc:`, `oidcid:`, `entra:`), so it is never compared with a requester or an approver. The client ID is set because attestation requires one. |
| on behalf of | none. A worker acts as itself. |
| sources, policies | the workspace source and grant policy. **No label policy**, whatever the label grants file says, so labelled evidence stays redacted on a pull. |
| provenance | the issuer, the raw `sub`, and the mapping digest |
| correlation ID | taken from `Shoal-Correlation-ID`, or minted (`oidcexec-correlation-…`). Every dispatch route needs one (#527). |

The explorer refuses a token on the executor audience when any of these is
true:
- it carries `act`, `may_act`, a claim-overage indicator (`_claim_names`,
  `_claim_sources`, `hasgroups`) or the configured `-oidc-delegation-claim`;
- it also names a workspace or approver audience (and a human token naming
  the executor audience is refused on the human branch too);
- the service assertion fails;
- its `sub` is not mapped;
- it is signed by any key other than the executor issuer's.

The human branches refuse an executor's credential as described under "The
mapping file". At startup the explorer also refuses a mapping entry whose
`executor_ref` is not among `-fleet-executor-refs`, naming the entry's
position. Every external reference must already be in that list. No
descriptor could register against such a reference, so the credential would
be bound to nothing.

Every refusal is the same generic `401`. At startup the explorer prints the
mapping digest and the number of executor credentials it holds.

### Projecting the token

Give each gateway its own ServiceAccount, so its token's `sub` is
`system:serviceaccount:<namespace>:<name>`. Turn off automounting, and project
a token on the executor audience:

```yaml
spec:
  serviceAccountName: stripe
  automountServiceAccountToken: false
  containers:
    - name: gateway
      args:
        - -dispatch-token-file=/var/run/shoal/dispatch/token
      volumeMounts:
        - name: dispatch-token
          mountPath: /var/run/shoal/dispatch
          readOnly: true
  volumes:
    - name: dispatch-token
      projected:
        sources:
          - serviceAccountToken:
              audience: shoal-executors
              expirationSeconds: 3600
              path: token
```

The kubelet refreshes the file before the token expires (at 80% of its
lifetime). The explorer adds `-oidc-clock-skew` to the token's `exp`, and no
more.

To revoke a credential, remove its entry and roll the explorer. To rotate one
onto a new ServiceAccount, add the new subject under a new reference and
re-register the descriptor; one reference never has two credentials.

### In the chart

Set `explorer.auth.oidc.executorMapping` to the document as a map. The chart:
- renders it as JSON into a ConfigMap it owns (`<release>-executors`);
- mounts it read-only at `/etc/shoal/executors` and passes
  `-oidc-executor-mapping-file`;
- puts its checksum on the pod template, so a changed mapping rolls the pod.

`validate.yaml` refuses, at render time, the shapes the explorer would refuse
at startup:
- a wrong version;
- a non-canonical or non-`https` issuer, or one with a query or a `#`;
- a missing audience, or one shared with the human audiences;
- a missing or non-positive service assertion;
- blank, duplicate or out-of-charset entries;
- a placeholder.

It also refuses an `executor_ref` that is not in `explorer.fleet.executorRefs`:
no descriptor could register against it, so the credential would be bound to
nothing.

## Routes

A route maps one fleet action to one HTTP request. An action with no route is
never performed.

```json
[
  {
    "action": "charge",
    "method": "POST",
    "path": "/v1/accounts/{account}/charges",
    "effects": ["external", "egresses-content"],
    "idempotency": "key",
    "query": ["expand"],
    "conflict": {"status": [400], "pointer": "/error/type", "equals": ["idempotency_error"]},
    "retryable": [429, 503],
    "reference": {"pointer": "/id", "pattern": "ch_[A-Za-z0-9]+"}
  },
  {
    "action": "remove-item",
    "method": "DELETE",
    "path": "/v1/items/{id}",
    "effects": ["external", "egresses-content"],
    "idempotency": "natural",
    "conflict": {"status": [404], "pointer": "/error/code", "equals": ["resource_missing"]}
  }
]
```

Decoding is strict: unknown fields, duplicate keys, trailing data and an empty
table are refused, and every refusal names the route by index and action.
Field names must be spelled exactly. `encoding/json` matches fields
case-insensitively, with Unicode folding (`ſ` folds to `s`), so without this
`{"method":"GET","METHOD":"POST"}` would decode as POST while reading as GET.

- **Methods.** POST, PUT, PATCH, DELETE. GET, HEAD and OPTIONS are refused: a
  read performs no effect.
- **Idempotency.** `key` sends the action's `ExecutorKey` in
  `-idempotency-header`; `natural` requires PUT or DELETE, the only methods
  idempotent by specification; `unprotected` sends nothing and never re-sends
  a request that may have been written.
- **Paths.** A parameter is a whole segment, `{name}`. Values are
  percent-escaped as one segment, so `a/b` is `a%2Fb`, and empty, `.` and `..`
  values are refused.
- **Effects** are derived from the target and the declaration must match:
  `{external}` for a loopback target, `{external, egresses-content}` for
  anything else. Over-declaring egress is not the safe direction — the effect
  floor refuses only understatement, so an overstated set is silently denied by
  a policy forbidding egress.
- **Conflict** is allowed on `key` and `natural` routes, and always needs
  `status`, `pointer` **and** `equals`; a status-only rule is refused. The
  commonest same-key conflict on a byte-identical retry means "the original is
  still in flight" (409 `idempotency_key_in_use` and its relatives), and it
  arrives exactly on the written → timeout → retry path. Reading it as success
  records an effect that may yet fail. **In-progress codes must be
  `retryable`, never in `equals`;** only a body value that means "this exact
  request already succeeded" belongs there. A status may be both a conflict
  status and retryable, which is how a provider that answers 409 for both
  "already done" and "still in flight" is configured: the marker is success,
  anything else under that status retries under the same key until the
  provider replays the original outcome. A conflict status whose body lacks
  the marker and that is *not* listed retryable is recorded `outcome_unknown`,
  never `target_rejected`. For a natural DELETE, a conflict on 404 needs the
  target's not-found marker in the body; a 404 without it is
  `outcome_unknown`.
- **Reference** is a target-side identifier copied into the record. Exactly
  one of `pointer` or `header`; the pattern is anchored by the gateway and must
  not match the empty string; a value over 256 bytes is dropped.

### The action's input

```json
{"path": {"account": "acct_1"}, "query": {"expand": "balance"}, "body": {"amount": 100}}
```

Closed: no other top-level field, each spelled exactly; path and query values
are strings; every template parameter is present and no undeclared one is; a
DELETE route takes no body. `Route.InputSchema()`
renders this as the action's `input_schema`, so the explorer refuses a malformed
input at enqueue rather than after a claim.

### The request is a pure function of the record

`Binder.Bind(route, input, executorKey)` produces the method, URL, headers and
body, and nothing else contributes: no `Date` header, no request ID, no nonce, a
fixed `User-Agent`. The body is `json.Compact(input.body)`, and query parameters
are sorted. A re-claim therefore sends byte-for-byte the request the first claim
sent, which is what lets a target deduplicating on key plus body treat it as
the same request, and what lets a key conflict be read as evidence the effect
happened. The credential is attached per request, outside the binder.

### Startup check

`VerifyDescriptor` compares the resolved descriptor with the table: actions and
routes one to one, each action's effects equal to its route's, each
`input_schema` equal to the route's `InputSchema()`, and each `output_schema`
equal to the canonical closed schema (`OutputSchema()`). Any mismatch fails
closed. The input-schema check matters because the explorer enforces it at
enqueue: a looser one lets work be queued and claimed — setting
`EffectPossible` — before the binder refuses it.

## Classification

`Classify` is a pure function of the route and one observed attempt.

| observed | key / natural | unprotected |
|---|---|---|
| not written (DNS, dial, TLS, egress refused) | retry | retry |
| 2xx | success | success |
| 2xx with `Idempotent-Replayed: true` | success, `replayed` (key only) | success |
| conflict status, body marker matches | success, `conflict` | refused at config |
| conflict status, no marker (or body unreadable), status listed retryable | retry, same bytes | refused at config |
| conflict status, no marker (or body unreadable), not retryable | `failed/outcome_unknown` | refused at config |
| configured retryable status | retry, same bytes, honouring `Retry-After` seconds | `failed/outcome_unknown` |
| written, then error, timeout or reset | retry, same bytes | `failed/outcome_unknown` |
| 3xx (never followed) | `failed/outcome_unknown` | `failed/outcome_unknown` |
| other 4xx/5xx | `failed/target_rejected_NNN` | same |

"Written" is set from the first moment a byte may have reached a connection,
erring towards written: a write misread as never-sent is a second effect on an
unprotected route, while the reverse is only an honest `outcome_unknown`.

`ErrorCode` is closed: `request_not_sent`, `outcome_unknown`,
`target_rejected_NNN`, `retry_exhausted`, `input_invalid`. The dispatch client
refuses to send any other. A worker that gives up retrying uses
`request_not_sent` if no attempt was ever written and `retry_exhausted`
otherwise.

Success records `{status, idempotency, reference?}` and nothing else; evidence
is always empty. Response bodies are read through an `io.LimitReader` bounded by
`-max-response-bytes`, and an oversize body is success without a reference.

## Timing

Every margin is a server-issued difference applied to a local monotonic
instant; no local wall-clock reading is compared with a server timestamp.

- **PRECHECK**, on pull-page data, before anything is claimed:
  `deadline − created_at ≤ retention` for key routes, and
  `serverNow + T + 5s < deadline`, where `serverNow` is the pull response's
  `Date` plus one second plus the local time elapsed since it arrived. A
  failing record is skipped; it cannot be recorded on the action.
- **Anchoring**, from the claim response: `leaseLocal = t₀ + (claim_lease_until
  − updated_at)` and `deadlineLocal = t₀ + (deadline − updated_at)`, with `t₀`
  the local instant the claim ID was *first* sent. Both err early.
- **Send gate**, before every attempt: not draining,
  `deadlineLocal − now ≥ T + 5s`, and `leaseLocal − now ≥ T + 5s` (or
  `≥ L/2` with `-renew`).

An agent enqueuing with a conventional 30-second request deadline creates a
30-second action, which PRECHECK skips: `Deadline` is the enqueuing request's
own context deadline.

## The worker loop

`Worker.Run` is the loop, in `worker.go`; `WorkerConfig` is what the command
builds from `Config`.

```
PULL → FILTER → PRECHECK → [ATTEST] → CLAIM → BIND → SEND → CLASSIFY → COMPLETE
                                         └──────── FENCE_LOST ────────┘
```

- **PULL** only with a free slot (`-max-in-flight`, default 4) and room
  reserved in the unrecorded log. An empty page or an error backs off,
  jittered over `[d/2, d]`, doubling from the pull interval to 30s.
- **FILTER**: the record's agent, capability, and an action with a route. The
  pull page has no server-side filter, and a claim sets `EffectPossible`.
- **PRECHECK** as above. A skipped record is logged once per version.
- **ATTEST** when the action requires attestation (`AttestationRequirements`
  reads it from the resolved descriptor) and the attestation in hand expires
  before the explorer's now `+ L + 5s`. A presentation that is still too short
  is not claimed under.
- **CLAIM** with a fresh claim ID per attempt. `repull`, 404 and 409 send the
  worker back to PULL; a 409 on an attested action also forgets the
  attestation in hand.
- **BIND** failure sends nothing: `request_not_sent` is reported, then the
  action completes `failed/input_invalid`.
- **SEND** behind the send gate, re-checked before every attempt and once
  more after the credential is read. Retries reuse the bound request (same
  key, same bytes), at most five attempts, waiting the target's
  `Retry-After` capped at 30s or a jittered backoff.
- **COMPLETE** on the claim fence, with `effected` on a failure that may have
  left on an egress route: the request body of every attempt that may have
  been written (#427).

**Renewal.** Every `L/2` from the anchor of the last claim or extension, the
worker extends — bound on the fence (#629), re-attesting first when needed —
and re-anchors on the explorer's returned end, which the explorer clamps to
the deadline; never on the lease it asked for. A lease clamped to the deadline
is not extended again. An extension whose answer is lost keeps the end already
held and is retried after the plane timeout.

**FENCE_LOST** is a refused extension or the local lease end arriving:

| when | the worker |
|---|---|
| nothing sent | sends nothing more, reports `request_not_sent`, lets the claim lapse |
| a request in flight | lets it finish, then reports `effect_observed` (a success) or `outcome_unknown` |

The same report follows a completion the explorer refused (404, 409, …). A
completion whose answer may have committed (`indeterminate`, #506) stops
renewing, waits out the lease, and then reports through the ambiguity route,
which appends at `expected_version` 0. Each fence gets at most one report.

**Shutdown** (context cancelled): stop pulling and go not-ready (`draining`).
A claim with nothing sent reports `request_not_sent` and lapses. A request in
flight finishes and completes, falling back to the report and then to the
unrecorded log. The drain is bounded by `GracePeriod(T)`. `Kill` abandons
everything, as SIGKILL would; the next instance re-claims after the lapse and
resends under the same `ExecutorKey`.

## The unrecorded log

A report the explorer refused, did not find, or could not confirm after two
attempts is never "nothing to report" (#514). It is appended to
`unrecorded.jsonl` in the unrecorded directory:

- **Entry**: action ID, fence, claim nonce, route action, method, path
  template, outcome, target, the bounded reference, the record's correlation
  (a retry must send it), the explorer's status and the client's error kind,
  first and last times, attempts. No input, filled path, body, header or
  error text.
- **Durability**: every change rewrites the log through a temporary file that
  is fsync'd and renamed, then fsyncs the directory. A torn trailing line is
  tolerated on read; any other unreadable line refuses the start.
- **Bound**: 1024 entries and 1 MiB, kept by reserving room for one report
  before each claim. When no room is left the worker stops claiming and is
  not ready (`unrecorded_log_full`).
- **Retry**: every entry is presented again at start. One the explorer now
  records — an identical replay is accepted (#542) — leaves the log; the rest
  stay.
- **Clearing**: otherwise only `UnrecordedLog.Ack` / `Worker.AckUnrecorded`,
  which the command exposes as `shoal-gateway unrecorded ack`. The ack opens
  the log, so it runs against a stopped gateway's directory.
- **Signals**: the `unrecorded` and `unrecorded_cleared` log events carry the
  entry count (the gauge, also `Worker.UnrecordedEntries`), and `readiness`
  events carry the not-ready reason.

The directory also holds `effects-gateway.lock`, flock'd for the life of the
log: a second gateway on the same directory refuses to start.

## Egress

The target client reaches one host and one port. Each connection resolves the
name once and dials the checked address as a literal, and a `Control` hook
re-checks the socket's peer, so DNS rebinding cannot swap the address between
check and connect. Every resolved address must pass, not merely one.

Always refused: `0.0.0.0/8`, `169.254.0.0/16`, `fe80::/10`, `fd00:ec2::254`,
`100.100.100.200`, `168.63.129.16`, `192.0.0.192`, multicast, reserved and
unspecified addresses, and the same addresses embedded in NAT64 (`64:ff9b::/96`)
or 6to4 form. The RFC 8215 local-use NAT64 prefix `64:ff9b:1::/48` is refused
outright, because its embedding depends on a prefix length this code cannot
know. A zone (`%eth0`) is stripped before any check: no prefix contains a
zoned address.
Refused unless `-target-allow-private`: loopback, RFC 1918, RFC 4193,
`100.64.0.0/10` and `198.18.0.0/15`.

No proxy from the environment, no cookie jar, no redirects, no compression.
The explorer client is a separate client with a separate transport, and shares
the no-proxy, no-jar and no-redirect rules.

## Completion: lost responses, and the wire as found on main

**Lost or unreadable response (permanent).** The report may have committed
after any of these first answers:

- a transport error, or a 503 with `Shoal-Commit-Outcome: indeterminate`;
- a 502 or 504 — a proxy in front of the explorer can answer either after the
  explorer processed the request;
- a 2xx whose body does not decode, or does not describe this claim's terminal
  record at exactly the reported version plus one — the route answered
  success, so something committed, and `protocol` alone would hide it.

The client resends the identical body once; the completion route's replay
branch answers it with the committed record.

**Every 503 is possibly committed (interim, until #505).** Today
`ErrExecutionAmbiguous` and `ErrActionCommitted` (a durable write whose
publication failed) reach the wire as bare 503s with no
`Shoal-Commit-Outcome` header, so a header-less 503 from `/complete` may hide a
committed write. The client therefore treats *any* 503 from `/complete` like a
lost response. `ErrRecordingUnavailable` is also a bare 503; on the dispatch
surface every audit precedes its store write, so it is in fact a clean
refusal, but nothing on the wire tells it apart and the client never reads
error-message text. (The "committed" in `MarkCommittedInteraction` is about an
interaction record, not the action.) When #505 marks the genuinely
indeterminate sentinels, a header-less 503 means a clean refusal before any
write, and the trigger narrows back to the header; the resend on a genuinely
lost response stays.

**After a possibly-committed answer, only a record is definite.** Once the
first answer was any of the above, any 503, or a #492-shaped 400/500, the
client returns the committed record or `indeterminate`, nothing else:

| resend answered | result |
|---|---|
| 2xx with the record | the record (`recorded_otherwise` rules apply) |
| transport error, any 503, 502 or 504 | `indeterminate` |
| anything else — 409, 404, 400, 500, a 2xx that is not this report's record, … | one third read through the replay branch; a record settles it, anything else is `indeterminate` |

It never returns the first attempt's status, which describes a request whose
outcome the resend was sent to learn. A 409 or 404 answering the resend is not
definite: `ErrExecutionAmbiguous` means the first write may still be in flight
when the resend reads the record, which can then see the claim at the old
version with its lease lapsed and answer 409 for a report that commits a moment
later. A #492 400/500 answering the resend is not definite either: the first
answer may have been a genuine error that committed nothing while the resend
committed and had its record discarded. With nothing possibly committed before
it, a first-attempt 409 or 404 is definite and returned as it is.

**Recorded otherwise (permanent).** The committed record is compared with the
report — state, error code, and for a success the output as a JSON value. Any
difference returns the record with a `recorded_otherwise` error: the record is
final and must not be reported again. The record's version must be exactly
the reported version plus one: the route answers 200 only for a fresh terminal
write or a replay of one, and a terminal record does not move past that.

**#492 workaround (temporary).** Two behaviours of the completion route on
main, pinned as *current* behaviour by the real-handler tests:

- a durably recorded failure is answered HTTP 500;
- a success whose output the explorer refuses is recorded as failed
  (`invalid_executor_output`) and answered HTTP 400.

Both come from the `/complete` handler discarding the committed record
`CompleteClaim` returns alongside an error (#492). Until that is fixed the
client also resends after a 400 or 500, with the same rule for a lost resend.
When #492 lands the two pins flip to 2xx and the 400/500 trigger is removed.

`not-found` from `Claim` means re-pull, never "gone": the loser of a claim race
is told not-found deliberately.

`repull` from `Claim` is every answer after which the claim may have
committed: a transport error, any 503 (with or without the indeterminate
header), or a 502 or 504 that a proxy can answer after the explorer processed
the claim. It is never reported as a definite failure; but `Claim` returns no
action, so the caller holds no claim, executes nothing, and re-pulls. A claim
that did commit lapses at its lease and reappears on the pull page; a
completion against a claim the worker does not hold is refused. The original
error, with its kind, is the cause. The bare-503 case is interim: today
`ErrActionCommitted` reaches the wire as a bare 503, and after #505 a
header-less 503 is a clean refusal and narrows back to `unavailable`.

## Logging policy

One function writes log lines, and its argument has no free-text field.

- **Allowed:** action ID, claim fence, claim nonce (the CSPRNG part of the
  claim ID), route action, method, path *template*, status, classification,
  transport-failure kind, gate refusal, dispatch-error kind, byte counts,
  durations.
- **Never:** credentials, the action's input, the filled path or query, bodies,
  headers, or anything the target said in words.

In particular a target client's `err.Error()` is never logged: Go's
`*url.Error` embeds the full request URL, and the URL carries input-derived
path parameters. A failure is logged as its kind — `dns`, `dial`,
`connection_refused`, `tls`, `timeout`, `canceled`, `reset`, `egress_refused`,
`other` — computed from the error's type and never from its text. A test pins
that a `url.Error` carrying a secret path parameter does not reach the log, and
another fails if a field is added to the log record outside the policy.

## Deviations from the slice-1 design

- **`Idempotent-Replayed` counts only on a 2xx.** The design lists "2xx or
  `Idempotent-Replayed`" as success. Providers replay the original response
  whatever it was, so a replayed 402 is a declined payment repeated back; reading
  the header alone as success would record a decline as a completed effect.
- **A conflict status whose body cannot be read is `outcome_unknown`.** The
  design does not cover it; success and `target_rejected` would each be a guess.
- **3xx is `outcome_unknown`.** Redirects are never followed, and a 303 after a
  POST commonly means "created".
- **`-renew` is refused** rather than accepted and ignored, until #430 exists.
  The renewal arithmetic is implemented and tested.
- **The send gate without renewal** requires `leaseLocal − now ≥ T + 5s`; the
  design states only the renewing form (`≥ renewAfter`).
- **Conflict rules need a body value.** The design allows "status + optional
  JSON pointer value"; a status-only rule is refused, because the commonest
  same-key conflict on a retry means "still in flight". A conflict status
  without the marker retries if listed retryable and is otherwise
  `outcome_unknown`, not `target_rejected_NNN`.
- **Field names must be spelled exactly**, in the route table and the action's
  input, and the startup check also compares `input_schema`.
- **A DELETE route takes no body**, matching its `InputSchema()`.
- **The completion resend** recovers a lost or unreadable response — transport
  error, indeterminate 503, 502, 504, or a 2xx that is not this report's
  record (permanent) — and, until #505, any 503, and until #492 a 400 or 500;
  after any of these only a record is definite, and everything else is
  `indeterminate`. Not in the design.
