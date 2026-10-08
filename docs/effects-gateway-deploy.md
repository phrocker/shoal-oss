# Effects gateway: deployment

> **Not yet runnable.** There is no `cmd/shoal-gateway`, no worker loop and no
> chart entry. What exists is the core the worker will be built on, in
> `internal/effectsgateway`, merged ahead of the four prerequisites below so it
> can be reviewed on its own. Nothing in it performs an effect by itself.

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

The dispatch client has no `extend` or `ambiguity` method on purpose: a method
for a route that does not exist is one a worker could be written against and
that fails only in production.

## What exists

| file | what it is |
|---|---|
| `config.go` | flags, validation, lease arithmetic, credential sources |
| `routes.go` | the route table, the request binder, derived effects, the startup descriptor check |
| `classify.go` | the pure response classifier and the closed `ErrorCode` vocabulary |
| `timing.go` | clock anchoring, PRECHECK predicates, the send gate, the grace-period formula |
| `executorkey.go` | `ExecutorKey` decoding in both platform spellings |
| `dialer.go` | the egress-restricted target transport and the separate explorer client |
| `client.go` | the internal dispatch client: pull, claim, complete, resolve |
| `logging.go` | the one logging function, and the policy it enforces |

The dispatch client is tested against the real explorer composition — the
embedded store, the real recorder and publisher, the authenticated webapi
handler, over a socket — in
`cmd/shoal-explore-web/effects_gateway_client_test.go`. Its principal is the
enqueuer's, which is the only claimant possible before #480.

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
  records an effect that may yet fail. **In-progress codes must be configured
  `retryable`, never `conflict`;** only a body value that means "this exact
  request already succeeded" belongs in `equals`. For a natural DELETE, a
  conflict on 404 needs the target's not-found marker in the body; a target
  whose 404 carries none cannot have a replayed delete recognised, and it is
  recorded as `target_rejected_404`.
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
| configured conflict: status and body value both match | success, `conflict` | refused at config |
| conflict status, body unreadable or oversize | `failed/outcome_unknown` | refused at config |
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
  `≥ L/2` once renewal exists).

An agent enqueuing with a conventional 30-second request deadline creates a
30-second action, which PRECHECK skips: `Deadline` is the enqueuing request's
own context deadline.

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

**Lost response (permanent).** On a transport error, or a 503 with
`Shoal-Commit-Outcome: indeterminate`, the report may have committed. The
client resends the identical body once; the completion route's replay branch
answers it with the committed record. If the resend's answer is lost too, the
client returns `indeterminate` — never the first attempt's status, which
describes a request whose outcome the resend was sent to learn. A definite
answer to the resend (409, 404, …) is returned as it is.

**Recorded otherwise (permanent).** The committed record is compared with the
report — state, error code, and for a success the output as a JSON value. Any
difference returns the record with a `recorded_otherwise` error: the record is
final and must not be reported again. The version is only required to have
moved past the one reported against; it is not compared for equality.

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
  same-key conflict on a retry means "still in flight".
- **Field names must be spelled exactly**, in the route table and the action's
  input, and the startup check also compares `input_schema`.
- **A DELETE route takes no body**, matching its `InputSchema()`.
- **The completion resend** recovers a lost response (permanent) and, until
  #492, a 400 or 500; a lost resend is `indeterminate`. Not in the design.
