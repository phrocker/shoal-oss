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
