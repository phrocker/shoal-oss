# Holding an operation for human approval

Issue #451, slice 1. Adopted from ATPL's `on_marginal: require_ztat`; see
`docs/gateways.md`, "What comes from ATPL".

A registered action can require approval: `"requires_approval": true` on the
action in a fleet registration. Every new request for that action is then held
until a principal independent of the requester has approved that exact request.
Nothing is performed before then, across retry and restart.

## The model

**A held request is not an action.** It is an `ApprovalRecord`
(`pkg/explorer/fleet/approval.go`) stored in its own key space, `approval/`
rows in the dispatch table (`internal/explorerfleet/approval_store.go`). No
`ActionRecord` exists for it until it has been approved and the requester has
come back for it. Pull, Claim, Invoke and ExecuteClaim all start from an
`ActionRecord`, so held work cannot be pulled, claimed or performed through any
of them, and none of them needed an edit. This is the property the admission
seam already relies on: an admission never passes through the queued state
(`docs/admission-seam.md`).

A new `DispatchState` was the alternative and was refused. A "held" state falls
through Claim's checks into `applyClaim` unless every check learns about it,
and the approver's transition would land in the requester's outbox, which is
#480's mixed-identity wedge.

The lifecycle is three compare-and-set steps on the approval record:

1. **Request** (requester, `dispatch`). `POST /api/v1/fleet/approvals/request`
   with the same body an enqueue takes. The service builds exactly the
   version-1 record an enqueue would commit, under exactly the checks an
   enqueue makes, and stores it as the request with its `request_digest` and
   the `policy_generation` in force. The request is decidable until
   `expires_at = min(now + window, deadline)`; the window defaults to one hour.
   The answer is `202` with `state: pending`.
2. **Decide** (approver, `action_approve`).
   `POST /api/v1/fleet/approvals/{id}/decide` with `verdict` (`approve` or
   `refuse`) and the `request_digest` and `policy_generation` the approver
   reviewed. Expiry is checked first and written as `expired`. A decision
   identical to one already made replays; any other decision on a decided
   request conflicts.
3. **Materialize** (requester re-requesting). The same request again. The
   rebuilt request must equal the stored one field for field. If something
   other than this approval's work already occupies the identity, the request
   is refused with a conflict and stays `approved`. Otherwise the approval
   moves `approved → enqueued`, and only then is the `ActionRecord` created
   (version 1, from the stored request) through the same audit and publication
   path an enqueue uses, carrying `ApprovalRequestDigest`,
   `ApprovalPolicyGeneration`, `ApproverSubject`/`Actor`/`ClientID` and
   `ApprovedAt`. The answer is `201` with `state: enqueued` and the action.

Re-requesting is idempotent at every step: it answers `pending`, `refused`,
`expired` or `enqueued`, or conflicts if anything about the request changed.
Refused and expired are answers, not errors. Concurrent re-requests of one
approved request all receive the same enqueued action; exactly one
`ActionRecord` is written. A materialization that collides with a concurrent
one still landing is retried internally with a short jittered back-off, because
the durable store reports that case as a conflict or a transient not-found
rather than as the fleet sentinel. Each attempt judges the window at its own
time, so a retried `approved → enqueued` commit cannot land after
`expires_at`.

`POST /api/v1/fleet/approvals/pending` lists what the caller may decide;
`POST /api/v1/fleet/approvals/{id}/status` returns one approval, with its
input, to the requester or to an eligible approver. Pending omits inputs to
keep a page within the workspace output budget.

### Status reports the effective state

Expiry is written lazily, by the next request or decision, and a request whose
target has moved can never progress although its row is unchanged. Status
therefore reports three things: `state`, what the request effectively is now;
`stored_state`, the row; and `condition`, why they differ. Nothing is written.

| `state` | `condition` | meaning |
| --- | --- | --- |
| `pending` / `approved` | — | live |
| `expired` | `window_closed` | stored pending or approved, window closed, expiry not yet written |
| `enqueued` | — | the action exists; its own status says where it is |
| `enqueued` | `enqueued_without_action` | the second write was lost; the next re-request writes it |
| `unresolvable` | `identity_taken` | committed to become work, but another route's action holds the identity |
| `unresolvable` | `target_moved` | the agent's generation moved, or the agent is gone |
| `unresolvable` | `policy_generation_moved` | the policy generation in force (read from the generation authority, not the caller's token) moved since the request |
| `unresolvable` | `approver_mapping_moved` | stored approved, but the operator approver mapping in force is not the one the approval was given under; it can never materialize |
| `unresolvable` | `deadline_passed` | the action deadline passed; it can no longer be re-requested |
| `refused` / `expired` | — | final |

An action at the identity counts as this approval's work only if it is its
materialization: the same request, digest, approver and decision time — the
predicate a replay applies. `unresolvable` is never stored. An approver sees a request through Status and
Pending only while its target still resolves at the request's generation; the
requester always sees its own.

### Recovery after a crash

The `approved → enqueued` write commits the decision to make the work; the
`ActionRecord` is the second write. A crash between them is recovered by the
requester re-requesting — including after `expires_at` — **but only while the
request can still be re-requested**: before the action deadline, while the
agent is at the generation the request names, and while the policy generation
holds. A re-request after the deadline is refused before it reaches the
approval (`deadline_exceeded`), and one after the agent's generation moved
cannot resolve its target (`not_found`). The approval then stays `enqueued`
with no action, and Status reports it as `unresolvable` with the reason. No
work is created in either case; the requester must make a new request under
the new generation, which needs a new approval.

The record is built from the stored request and the stored materialization
time, so a retry builds the same work.

**An approval is a record, not a grant.** It grants the approver nothing. The
requester gets one queued record, which still runs only through its own
principal and the claim fence. Nothing reads the approval fields to authorize a
claim or an execution.

## Who may approve

All of these, checked on every approver path — decide, pending, and status
asked as an approver:

- Not under a narrowing the host applies, such as workspace settings. The host
  supplies the predicate (`ApprovalConfig.Narrowed`; `cmd/shoal-explore-web`
  answers from `webapi.EffectiveWorkspaceSettings`). A narrowing can only
  remove authority, so a principal holding approve and dispatch could narrow to
  approve alone and pass the next rule; separation is judged on the authority
  it actually holds. Pending refuses a narrowed caller outright (`401`). Decide
  and Status-as-approver run every other rule first and return the narrowing
  refusal only when the caller would otherwise be eligible on that record;
  any other failure answers `404` exactly as a missing ID does, so a narrowed
  caller cannot use the refusal to learn which approval IDs exist.
- `action_approve` authorized on the request's scope, and the request's target
  still resolving at the generation it names. Failing either answers as an
  absent record does.
- No identity in common with the request. The approver's subject and actor
  must not equal any of: the requester's subject, actor or delegation
  identities; the agent's ID; and, for the agent and every ancestor in its
  delegation chain, read from the registry, that agent's ID and its
  registrant's subject and actor. A delegated agent acts with authority its
  parent granted, so the parent agent and the parent's registrant are parties
  to the work. Equality is across fields, as in
  `internal/decisionadjudication`: acting for someone does not establish
  independence from them.

  For an OIDC approver this is **one comparison, not two**. A mapped approver
  is minted with actor = subject (`oidc:<iss>#<sub>`, below), so the subject
  and actor checks test the same identity, and the separation margin is
  exactly "the approver's subject must not overlap anyone involved". That is
  not a weakening: before the mapping every OIDC token's actor was the
  literal `shoal-explore-web-oidc`, a constant no principal controlled — and
  because the requester's actor is involved, that constant made every OIDC
  approver overlap every OIDC requester. No OIDC approver could approve any
  OIDC request; the flow failed closed, under a refusal ("approver is not
  independent of the request") that misdescribed the cause.
- No delegation. A decision carrying `OnBehalfOf` is refused.
- The approver must **fail** `dispatch`, `invoke` and `execute` on the scope.
  Under shared scopes, which every OIDC-minted principal has, holding approve
  alone separates nothing: two principals with dispatch and approve could each
  approve the other's work. Execute is included because an approver that may
  claim and perform the work it approved has not been separated from the
  effect.
- The same policy generation: the one named in the decision, the one the
  approver's credential is under, and the one in force according to the
  generation authority (`ApprovalConfig.Generations`). A token minted before
  the policy moved still carries the old generation.

What these rules deliberately do not do:

- **Client ID is not compared.** Independence is between people and agents,
  not between client applications; two people using one console are
  independent, and one person using two consoles is caught by subject and
  actor. This matches `internal/decisionadjudication`.
- **Separation is per credential.** An approver is judged on the credential it
  presents. One that holds dispatch under another token is not detected; the
  identity rule is what prevents it approving its own request in that case.

`auth.OperationActionApprove` (`action_approve`) is granted by
`auth.ServiceRoleActionApproval` (`action_approval`) and, in the shipped
binary, by exactly one list: `oidcApproverOperations`, minted only through the
approver mapping below. It is in none of the three workspace OIDC mappings,
the unmapped fallback, or the `-dev-auth` principal. Two tests hold that:
`oidc_approve_grant_test.go` parses every package-level `[]auth.Operation` in
the command and requires approve in the approver list alone (and that list to
be approve alone), and `oidc_approve_mint_test.go` mints a decision through
each authenticator — with and without an approver mapping in force, including
workspace tokens that carry the approver claim and value — and asks it whether
it authorizes approve.

### The OIDC approver mapping

`-oidc-approver-mapping-file` (environment `SHOAL_OIDC_APPROVER_MAPPING_FILE`)
names an operator file mapping OIDC humans to the approver role. **No file
means no approvers.** It is never in ATPL: ATPL is written by registrants,
and letting a registrant name who approves its own agents' work is the
conflict of interest #419 forbids.

```json
{
  "version": "shoal.approvers/v1",
  "issuer": "https://idp.example.com/realms/shoal",
  "audience": "shoal-approvals",
  "client_ids": ["shoal-console"],
  "claim": ["realm_access", "roles"],
  "values": ["shoal-approvers"],
  "max_values": 64,
  "human_assertion": {"claim": ["idtyp"], "absent": true}
}
```

Startup refuses the file, and the server does not start, when: a field is
unknown, a key is repeated, or anything follows the object; `version` is not
`shoal.approvers/v1`; `issuer` is not byte-equal to the trimmed
`-oidc-issuer` (no trailing-slash, case or whitespace leniency); `audience` is
empty or is one of the workspace audiences; `client_ids`, `claim` or `values`
is empty; any string has leading or trailing whitespace or a control
character; `claim` is not a list of path segments (a dotted string is refused,
and a segment containing a dot names a key containing a dot, never a path);
`max_values` is outside 1–1024; `human_assertion` is missing or gives other
than exactly one of `absent: true` and `equals`. The mapping also requires the
default OIDC identity (subject claim `sub`, identities `oidc:<iss>#<sub>`) and
refuses the legacy Entra identity mode, in which the same human would carry an
`entra:` identity as a requester and an `oidc:` one as an approver and could
approve their own request.

**Minting is decided by audience and fails closed.** A token on a workspace
audience is minted by the workspace mappings, which never grant approve. A
token on the approver audience is minted as an approver or denied — there is
no fallback to reader. A token carrying both is denied. On the approver
audience every one of these is required:

- `azp` is one of `client_ids`, and `sub != azp` (a client acting as itself is
  a client-credentials grant, whatever groups it holds);
- none of `act`, `may_act`, `_claim_names`, `_claim_sources`, `hasgroups`, or
  the configured `-oidc-delegation-claim` is present (delegation, or a group
  list the issuer truncated);
- the human assertion holds;
- the value at `claim` is a string or an array of at most `max_values`
  strings, and one of them equals a configured value **byte for byte** — no
  trimming, case folding or Unicode normalization;
- where the token also carries the workspace authorization claim, none of its
  values maps to `-oidc-fleet-values` (the human holds dispatch).

Every failure is the same generic authentication denial. The decision grants
`action_approve` and nothing else, with subject = actor = `oidc:<iss>#<sub>`,
client `oidc:<iss>#<azp>`, no `OnBehalfOf`, the workspace policy generation,
and an `auth.GrantProvenance` of issuer, subject, claim path, matched value
and the mapping digest. The provenance is appended to the authorization
fingerprint only when present, so every existing fingerprint is unchanged.

**The mapping digest is pinned, not the generation.** The workspace policy
generation stays fixed configuration: deriving it from a hash of the mapping
could move it backwards. Instead the digest of the mapping in force
(canonical; reordering a set does not move it) is `ApprovalConfig.ApproverMapping`:

- `decide` requires the approver decision's mapping digest to equal the one in
  force (both zero when no mapping is configured; a decision without
  provenance is refused while one is), and stores it on the record as
  `ApproverMappingDigest` beside `ApproverProvenance`;
- the requester's re-request checks it again before `approved → enqueued`. A
  changed mapping refuses (`409`, `ErrApproverMappingMoved`), the row stays
  approved, and Status reports `unresolvable` / `approver_mapping_moved`;
- a pending request is unaffected until it is decided, and is then decided
  under the mapping in force.

**Audit.** Each approval transition's audit carries the acting principal's
provenance; the trusted session commits to it (its query digest covers it) and
the durable approval record holds the values. No token byte is recorded
anywhere.

**Disclosure.** The mapping is never served: the browser login configuration
is identical with and without it, and no approval response carries the
provenance or the digest. Non-approvers keep their `404`s. The requester sees
who approved their own request (`approver`, `approver_actor`) — that is
accountability for that request, and it is intended.

**Not detected.** A human who holds the fleet mapping through a group their
approver token does not carry is not detected; separation is per credential,
and the identity rule is what stops them approving their own request. Keep
the approver and fleet groups disjoint.

## What enqueue, invoke and admission do

- **Enqueue and invoke** of an approval-required action return
  `ErrApprovalRequired`: HTTP `409`, code `conflict`. Conflict, deliberately:
  the caller is authorized and the request is well formed, so neither `400` nor
  `401` is true, and a retry of the same request will never succeed, so `503`
  would mislead. The request conflicts with the action's registered state and
  the remedy is a different route. Clients should branch on the `409` from these
  two routes and resubmit through the approval route. A refused enqueue is not
  audited: nothing is written, no action exists for an audit to name, and the
  approval route, which is audited at every transition, is where this work goes.
- **Invoke on an identity an approval already materialized** reaches enqueue's
  replay branch, which returns the existing record before the guard, and Invoke
  then claims and runs it in process if an in-process executor is bound. That
  is the one approved request, run once, by its requester under the claim
  fence — the same work any worker would claim — and not a way round the
  approval. It is the case "synchronous invoke of an approval-required action"
  in Deferred refers to: it works by accident of the replay branch rather than
  by design, and slice 1 neither relies on it nor forbids it.
- **The pre-call admission seam (Path B)** denies an approval-required action
  durably with no reason, like any other denial. Holding is not available
  there: the caller is waiting on the answer with the payload in hand.

## Work that already exists when the flag is registered

`RequiresApproval` gates new work. It does not reach a record already queued,
and the enqueue guard sits after the replay branch for the same reason: a
replay creates nothing, and refusing it would only make a caller's retry fail
where its first attempt succeeded.

What an operator should expect when registering the flag on a live agent:

- Registering it is a new generation of the agent. A queued record names the
  generation it was enqueued against, so every **not-yet-claimed**
  old-generation record stops resolving: Pull omits it and Claim answers not
  found. It is not deleted and does not run.
- A record **already claimed** before the flag is outside this change: its
  claimant still holds the claim it was granted, and completion re-resolves the
  binding, so it too stops resolving once the generation moves. (A claim-time
  generation re-check is deferred to #438/#430.)
- An enqueue retry of an old record replays it as it is; a new enqueue at the
  new generation is refused with `409`.
- An admission granted or denied before the flag replays from its record.
- The flag cannot be removed by re-registration or by a delegate: dropping it
  widens authority and is refused like any other widening. Removing it means
  revoking the agent and registering a new one.

The same generation rule applies to heartbeats: a heartbeat moves the agent's
generation, so a request held across a heartbeat no longer matches its target
and becomes `unresolvable`. Approval windows should be shorter than the
heartbeat interval; this is the same property queued dispatch work already has.

## Storage and compatibility

- The registry mutation digest appends the flag only when it is true, so every
  existing digest, and every heartbeat or revoke retry spanning the upgrade, is
  unchanged. The JSON form omits the key when false.
- The descriptor codec writes version 4, with one flag byte per action, only
  for a descriptor in which some action requires approval. Every other
  descriptor is still version 3, byte for byte. A previous build refuses to
  decode version 4 rather than reading the action without its flag.
- The new `ActionRecord` fields are additive; gob decodes older records with
  them absent, and `Validate` requires them all or none.
- `ApprovalRecord.ApproverMappingDigest` and `ApproverProvenance` are
  additive gob fields: a record a previous build wrote decodes with both
  zero, and a previous build reads a record that has them by skipping them.
  `Validate` refuses either on an undecided record and a digest that is not
  its provenance's.
- `approval.*` event kinds are reserved: the public publish route refuses them
  and no operation may publish them through the trusted path. No approval
  events are published in this slice.
- ATPL (#452): a policy file declares the requirement per action as
  `"approval": {"required": true}`, which compiles to `requires_approval`.
  Inheritance carries it and delegation cannot drop it; plan treats adding it
  as a narrowing and omitting a live one as a refused widening; export writes
  it, and the export recompiles to the same policy digest. A policy without
  approval keeps the digest it had before (`docs/atpl.md`). The earlier
  `refused-approval` plan kind and the export refusal are gone.

## Clock skew between replicas

A transition written on a replica whose clock is behind the one that wrote the
row clamps its timestamps forward to the row's latest (`RequestedAt`,
`UpdatedAt`, `DecidedAt`, `MaterializedAt`) instead of failing validation. The
clamp moves time forward by at most the skew and never past `expires_at`,
because every transition has already checked the window.

The materialized action's `UpdatedAt` is the clamped materialization time. If
the materializing replica is behind the deciding one, that time is ahead of
its clock, and the lifecycle publisher, which bounds a publication's retry
window from `UpdatedAt` against the local clock, refuses to publish: the
caller is told the action committed and needs reconciliation, and a
re-request once the clock has caught up replays the one record. Every dispatch
transition reconciled on a lagging replica has the same property.

## Residuals

- **Pre-existing: an admission denial cannot be published by the hosted
  publisher.** The denial commits a cancelled record whose authorized
  operations are `[invoke]`, and the hosted publisher permits `action.canceled`
  only under `dispatch`. The first answer to a denied admission is therefore a
  `503` "requires reconciliation" over a denial that did commit; the replay
  answers `denied`. This affects every admission denial on main, including the
  effect-ceiling one, and is in #480's area. It is safe — nothing is granted —
  and it is pinned by the acceptance tests so a fix flips them visibly.
- **Approval identities are caller-chosen**, as dispatch action identities
  are. A principal with dispatch on the scope can enqueue an ordinary action at
  the identity of someone's held request. Found before materialization, the
  approval stays `approved` and the re-request conflicts; landing between that
  check and the commit leaves the approval `enqueued` under someone else's
  action, which Status reports as `unresolvable` / `identity_taken`. A conflict on request also reveals that something exists at
  an identity. Both are the existing dispatch identity properties.
- **An in-scope approver can learn that an approval ID exists.** Without a
  narrowing, a caller that holds `action_approve` on a request's scope but is
  ineligible to decide it — not independent of it, holding dispatch, invoke or
  execute, or acting on someone's behalf — is told why (`401`), where a
  missing ID or a request outside its scope answers `404`. This is kept
  deliberately: such a caller already holds approve on that scope, and the
  specific refusal is what lets it see that it is the wrong approver. Only a
  narrowed caller is concealed, because the narrowing is what it could vary to
  probe.
- **Response timing is an existence oracle for narrowed callers.** The error
  bodies for a missing ID, a request in another scope and a request the caller
  is not independent of are byte-identical, but the work behind them is not:
  measured at roughly 65µs, 650µs and 1.5ms respectively, because each answer
  is reached after a different number of store and registry reads. Closing it
  would need constant-time answers, padded to the slowest path; that is not
  done.
- **A generation move inside the last window.** The in-force policy
  generation is checked immediately before the `approved → enqueued` commit,
  and the agent generation is resolved by the re-request just before that. A
  generation that moves between those checks and the store's compare-and-set
  is not seen. This is the same uncoordinated window a plain enqueue has: the
  registry and the policy authority are not part of the dispatch store's
  transaction.
- **Stranded rows.** A request whose target generation or policy generation
  moves, or whose deadline passes, stays in its stored state forever; nothing
  sweeps it. Status reports it as `unresolvable`, Pending omits it, and neither
  decide nor re-request can move it. A sweeper is deferred.
- **Audits record attempts.** Approval audits precede their write, and each
  attempt has its own session, keyed on the acting principal and request ID, so
  a lost compare-and-set or a crash cannot wedge the record. An audit whose
  write lost a race describes a decision that was attempted, not made; the
  approval row is the outcome.
- **The materialization audit** uses the dispatch action recorder's session
  identity (phase, action, version). A crash after that audit and before the
  action write is recovered by a retry under the same credential; under a
  different one the audit session conflicts. This is the property every
  dispatch transition already has.

## Deferred

- Dataset export of approvals and refusals as adjudications (#401, #419). Every
  field it needs is stored on the approval record.
- Approval lifecycle events (needs #480 item 1).
- Approver pools per action or descriptor (a later ATPL version may only
  reference an operator-defined pool), a second approver-only issuer, and
  quorum.
- Synchronous invoke of an approval-required action, by design rather than
  through the replay branch.
- A claim-time generation re-check (after #438/#430).
- Requester withdrawal of a held request, and a sweeper for stranded rows.
