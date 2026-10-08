# Holding an operation for human approval

Issue #451, slice 1. Adopted from ATPL's `on_marginal: require_ztat`; see
`docs/gateways.md`, "What comes from ATPL".

A registered action can require approval: `"requires_approval": true` on the
action in a fleet registration. Every new request for that action is then held
until a principal distinct from the requester has approved that exact request.
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
   rebuilt request must equal the stored one field for field. The approval
   moves `approved → enqueued`, and only then is the `ActionRecord` created
   (version 1, from the stored request) through the same audit and publication
   path an enqueue uses, carrying `ApprovalRequestDigest`,
   `ApprovalPolicyGeneration`, `ApproverSubject`/`Actor`/`ClientID` and
   `ApprovedAt`. The answer is `201` with `state: enqueued` and the action.

Re-requesting is idempotent at every step: it answers `pending`, `refused`,
`expired` or `enqueued`, or conflicts if anything about the request changed.
Refused and expired are answers, not errors.

`POST /api/v1/fleet/approvals/pending` lists what the caller may decide;
`POST /api/v1/fleet/approvals/{id}/status` returns one approval, with its
input, to the requester or to an eligible approver. Pending omits inputs to
keep a page within the workspace output budget.

**Crash safety.** The `approved → enqueued` write commits the decision to make
the work; the `ActionRecord` is the second write. A crash between them is
recovered by the requester re-requesting, at any later time, including after
`expires_at`. The record is built from the stored request and the stored
materialization time, so a retry builds the same work.

**An approval is a record, not a grant.** It grants the approver nothing. The
requester gets one queued record, which still runs only through its own
principal and the claim fence. Nothing reads the approval fields to authorize a
claim or an execution.

## Who may approve

All of these, checked on every decision:

- `action_approve` authorized on the request's scope, and the request's target
  still resolving at the generation it names. Failing either answers as an
  absent record does.
- No identity in common with the request: the approver's subject and actor must
  not equal the requester's subject, actor or any delegation identity, the
  agent ID, or the agent's registrant. Equality is across fields, as in
  `internal/decisionadjudication`: acting for someone, or changing only the
  client, does not establish independence from them.
- No delegation. A decision carrying `OnBehalfOf` is refused.
- The approver must **fail** `dispatch`, `invoke` and `execute` on the scope.
  Under shared scopes, which every OIDC-minted principal has, holding approve
  alone separates nothing: two principals with dispatch and approve could each
  approve the other's work. Execute is included as well as the two enqueue
  operations because an approver that may claim and perform the work it
  approved has not been separated from the effect.
- The same policy generation, both the one named in the decision and the one
  the approver's credential is under.
- Not under workspace settings. A workspace narrowing can shed the dispatch an
  approver holds and pass the check above, so the hosted binding refuses a
  decision made under one.

`auth.OperationActionApprove` (`action_approve`) is granted by
`auth.ServiceRoleActionApproval` (`action_approval`) and nothing else grants it.
It is in **no** operation list the shipped binary mints — not the three OIDC
mappings and not the `-dev-auth` principal — and
`cmd/shoal-explore-web/oidc_approve_grant_test.go` asserts that over every
`[]auth.Operation` the command defines. An approver role mapping is deferred.

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
and cannot be decided or materialized. Approval windows should be shorter than
the heartbeat interval; this is the same property queued dispatch work already
has.

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
- `approval.*` event kinds are reserved: the public publish route refuses them
  and no operation may publish them through the trusted path. No approval
  events are published in this slice.
- ATPL export refuses an approval-required action by path (`docs/atpl.md`).

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
  the identity of someone's held request, after which that request cannot
  materialize (it conflicts), and a conflict on request reveals that something
  exists at an identity. Both are the existing dispatch identity properties.
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
- **Separation is judged on one credential.** An approver that holds dispatch
  under another token is not detected; the identity rule is what prevents
  self-approval in that case.
- **ATPL plan** does not compare the flag, so a policy that omits it plans an
  approval-required live agent as unchanged; an apply that rewrites it is
  refused by the registry as a widening.

## Deferred

- Dataset export of approvals and refusals as adjudications (#401, #419). Every
  field it needs is stored on the approval record.
- Approval lifecycle events (needs #480 item 1).
- An approver role mapping for OIDC.
- Synchronous invoke of an approval-required action.
- A claim-time generation re-check (after #438/#430).
- Requester withdrawal of a held request.
- `approval: {required: true}` in ATPL policy files (#452).
