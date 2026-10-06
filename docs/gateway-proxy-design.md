# Gateway proxy: reference design

How Shoal governs what an agent is permitted to *do*, as opposed to what a model
is permitted to be told. The second is `cmd/shoal-llm-proxy` (#390,
`docs/llm-proxy-deploy.md`). This is the design for the first (#391).

Nothing here is implemented yet. It exists because two decisions in #391 were
underdetermined in a way that would have produced the wrong deployment, and one
of its scope items asks for a mechanism that does not exist.

**Several of its claims turned out to be worse than underdetermined.** Two
adversarial passes against the dispatch code found **four blockers**: the
topology this document draws is the one the authorization predicate forbids, the
descriptor heartbeat invalidates the claims the design depends on, no shipped
executor may perform an external effect at all, and `ClaimID` uniqueness is
load-bearing and was unspecified in a way that produces undetectable duplicate
effects. All four are read first, because everything after them is contingent on
how they are resolved.

- [Blocked: a worker cannot learn what to do](#blocked-a-worker-cannot-learn-what-to-do)
- [Blocked: the claimant must be the enqueuer](#blocked-the-claimant-must-be-the-enqueuer)
- [The second blocker: every heartbeat invalidates every claim](#the-second-blocker-every-heartbeat-invalidates-every-claim)
- [The third blocker: no executor may perform an external effect today](#the-third-blocker-no-executor-may-perform-an-external-effect-today)
- [The fourth blocker: `ClaimID` uniqueness is load-bearing and unspecified](#the-fourth-blocker-claimid-uniqueness-is-load-bearing-and-unspecified)
- [What a worker actually echoes: not the fence](#what-a-worker-actually-echoes-not-the-fence)
- [Requirements this design places on #430](#requirements-this-design-places-on-430)
- [The thing that makes this different](#the-thing-that-makes-this-different)
- [Two paths, and why both](#two-paths-and-why-both)
- [What the fence does not protect](#what-the-fence-does-not-protect)
- [The fence, and what it costs](#the-fence-and-what-it-costs)
- [Cancellation, which this design forgot](#cancellation-which-this-design-forgot)
- [What an operator cannot do](#what-an-operator-cannot-do)
- [Lease arithmetic](#lease-arithmetic)
- [Reporting an ambiguity when the fence is gone](#reporting-an-ambiguity-when-the-fence-is-gone)
- [The unit of deployment is the operational surface](#the-unit-of-deployment-is-the-operational-surface)
- [What a hostile target can do to the worker](#what-a-hostile-target-can-do-to-the-worker)
- [What this pod must not have](#what-this-pod-must-not-have)
- [Kubernetes reference](#kubernetes-reference)
- [Failure modes an operator will see](#failure-modes-an-operator-will-see)
- [Prerequisites](#prerequisites)
- [Specified nowhere, and needed on day one](#specified-nowhere-and-needed-on-day-one)
- [Open decisions](#open-decisions)

## Blocked: a worker cannot learn what to do

**The action's input is never sent to a remote worker.** This is more
fundamental than the four blockers below and was found last, which is its own
lesson about reviewing a design by reading its prose.

`fleetActionWire` (`pkg/explorer/webapi/fleet_dispatch.go:356-378`) carries
`id`, `version`, `agent_id`, `capability`, `deadline`, `claim_id`,
`claim_lease_until`, `output`, `error_code` and the evidence snapshot fields.
It does **not** carry `input`. `encodeFleetAction` (`:486-526`) never emits it,
and every one of the seven responses that returns an action goes through that
encoder. `Input` appears on the *enqueue* wire only.

So a gateway can pull an action, claim it under a fence, and complete it without
ever learning which request to make. Three statements in this document assume
otherwise — "the worker reads an action's input, not the corpus", "`record.Input`
is immutable, but the *request* is not the input", and "the registered `Action`
carries the declared effect set and an input schema".

The fix is in the explorer, on `fleetActionWire`, and it belongs in the
prerequisites table alongside the rest.

## Blocked: the claimant must be the enqueuer

**Path A as drawn cannot work against the current dispatch surface, and this is
the first thing to read.**

`Pull` filters every candidate through `sameActionPrincipal`
(`pkg/explorer/fleet/dispatch_service.go:1209`), and `Claim` and `CompleteClaim`
reach the store only through `authorizedCurrent`, which applies the same
predicate (`:1329`). That predicate requires the caller's decision to match the
record on **all four** of `Subject`, `Actor`, `ClientID` and the full
`OnBehalfOf` chain, by equality (`:1361`).

So the topology this document draws —

```
agent ──enqueue──▶ [dispatch queue] ──▶ gateway worker
```

— is precisely the one the code forbids. An agent enqueues with its own token;
the record stores the agent's principal. The gateway pulls with its own token;
every record fails the predicate and is **silently skipped**. The gateway
receives an empty page, forever, with no error and nothing in the explorer's logs
to explain it. An operator sees a ready worker, a growing queue, and zero
effects.

Delegation does not rescue it. `OnBehalfOf` is compared by whole-chain equality,
not as a prefix or a subset, so a worker acting on behalf of the enqueuing agent
has a strictly different chain and is refused for that reason. `OperationDelegate`
exists for narrowing a *descriptor*, which is a different question.

Three ways out, none free:

1. **The gateway enqueues its own work.** Then the agent is not the enqueuer, the
   diagram above is wrong, and `Subject`/`Actor` on the record identify the
   gateway rather than whoever wanted the effect — losing exactly the attribution
   the evidence record exists to carry.
2. **The gateway presents a credential that mints the enqueuer's decision
   tuple.** This is impersonation: one gateway would hold the identity of every
   agent that can enqueue to it. It also collapses the per-surface isolation this
   document argues for, since two gateways sharing a principal can claim each
   other's actions.
3. **Relax the predicate for claiming, deliberately and narrowly.** A third
   prerequisite alongside #430: a way for a registered executor to claim work
   enqueued by another principal without inheriting that principal's identity —
   for instance by binding the claim to the record's `AgentID` and the
   executor's own registration rather than to the enqueuer's decision.

Option 3 is the only one that keeps both the attribution and the isolation, and
it is a change to the authorization model rather than a new route. It needs the
same care `sameActionPrincipal` already shows: that predicate is applied at four
separate points and normalises absent and foreign to one `ObjectNotFound`, which
is what stops it being an existence oracle. Loosening it is the sort of change
that reintroduces one.

Until this is settled the rest of this document describes a system that cannot be
built, and the deployment model below is contingent on which option is chosen —
options 1 and 2 change what a gateway's identity even means.

## The second blocker: every heartbeat invalidates every claim

`Heartbeat` sets `next.Generation = request.ExpectedGeneration + 1`
(`pkg/explorer/fleet/service.go:275`) — **every** descriptor lease renewal moves
the generation. A queued record pins `AgentGeneration` at enqueue
(`pkg/explorer/fleet/dispatch_service.go:181`), and `resolveActionBinding`
returns `ObjectNotFound` when `descriptor.Generation != generation` (`:1431`).

The codebase states this itself, in `pkg/explorer/fleet/admission.go:457`:
"blind to any record whose generation has moved, **which Heartbeat does on every
lease renewal**."

So a gateway that heartbeats for liveness — which this document called
`claimLease` "the heartbeat interval", conflating two unrelated heartbeats —
invalidates its own in-flight claim and its own queued actions. The worker
performs the effect and then `complete` returns **not-found**, which is the most
misleading answer the API can give: not `ErrClaimLost`, not an ambiguity, but a
claim that the action does not exist. A worker following this document's ordering
would reasonably conclude it had the wrong ID.

The same mechanism makes a **rolling restart** strand an in-flight effect: a new
replica registers, the generation moves, and the draining replica finishes its
operation and cannot report it. The grace period this document sizes so carefully
is correctly sized and completely ineffective.

It also means two replicas cannot share one descriptor identity, because
`Heartbeat` and `Register` are compare-and-swap on `(RegistrationKey,
ExpectedGeneration)`: the replicas race, and each win invalidates the other's
live claims. "`replicas: 2` is safe because of the fence" was a claim about the
claim, and the descriptor is a second shared mutable object this document did not
consider.

Resolving this needs a decision recorded here, not discovered later: either the
gateway never heartbeats and registers once with a long lease out-of-band
(accepting that its descriptor liveness signal then means nothing), or the
generation pin has to tolerate a descriptor that has only been heartbeated —
which is a semantic change to what the pin is for.

## The third blocker: no executor may perform an external effect today

A gateway descriptor cannot be registered against the shipped explorer.

Registration resolves the executor reference and checks the declared effects
against that executor's ceiling (`pkg/explorer/fleet/service.go:720`).
`executorCeiling` returns `nil` for any executor that does not implement
`EffectBounded` (`pkg/explorer/fleet/model.go:492-497`), and `exceeds(nil)` is
true for any non-empty declaration — so an unbound reference permits nothing at
all. The only bound executor in the binary is `AskExecutor`, whose ceiling is
`{reads-corpus}` or `{reads-corpus, egresses-content}` and which **deliberately**
excludes external mutation; its comment says declaring it "would raise the
ceiling enough for genuinely external actions to resolve here"
(`pkg/explorer/webapi/fleet_executor.go:217-219`). Its floor equals its ceiling,
so `{external}` also fails the floor check.

So the effect ceiling this document cites as the mechanism that makes the design
safe currently refuses the entire gateway, at registration and again at
resolution. The missing piece is a host-side executor binding that declares an
external-mutation ceiling and no floor — which belongs in the explorer, not in
the gateway, and is invisible from this document's own vantage point.

## The fourth blocker: `ClaimID` uniqueness is load-bearing and unspecified

This one is the most dangerous of the four, because it produces a duplicate
effect that is neither prevented nor detectable, in steady-state operation, with
no partition and no outage.

`Claim` has a replay branch that returns success to any caller presenting the
same `ClaimID` and the same `Lease` against a record one version ahead
(`pkg/explorer/fleet/dispatch_service.go:267-278`). `completeClaim` has the
mirror, returning the committed record and a 200
(`pkg/explorer/fleet/dispatch_service.go:611-621`). Nothing requires `ClaimID` to
be unique: `validateOpaque` enforces only non-empty and at most
`MaxActionIDBytes` (`:240`).

Two replicas pull the same record at version V and both claim with
`ExpectedVersion=V`. A wins. B's request matches the replay branch — same
`ClaimID`, same `ClaimLease`, version is `V+1` — and **B is told it holds the
claim.** Both perform the effect. Both then report; the second is matched as "my
work already committed" and gets a 200 with A's record. One completion on the
record, two effects, both workers told they succeeded, nothing rejected.

The implementation knows the risk and says so in its own test
(`pkg/explorer/fleet/dispatch_completion_test.go:482`): recognising a terminal
record at the expected version as "my work already committed" is only safe if it
really was this reporter's work. That test passes because its stranger uses a
different `ClaimID`.

Two replicas of one Deployment reading one values file is exactly the
configuration that produces a shared `ClaimID`, and an earlier draft of this
document pushed an implementer toward it: it said the idempotency key must never
derive from the claim, which reads as "the claim is not a per-attempt identity".

So, normatively: **`ClaimID` must be freshly generated per worker per claim
attempt**, from a CSPRNG, and must encode pod identity so the record can answer
which replica performed an effect — `hostname ‖ nonce`. Unpredictability does **not** close the sibling problem, which an earlier draft
claimed it would. `encodeFleetAction` emits `claim_id` and `version` on every
action response (`pkg/explorer/webapi/fleet_dispatch.go:521`), and `Status`
authorizes through `sameActionPrincipal` — which every replica satisfies. So a
sibling does not need to guess: it reads the live `claim_id` off `Status` and can
forge a completion. A CSPRNG nonce publishes its entropy to exactly the parties it
was meant to exclude.

So the fence protects against a different principal and not against a sibling,
and nothing a worker chooses can fix that. Either completion must bind to
something the record does not publish, or co-replicas must not share a principal
— which is the first blocker again, from a third direction.

## What a worker actually echoes: not the fence

This document said "the fence is `(ClaimID, ClaimFence)`" and "report under the
same fence". `ClaimFence` is returned to the worker
(`pkg/explorer/webapi/fleet_dispatch.go:373`) and **there is no field to send it
back.** The completion wire carries `context`, `expected_version`, `claim_id`,
output, error and evidence (`pkg/explorer/webapi/fleet_dispatch.go:317-327`), and
`completeClaim` validates `(Version, ClaimID, state)`
(`pkg/explorer/fleet/dispatch_service.go:622-627`). `ClaimFence` is used only for
the store's internal compare-and-swap.

What a worker echoes is **`(claim_id, expected_version)`**, where
`expected_version` is the version of the *claimed* record — what `Claim`
returned, not what `Pull` offered. Those differ by one, and guessing the pulled
version makes every completion fail with `ErrClaimLost` after the effect has
happened, which is the exact post-effect ambiguity this design exists to avoid.

`ClaimFence` remains the right predicate for an operator asking "was this
claimed more than once"; it is not part of the worker's protocol.

## Requirements this design places on #430

Renewal is a prerequisite, and these are constraints on it that only this
document is positioned to state:

- **Renewal must not advance `ClaimFence`.** `applyClaim` is the only writer of
  the claimed transition and it increments the fence unconditionally
  (`pkg/explorer/fleet/dispatch_service.go:339` onward). If renewal reuses it —
  the architecturally obvious implementation — then every renewal breaks the
  identity an operator uses to count re-claims, and `ClaimFence > 1` stops
  meaning "claimed twice".
- **Either renewal must not advance `Version`, or it must return the new one.**
  `completeClaim` refuses on a version mismatch. If renewal is a versioned
  mutation like every other transition in that file, a worker's
  `expected_version` — captured at claim — is stale by exactly the number of
  renewals, and `complete` fails with `ErrClaimLost` **because of the worker's
  own renewals.** Every long operation would then report a false ambiguity.
- **A refused renewal must be distinguishable from a transport failure**, or a
  worker cannot tell "my claim is gone, treat this as ambiguous" from "retry".

## The thing that makes this different

The LLM proxy governs a disclosure. If it refuses, nothing left the host, and if
it allows, the worst case is that content reached a provider. Withholding is
meaningful right up to the moment of the call.

A gateway governs an effect. Under #381's taxonomy these operations are
`EffectMutatesExternal`: they change something outside Shoal's evidence record,
and once performed they cannot be withheld, compensated or recalled. Shoal does
not undo them and will not pretend to.

Two consequences shape everything below.

**At-most-once is the requirement, not at-least-once.** A duplicate disclosure
is the same disclosure. A duplicate payment is a second payment. Anything in
front of an irreversible effect that can be retried by a client is wrong by
construction.

Requirement, not guarantee — and the distinction is the sharpest thing in this
document. See [What the fence does not
protect](#what-the-fence-does-not-protect).

**The declaration cannot come from the request.** `EffectMutatesExternal` is a
property of the target, not of the code — the same statement
`pkg/explorer/fleet/model.go` makes about egress, which #390 got wrong in the
direction that looks safe. An arbitrary `POST` tells you nothing about whether
it mutates anything. So the effect set has to be declared in advance, per
registered action, and validated before anything external happens.

## Two paths, and why both

#391's opening describes an inline proxy — "a proxy in front of an operational
surface... that admits each operation against Shoal before performing it" —
while its scope and every one of its fence-related acceptance criteria describe
pull, claim, perform and report on the dispatch queue. Those are different
architectures. Both are wanted, for different operations, and conflating them is
how someone ends up building a Service that agents point at and discovering
later that it cannot promise at-most-once.

### Path A — fenced, queue-driven (the default)

```
agent ──enqueue──▶ [dispatch queue]
                         │
                    pull │ claim (fence)
                         ▼
                   gateway worker ──▶ operational surface
                         │
                  complete under the same claim_id
```

The registered `Action` carries the declared effect set and an input schema. The
effect ceiling is enforced where the action is *registered* (`pkg/explorer/fleet/service.go:720`)
and re-checked where it is *resolved* (`pkg/explorer/fleet/dispatch_service.go:1483`) — the second
check exists because a host can rebind an executor reference to a narrower
ceiling while descriptors registered under the old one are still live, and those
must stop resolving rather than keep running. So an action whose effects exceed
its executor's ceiling never reaches a worker, which is how #391's "refused
before anything external happens" is met.

It is worth being precise that this is not an enqueue-time check. Enqueue does
not re-evaluate the ceiling; registration and resolution do. An operator
tightening a ceiling should expect already-queued work to stop resolving, not to
have been rejected when it was enqueued.

The claim then establishes a fence. A second worker cannot claim a *live* action,
and a report under a stale fence is rejected rather than accepted — so the
**record** admits at most one completion per claim. That is necessary and it is
not sufficient; the next section is about why.

This is the only path with a fence, so **every non-idempotent operation must use
it.**

### Path B — synchronous, admission-based

```
agent ──▶ gateway ──▶ /api/v1/admission/request
                 ──▶ operational surface
                 ──▶ /api/v1/admission/report
```

For agents that want "may I do this, now" semantics and cannot restructure
around a queue. It reuses the admission seam (#388) exactly as the LLM proxy
does, and the ceiling is enforced there too: `pkg/explorer/fleet/admission.go:401` refuses a
declaration that exceeds the registered action's effects, so Path B cannot
declare its way to something broader than it was registered for.

What it does not have is a fence, and therefore **no at-most-once guarantee**:
the agent's own HTTP client can retry, and a retry re-performs the effect.

That missing guarantee must be a configuration-time refusal rather than a
runtime surprise. The operator declares per route whether the target operation
is idempotent, and Path B refuses to serve a route not declared so. The gateway
cannot determine idempotence by inspection, and guessing it is the one mistake
here that is unrecoverable — so it is the operator's assertion, recorded in the
values file where a reviewer can see it.

A route declared idempotent and not actually idempotent is an operator error
Shoal cannot detect. The design's job is to make the assertion explicit and
auditable, not to pretend it can be verified.

Three corrections to how that was specified:

**The gate was on the wrong noun.** `target.idempotent` is a per-*target*
boolean, and what is actually served is a caller-supplied request against
`target.baseURL`. An operator who ticks it because "we only ever call
`PUT /users/{id}`" has asserted nothing about `POST /payments`, which the gateway
will proxy to the same base URL for any caller that reaches the listener. The
assertion has to be per `(method, path template)` against a deny-by-default route
table — at which point the "require `PUT`/`DELETE`/`GET`, refuse `POST`" idea in
the open decisions becomes enforceable rather than a weak proxy, because there is
something to attach it to.

**Path B is hard-bounded at five minutes and cannot be extended.** The admission
service refuses a lease above `MaxActionClaimTTL`
(`pkg/explorer/fleet/admission.go:338`), admissions are explicitly not
reclaimable, and #430's renewal is on `DispatchService` — not here. So a Path B
operation has at most five minutes minus the report window, forever. That bound
decides which operations can use Path B at all and this document never stated it.

**The shared principal is worse than "unresolved".** The admission identity is
derived from the decision plus a caller-chosen `request.ID`
(`pkg/explorer/fleet/admission.go:365`, and `admissionActionID` at `:774`). With the LLM proxy's answer — one
configured descriptor for the whole gateway — every Path B caller shares one
admission-ID namespace keyed on a value callers pick. Caller A submitting
caller B's in-flight `request.ID` with a different declaration gets a conflict,
which is an existence oracle on B's in-flight admissions and a denial of service
on B's own work. Submitting an *equivalent* request with B's `TokenID` returns
B's live grant, and the only thing standing in the way is byte equality on a
`TokenID` this document never required to be unpredictable.

So Path B needs a per-caller principal **before** it is built, not as an open
decision, and `TokenID` must be CSPRNG-generated at a stated width. That, plus
the output-constraint problem above, is the strongest argument for Path B being a
separate command rather than a values key on this one.

## What the fence does not protect

The fence is a lock on Shoal's record. It is not a lock on the target, and
nothing in Shoal can make it one.

A worker that is partitioned, or paused long enough by GC or by the scheduler,
can still be inside its operation when its lease expires. The action is then
requeued, a second worker claims it under a new fence, and performs the same
operation. The first worker's completion is rejected when it returns — correctly
— but the effect has happened twice. The record is consistent and the world is
not.

This is the standard limit of fencing tokens: they only confer mutual exclusion
if the **resource** validates them. Shoal's record validates the fence; Slack
does not.

So the honest statement of Path A's guarantee has two cases, and which one
applies is a property of the target rather than of this design:

| target | guarantee |
|---|---|
| accepts a key **and** deduplicates for the whole first-attempt-to-deadline interval | at-most-once at the target |
| accepts a key, window unknown or shorter than that interval | **unknown**, which is worse to reason about than absent — the operator will believe the key is working |
| accepts no key | at-most-once only absent **every** cause this document lists: a partition, a GC or scheduler pause, a plane outage during a held claim, clock skew, and two replicas sharing a `ClaimID`. The last two need no fault at all, and the shared-`ClaimID` case is **undetectable**, because `Claim`'s replay branch never calls `applyClaim` so `ClaimFence` stays at 1 |

An earlier version of this table had two rows and both were wrong. Row one
omitted the retention condition the body states two paragraphs later, so a target
with a one-hour window and a twenty-hour deadline read as at-most-once. Row two
said "absent a partition" when this document's own text names a GC pause, a
scheduler pause and a plane outage as sufficient.

Where the target accepts one, the gateway must send an idempotency key, and
**the key must be derived from the action, never from the claim.** Two claims of
one action are exactly the case that needs collapsing, so a key including
`ClaimFence` or `ClaimID` would differ between them and the target would treat
the second as new work — defeating the mechanism precisely when it is needed.
The action's ID is the stable identity; the claim is not.

Two things that "derived from the action" does not settle on its own:

**The encoding.** An action ID is an opaque byte string, not a header value, so
the key needs a canonical ASCII form. Which one is settled below, and it is not
the action ID: see "the platform already computes this key".

**The retention horizon, which is the part that actually decides the
guarantee.** Accepting a key is not deduplicating against it forever. Providers
hold keys for a bounded window — a day is typical — and the second attempt here
is not a client retry milliseconds later; it is a *re-claim* after a lease
expiry, bounded only by the action's `Deadline`, which may be up to
`MaxActionDeadline` (24h). If the target's window is shorter than the action's
deadline horizon, the key stops collapsing duplicates exactly in the long-running
case this design exists to support.

So the operator declares the target's window and the gateway enforces the
relationship at claim time, before any effect:

```
Deadline - now  ≤  target.idempotencyRetention
```

An action whose remaining deadline outlives the target's dedup window is refused
at claim rather than performed under a guarantee that has silently lapsed. With
no declared retention, the target is in the second row of the table above.

**The key must be domain-separated, and `base64url(actionID)` is not.** Action
IDs are arbitrary caller-supplied opaque bytes, so the string Shoal would send a
third party as a deduplication key is fully chosen by anyone who can enqueue.
Two consequences, and the first is worse than a duplicate:

A **collision suppresses a real effect.** The target's key namespace is scoped to
the credential and shared with everything else using that provider account. A key
that has already been used makes the provider return its cached success and *not
perform the effect*. The worker reports `succeeded`. The record says the payment
was made; it was not — with no `EffectPossible` nuance, no ambiguity and no fence
anomaly to find. That is the one failure mode worse than duplication, because it
is invisible.

And a caller can **burn keys deliberately**: pick an action ID that collides with
the key a future legitimate effect will use, and that effect is silently
suppressed.

**The platform already computes this key, and two earlier drafts of this section
reinvented it — the second time worse than the first.**

`ExecutorKey` is derived at enqueue as
`SHA-256("shoal.fleet.executor-key.v2" ‖ actionID ‖ idempotencyKey)`
(`pkg/explorer/fleet/dispatch_service.go:1500-1507`), where `IdempotencyKey` is
a **required** enqueue field. It is already handed to in-process executors *as
their idempotency key* (`pkg/explorer/fleet/dispatch_service.go:512`). So the
thing this section spent seventy lines deriving is a solved problem with a
shipped, domain-separated, caller-independent construction.

And the construction this document proposed —
`SHA-256("shoal.gateway.v1" ‖ workspace ‖ surface ‖ actionID)` — was **not
injective**, because it concatenated without length-prefixing. A caller able to
enqueue against two surfaces could choose an `actionID` making surface A's key
equal surface B's, which is precisely the deliberate key-burning attack described
two paragraphs above. `executorKey` length-prefixes every field through
`writeDispatchTupleField`, and `admissionActionID` does the same, with
`disclosureDigest`'s comment explaining why. Writing a digest by hand and
omitting that is the ordinary way to get this wrong.

So: **the key is the action's `ExecutorKey`, base64url-encoded.** It is
fixed-width, so the 255-character provider cap that the raw action ID would blow
past at `MaxActionIDBytes` is not a concern. It is derived from a required
idempotency key rather than from a caller-chosen identifier alone, which narrows
the collision surface the raw form opened.

The remaining problem is that `ExecutorKey` is **not on the read wire either** —
same omission as `Input`. So the prerequisite is "expose `ExecutorKey` and
`Input` on the dispatch read surface", not "invent a gateway-side digest". That
is a smaller change and a better one.

**A key-conflict response means success, not failure.** This is the single most
likely implementation error and the default reading gets it backwards. Providers
in this family answer a reused key with a differing payload as `400
idempotency_error` — not as the original object. A worker treating non-2xx as
"the effect did not occur" reports `Failed=true`, and the record then says
*failed, effect possible* when the truth is *succeeded, exactly once*. The
operator retries, which requires a new action ID, which under any
action-derived scheme means a **new key** — so the deliberate retry is
unprotected and produces the second payment. Misread, the key mechanism becomes
the cause of the duplicate.

So: a key-conflict response is positive evidence the effect occurred and must be
reported as `succeeded`.

**The request bytes must be a pure function of the action record.** Both the
conflict case and targets that deduplicate on key *plus* body require the second
attempt to be byte-identical to the first. `record.Input` is immutable, but the
*request* is not the input: a `Date` header, a generated request id, a nonce, or
a timestamp-bearing signature all differ between claim one and claim two. No
clock, no RNG, and any signature over a timestamp derived from the record — not
from `time.Now()`.

**The retention check is on the first-attempt instant, not the claim instant.**
The provider's window starts when the first request arrives. With renewal, a
worker can claim, renew for hours, and attempt late — at which point the
remaining deadline may fit the retention while the interval the provider is
actually measuring does not. Stated correctly the bound is from first attempt to
deadline, which means it has to be evaluated where the request leaves, not where
the claim is taken.

**One action record per intended effect is an obligation on the agent, and this
design cannot discharge it.** An agent whose enqueue response is lost and which
retries with a *fresh* action ID creates a second action, a second key and a
second effect. The fence does not help; both actions are legitimate. That is the
common client idiom and it is exactly what this document's opening warns about,
so the obligation has to be assigned explicitly: the action ID must be a
deterministic function of the intent. Where an agent has no natural one, it needs
to be told what to use, and this design does not currently say.

**These two claim-time rules can be jointly unsatisfiable, and an
implementation has to say so rather than let them interact silently.** Claiming
requires both

```
operationTimeout + reportWindow < Deadline - now  ≤  target.idempotencyRetention
```

so a retention window shorter than `operationTimeout + reportWindow` leaves an
empty interval and **nothing is ever claimable**. Two individually correct
guards then refuse every action, and the operator sees a gateway that never
takes work with no indication that the pair is the problem. Validate the
relationship where the configuration is read, not per claim:
`idempotencyRetention` must exceed `operationTimeout + reportWindow` whenever it
is set at all.

Where the target does not accept one, the design must not claim what it cannot
deliver. `EffectPossible` is set at claim time for any external-mutating or
egressing action and is **never cleared** — it is only ever set true
(`pkg/explorer/fleet/dispatch_service.go:376`, `:698`) — so it survives the
requeue and the second claim. An operator reconciling a duplicated effect has
that flag and the fence history; they do not have prevention. For targets where
a duplicate is unacceptable and no idempotency key exists, the correct answer is
that this gateway is not sufficient, and saying so is better than shipping a
guarantee that holds only until the first partition.

## The fence, and what it costs

`DispatchService.Claim` sets `EffectPossible = true` for any action whose effects
contain `EffectMutatesExternal` or `EffectEgressesContent`
(`pkg/explorer/fleet/dispatch_service.go:374`). That flag is the honest answer to "did this run",
and it is set at claim time precisely because the claim is the last moment
before an effect becomes possible.

The ordering the worker must follow is non-negotiable. What it echoes is
`(claim_id, expected_version)` — see "What a worker actually echoes", above;
`ClaimFence` is not part of the worker's protocol and cannot be sent back:

1. Check the fence is live.
2. Perform.
3. Report under the same `claim_id` and the **claimed** record's
   `expected_version` — not the version `Pull` offered, which is one behind.
4. Treat a rejected report as an ambiguity to surface, never as an error to
   swallow or a success to assume.

Step 4 is the whole point. A rejected report means the record no longer agrees
that this worker owns the action — so the effect may have happened under a
record that says it did not.

**Step 1 needs a floor, and "check the fence is live" does not provide one.** A
liveness check is a predicate on a past instant. A worker that checks with 200ms
of lease left and then starts a ten-minute operation has followed the ordering
exactly and is guaranteed to finish outside its lease. The margin invariant above
is evaluated at *claim*, against `Deadline`; the bound that expires first is
`ClaimLeaseUntil`, and nothing bounds the delay between claiming and the first
request leaving — DNS, a renewal cycle, request construction.

So step 1 is a predicate evaluated immediately before the request leaves the
socket, not once at claim. But the obvious form of it is **unsatisfiable**, and
writing it down that way was a self-inflicted contradiction worth keeping
visible:

```
ClaimLeaseUntil - now  ≥  operationTimeout + reportWindow     ← WRONG
```

`ClaimLeaseUntil` is at most `claim_instant + MaxActionClaimTTL`, so the
left-hand side can never exceed **300 seconds** — renewal included, because a
renewal is bounded by the same ceiling. The sketch sets `operationTimeout: 10m`
deliberately, to exercise renewal, which makes the right-hand side 605 seconds
and the predicate false at every instant. The worker would claim and then refuse
to perform, forever.

The error was transplanting `validateDurations` from the LLM proxy
(`cmd/shoal-llm-proxy/main.go:523`), where the shape is sound **because that
proxy has no renewal** — its lease really is the bound on the whole call. A
renewing worker's lease is a silence interval, and an operation is expected to
span many of them, so no lease-relative predicate can gate the start of one.

What the worker must actually check before each request is that it can *renew*
through the operation and still report inside the action's deadline:

```
Deadline - now ≥ operationTimeout + reportWindow      (the real bound)
ClaimLeaseUntil - now ≥ renewAfter                    (renew before going silent)
```

The first is the same inequality the claim-time check uses, re-evaluated because
time has passed. The second is the only lease-relative thing a worker can
usefully assert: not that the operation fits in the lease, but that the next
renewal is not already overdue.

**And every one of these margins is computed on the worker's clock against a
server-issued timestamp.** There is no skew allowance anywhere in
`pkg/explorer/fleet`; the server decides expiry on its own clock. A worker 30
seconds fast concludes it has lost a lease the server still considers live,
abandons, and lets the action be re-claimed and performed twice. A worker 30
seconds slow computes 35 seconds of margin where five exist, performs, and has
nothing left to report with. Two workers skewed in opposite directions have
overlapping beliefs about one server-side boundary, so two claims can be live in
each worker's own view — a duplicate caused by a clock, with both workers healthy
and both planes reachable.

A fixed five-second margin is below ordinary step tolerances on a cluster with
one bad node, so the margin is not merely untunable, it is **underived**. The
cheap fix is to make the claim response carry the server's `now` so every margin
is computed in server time with a measured round-trip bound; the alternative is
to state a skew budget inside the margin and the clock discipline it assumes.
This design currently does neither, and the conceded limitation ("partitioned, or
paused by GC or the scheduler") does not cover skew.

## Cancellation, which this design forgot

`Cancel` appears nowhere in the document until this section, and it is the lever
an operator reaches for first.

**An operator cannot reach `Cancel` at all**, which is stronger than the timing
problem below and was missed because the analysis assumed they could. `Cancel`
goes through `authorizedCurrent` (`pkg/explorer/fleet/dispatch_service.go:845`),
which applies `sameActionPrincipal` and normalises a mismatch to
`auth.ObjectNotFound()`. An operator is not the enqueuing principal, so they are
told the action does not exist. `TeamActions` is explicitly the surface that does
not require the reader to be the originating principal; `Cancel` is not.

That is the first blocker surfacing on a second route, and it means the race
below is between the *enqueuing agent* and a re-claiming worker, not between an
operator and a worker.

For a caller who can reach it, it is refused while a claim is live:
`if current.State == DispatchClaimed && now.Before(current.ClaimLeaseUntil)` →
`ErrActionConflict` (`pkg/explorer/fleet/dispatch_service.go:870-871`). The renewal
rule above keeps `ClaimLeaseUntil` in the future for as long as the worker lives,
so **the cancel window never opens.** An operator who discovers mid-operation
that a payment is going to the wrong account can kill the pod — which strands the
effect, and may duplicate it on re-claim — or wait out the deadline, up to 24
hours.

Worse, the cancel window and the re-claim window open at the *same instant*, both
gated on `!now.Before(ClaimLeaseUntil)`. So they race:

1. Worker 1 claims, performs the effect, and is then paused by the node.
2. The lease expires at `T`.
3. At `T+1ms` the operator cancels. `Cancel` wins the compare-and-swap.
4. Worker 1 resumes and reports. `completeClaim`'s replay arm accepts only
   `Succeeded` or `Failed`, deliberately excluding `Canceled`
   (`pkg/explorer/fleet/dispatch_service.go:611-621`), so worker 1 gets
   `ErrClaimLost`.

The durable record now says **`canceled`** — which elsewhere in this codebase
means "Shoal refused" — for a payment that was made, and the published
`action.canceled` event says the same. If `Claim` wins the race instead, the
operator's cancel returns `ErrActionConflict` and the second worker duplicates
the effect the operator was trying to stop.

The honest conclusion is that cancel should be **refused outright for any action
with `EffectPossible` set**, and replaced by a distinct abandon transition that
does not assert a refusal. A record that says "cancelled" about an effect that
happened is worse than one that says "unknown".

## What an operator cannot do

The document asks what an operator will *see*. These are the things they will
certainly need to *do*, and cannot:

**Stop an in-flight effect.** Above. The window never opens.

**Find every action that may have double-executed.** This document leans on
`EffectPossible` in several places as the reconciliation signal, and it is not
one. `applyClaim` sets it on every claim of an external action, and
`applyExecutionResult` sets it **unconditionally for every completion of any
effect class** (`pkg/explorer/fleet/dispatch_service.go:698` — no declaration
check). So it is true for the overwhelming majority of perfectly healthy actions.
It is an honest answer to "could this have had an effect" and useless as an
answer to "did this run twice".

The predicate that answers the real question exists and this document never named
it: the fence is incremented per claim, so **`ClaimFence > 1 ∧ EffectPossible`**
is exactly "claimed more than once, with an effect possible each time". That is
the query an operator runs at three in the morning, and nothing exposes it —
`Status` is per-action and `TeamActions` is a bounded page with no fence filter.
It should be listable, and a renewal that advanced the fence would destroy it
(see the requirements on #430).

**Deliberately replay a failed effect.** A terminal action cannot be re-claimed,
and re-enqueuing at the same ID replays the terminal state back with a 200 — so
an operator retrying is told "done" and nothing runs. Re-running therefore needs
a new action ID, which under an action-derived key scheme means a **new key**, so
the deliberate retry has no deduplication protection at exactly the moment it is
most needed: the operator is retrying something whose outcome is `failed,
EffectPossible` — that is, unknown. The at-most-once argument evaporates the
moment a human invokes it. This needs either a replay transition that preserves
the action ID, or a key derived from something stable across replays.

**Drain a surface for maintenance.** There is no way to stop pulling without
killing the pod, and the queue keeps accepting enqueues. "Drain Slack for an
hour" means scale to zero, let the queue build, scale back, and absorb a herd of
actions whose remaining deadline has shrunk by an hour — at which point the
retention check starts refusing them at claim, which is fail-safe and will look
like a total outage.

**Rotate a target credential without voiding the guarantee.** Per-request file
reads make the *next* request use the new credential. But at most providers the
idempotency namespace is scoped to the credential, so a key burned under v1 does
not deduplicate a retry under v2. A rotation silently drops every in-flight
action to the weakest row of the guarantee table, for the remainder of its
deadline horizon. This document related rotation to neither the retention window
nor the re-claim horizon, and it must.

## Lease arithmetic

This is where #391 asks for something that does not exist, and the reason #430
is now a prerequisite.

There is no claim-renewal route. `Heartbeat` is agent liveness on `Service`, not
lease renewal on `DispatchService`. `MaxActionClaimTTL` is five minutes and the
service **refuses a longer lease rather than clamping it**. `ClaimLeaseUntil` is
additionally clamped to the action's `Deadline`.

So today the fenced window is at most five minutes, fixed at claim time. Any
operation that can outrun it produces exactly the outcome #391 exists to
prevent.

#430 separates the two bounds that one number currently conflates:

| bound | meaning | value |
|---|---|---|
| `MaxActionClaimTTL` | the **ceiling** on how long a worker may be silent before it is assumed gone | 5m. The silence interval itself is `claimLease`, which must not exceed it |
| `Deadline` | how long the **whole operation** may take | set at enqueue, ≤ `MaxActionDeadline` (24h) |

Both already exist and both are already enforced, so renewal needs no new
ceiling. With it, the worker's rule is:

```
claim with lease L ≤ MaxActionClaimTTL
renew every L/2 while the operation runs
abandon if a renewal is refused
total duration bounded by the action's Deadline
```

`reportWindow` has to be a defined number before any of that is normative, since
it appears in two invariants and a grace-period calculation. It is a **fixed
conservative margin of 5 seconds**, not a setting: the same value and the same
reasoning as `minimumReportWindow` in `cmd/shoal-llm-proxy/admission.go:108`,
because it is the same act — one authenticated POST to the explorer after the
work is done. Making it configurable would invite an operator to tune away the
margin that keeps a completed effect reportable, which is the one thing here
that must not be tunable.

It is deliberately the same number as the proxy's rather than coincidentally so.
If the two ever need to differ, that is the signal to export one constant from
`pkg/explorer/fleet` and have both derive from it, rather than to let two
unexported fives drift apart.

And the invariant to enforce **at claim time, before any effect** — the same
shape as `validateDurations` in the LLM proxy:

```
operationTimeout + reportWindow < Deadline - now
```

An action whose deadline cannot accommodate the configured operation timeout plus
the report is refused at claim. Refusing there is free; discovering it after the
effect is not.

## Reporting an ambiguity when the fence is gone

A worker whose renewal is refused mid-operation cannot report through
`complete` — that route is gated by the fence it has just lost. This is the case
#391 requires be recorded "with enough detail that an operator can reconcile",
and the obvious channel is unavailable by construction.

Part of this is already solved and part of it is not, and an earlier draft of
this document got the division wrong by proposing a lifecycle event the public
route cannot carry.

**The fact survives without the worker doing anything.** `EffectPossible` is set
at claim time for external-mutating and egressing actions and is never cleared —
only ever set true (`pkg/explorer/fleet/dispatch_service.go:376`, `:698`). It
therefore persists across lease expiry, requeue and re-claim. A record that has
ever been claimed for an external effect carries "an effect may have happened"
permanently, which is the part that matters most and costs nothing.

**The detail does not, and the obvious route refuses it.**
`/api/v1/fleet/events/publish` cannot publish a lifecycle event:
`fleetevents.Service.Publish` rejects the five **exact** lifecycle kinds —
`action.enqueued`, `action.canceled`, `action.claimed`, `action.completed`,
`action.failed` (`pkg/explorer/fleetevents/service.go:294-303`) — with "fleet
lifecycle event kinds require trusted publication". `action.*` is **not** a
reserved prefix, which an earlier draft of this paragraph asserted: a worker
could publish `action.effect_ambiguous` through the public route, into the
namespace every consumer filtering on `action.` is reading. That is a reason to
choose a `gateway.` kind deliberately rather than a reason it is forced. And
`PublishLifecycle` is an in-process path requiring a reconcile capability and a
`LifecycleReceipt` carrying a request ID, an authorization fingerprint, a UTC
expiry and a matching correlation ID (`pkg/explorer/fleetevents/service.go:254-291`).
An out-of-process worker can produce none of that, and should not be able to —
that gate is what keeps the lifecycle record trustworthy.

So the worker's options are a **non-reserved** event kind of its own
(`gateway.effect_ambiguous`, say) through the public route, which needs a stated
consumer contract because nothing correlates it to the `ActionRecord`
automatically; or a dedicated route that attaches an ambiguity note to an action
the caller can no longer complete.

The second is better and is probably a prerequisite rather than a follow-on. An
ambiguity that is reconcilable only by convention is one an operator has to know
to go looking for, and the whole point is that they will be reconciling under
time pressure. A route authorized like `complete` but accepting a *lost-fence*
report would attach the detail to the record that already carries
`EffectPossible`, which is where someone reconciling will actually look.

Either way the gateway needs an authorization separate from its dispatch
authorization, so losing a claim does not also cost it the ability to say so.

## The unit of deployment is the operational surface

One binary, deployed once per operational surface. Not one worker pool holding
every credential.

```
shoal-gateway-slack     descriptor A   ServiceAccount A   Secret A   egress → slack.com
shoal-gateway-github    descriptor B   ServiceAccount B   Secret B   egress → api.github.com
shoal-gateway-postgres  descriptor C   ServiceAccount C   Secret C   egress → db:5432
```

The argument is blast radius, and it is the same argument that put the LLM proxy
in a separate process from the explorer. A single pool is one pod that can mutate
every external system Shoal governs, holding every credential, reachable by one
compromise of whichever target has the weakest client library. Per-surface, a
compromise reaches one surface.

Three things follow and are the real reason this is not merely tidier:

**Network policy becomes expressible — to the extent the substrate allows.** A
per-surface worker needs egress to exactly one target; a pool needs egress to all
of them, which is not a policy so much as a hole.

An earlier draft added "and the explorer needs egress to none", which is false
for supported deployments. `shoal-explore-web` takes `-chat-base-url` and
`-embedding-base-url` and the chart has a guard specifically for a **remote**
chat provider needing a credential Secret (`deploy/helm/shoal/templates/validate.yaml:96`),
so a workspace configured with a hosted model egresses by design. The asymmetry
is real only in a loopback-only configuration, where the explorer's providers are
sidecars and its egress set is genuinely empty.

What holds unconditionally is narrower and still worth having: the explorer's
egress set is its *model providers*, and the gateway's is its *operational
target*. Those are different destinations with different credentials, and keeping
them in different pods is what lets a policy say so. Collapsed into one pod, the
union is the policy.

How enforceable the single-target half is depends on where the target lives, and
an earlier draft of this document overstated it. A standard `NetworkPolicy`
selects pods, namespaces and CIDRs — **not DNS names.** `slack.com` and
`api.github.com` are not expressible in it, and egress to the cluster DNS
service has to be permitted explicitly or name resolution fails before any
policy question arises. So:

| target | exact-target egress policy |
|---|---|
| in-cluster (a database `Service`) | expressible in standard `NetworkPolicy` |
| external FQDN | needs an FQDN-aware CNI (Cilium and similar), a pinned CIDR set, or an egress proxy the policy *can* name |

A pinned CIDR set for a SaaS target is a maintenance trap and should be treated
as one; the provider changes it and the gateway fails closed at an unhelpful
moment. Where none of the three is available, per-surface deployment still buys
the credential and capability isolation below, and the network half should be
described as what it is — unenforced — rather than assumed.

**Credentials stop being ambient.** Each Deployment mounts one target credential.
A worker performing a Slack post has no path to the database password, by
absence rather than by code.

**Capability scoping becomes meaningful.** Each worker registers its own
descriptor with its own capability and action names, so the fleet's own
authorization bounds what each one may claim. A pool shares one descriptor and
every action it can claim.

The cost is honest: more Deployments, more Secrets, more NetworkPolicies, and a
chart that renders a set rather than a singleton. For a component whose job is
irreversible external effects, that is the right trade.

## What a hostile target can do to the worker

This document named the threat — "reachable by one compromise of whichever target
has the weakest client library" — and then listed only absences that do not
address it. The worker holds credentials that mutate production systems *and*
parses bytes from the system it is pointed at. That is the pairing that matters.

**Redirects.** A target answering `302` to the explorer's own URL, or to a
link-local metadata address, gets its request replayed there by a default Go
client. The `NetworkPolicy` does not help: the explorer is an *allowed*
destination. If the worker shares one `http.Client`, a cookie jar, or a
transport-level `Authorization` header between its two destinations, the target
harvests the explorer token — and with it `complete` on every action this
principal owns. So: redirects not followed, two separate clients, two separate
credential stores, and the target credential attached per request rather than per
transport. This is the same rule #417 arrived at for the LLM proxy, for the same
reason, and it transfers.

**Response size.** `MaxActionOutputBytes` is enforced at the explorer, after the
worker has already buffered the body. A target returning an unbounded body
OOM-kills the worker mid-operation — which is the stranding path above, now
deliberately triggerable by the target. The worker needs its own read limit,
below the explorer's bound.

**Slow responses.** A target that holds every connection open for exactly
`operationTimeout` burns one claim per action and leaves a terminal record with
`EffectPossible` set, and nothing re-drives a terminal action. A hostile or
merely degraded target converts the queue into permanently ambiguous records at
one per `operationTimeout`, and the operator has no replay.

**What reaches the record.** `Output` is validated against the `OutputSchema` the
*gateway itself registered*, and the supported schema keywords are
`{type, properties, required, items, enum, additionalProperties}`
(`pkg/explorer/fleet/dispatch_model.go:835`) — **no `maxLength`, no `pattern`**.
So any field declared a string accepts up to `MaxActionOutputBytes` of
target-controlled text, which is canonicalised into the durable record, published
on `action.completed`, and surfaced through `Status` and the team overview to
whatever renders them. `ErrorCode` is constrained; `Output` is not.

The design must state what of a response may enter `Output` — a status code, a
target-side identifier matched against a closed shape, and nothing else — and
must name where those bytes get rendered. "The worker parses external responses"
is currently given as a reason to deny it a Kubernetes token and nowhere as a
reason to constrain what it forwards *inward*.

**Evidence anchors.** `CompleteClaim` accepts a list of `EvidenceRef` over HTTP
with caller-supplied node, edge and assertion IDs and a `Visibility` label set,
where an empty set is **public**. `EvidenceRef` is documented as coming from a
*trusted* executor (`pkg/explorer/fleet/dispatch_model.go:59`) — and this design
introduces the first executor that is not one. This document lists "no corpus
access" as an enforced absence; that is about *reads*. The write side lets the
one pod holding production credentials attach fabricated anchors citing real IDs,
publicly visible, to an action record, without ever reading the corpus.

So a gateway sends `Evidence` empty, always, with no snapshot ID. And that is
currently a convention the worker could violate rather than an invariant the
explorer enforces, because `completeClaim` has no notion of an untrusted
executor. That asymmetry is worth raising as its own prerequisite: the one place
this trust boundary is written down says "trusted executor", and this design
breaks that assumption.

## What this pod must not have

Stated as absences, because each one is enforced by not being configured rather
than by a check:

- **No corpus access, for reads.** The worker reads an action's input, not the
  corpus. Its authorization covers dispatch pull, claim and complete — **not
  "extend", which does not exist** until #430 lands, and this list previously
  named it in the present tense as an enforced absence. Plus event publication.
  Nothing else. Note that "no corpus access" is a statement about reads only; see
  the evidence-anchor problem above for the write side.
- **No Kubernetes API credential.** `automountServiceAccountToken: false`. The
  worker speaks HTTP to the explorer and HTTP to its target and touches the API
  server nowhere — and this is the pod that both parses external responses and
  holds credentials that mutate production systems. Same reasoning as #417, more
  sharply.
- **No egress except its own target, the explorer, and cluster DNS** — by
  `NetworkPolicy` only where the substrate can express the target, which for an
  external FQDN it generally cannot (see the table above). Where it cannot, this
  is an intention and not an invariant, and listing it among enforced absences
  was the same overstatement corrected two sections earlier. The DNS exception is
  stated rather than implied: a policy
  that omits egress to the cluster DNS service breaks resolution of every
  hostname-based explorer or target URL before any application traffic is
  attempted, and the failure looks like an unreachable plane rather than a
  policy mistake. An earlier draft listed this invariant as two destinations,
  which contradicted the network-policy section above it.
- **No credential in an environment variable where it must rotate.** The `-file`
  forms exist for that, and per-request reading is what makes them work.

## Kubernetes reference

Shape only; the chart lands under #387 alongside the LLM proxy's, disabled by
default.

```yaml
gateways:
  - name: slack
    enabled: false
    identity:
      agentID: ""            # unpadded base64url, as #417 learned
      agentGeneration: 1
      capability: effects.http
      action: post
    target:
      baseURL: ""            # https, or http to loopback only
      credentialSecretName: ""
      credentialFile: ""     # preferred: rotates under a running pod
      # Path B's gate is per route, not per target — a target-wide boolean
      # asserts nothing about POST /payments. See "Two paths".
      routes: []             # deny-by-default: {method, pathTemplate, idempotent}
      # Where the target accepts an idempotency key, name the header. The key is
      # derived from the action ID and never from the claim, so two claims of one
      # action collapse at the target. Empty means the target has no such
      # mechanism, and Path A's guarantee is then the weaker of the two cases in
      # "What the fence does not protect" — which an operator should have to see.
      idempotencyKeyHeader: ""
      # How long the target deduplicates against that key. The second attempt
      # here is a re-claim after a lease expiry, not a client retry, so this has
      # to cover the action's whole deadline horizon or the key stops collapsing
      # duplicates in exactly the long-running case renewal exists for. The
      # gateway refuses at claim when Deadline-now exceeds it.
      idempotencyRetention: ""
    dispatch:
      url: ""                # the explorer's authenticated API
      tokenFile: ""
      claimLease: 60s        # ≤ MaxActionClaimTTL; the silence interval
      renewAfter: 30s        # claimLease / 2; floor it well above the
                             # round-trip plus clock skew, or a renewal can
                             # arrive after the lease it meant to extend
      # Deliberately longer than claimLease, because that is the case the
      # design exists for. An operationTimeout inside the lease never needs a
      # renewal, so a sketch showing 30s against a 60s lease would make #430
      # look like decoration and let an implementation skip it.
      operationTimeout: 10m
      # Must exceed operationTimeout + reportWindow (not claimLease). A long
      # operationTimeout therefore forces a long grace period, which slows
      # rollouts — the same trade the proxy's lease makes, and the reason to
      # keep operationTimeout as tight as the surface actually needs.
      terminationGracePeriodSeconds: 615
    paths:
      queue: true            # Path A
      inline: false          # Path B, refused for any route not in target.routes
```

Per gateway, the chart renders a `Deployment`, a `ServiceAccount`, a
`NetworkPolicy`, and a `Service` only if Path B is enabled.

An earlier draft added "Path A needs no inbound listener at all, which is itself
a security property worth keeping", and then specified two probes — which is the
fourth time this document asserted a property and contradicted it nearby. A
kubelet `httpGet` or `tcpSocket` probe dials the pod and therefore requires a
listener; the house pattern is exactly that, a second listener on a health port
with both probes pointed at it. The two can only be reconciled with `exec`
probes, so Path A's no-listener property is available **only if the probes are
`exec`**, and that has to be the stated choice rather than an accident.

There is **no Service** for Path A either way, which has a consequence for what
readiness means — below.

Deployment properties, with the reasoning that is not obvious:

- **`replicas: 2` or more is safe for Path A only once `ClaimID` is per-attempt
  and the descriptor is not shared.** Both were wrong in an earlier draft, which
  said two workers cannot hold one action: the replay branch hands a live claim
  to a caller presenting the same `ClaimID` and lease (fourth blocker), and the
  descriptor is a second shared mutable object whose heartbeat races (second
  blocker). The fence makes replicas safe for the *claim*; it says nothing about
  either of those.
- **Nothing identifies which replica performed an effect** unless `ClaimID`
  carries it. Both replicas present the same descriptor, token and decision, so
  `Actor`, `Subject`, `ClientID` and the execution fingerprint are byte-identical.
  After a duplicated payment, "which pod, and did one do it twice or two do it
  once each" is unanswerable from the record. `ClaimID` is the only per-attempt
  field, which is a second reason it must be fresh and pod-identifying.
- **`terminationGracePeriodSeconds` must exceed `operationTimeout` plus the
  report window — not `claimLease`.** With renewal in place `claimLease` is only
  the silence interval, and an operation is *expected* to outlive it across
  several renewals, so sizing the grace period against the lease would cut off
  exactly the long operations renewal exists to permit. The bound that matters is
  how long the work itself can run plus the time to report it. On `SIGTERM` the
  worker stops pulling, finishes or abandons the claim it holds, reports, and
  exits; a grace period shorter than that reintroduces the stranding #417 fixed
  for the proxy, and here the stranded thing is an irreversible effect rather
  than a completion.

  This is a correction to an earlier draft, which carried over the proxy's
  "drain for the lease" rule without noticing that renewal changes what the
  lease means.

  It has a cost worth stating where someone sizing a cluster will see it: a long
  `operationTimeout` forces a long grace period, and a long grace period slows
  every rollout of that gateway. The knob is `operationTimeout`, and it should be
  as tight as the surface genuinely needs rather than set to a round number.
- **The grace period is honoured for pod deletion and not much else.**
  `terminationGracePeriodSeconds` applies to API-initiated deletion and eviction,
  is **capped by the kubelet's `shutdownGracePeriod`** on a node shutdown
  (commonly 30s, often unset), and is not honoured at all by abrupt node loss or
  most spot-termination windows. So a 615-second grace period is aspirational on
  any node-level event — and a node-level event here is exactly "worker killed
  mid-operation", the row that may duplicate the effect. An earlier draft stated
  the rule as though it guaranteed the drain.
- **A `PodDisruptionBudget` is about availability, not correctness, and is still
  worth setting.** A worker holding no claim is freely evictable; one holding a
  claim finishes it inside the grace period *when the grace period is honoured*.
  Without a budget a node drain can take every replica at once.
- **Readiness means "pulling", and for Path A it enforces nothing.** There is no
  Service and no endpoint set to be removed from, so readiness only stalls
  rollouts. A worker that cannot reach the explorer takes no work because it
  *cannot pull*, not because a probe failed — the fail-closed property is a
  consequence of the pull model, and an earlier draft had it backwards by
  presenting readiness as the mechanism. Readiness is still worth reporting,
  because it is what tells an operator the difference between an idle queue and
  an unreachable plane.

That last point is a real asymmetry with the LLM proxy, and it needs splitting by
what the worker was doing when the plane went away.

**For work not yet claimed, Path A denies by construction.** There is no caller
waiting and no refusal to synthesise: the worker cannot pull, so it takes no
work, and nothing happens. That is the strongest fail-closed property anywhere in
this design and it is free.

**For a claim already held, it is not fail-closed at all.** A worker that loses
the plane mid-operation may still reach the target — they are different
destinations and nothing couples their availability — so it can perform the
effect and then fail both renewal and completion. That is the ambiguity above,
reached through an outage rather than through a partition between worker and
target. An earlier draft of this document claimed the fail-closed property
without that qualification, which was the same mistake as claiming the fence
protects the target: true of the record, not of the world.

Path B has to deny explicitly, exactly as the LLM proxy does.

## Failure modes an operator will see

Rebuilt from the corrected body rather than edited, because every previous
version of this table restated a conclusion the body had already changed.

| what happened | what the record says | what the operator sees |
|---|---|---|
| plane unreachable, nothing claimed (Path A) | action stays queued | no effect. Readiness reports it; the pull model is what prevents it |
| plane unreachable while a claim is held | claim expires; if the generation moved, **permanently unclaimable until `Deadline`** | the effect may have occurred. `EffectPossible` does not say so — it is true for nearly every action; the discriminator would be an `ErrorCode` this design has not specified |
| plane unreachable (Path B) | nothing | `503`, distinguishable from a denial |
| one over-ceiling action **of this principal's** in a pull page | nothing | the gateway stops pulling every action in that page, including unrelated ones. `sameActionPrincipal` runs first, so it is not cross-tenant |
| effects exceed the executor ceiling at registration | the descriptor is refused | registration fails; no action exists |
| ceiling tightened after enqueue | action stops resolving for claim | the gateway goes dark, per the row above |
| descriptor heartbeat during a held claim | no completion accepted | `complete` returns **not-found**, not an ambiguity |
| rolling restart during an effect | no completion accepted | same; the grace period does not help |
| two replicas sharing a `ClaimID` | **one** completion, two effects | nothing. Neither prevented nor detectable |
| the enqueuing agent cancels a live claim | unchanged | `ErrActionConflict`. Once #430 lands the window never opens at all; today it opens within 5m |
| an operator attempts to cancel | unchanged | **`ObjectNotFound`** — cancel is unavailable to non-enqueuers by authorization, not by timing |
| the enqueuing agent cancels in the re-claim window after an effect | `canceled` | a record asserting Shoal refused, for an effect that happened |
| target unreachable | `failed` | **indistinguishable from the next row** without an `ErrorCode` vocabulary this design has not specified |
| target timed out after the request left | `failed` | the effect may have occurred. `EffectPossible` does not say so — it is true for nearly every action; the discriminator would be an `ErrorCode` this design has not specified |
| target returns a key conflict | `succeeded`, if implemented correctly | the effect happened exactly once. Misread as a failure, this becomes the cause of a duplicate |
| idempotency key collides with an unrelated use | `succeeded` | **nothing happened and nothing says so** |
| renewal refused mid-operation | claim lost, no completion | `EffectPossible` only, until a lost-fence route exists |
| report under a stale fence | rejected | the rejection is surfaced — but a sibling with the same `ClaimID` is accepted |
| worker killed mid-operation | lease expires | a second claim may duplicate. Nothing on the record distinguishes this from a healthy completion |
| clock skew between worker and explorer | varies | a duplicate or an abandoned effect, with both workers healthy |
| target dedup window shorter than the interval | refused at claim | **nothing is recorded.** There is no mechanism to tell them; see below |
| operator retries a failed effect | a new action, a new key | the retry is unprotected, at the moment protection matters most |

Three things this table cannot do, which are design gaps rather than
presentation ones:

**It cannot distinguish "target unreachable" from "timed out after the request
left".** Both produce `failed` with `EffectPossible` set, because that flag is
assigned at claim and again unconditionally at completion. The only possible
discriminator is `ErrorCode`, and this design does not specify one. Those two
rows are the difference between "nothing happened" and "something may have", so
the `ErrorCode` vocabulary is load-bearing and has to be enumerated.

**It cannot tell an operator that a horizon did not fit.** A worker should refuse
*before* claiming, which leaves the action queued and re-offered and refused
again, with nothing on the record to distinguish it from an idle gateway. If it
claims first in order to report a failure, `EffectPossible` is already set and the
record then asserts a possible effect for something never attempted. Neither is
acceptable, and the design needs a way to record a pre-claim refusal.

**It cannot answer "what may have double-executed".** `EffectPossible` is true
for nearly everything. The predicate is `ClaimFence > 1 ∧ EffectPossible`, and
nothing exposes it.

The rows carrying `EffectPossible` with "may have occurred" are the cost of
governing something irreversible and cannot be designed away. The rows carrying
"nothing" in the operator column are different: those are this design's
unfinished work.

## Prerequisites

Four, not one. Each blocks #391 and each lives outside the gateway.

| # | what | where |
|---|---|---|
| **new** | emit `input` and `executor_key` on the action read wire — without `input` a worker cannot learn what to do, and `executor_key` is the idempotency key the platform already derives | `fleetActionWire` |
| **#430** | claim renewal, with the three constraints above (fence must not advance; version handling; a distinguishable refusal) | `DispatchService` |
| **new** | a claimant that is not the enqueuer — see the first blocker. The only option that keeps attribution *and* isolation | the authorization predicate |
| **new** | an executor binding that declares an external-mutation ceiling and no floor — see the third blocker | the explorer host |
| **new** | a lost-fence ambiguity report attached to the action record | `DispatchService` |

And two that are smaller but have to be settled before an implementation starts,
because getting them wrong is silent:

- **`ClaimID` must be fresh per claim attempt, CSPRNG, pod-identifying.** The
  fourth blocker. This is a requirement on the *worker*, so it needs no fleet
  change — only that nobody reads "do not derive the key from the claim" as
  "the claim is not a per-attempt identity".
- **An `ErrorCode` vocabulary** that distinguishes "the request never left" from
  "the request left and the outcome is unknown". Without it two failure rows are
  the same record.

## Specified nowhere, and needed on day one

A third adversarial pass asked what an implementer hits immediately. These are
not design debates; they are omissions, and each one is reachable in the first
hour of work:

- **`Deadline` is the enqueuing caller's own request-context deadline**
  (`pkg/explorer/fleet/dispatch_service.go:189`), not a separate field. An agent
  using a conventional 30-second HTTP deadline creates a 30-second action, which
  fails the claim-time margin on every claim. Every `Deadline - now` expression
  in this document depends on agents knowing that, and `equivalentEnqueue` pins
  the exact timestamp, so an enqueue retry must reuse it.
- **Registration and startup.** The sketch pins `agentGeneration: 1` as a static
  value, and `Generation` is a compare-and-swap counter that both `Register` and
  `Heartbeat` advance. A literal in a values file is wrong the moment anything
  re-registers. Who registers the descriptor, with what `RegistrationKey`, and
  how a running pod learns the current generation, is unspecified.
- **The pull loop.** `PullActionsRequest` is `{After, Limit, Context}` — there is
  **no capability or action filter**, so a worker receives everything matching its
  principal and must filter client-side *before* claiming, since the claim is what
  sets `EffectPossible`. `Pull` does not refill a page its filter emptied, so a
  worker that always sends an empty cursor can be starved. No cadence, page size,
  or cursor-persistence rule is stated.
- **Every call needs a request context** with a fresh request ID, a non-empty
  reason code and a future UTC deadline. The reason code lands on the durable
  transition, and this document specifies no vocabulary for it.
- **Input that fails the worker's own validation.** The worker has already claimed
  before it can inspect anything, so `EffectPossible` is set. This is the same
  unrecordable-pre-claim-refusal gap the failure table names, and it needs the
  same answer.
- **The dispatch token's authorization.** This document names the blast radius
  and never narrows it: the operations (`OperationInvoke` for pull, claim and
  complete; `OperationDispatch` for status; `OperationEventPublish`), the
  `(sourceID, policyID)` scopes the descriptor must carry, and who mints the
  token are all unstated.
- **Two deployments against one target.** Per-surface isolation is argued for
  replicas inside one Deployment. A canary, a blue/green pair, or two clusters
  sharing a provider account are not covered — and since the dedup key is derived
  per action rather than per deployment, they must agree on the action identity or
  dedup silently stops working.
- **The reconciliation query.** `claim_fence` and `effect_possible` are both on
  the wire, so `ClaimFence > 1 ∧ EffectPossible` is *expressible*; what is
  missing is a server-side filter. `TeamActions` pages without a principal check
  but requires `OperationTeamOverviewRead` and **mandatory** source and policy
  filters, which an operator must know to run the query at all.

## Open decisions

- **Does the descriptor heartbeat or not?** The second blocker. Either the
  gateway registers once out-of-band with a long lease and its liveness signal
  means nothing, or the generation pin has to tolerate a heartbeated descriptor.
- **How is cancellation expressed for an action with `EffectPossible` set?** The
  current behaviour records "cancelled" for an effect that happened. The likely
  answer is to refuse cancel outright there and add an abandon transition that
  asserts nothing about whether the effect occurred.
- **Is a replay transition needed that preserves the action ID?** Without one, a
  deliberate operator retry has no deduplication protection, at the moment it
  matters most.
- **Does Path B belong in this binary at all?** Its principal problem, its
  five-minute hard bound, and the output-constraint question are all different
  from Path A's. The case for a separate command is now stronger than the shared
  target configuration that argued for one.
- **How a lost-fence ambiguity reaches the record.** The public events route
  cannot carry a lifecycle event, by design, and a non-reserved event kind is
  reconcilable only by convention. A route that attaches an ambiguity note to an
  action the caller can no longer complete is the better answer and is probably
  a second prerequisite rather than a follow-on. Until it exists, the durable
  signal is `EffectPossible` alone — the fact without the detail.
- **Whether a target without an idempotency key is in scope at all.** Path A
  gives at-most-once at the target only where the target accepts a key derived
  from the action *and* deduplicates against it for the action's whole deadline
  horizon. Where it does not, a partition or an outage during a held claim can
  duplicate the effect and the design can only make that detectable. For some
  surfaces that is acceptable; for others the right answer is that this gateway
  is not sufficient, and the values file should have to say which.
- **Should a declared-idempotent route be verified at all?** Nothing can verify
  it. With a per-route table there is at least something to attach a rule to:
  require such routes use methods idempotent by specification (`PUT`, `DELETE`,
  `GET`) and refuse `POST`. A weak proxy for the real property, but it refuses
  the most common mistake.

  The two earlier bullets here — whether Path B belongs in this binary, and what
  identifies its principal — were superseded by the Path B corrections above,
  which answer both: a per-caller principal is a prerequisite rather than an open
  question, and that plus the five-minute hard bound and the output-constraint
  problem make the case for a separate command stronger than the shared
  configuration that argued for one.
