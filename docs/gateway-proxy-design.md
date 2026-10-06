# Gateway proxy: reference design

How Shoal governs what an agent is permitted to *do*, as opposed to what a model
is permitted to be told. The second is `cmd/shoal-llm-proxy` (#390,
`docs/llm-proxy-deploy.md`). This is the design for the first (#391).

Nothing here is implemented yet. It exists because two decisions in #391 were
underdetermined in a way that would have produced the wrong deployment, and one
of its scope items asks for a mechanism that does not exist.

- [The thing that makes this different](#the-thing-that-makes-this-different)
- [Two paths, and why both](#two-paths-and-why-both)
- [What the fence does not protect](#what-the-fence-does-not-protect)
- [The fence, and what it costs](#the-fence-and-what-it-costs)
- [Lease arithmetic](#lease-arithmetic)
- [Reporting an ambiguity when the fence is gone](#reporting-an-ambiguity-when-the-fence-is-gone)
- [The unit of deployment is the operational surface](#the-unit-of-deployment-is-the-operational-surface)
- [What this pod must not have](#what-this-pod-must-not-have)
- [Kubernetes reference](#kubernetes-reference)
- [Failure modes an operator will see](#failure-modes-an-operator-will-see)
- [Open decisions](#open-decisions)

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
                  complete under the same fence
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
| accepts an idempotency key | at-most-once at the target |
| does not | at-most-once absent a partition; a partition can duplicate, and the duplicate is **detectable but not preventable** |

Where the target accepts one, the gateway must send an idempotency key, and
**the key must be derived from the action, never from the claim.** Two claims of
one action are exactly the case that needs collapsing, so a key including
`ClaimFence` or `ClaimID` would differ between them and the target would treat
the second as new work — defeating the mechanism precisely when it is needed.
The action's ID is the stable identity; the claim is not.

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

The fence is `(ClaimID, ClaimFence)`. The ordering the worker must follow is
non-negotiable:

1. Check the fence is live.
2. Perform.
3. Report under the **same** fence.
4. Treat a rejected report as an ambiguity to surface, never as an error to
   swallow or a success to assume.

Step 4 is the whole point. A rejected report means the record no longer agrees
that this worker owns the action — so the effect may have happened under a
record that says it did not.

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
| `MaxActionClaimTTL` | how long a worker may be **silent** before it is assumed gone | 5m — a heartbeat interval |
| `Deadline` | how long the **whole operation** may take | set at enqueue, ≤ `MaxActionDeadline` (24h) |

Both already exist and both are already enforced, so renewal needs no new
ceiling. With it, the worker's rule is:

```
claim with lease L ≤ MaxActionClaimTTL
renew every L/2 while the operation runs
abandon if a renewal is refused
total duration bounded by the action's Deadline
```

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
`fleetevents.Service.Publish` rejects any reserved `action.*` kind outright
("fleet lifecycle event kinds require trusted publication"), and
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
of them, which is not a policy so much as a hole. The explorer needs egress to
none, and that asymmetry is only enforceable if the gateway is not also the
explorer.

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

## What this pod must not have

Stated as absences, because each one is enforced by not being configured rather
than by a check:

- **No corpus access.** The worker reads an action's input, not the corpus. Its
  authorization covers dispatch pull, claim, extend and complete for its own
  descriptor, plus event publication. Nothing else.
- **No Kubernetes API credential.** `automountServiceAccountToken: false`. The
  worker speaks HTTP to the explorer and HTTP to its target and touches the API
  server nowhere — and this is the pod that both parses external responses and
  holds credentials that mutate production systems. Same reasoning as #417, more
  sharply.
- **No egress except its own target and the explorer.** By `NetworkPolicy`.
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
      idempotent: false      # gates Path B; see "Two paths"
      # Where the target accepts an idempotency key, name the header. The key is
      # derived from the action ID and never from the claim, so two claims of one
      # action collapse at the target. Empty means the target has no such
      # mechanism, and Path A's guarantee is then the weaker of the two cases in
      # "What the fence does not protect" — which an operator should have to see.
      idempotencyKeyHeader: ""
    dispatch:
      url: ""                # the explorer's authenticated API
      tokenFile: ""
      claimLease: 60s        # ≤ MaxActionClaimTTL; the heartbeat interval
      renewAfter: 30s        # claimLease / 2
      operationTimeout: 30s
    paths:
      queue: true            # Path A
      inline: false          # Path B, refused unless target.idempotent
```

Per gateway, the chart renders a `Deployment`, a `ServiceAccount`, a
`NetworkPolicy`, and a `Service` only if Path B is enabled — Path A needs no
inbound listener at all, which is itself a security property worth keeping.

Deployment properties, with the reasoning that is not obvious:

- **`replicas: 2` or more is safe for Path A, and that is because of the fence**,
  not despite it. Two workers cannot hold one action. This is the opposite of the
  explorer, which refuses a second replica.
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
- **No `PodDisruptionBudget` subtlety.** A worker holding no claim is freely
  evictable; one holding a claim finishes it inside the grace period.
- **Readiness means "pulling".** Liveness means the process is up. A worker that
  cannot reach the explorer is not ready, and not being ready is the correct
  fail-closed state: it simply stops taking work, and no effect occurs.

That last point is a real asymmetry with the LLM proxy and worth stating plainly:
**for Path A, an unreachable decision plane denies by construction.** There is no
caller waiting and no refusal to synthesise — the worker stops pulling and
nothing happens. Path B has to deny explicitly, exactly as the LLM proxy does.

## Failure modes an operator will see

| what happened | what the record says | what the operator sees |
|---|---|---|
| plane unreachable (Path A) | action stays queued | worker not ready; no effect |
| plane unreachable (Path B) | nothing | `503`, distinguishable from a denial |
| effects exceed the executor ceiling at registration | the descriptor is refused | registration fails; no action exists |
| ceiling tightened after enqueue | action stops resolving for claim | queued work stops being handed out; no worker involved |
| target unreachable | `failed` under fence | `EffectPossible` true, effect did not occur |
| target timed out after the request left | `failed` under fence | `EffectPossible` true, effect **may** have occurred |
| renewal refused mid-operation | claim lost, no completion | lifecycle event naming the ambiguity |
| report under a stale fence | rejected | the rejection is surfaced, not swallowed |
| worker killed mid-operation | lease expires, action requeued | `EffectPossible` true on the old record |

The two rows carrying `EffectPossible` with "may have occurred" are the ones that
cannot be designed away. They are the cost of governing something irreversible,
and the design's obligation is to make them visible rather than to round them to
success or failure.

## Open decisions

- **#430 must land first.** Without renewal the fenced window is five minutes and
  the SSH and database surfaces #391 names are unreachable.
- **How a lost-fence ambiguity reaches the record.** The public events route
  cannot carry a lifecycle event, by design, and a non-reserved event kind is
  reconcilable only by convention. A route that attaches an ambiguity note to an
  action the caller can no longer complete is the better answer and is probably
  a second prerequisite rather than a follow-on. Until it exists, the durable
  signal is `EffectPossible` alone — the fact without the detail.
- **Whether a target without an idempotency key is in scope at all.** Path A
  gives at-most-once at the target only where the target accepts a key derived
  from the action. Where it does not, a partition can duplicate the effect and
  the design can only make that detectable. For some surfaces that is
  acceptable; for others the right answer is that this gateway is not sufficient,
  and the values file should have to say which.
- **Does Path B belong in the same binary?** It shares the target configuration
  and credential handling, which argues yes. It has entirely different semantics,
  which argues for a separate command so the missing guarantee cannot be reached
  by flipping one values key. Currently specified as one binary with `paths.inline`
  defaulting false and refused unless `target.idempotent` is asserted.
- **What identifies the agent on Path B?** Path A has a registered descriptor.
  Path B's caller is whoever reached the listener, and the LLM proxy's answer — a
  single configured descriptor for the whole proxy — loses per-agent attribution.
  Unresolved, and it should be resolved before Path B is built.
- **Should a declared-idempotent route be verified at all?** Nothing can verify
  it. An option is to require that such routes use HTTP methods that are
  idempotent by specification (`PUT`, `DELETE`, `GET`) and refuse `POST`, which
  is a weak proxy for the real property but refuses the most common mistake.
