# The pre-call admission seam

An out-of-process caller with a side effect in hand and no way to ask whether
it may perform it is the gap `docs/enforcement-plane.md` describes. This is the
seam that closes it, and the prerequisite for the LLM proxy.

Every other enforcement surface in Shoal adjudicates work Shoal performs.
`/api/v1/ask` runs the inference. `/api/v1/fleet/actions/invoke` enqueues work
Shoal's own executor runs. The authorized client withholds documents as it
retrieves them. A proxy holding a prompt it has not yet sent has none of these:
it does not assemble the response, it does not run under Shoal's executor, and
by the time content exists the egress has already happened.

## Stop, not withhold

Withholding removes evidence from a response that is produced anyway. A caller
that assembles its own payload cannot be served that way. It needs a decision
before the effect, and a refusal here is a real stop: nothing left the host.

## An admission is a dispatch action

There is no new durable record. An admission is an `ActionRecord`, and the
token is its claim:

| admission | dispatch state | lifecycle event |
| --- | --- | --- |
| requested | `queued` | `action.enqueued` |
| granted | `claimed` | `action.claimed` |
| refused | `canceled` | `action.canceled` |
| reported | `succeeded` / `failed` | `action.completed` / `action.failed` |
| outstanding | `claimed`, unreported | — |

This is not an economy. A remote worker holding a claim and a proxy holding an
admission token are the same situation: an out-of-process actor holding
permission Shoal granted, whose outcome Shoal has not yet seen. A second record
type would have given the two different durability and a second place for them
to disagree.

The consequence worth stating: an outstanding admission and an outstanding
claim are indistinguishable, because they are the same thing.

## The three answers

`POST /api/v1/admission/request` returns one of:

- `denied` — the call must not happen. No token is issued, because a token is
  something a caller can report an effect against.
- `allowed` — it may happen as declared.
- `allowed_with_obligations` — it may happen with the returned references
  withheld from the payload.

The middle case is the point. Refusing a whole call is blunt; "you may proceed
without these" is a decision a caller can comply with.

A denial is an HTTP 200 carrying `"outcome": "denied"`. An unreachable decision
plane is a transport failure. The two must never be confusable, because a proxy
has to treat them oppositely: the first is the answer, the second is an outage
it must fail closed on itself.

## What is checked

1. The request context and the caller's `auth.Decision`, through the same
   `begin` every dispatch call uses. The request and correlation identity come
   from the decision, not the body.
2. The declared effect set against the action's declared effects, resolved
   under that decision. The executor ceiling is already enforced inside
   `resolveActionBinding`, so an action can neither declare more than its
   executor may do nor be admitted for more than it declares.
3. An empty effect set is refused as invalid, not read as "nothing". An
   admission that declares nothing would be the cheapest request and the most
   permissive answer.
4. An unrecognised effect class is refused. The transport passes an unknown
   class through verbatim rather than dropping it, so an unclassifiable request
   stays unclassifiable rather than becoming an empty one.

## Obligations

`Disclosures` on the request are the corpus references the caller says its
payload would carry. Obligations are computed over exactly that set, and
expressed in exactly that set, so a response never names anything the caller
did not name first.

Two stages, both narrowing:

1. `decision.AuthorizeObject(OperationRetrieve, …)` per reference — the same
   object-scoped check the authorized read path applies before a document
   enters a candidate set.
2. `authorized.Client.RestrictDisclosure`, which resolves each reference's
   registration rule and charges the co-occurrence budget exactly as a read of
   the same documents would, and reports what survives.

Stage one is honestly a request-level gate, not a per-reference one. An
admission names one source and one policy; only the object identity varies, so
every reference in a request gets the same answer. Per-reference authorization
is the registration rule attached to each document, which is what stage two
resolves. A deployment that does not wire a restrictor therefore gets weaker
obligations than one that does, and nothing in the service can detect that —
the wiring is a deployment invariant. Hosted startup wires it.

The second is how an existing withholding control becomes an obligation instead
of a refusal. It is not a new policy language: it is `MosaicBudget`, charged
pre-call, answering over a caller-supplied set instead of over a result this
process assembled.

A restrictor that returns a reference authorization already withheld does not
admit it — the result is intersected, so an implementation can only narrow. A
restrictor that errors stops the request; it is not read as "withhold nothing".

**Obligations say nothing about why.** Separating "you are not authorized for
this" from "your budget is spent" would turn admission into an authorization
oracle over document identities: a caller would learn which of a guessed set it
may read by reading the reason. The authorized read path reports those two
classes apart because there the counts describe a corpus the caller is already
reading; here they would describe identities the caller merely named.

## Reporting

`POST /api/v1/admission/report` closes the loop. Without it admission is a
stateless gate: permission granted and no record of whether the call happened.

A token is one-shot. Dispatch's completion path treats a terminal action at the
reported version under the same claim as the reporter's own lost response and
replays it, which is right for a worker retrying a delivery and wrong here — it
would let a caller report one outcome, then report a different one against the
same token and be told the second was recorded. So the admission path resolves
the terminal case first and accepts only a byte-identical replay of what is
already committed. Everything else is `conflict`.

A report is either an outcome or a failure, never both. A failure without an
error code and an outcome carrying one are both refused, so the committed
record's shape does not depend on which branch resolved first.

## Outstanding

`POST /api/v1/admission/outstanding` lists admissions this caller was granted
and has not reported. An expired token is reported as `expired`, which is not a
resolution: it is the point at which Shoal permitted an effect and will never
learn what happened. The record stays claimed — it never quietly becomes
something else.

## What this is not

It does not enforce that a caller honours an obligation. No plane outside the
caller's own process can. This is a declaration seam of the same kind as the
execution boundary: it stops a mismatch between what a caller claims and what
it is permitted, and cannot stop a caller that lies about what it did.

It does not accumulate risk across calls. The report is the input such an
accumulator would charge, and it is durable and published as a lifecycle event
so one can be attached without changing this surface.

It is not the proxy. The proxy is a separate binary, and that is why this is an
HTTP surface rather than a Go interface: a process that handles untrusted
prompt content from arbitrary callers must not share an address space with the
policy store and the corpus.
