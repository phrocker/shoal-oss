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
| granted | `claimed` | `action.claimed` |
| refused | `canceled` | `action.canceled` |
| reported | `succeeded` / `failed` | `action.completed` / `action.failed` |
| outstanding | `claimed`, unreported | — |

This is not an economy. A remote worker holding a claim and a proxy holding an
admission token are the same situation: an out-of-process actor holding
permission Shoal granted, whose outcome Shoal has not yet seen. A second record
type would have given the two different durability and a second place for them
to disagree.

The consequence worth stating: an admission and a claim are the same kind of
thing, so the same machinery serves both. They are still told apart — see the
admission marker below — because the two surfaces close them differently.

### An admission is never queued

The table has no `queued` row, and that is the stop rather than an omission.

Every check runs before anything durable is written, and the answer is then
committed as exactly one record at version one — claimed for a grant, cancelled
for a refusal. Writing a queued record first and deciding afterwards would open
a window in which an ungranted, possibly refused, admission exists as an action
waiting for a worker. `Pull` returns it to the same principal, `Claim` on the
dispatch surface hands out a live claim against it, and under the execution
boundary a claim is permission to perform the declared effect out of process. A
refusal would become permission through a different door.

Cancelling the queued record when a later step fails does not close that
window: the cancel is a second mutation that can fail for the same reasons the
first step did, and when it does the orphan is still there. The only safe
failure mode is having written nothing — the caller gets a transport error and
fails closed, which is what a caller of this surface must do with an
unreachable decision plane anyway.

One integration consequence: `action.enqueued` is never published for an
admission. Anything reading the lifecycle stream — the accumulator in #389 in
particular — must key on `action.claimed` and `action.canceled`.

### The record carries what was admitted

`ActionRecord.AdmittedEffects` holds the declared effect set and
`AdmittedDisclosures` a digest of the declared corpus references, and both are
part of `equivalentEnqueue` — the record's retry identity.

Without that, a caller could ask with corpus references, receive obligations
restricting them, then replay the same action ID, idempotency key and token
with the references removed: the record would be recognised as the same
request, obligations would be recomputed over nothing, and the reply would be
an unrestricted allow for a token that is already live. The declaration has to
be pinned by the record, not merely adjudicated on the way past it. A retry
that changes it is a conflict.

The references are a digest rather than a list because they are corpus
identities the caller supplied, and copying them into a dispatch record would
put a caller's claimed reading list somewhere the team overview reads. Refusing
a changed retry needs only equality.

`AdmittedObligation` holds the obligation the grant returned, as a bitmap over
the canonical order of the declared references. **A replay returns that
obligation; it never recomputes one.** The co-occurrence budget is windowed and
moves as an identity reads, so recomputing would let a caller replay its way
into a weaker obligation while holding the same live token — and would make an
already-granted admission unrecoverable for as long as the restrictor was
unreachable, which is the case a retry exists for. Positions rather than
identities, for the same reason the declaration is a digest: a position
discloses nothing without the list it indexes, and the caller supplies that
list again on the retry, where the digest proves it is the same one.

### An admission is distinguishable from a dispatch action

`AdmittedEffects` is the marker — an admission is refused before it reaches a
record unless it declares an effect, and no dispatch enqueue ever sets one.

Both read paths check it. `report` refuses a dispatch action as not-found:
without that, a claimed dispatch action owned by the same caller passes every
other check and gets completed under admission's semantics rather than its own,
and those differ where it matters — a reported failure is a receipt here and
the executor's error there, so a worker would be told its failed work had
succeeded. `outstanding` filters on it too, because listing a record this
surface refuses to close would name work the caller cannot act on.

This narrows a claim made earlier in this document. Before the marker existed,
an outstanding admission and an outstanding claim genuinely were
indistinguishable; they are not any more, and the surface no longer pretends
otherwise.

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

A report is either an outcome or a failure, never both, and all three ways of
violating that are refused: a failure without an error code, an outcome
carrying one, and a failure carrying an outcome. The last matters most — the
completion path discards a failed report's outcome, so the record would say
nothing about it and the replay comparison would read any two failures with the
same error code as the same report.

The error code and the outcome are both validated *before* anything is
completed. The completion path is written for an executor that has already
performed the work, so it records a malformed result as
`invalid_executor_error` or `invalid_executor_output` rather than refusing it.
Reaching that from here would spend a live one-shot token on a failure the
caller was never told about and leave it unable to report the outcome it
actually has. Nothing about the underlying call is known from a malformed
report, so it is refused with no durable transition and the token stays usable.

**Reporting a failure is a successful report.** The completion path is built
for an executor, where a failed outcome is the executor's error and is returned
alongside the committed record; here the failure is the news, not an error in
delivering it. So a committed record that matches the report is a receipt.
Propagating the completion path's error instead would make the first response
to a reported failure an error and the identical retry a receipt, so what the
caller saw would depend on whether its own report had committed — the exact
confusion the one-shot token exists to remove. The conversion is guarded by the
replay comparison itself: an outcome the service cannot confirm it wrote stays
an error.

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
