# Rendering records as text

`pkg/narrate` renders Shoal records as English sentences, deterministically and
without a model (#498). It is the renderer described in
[local-language.md](local-language.md#renderer-records-to-text). It narrates:

| Record | Function | What it says |
| --- | --- | --- |
| `fleet.ApprovalStatus` | `Approval` | the effective state, the stored state when it differs, the condition, the history, what the request is waiting on, what can happen next |
| `fleet.ActionRecord` | `Action` | the dispatch state, the error code of a failure, evidence returned, what the record cannot establish, how it got here, what blocks the next step, what can happen next |
| `[]fleet.ActionTransition` | `ActionHistory` | one sentence per committed transition, with consecutive claims aggregated |
| `decision.PredictionRecord` | `Prediction` | the result, why a whole request abstained or failed, each answer, the picture's evidence gaps, the next step |
| `decision.EvidenceEligibility`, `decision.InspectionRanking` | `Eligibility`, `Ranking` | which subjects may be scored, and every reason a subject is held for ordinary inspection |
| `interaction.Session` | `AssertedReason` | the caller-asserted reason, quoted and attributed |

Nothing else is narrated. The package reads no clock, network or model; a
narration is a function of the record and `Options`.

## Output

Each function returns `[]Sentence`:

```go
type Sentence struct {
    Role     Role     // outcome, reason, gap, history, blocked, next, detail
    Key      string   // the catalog message it was realized from
    Text     string
    RecordID string   // "<kind>:<id>"
    Refs     []Ref    // evidence, records and principals it was derived from
    Quotes   []Quote  // the untrusted spans inside Text, in order
    Spans    []Span   // byte offsets of every quoted span and bare identifier
}
```

Sentences are ordered by role, which is the content-selection rule: what
happened, why, what is missing, how it got here, what blocks the next step,
what can happen next, then quoted detail. Lists are capped
(`Options.MaxListItems`, then "and N more"), and so are a prediction's
per-answer sentences (`Options.MaxAnswers`).

`RecordID` and every `Ref.ID` are plain tokens (`[A-Za-z0-9]` then
`[A-Za-z0-9._:@/+-]`, ending alphanumeric). An identity that is not one, such
as caller-chosen action-ID bytes, is written as `hex:` and its bytes, so a
reference can be printed safely and still resolved.

A sentence that names a principal or a claim carries it as a reference: the
requester (`principal`, `request`), the approver (`principal`, and the
decision's `request`), the claimant (`principal`, `claim`), the decision
requester (`principal`, `release`, `correlation`) and the asserting caller.

`Spans` marks every bare identifier (`identifier`) and every quoted span
(`quote`) by byte offset, so a reader can style record data apart from the
catalog's words — an agent named `approved` is then visibly an identifier.

## The message catalog

The catalog is data: [`pkg/narrate/catalog/en.json`](../pkg/narrate/catalog/en.json),
embedded at build time. A translation is another file of the same shape loaded
with `ParseCatalog`; no Go changes.

```json
{
  "locale": "en",
  "formats": { "time": "2006-01-02 15:04:05 UTC", "group": ",", "decimal": "." },
  "messages": { "dispatch.error.outcome_unknown": "The failure was reported as …" }
}
```

Keys are `<record kind>.<part>.<outcome>[.<condition>]`, for example
`approval.outcome.unresolvable.target_moved`, `dispatch.error.target_rejected`
and its `.next`, `decision.reason.failed.deadline_exceeded`. Messages use a
subset of ICU MessageFormat, implemented in the package (no new dependency;
plural categories come from `golang.org/x/text/feature/plural`, already in
`go.mod`):

| Syntax | Value |
| --- | --- |
| `{name}` | a rendered fragment, number, time or duration |
| `{n, number}` | integer (grouped per locale) or a probability |
| `{t, time}` | always converted to UTC and formatted with the locale's layout; a zero time is "an unrecorded time" |
| `{d, duration}` | the two most significant units, truncated toward zero; a negative duration is refused, not printed as its magnitude |
| `{xs, list}`, `{xs, list, or}` | joined with the locale's list patterns |
| `{n, plural, =0 {…} one {# …} other {# …}}` | CLDR plural rules of the locale |
| `{s, select, a {…} other {…}}` | a closed-set selector |

Apostrophes follow ICU's optional-quoting rule: `''` is an apostrophe and `'{`
starts a quoted literal; any other apostrophe is literal.

A probability is rounded to three places unless that would print 0 or 1 for a
value that is neither; then it is printed exactly, so 0.9996 never reads as
certainty. Coverage counts are always exact `N of D`, never percentages.

## The coverage rule

Every outcome, error code, condition, state, transition and decision result has
a template. It is enforced three ways:

- **At load.** `ParseCatalog` refuses a catalog that lacks any key in
  `RequiredKeys()`, carries an extra key, has a pattern that does not parse, or
  reads an argument the English message does not read, or reads it as a kind
  the renderer does not supply. A plural is allowed only where English reads
  that argument as a plural, because `number` also carries probabilities and a
  float cannot select a plural form. A translation therefore cannot fall back
  to an unreviewed sentence, or fail, at run time.
- **Parity with source.** The vocabularies are read from their owners' source
  by AST, never from this package's lists: `DispatchState`, `ApprovalState`,
  `ApprovalCondition`, the kinds `NewActionTransition` accepts and every
  value assigned to an `ErrorCode` in `pkg/explorer/fleet` (literals and
  constants; any other expression fails the test, except a copy of another
  `ErrorCode` field); `ResultStatus`,
  `AnswerStatus`, `AnswerKind`, `Disposition` and `InspectionReason` in
  `pkg/decision`; the error-code constants of `internal/effectsgateway`; and the
  `terminal(…)` reasons of `internal/decisionservice`. The status table in
  [approval.md](approval.md#status-reports-the-effective-state) is parsed too,
  and so is `ApprovalService.effectiveState` itself: the (state, condition)
  pairs it can return, with `current.State` and the `unreachable` conditions
  resolved, must equal `EffectiveApprovals`. Every state fleet sets a record to
  must be the destination of a `DispatchEdges` edge, and every edge's kind must
  be the one `actionEventKind` gives its destination. Constants are read whether
  declared with their type (`X T = "x"`) or converted (`X = T("x")`), and a
  constant of the type the reader cannot evaluate fails the test.
  Adding a value in any of those places without a template here fails
  `TestParity*` and `TestCoverage*`.
- **By rendering.** `TestCoverage*` renders a record for every value read from
  source — every dispatch state, with and without admission and at five
  clock positions; every effects-gateway code including `target_rejected_400`
  to `_599`; every fleet-written code; every dispatch transition edge; every
  approval status row from every stored state it can arise from; every result
  status, service reason, answer kind, abstention, disposition and inspection
  reason — and fails if any falls back. `TestCoverageEveryKeyIsReachable`
  fails if the catalog has a template nothing can say.

The effects gateway's closed `ErrorCode` set is mirrored as constants
(`GatewayErrorCodes`, `TargetRejectedStatus`) rather than imported, so this
public package does not depend on `internal/`; the parity test imports the
gateway and checks `ValidErrorCode` agrees on every candidate code.

Golden files under `pkg/narrate/testdata/golden` fix the wording. Change them
with `go test ./pkg/narrate -run TestGolden -update` and review the diff.

### Values the renderer does not know

A fleet record is decoded from storage a newer build may have written, so an
unknown dispatch state, transition kind or approval state is rendered by an
`.unrecognized` template that quotes the value. A decision record can only be
built by `pkg/decision`'s validating constructors, so an unknown value there
means the two packages are out of step, and the renderer returns an error.

An `ErrorCode` outside the gateway and fleet sets is the executor's own text:
an executor may record any bounded string. It is quoted, attributed to the
executor, and its next step is to reconcile with the target. The same holds for
a predictor's own whole-request or per-answer reason.

### Reports are not findings

Fleet checks only an `ErrorCode`'s length and whitespace, so an executor can
report any code, including a gateway code or one fleet itself writes
(`invalid_executor_output`, `executor_error`). The record does not say who
assigned it (#508). Every error sentence is therefore phrased as a report — "The
failure was reported as outcome_unknown, which, if accurate, means …" — and
never states a gateway or fleet meaning as fact. Retry advice is derived only
from the record's `EffectPossible` and state: while an effect is possible, the
next step is always to reconcile with the target, whatever the code says.

Likewise, the decision service writes its own whole-request reasons and passes
a predictor's through unchanged, and the result does not say which (#509). A whole-request abstention or failure reason
is attributed to "the predictor or the service" (`predictor_or_service` on a
quote), its meaning is conditional ("if accurate"), and its next step starts
"If …". A success is "reported as succeeded by" the claimant, or the admitted
caller for an admission.

## State machines

`DispatchEdges` and `ApprovalEdges` are the transition tables. Each edge has a
history template (`dispatch.transition.<edge>`, `approval.transition.<edge>`)
and, if it leaves a state, a next-step fragment (`….next.edge.<edge>`).

- **What next** is the list of outgoing edges from the current state.
- **Why not yet** comes from the guard blocking an edge. With `Options.Now`
  set, the dispatch guards are evaluated as `pkg/explorer/fleet` evaluates
  them — completion needs a live lease, a re-claim or cancel needs a lapsed
  one, a claim needs the deadline ahead — and blocked edges become `blocked`
  sentences. Without `Now`, nothing time-dependent is asserted and each option
  states its condition. Approval conditions are their own guard: the condition
  is the reason and `approval.next.<condition>` the remedy.
- **Histories aggregate.** An `ActionRecord` carries no per-claim history, but
  its claim fence counts claims, and a re-claim is only possible after the
  previous lease lapsed, so "claimed 3 times; the first 2 leases lapsed without
  a report" is derived from the record. In `ActionHistory`, a run of
  consecutive claims becomes one sentence.

## Untrusted text

The renderer must not become an injection channel. Message arguments are typed:
a bare Go `string` is refused, so text reaches a sentence only through one of
three constructors:

- catalog text, numbers, times and durations;
- **identifiers** (agent, action and principal IDs, labels, units): shown bare
  only if they are short plain tokens, otherwise quoted;
- **quoted spans** for everything a caller, executor, target, predictor or
  evidence builder supplied: action input and output, unrecognized error
  codes, predictor reasons, subject reasons, caller-asserted reason codes and
  sources.

A quoted span is attributed in the sentence ("Input supplied by alice: “…”",
"alice asserted the reason code “…”; Shoal records this but does not verify
it") and in its `Quote` (`Attribution`, `By`). `By` is a principal ID from the
record, so it is escaped by the same rules and bounded to 64 runes. Inside it a backslash is
doubled; the quotation marks, the ASCII quote and the ellipsis are escaped;
every non-printable rune (controls, newlines, bidirectional and other format
characters, separators other than the ASCII space, private-use and unassigned
code points) and a combining mark at the start are written as `\u{…}`;
invalid UTF-8 is written as `\x…`. At most `Options.MaxQuoteRunes` (default
120, ceiling 2048) runes are shown, followed by an unescaped `…` and
`Truncated`.

Patterns are parsed once at load and values are inserted into the parsed tree,
so braces, `#` or apostrophes in a value are text, not syntax.
`TestUntrustedTextCannotAlterStructure` renders every untrusted field with
newlines, carriage returns, closing quotes, template syntax, bidi overrides,
zero-width and tag characters, separators, controls, invalid UTF-8, combining
marks, fake ellipses and escape look-alikes, private-use code points and
28 KB strings, and requires the same sentences, roles and keys, identical text
outside the quoted spans, no unprintable character anywhere (including in
`Quote.By`), spans that cut the text where they say, and token-only record and
reference IDs.

The renderer reads no clock. `TestNoModelNetworkOrClock` pins the package's
direct imports, refuses `time` imported under any other name, and resolves
selectors by import path to deny `Now`, `Since`, `Until`, `After`, `Tick`,
`NewTimer`, `NewTicker`, `AfterFunc`, `Sleep`, `LoadLocation`, `Local` and
`t.Local()`.

## Follow-ups

- Per-claim attempt history (claimant, lease, outcome per attempt) is not on
  `ActionRecord` on main; aggregation uses the claim fence. When attempt
  history lands (#438/#430), add an `ActionRecord` history sentence per
  attempt.
- Neither `ErrorCode` nor a decision result records who assigned the code or
  reason: #508 (fleet: an action record doesn't say who assigned its error
  code) and #509 (decision: a result doesn't say whether the service or the
  predictor set its reason). Once they are resolved, codes and reasons Shoal
  itself assigned can be narrated as Shoal's determinations.
- The source state of each `DispatchEdges` edge is checked against fleet's
  guards by review, not by test: the guards are spread through fleet's
  services. A transition table exported by fleet would let the test check them.
- Decision-service reasons are string literals in `internal/decisionservice`;
  exporting them as constants would let this package reference them directly.
- Only English ships. Wording should be reviewed by someone outside the
  project before a translation is made from it.
