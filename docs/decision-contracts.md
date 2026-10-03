# Decision task and picture contracts

`pkg/decision` is the first production-package slice of #402. It constructs
immutable, content-addressed task definitions, measured evidence pictures,
predictor identities, requests and validated prediction records.
It does not yet serve predictions or provide the #418 HTTP API. Existing
experimental models and persisted Mosaic records are unchanged.

A `TaskSpec` pins owner, version, input schema, evidence/label/evaluation policies,
prediction/label/action units and aggregation rules. Questions distinguish choice,
ordered rubrics and proposition probability. Questions and unordered labels are
canonical sets; ordinal label order changes identity. Registry services must
resolve these references to immutable, authorized definitions.

A `PictureManifest` binds an existing validated `inference.ContextPack` to an
observation run, inventory/scope, builder, ontology projection, exact input digest,
tokenizer/budget and availability cutoff. The pack already pins exact evidence,
source snapshot, authorization and any source ontology. The ontology projection
reference identifies the feature-building contract separately. All references
in the new JSON-addressed configs require bounded UTF-8; existing Shoal opaque
ID contracts are unchanged.

Every supplied subject has evidence or an explicit unsupported/missing/out-of-scope
disposition. Every pack anchor is accounted for. Document evidence must match its
source artifact and revision. `InventoryCoverage` computes supported/total over
this supplied inventory, not undiscovered real-world dependencies. Additional
measurements preserve their units, methods and unknown denominators. Known zero
is distinct from unknown; neither is a completeness claim.

Source observations retain artifact/revision/digest, common origin, evidence role,
author control, authority-policy reference and optional attestation reference.
Predictions stay distinguishable from observations and approved procedures.
Observation time must be at or before receipt time, and receipt time must be
at or before the cutoff (`ObservedAt <= ReceivedAt <= Cutoff`). The cutoff
boundary is inclusive; authorization expiry remains exclusive. Timezone and monotonic-clock details
do not change identity; a later receipt or collection does. The existing source
snapshot and its historical `AsOf` remain unchanged. Acquisition time is not a
source event timestamp; an old incident can be acquired during a new observation.

Constructors validate structure and reference consistency. They **do not** prove
that digests match retained bytes, that attestations are authentic, that an
inventory is exhaustive, that graph edges came from the claimed sources, or that
a caller may read or train on the evidence. The #403/#418 service must verify
those facts, stamp receipt times, apply current authorization and resolve trusted
registry policy; #419 owns adversarial tests. A supplied role or authority-policy
reference is never permission. The cutoff/auth-pin check is historical structural
validation, not current permission to retrieve a receipt.

The configs and all getters are copy-isolated. Count bounds and conservative
aggregate byte preflight limit copying/serialization; the final encoded record
is also bounded. IDs use a versioned namespace and the normalized structs encoded by Go
`encoding/json.Marshal`, including its struct field order and escaping rules.
This is deterministic within this Go contract, not a language-independent
canonical JSON standard; clients must not assume ordinary JSON serialization
in another language reproduces these digests.
These are Go contracts, not a public wire or persisted storage codec: future
HTTP/storage adapters must define explicit versioned envelopes and must not JSON
marshal the private immutable objects directly.

## Two consumers of the same contracts

| Consumer | Task and units | Training support |
| --- | --- | --- |
| Source analysis (#407) | Authorization relevance/priority over revision-pinned declarations and dependencies | Source witnesses and verified defects become separately typed labels under the task's label policy. |
| Review guidance (#409) | Inspection priorities over the complete source inventory | Attributed review proposals are independently adjudicated; no-comment, approval and merge do not mean clean. |

Both refer to a task and picture without adding PR or GitHub concepts to core.
A non-code service-prerequisite task is exercised in the contract tests; it does
not demonstrate an operational risk classifier. Neither a valid task nor a valid
picture authorizes exclusion or execution.

## Predictor, request and result contracts

`PredictorIdentity` binds artifact digests for weights, tokenizer and environment,
plus runtime, formatting, preprocessing, calibration, effective device/precision
and batch policy. The required preprocessing reference pins normalization, feature
extraction, chunking and truncation rules separately from output formatting.
A versioned uncalibrated configuration is valid; identity is not proof of quality.
The registry must verify artifacts and supported runtime settings. Provider-native
rounding tolerance is explicitly bounded and separate from replay tolerance.

`DecisionRequest` binds a validated task, matching picture, predictor and resolved
release to a principal, correlation, subject set and deadline. It checks membership,
chronology and answer-count bounds. Request admission also uses the same response
byte accounting as result validation, reserving space for every pair to return
any task label without a distribution, a proposition probability, or an abstention
with reason `unavailable`. `MaxAnswers` is an upper count limit, not a promise that
every ID/label combination fits. Optional distributions and longer reasons must
still fit the actual response budget. The subject set is explicit: a consumer can
request a subset, but this never implies permission to omit other subjects from
review. The future service authenticates the principal, resolves the release and
reserves idempotency against this request before inference.

`PredictionRecord` validates the returned request/predictor identities and device.
Completed responses cover every requested subject/question pair exactly once.
Choice/ordinal results may return labels alone, allowing classifiers that do not
produce probabilities. If supplied, a distribution must include exactly the task
labels, finite values in [0,1], and a sum within the pinned rounding tolerance.
Native values are preserved; selected labels are not recomputed from argmax.
Proposition questions require a finite probability in [0,1]. Model confidence,
ranking margins and calibrated probability must not be substituted for each other.

Abstention carries a reason and no prediction. Unknown or unsupported subjects and
truncated input cannot receive scored answers in this first conservative contract.
This does not yet implement the task's broader evidence-eligibility policy.
Whole-request failure/abstention carries no partial results. A timeout/failure can
be recorded after the deadline; successful results cannot. Recording historical
failure grants no right to retrieve it after authorization expires. Times, device
and provider identity are executor assertions until the service attests them.

These records are not durable receipts. The store must still commit one result per
reserved request, authenticate outcomes, recover uncertain requests and enforce
current access. No predictor is executed by these constructors, and valid output
does not authorize exclusion or operational permission.

## Remaining delivery

#402 still needs ranking/aggregation semantics, full task-specific evidence
eligibility and serving-adapter integration. #403 owns durable receipts,
idempotency, attributed outcomes and authorization. #418 exposes those through
an authenticated client/API slice. #405/#406 implement authorized training and
release lifecycle; #419 enforces trust boundaries. The offline showcase remains
the measured model baseline until those integrations are implemented.

Validation: `go test -race ./pkg/decision ./pkg/inference ./pkg/contextpack`.
