# Collector-backed decision request registration

Decision evaluation currently consumes an immutable registered request. The next admission layer connects already enrolled collectors to operator-approved task profiles, while preserving exact source bytes and separating registration from execution authority.

## Intended public boundary

A registration selects an operator-provisioned immutable profile and registered observation IDs, with bounded original artifact bytes. The profile pins the task, evidence policy, builder/preprocessor, model release and source-resource policy. The server derives subjects, citations, picture, numeric input and request identity. Caller-selected observations define a measured selected-source set; they do not prove repository, PR or service completeness.

The collector registry retains artifact references, not original bytes. Admission must compare supplied bytes against the recorded artifact digest and size and retain exact originals through `decisionartifacts`. Missing or mismatched bytes fail closed. It must not fetch collector-named URIs or substitute extractor payloads for original artifact content. Neither code comments nor observation payloads can select execution policy or run a builder.

The initial material resolver accepts only the current enrollment. It validates collector, generation, exact enrollment, stored row domains, current quarantine and the original artifact reference. Superseded enrollment records are unavailable through this boundary until a trustworthy historical index exists. A mandatory authority separately checks source access, task purpose and combined disclosure; domain-level Read permission alone is insufficient.

## Durable preparation and readiness

The internal registration primitive freezes a preparation under the authenticated domain, original attribution and idempotency key. Its request lookup uses domain, subject and request ID, matching the existing artifact catalog's namespace. Neither namespace confers authorization.

Preparation retains the exact existing canonical artifact-record bytes and their digest before publishing the primary descriptor and request alias. The descriptor pins profile revision, builder, selected-source commitments, derived identities, original authentication fingerprint and accepted times. A interrupted write may leave an orphan preparation blob or a preparing registration. Exact retries reconcile those states; no timeout, deletion or invented rollback converts them into a successful ready request.

Readiness records exact durable artifact retention, not perpetual authority. A mandatory trusted guard validates the original record and catalog retention before the ready transition and on ready retries. The orchestration layer must perform a final joint current authorization check after all store IO before returning or serving the request. Storage success followed by denial still does not imply rollback.

Use separate catalog authority paths: the admission instance may recognize a preparing request solely to retain its original bytes; the execution instance requires a ready request. General `LoadAuthorized` or evaluation must not gain access merely because a preparing alias exists.

## Retry constraints

The existing catalog requires the current admission fingerprint to equal the fingerprint frozen in the picture. If grants change after preparation, an old pending request may no longer be retainable through that admission path. The first integration must fail closed, preserve its preparation and require a new key for a newly authorized request. It must not refresh the old request's timestamps or pins, forge the old decision, or replace its source/model identity. A future recovery API would need an explicit authority contract for retaining the exact original request under refreshed current rights.

An already retained ready request is still subject to current source/task permissions and quarantine on every read and execution. Valid hashes and durable bytes do not override revocation, establish source truth, grant training permission or permit promotion.

## Delivery boundaries

The preparation store and canonical codec helpers are internal building blocks. They do not supply the operator profile registry, source-purpose authority, builder integration, public registration endpoint or deployed host wiring. Those layers require integration tests with real collector enrollment, exact source bytes, the existing decision HTTP API, engine restart and durable receipt replay.

Required adversarial cases include digest/size substitution, collector-generation and enrollment changes, revocation during IO, preparing requests reaching execution, changed profile selection under one key, lost acknowledgements between each durable stage, refreshed grants during retry, and quarantine after successful inference. The source and review exemplars should share these contracts while retaining full-review fallback until operational evaluation justifies a change.
