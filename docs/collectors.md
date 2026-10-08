# External collectors

This is slice 1 of #446. It lets an out-of-process collector register as an
evidence source, report raw artifact references, and attribute each
observation to the extractor version that produced it, through public
contracts only. It follows "Core and extensions" in `docs/gateways.md` and the
authority rules of #419.

Nothing here is mounted in an existing binary. A host constructs the registry
and calls `MountCollectors` itself, as for decisions (`docs/decision-http-api.md`).

## Packages

| Package | Role | May import |
| --- | --- | --- |
| `pkg/collector` | Contracts: `Registration`, `EnrollRequest`, `ExtractorRef`, `ArtifactRef`, `Observation`, `Confidence`, `AttestationReport`, `AttestationResult` | stdlib, `pkg/shoal` |
| `pkg/collector/api` | Wire types, strict decoding, Go client | `pkg/collector`, `pkg/shoal` |
| `pkg/sdk` | Versioned facade: `sdk.New(...).Collectors()` and `.Decisions()`; `sdk.ProtocolVersion = 1` | `pkg/shoal`, `*/api` |
| `internal/collectorregistry` | Engine-backed registry, `Provider` wire adapter, `Source` adapter | core |
| `internal/collectorattest` | Attestation verifiers | core |
| `pkg/explorer/webapi` (`collectors.go`) | HTTP routes | core |

`pkg/decision` itself is not an extension dependency: it pulls `internal/`
packages transitively. Extensions use `pkg/decision/api`.

## Authority

Authority is assigned in one place: `Registry.Provision`, a trusted Go API with
no HTTP route. It binds a collector ID to one authenticated principal (subject,
client ID and authorization domain), an authority ceiling (a set of authority
policy IDs), a `Control` and a `Mode`.

- `Control` mirrors `decision.Control` by value (`candidate_controlled`,
  `external_controlled`, `registry_controlled`, `unknown`). It says who
  controls the observed content; it is not a trust level.
- `Mode` is `server_observed` or `imported`: whether the collector saw the
  activity happen or imported records produced elsewhere.

Enrollment (`POST /api/v1/collectors/enroll`) states the authority subset the
collector wants, the extractors it runs and optionally an attestation report.
A request for any authority ID outside the ceiling is **refused**
(`403 permission_denied`) and nothing is written; it is never trimmed to fit.
Control, mode and the ceiling are not request fields; a body carrying them is
rejected as unknown fields. No field carries a model score, and extraction
confidence never maps to authority.

A collector's identity (authorization domain, subject and client ID) is fixed
for the life of its collector ID. Provisioning an existing ID with a different
identity is a conflict, even after revocation; use a new collector ID. Revoked
collectors may be re-provisioned with new authority, control or mode under the
same identity.

Only the provisioned principal can enroll or submit for a collector. Any other
principal, including a delegated decision for the right subject, gets
`404 not_found`, as if the collector did not exist. Writes require
`auth.OperationIngest`; reads require `auth.OperationRead`. No new operation was
added.

## Artifacts and observations

`POST /api/v1/collectors/artifacts` records an `ArtifactRef`: collector-chosen
ID, SHA-256 digest, size, media type and observed time. Shoal records the
reference only; retaining the bytes is #447.

`POST /api/v1/collectors/observations` records an `Observation`. Its ID is
derived (`collector.NewObservation`) from the collector, the artifact, the
extractor ID and version, and the content (subject, kind, payload, confidence,
observed time). Consequences:

- A new extractor version over the same artifact is a new observation with
  the same artifact lineage. Earlier observations are never overwritten.
- Resubmitting identical content returns the original receipt.

An observation is accepted only from the collector's provisioned principal,
only for an extractor its current enrollment declares, and only over an
artifact that same collector recorded in its current generation.

Artifacts are stored per generation: an artifact ID used in an earlier
generation is invisible to the next one, which records its own reference.
Every artifact and observation row also stores the authorization domain it was
written in.

`Confidence.Disposition` is `extracted`, `low_confidence` or `unextractable`,
with an optional value in [0, 1]. It is how sure the extractor is that it read
its input, not a judgement about the content.

## Revocation and quarantine

`Registry.Revoke` (trusted Go API) increments the collector's generation and
marks it revoked. Every artifact and observation records the generation it was
written under. `GET /api/v1/collectors/observations/{id}` computes
`status` from the collector's current registration: `quarantined` when the
collector is revoked or the generation differs, otherwise `active`. Rows are
never rewritten. Revoked collectors cannot enroll or submit. Re-provisioning
starts the new generation; the old generation stays quarantined, its
artifacts cannot carry new observations, and resubmitting an identical
observation first recorded in it is refused rather than re-activated.

Any caller with `OperationRead` in the authorization domain stored with an
observation may read it. Reads are authorized against that stored domain, never
against a later registration. Finer read scoping (by authority policy) is not
implemented in this slice.

## Retries

Every write is idempotent and reports `indeterminate` (`503` with
`Shoal-Commit-Outcome: indeterminate`; `api.ErrIndeterminate` in the client)
when the durable outcome is unknown. Retry with the identical request.

- Enroll takes an `Idempotency-Key`. Same key and request return the original
  receipt, including after a later enrollment superseded it and after its
  attestation expired (expiry is applied when observations are mapped, below). A different
  request under the same key, or a key first used in an earlier generation, is
  a conflict. Re-enrolling (new key) within the ceiling replaces the current
  enrollment without changing the generation.
- Artifacts are keyed by collector and artifact ID, observations by their
  derived ID; neither takes an `Idempotency-Key`. A different artifact under an
  existing ID is a conflict.

The three write routes are listed in `requestMayCommit`
(`pkg/explorer/webapi/workspace_settings.go`), so an over-budget response is
reported as indeterminate rather than as a failure. Their receipts carry IDs,
generation, state and receipt time, never submitted payloads or evidence, so
their size does not grow with what was submitted. With every ID at
`shoal.MaxIDBytes` the largest receipts measure 1600 bytes (enroll), 2860
(artifact) and 2982 (observation)
(`pkg/collector/api/receipt_size_test.go`). A workspace `OutputBytes` budget of
at least `api.MaxReceiptBytes` (3072) therefore always returns a readable
receipt. Below the size of a particular receipt, a write that committed
reports indeterminate: safe, because a retry returns the same receipt, but
unreadable under that budget. Content is read back with the GET route, which
commits nothing and is not listed.

## Attestation

`internal/collectorattest.Set` dispatches a report by kind:

- A kind with a configured verifier is **verified or refused**. A refusal
  fails the enrollment with nothing written; it is never downgraded to a claim.
- A kind with no verifier is recorded as a **claim**: the evidence digest is
  kept, no verifier is named, and `AttestationResult.ID()` is empty, so a claim
  cannot become a `decision.Source.AttestationID`.

The one verifier, `ed25519-statement`, checks a statement signed by an
operator-configured Ed25519 key over the collector ID, image digest, issue and
expiry times, and a nonce `collector.EnrollNonce(collectorID, key)` bound to the
enrollment's idempotency key. Forged signatures, expired or not-yet-valid
statements, validity windows above the configured maximum, a different
collector and a nonce minted for another key are refused. Because an
idempotency key cannot be reused across generations, a captured statement
cannot be replayed after re-provisioning either.

**Residual trust.** This verifier proves only that the holder of the operator
key vouched for this enrollment. It does not prove which code runs: the
operator's signing process and key custody are trusted. Nitro, SGX/TDX and
cosign verification are later verifiers behind the same interface. Results
are stored as provenance; no policy consumes them yet.

## Mapping to decision sources

`collectorregistry.Source` maps an active observation onto `decision.Source` by
value: `OriginID` is the collector, `AuthorityPolicyID` must be granted by the
observation's enrollment and still provisioned, `Control` is the provisioned
control. `AttestationID` is set only for verified attestation, and only when
the observation's observed and received times both fall within the
statement's `[IssuedAt, ExpiresAt)`. An enrollment lasts until revocation but
its attestation does not; outside that window the attestation has lapsed
(`collectorregistry.AttestationLapsed`) and the source carries none. This is an
**internal adapter, not an agreed contract**; how pictures consume collector
observations belongs to the decision track (#418).

## Boundary

`internal/importboundary` parses source files (it does not need the go command,
so it works with `GOWORK=off`) and fails when:

- A. any package in the root module (not only `pkg/`, `internal/` and
  `cmd/`) imports `extensions/`;
- B. an extension module (any `go.mod` under `extensions/`, at any depth)
  imports a repository package outside `pkg/sdk`, `pkg/collector`,
  `pkg/collector/api`, `pkg/decision/api`, `pkg/shoal`; or declares a module
  path other than the repository module plus its directory (a module naming
  itself `.../internal` would otherwise exempt its own imports, and Go's
  `internal` rule does not protect this tree from such a module); or replaces
  anything other than the repository module with a relative path to this
  tree; or a Go file under `extensions/` sits outside every extension module;
- C. the in-repository import closure of those packages contains `internal/`.

The scan descends into every directory, including `testdata`, `vendor` and
names beginning with `_` or `.`. `go build ./...` skips those, but Go still
compiles them when a package imports them by explicit path. The only
directories skipped are nested modules (those with their own `go.mod`) and the
repository's `.git`. The checker's fixtures need no special case, because each
fixture tree is a nested module. Any symlink in the root module or under
`extensions/` is a violation. The walk does not follow symlinks, but the go
command and `go.work` do. The repository currently contains none.

An extension's `go.mod` is parsed with every known directive recognized and
parentheses split from adjacent tokens (`replace(` counts the same as
`replace (`). An unknown directive, an unbalanced or nested block, or any
other line the parser cannot read exactly is reported as a violation, never
skipped.

Fixtures under `internal/importboundary/testdata` prove each rule detects a
violation. CI also vets and tests every module under `extensions/` on its own
`go.mod`, and fails if a core package links `golang.org/x/crypto/ssh`, an RDP
library or Guacamole (a failing `go list` fails that check rather than passing
it).

`extensions/example-collector` is a synthetic file-tail collector that imports
from Shoal only `pkg/sdk` and the allowlisted `pkg/collector` and `pkg/shoal`. Run it against a host that provisioned it:

```sh
SHOAL_TOKEN=... go run ./extensions/example-collector \
  -base-url https://shoal.example -collector collector:tail \
  -authority authority:logs -file /var/log/app.log
```

## Deferred

- Public admission and report contracts in the SDK, and moving
  `cmd/shoal-llm-gateway` onto it (after #443/#451).
- Policy requiring verified attestation for `EffectMutatesExternal` executors
  (with ATPL `runtime`, #457); hardware and code attestation verifiers.
- HTTP provisioning and revocation. These need an admin operation kept out of
  every OIDC-minted mapping, as `OperationExecute` is.
- Raw artifact byte retention (#447).
- Building picture sources from observations (#418) and propagating quarantine
  into datasets, calibrations and releases (#419).
