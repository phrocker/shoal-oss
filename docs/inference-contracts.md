# Grounded inference contracts

`pkg/inference` is Shoal's public boundary for grounded generation. It is
independent of storage engines, RPC schemas, model vendors, and provider SDKs.
`pkg/inference/harness` implements a bounded adapter for trusted tool-using
agent runtimes. Low-level text and embedding transports remain separate.

## Contract

- `EvidenceAnchor` is a discriminated immutable value containing exactly one
  exact `document.Citation` plus quote, or one native `graph.Path`.
- `ContextPack` owns a normalized query, canonically ordered evidence,
  optional ontology schema/version identity, a snapshot pin, an authorization
  fingerprint pin, and bounded metadata.
- `Claim` contains a stable content-derived ID, subject, predicate,
  `ontology.Value`, confidence, observed/inferred status, exact evidence
  references, and model/prompt provenance.
- `InferenceResult` binds canonical claims and unresolved or unsupported
  entries to one context pack.
- `Generator` is the high-level interface applications implement to turn a
  `ContextPack` into an `InferenceResult`.

Constructors validate UTF-8 semantic values, nested document and graph
contracts, finite confidence, time ranges, count and byte ceilings, duplicate
canonical keys, canonical ordering, and evidence membership. Returned slices,
paths, and metadata are defensive copies.

Model provenance records provider, model, optional version, canonical
parameters, and an optional seed. Prompt provenance records template identity,
version, and hash. Neither type contains credentials or raw prompt text.

Applications remain responsible for resolving a document citation and checking
its quote against retained canonical source with
`document.ValidateCitationQuote`. They also remain responsible for executing a
model, enforcing authorization at retrieval time, and implementing the
`Generator` interface.

## Tool-using agent harness

The harness exposes typed `retrieve`, `open_section`, `neighbors`, and `stop`
actions only. It validates logical IDs, correlation, canonical tool results,
snapshot and authorization pins, evidence additions, final claim anchors,
steps, elapsed time, observable token usage, graph hops/fan-out, repetition,
and cancellation. Unknown actions and mismatched or stale results fail closed.

`Runner` and `Session` are provider-neutral boundaries. `FakeRunner` supplies
deterministic scripted actions and faults for tests. `ModelRunner` adapts a
low-level `pkg/model.TextGenerator` into the same loop by prompting for one
strict JSON action per iteration, parsing only the allowlisted tools, and
constructing final `Claim` or `Issue` values through `pkg/inference`
constructors. Every JSON protocol ID is base64-encoded opaque bytes before it
is shown to the model and decoded before request/result construction, so
non-UTF-8 Shoal IDs round-trip losslessly. Malformed JSON, arbitrary tools,
and final claims that cite evidence outside the assembled context fail closed
instead of being passed through.

Budgets are caller-visible and enforced by the harness: model-call steps,
wall-clock duration, input token preflight through a configured estimator (or a
conservative byte count), input/output token usage reported by the provider,
retrieved evidence anchors, graph hops, graph node count, per-action fan-out,
and repeated-action cycles. Cancellation and deadline errors are returned
explicitly; provider failures are reported as unavailable with no fallback.
Each `Run` returns a trace with iterations, decisions, tool evidence IDs,
budget consumption, stop reason, and failures, including usage for model calls
that return invalid JSON or mismatched provenance.
For compatibility, omitted evidence and graph-node budgets default from the
fan-out budget.

`ExplorerToolHost` connects those actions to an already authorized Explorer
client. Retrieval preserves the original snapshot `AsOf`, section opening
hydrates only issued document/revision/section IDs, and neighbor expansion
requires `explorer.BoundedClient` so depth, fan-out, and node caps are enforced
before graph materialization. Tool results carry the original snapshot and
authorization pins; clients that expose authorization-pin validation are
checked before and after tool execution so the loop never widens visibility.

Evaluation records contain provenance, budgets, action kinds, and token usage
only; they exclude prompts, correlation IDs, tool inputs/results, credentials,
raw authorization grants, URLs, storage coordinates, shell, and filesystem
access.

The harness implements `inference.Generator`. Its returned result remains
bound to the supplied context pack and carries canonically verified evidence
additions, so `InferenceResult.ValidateFor` succeeds for the caller's original
pack. `Run` additionally returns a `Record` with the exact expanded context and
full in-memory transcript. Callers must not log that record; only the optional
`Recorder` receives the separate redacted `EvaluationRecord`.

No Copilot, SDK-process, or other hosted execution backend is bundled. Future
backends can implement `Runner` without changing inference contracts; the
package deliberately includes no arbitrary subprocess runner.

## Explorer context construction

`pkg/contextpack` implements the deterministic construction layer between
authorized Explorer retrieval and `inference.ContextPack`. Its builder:

- validates and canonically orders retrieval results before preserving exact
  citation/quote and graph-path evidence;
- rehydrates and revalidates every available citation and path through the
  public `Document` and `Neighborhood` interfaces;
- binds the pack to caller-supplied snapshot, authorization, policy metadata,
  and optional ontology identities without deriving access from UI filters;
- records content-derived retrieval/result/explanation identity without
  placing explanation prose in model context;
- applies fail-closed result, evidence, document, section, span, hydration-byte,
  graph, path, quote, metadata/provenance, total-context byte, and optional
  caller-tokenizer limits; and
- exposes explicit `OpenSection` and `ExpandNeighbors` operations that produce
  a new immutable pack. Duplicate evidence is canonical-deduplicated only when
  its immutable anchor identity and content are identical.

Callers may supply already authorized public document views and neighborhoods,
or let the builder hydrate missing values through `contextpack.Reader`.
Callers that configure `MaxContextTokens` also supply a provider-neutral
`TokenEstimator`; the builder invokes it only on the completed immutable pack
and never contacts a model provider.
Unauthorized and absent content therefore retain the reader's
indistinguishable not-found behavior. The package contains no storage rows,
Accumulo visibility expressions, credentials, raw grants, generated prose, or
model/provider calls.
