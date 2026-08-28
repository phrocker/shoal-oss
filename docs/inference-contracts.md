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
deterministic scripted actions and faults for tests. Evaluation records contain
provenance plus redacted action identities and digests; they exclude prompts,
credentials, raw authorization grants, URLs, storage coordinates, shell, and
filesystem access.

Harness `Generate` returns a `Record` containing both the final
`InferenceResult` and the exact expanded `ContextPack` in its transcript.
Because iterative tools can add evidence, returning the result alone through
the base `inference.Generator` would lose the context required by
`InferenceResult.ValidateFor`.

No Copilot, SDK-process, or other hosted execution backend is bundled yet. A
future backend can implement `Runner` without changing inference contracts;
the package deliberately includes no arbitrary subprocess runner.
