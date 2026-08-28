# Grounded inference contracts

`pkg/inference` is Shoal's public boundary for grounded generation. It is
independent of storage engines, RPC schemas, model vendors, and provider SDKs.
Runtime LLM orchestration and low-level text or embedding provider transports
are not implemented by this package.

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
