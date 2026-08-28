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
- applies fail-closed result, evidence, document, section, graph, path, quote,
  metadata/provenance, and total-context limits; and
- exposes explicit `OpenSection` and `ExpandNeighbors` operations that produce
  a new immutable pack. Duplicate evidence is canonical-deduplicated only when
  its immutable anchor identity and content are identical.

Callers may supply already authorized public document views and neighborhoods,
or let the builder hydrate missing values through `contextpack.Reader`.
Unauthorized and absent content therefore retain the reader's
indistinguishable not-found behavior. The package contains no storage rows,
Accumulo visibility expressions, credentials, raw grants, generated prose, or
model/provider calls.
