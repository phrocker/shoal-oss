# Shoal Explorer public contract

## Status and scope

This document is the concise normative, storage-neutral contract for public
Shoal document, graph, retrieval, error, and code-ingestion values. The terms
**MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

`pkg/explorer.Client` currently exposes `Ingest`, `Documents`, `Document`,
`Connect`, `Neighborhood`, and `Retrieve`. Those operations form a useful
embedded/product facade, but they are not yet a general document/graph storage
repository contract. Storage publication, history, deletion, durable policy
persistence, migration, and mutation APIs remain additive work.

`pkg/explorer/authorized.Client` is the M2 authorization-enforcing wrapper for
that facade. Deployments must use it with a trusted `pkg/explorer/auth`
resolver, policy selector, policy catalog, and generation reader; constructing
or retaining an unwrapped client remains an explicitly unauthenticated path.

## Identity, bytes, and bounds

- `shoal.ID` is an opaque byte string. Implementations MUST retain every byte,
  including NUL and non-UTF-8 bytes accepted through direct Go values.
- IDs order lexicographically by unsigned raw bytes. Implementations MUST NOT
  apply Unicode normalization, case folding, trimming, parsing, regeneration,
  or a generic ID-generation scheme.
- A public ID is required where its containing value says so and is at most
  1,024 bytes.
- Metadata is opaque application data, not an authorization channel. Per
  object it is limited to 256 entries, 256 bytes per key, 4 KiB per value, and
  256 KiB total. Validation MUST NOT rewrite accepted keys or values.
- Scores and graph weights MUST be finite.
- Kind, type, title, heading, and source-version strings are at most 4 KiB.
  Graph nodes MAY have an empty kind. A node has at most 64 nonempty labels,
  each at most 256 bytes.
- Transport encodings MAY impose additional representability checks. In
  particular, protobuf string fields require valid UTF-8; that wire rule does
  not redefine the direct Go ID contract.

## Documents, revisions, and source ranges

- `(Document.ID, Revision.ID)` identifies one immutable revision.
  `Document.RevisionID`, `Revision.DocumentID`, and every section/span owner
  MUST agree.
- `Revision.CreatedAt` and `Revision.SourceVersion` are source metadata. They
  are not publication sequence, commit order, a latest-winner rule, or an
  `AsOf` frontier.
- Canonical revision source is valid UTF-8. `SourcePosition.Offset` is a
  zero-based UTF-8 **byte** offset. `SourceRange` is half-open
  `[Start.Offset, End.Offset)`.
- Empty ranges are valid, including `[len(source), len(source))`. Both
  endpoints MUST be in bounds and on UTF-8 rune boundaries.
- A revision contains exactly one connected, acyclic section tree. Its root
  is `Document.RootSectionID` and has empty `ParentID`; every other section has
  one same-revision parent.
- Child section ranges are contained by their parent. Span ranges are
  contained by their section. Child sections and direct spans share one
  sibling `Order` namespace; duplicate orders are invalid.
- `Span.Text` MUST equal the exact canonical source byte slice named by its
  range. Spans are not a source reconstruction format.
- Initial hard maxima are 512 MiB canonical source, 100,000 sections, and
  1,000,000 spans per revision.
- These rules apply at new operation boundaries and explicit validation.
  Implementations MUST NOT reinterpret old persisted alpha records merely
  because they are passively loaded.

## Citations, quotes, and evidence

- `Citation.Validate` is structural: document/revision identity, at least one
  section or span anchor, bounded IDs, and a structurally valid range.
- Contextual resolution additionally requires the exact retained revision,
  canonical source bytes, and referenced sections/spans. If both section and
  span are present, the span belongs to the section, and the cited range is
  contained by every named object.
- A canonical quote is exactly
  `source[Start.Offset:End.Offset]`. Submitted quote text, indexes, spans, and
  the latest revision are not quote authority.
- Citation validation helpers do not imply that a storage implementation can
  hydrate retained source. Missing hydration is an unavailable capability,
  not permission to approximate a quote.
- `retrieval.Evidence` structurally requires a valid citation, a finite score,
  and, when present, a valid directed `graph.Path`. Canonical quote equality
  remains a document-context check.

## Graphs, paths, and neighborhoods

- `graph.Edge` is directed from `From` to `To`. Cycles, self-edges, and
  parallel edges with distinct IDs are valid.
- `graph.Path.Edges[i]` MUST run from `Path.Nodes[i].ID` to
  `Path.Nodes[i+1].ID`. Repeated nodes and directed cycles are valid; reverse
  or mixed walks are not paths.
- For `B <- A -> C`, a neighborhood rooted at `B` MAY contain `A`, `C`, and
  the canonical edges when its requested depth/direction permits. It MUST NOT
  report `[B,A]` or `[B,A,C]` as a `graph.Path`.
- Incoming or mixed traversal is therefore neighborhood-only unless every
  emitted path step is directed from the current path tail. No replacement
  public walk type is defined.
- The current embedded `Neighborhood` expands both incoming and outgoing
  edges, normalizes depth zero to one, rejects depth above 16, and serializes
  final nodes by raw node ID and edges by raw edge ID. It does not return
  paths.

## Reserved `interaction.*` namespace

Interaction provenance lives in the same corpus as content, under a reserved
node-kind and edge-type namespace. Phase 1 of issue #266 defines:

- Node kinds `interaction.session`, `interaction.turn`, `interaction.tool_call`,
  and `interaction.tombstone`. Content MUST NOT use the `interaction.` kind
  prefix; ingest and `Connect` reject it.
- Edge types `interaction.has_turn`, `interaction.has_tool_call`,
  `interaction.retrieved`, and `interaction.cited`. Retrieved and cited are
  distinct: retrieval records everything the model was shown, citation records
  only what the answer referenced. Recording citations alone would understate
  exposure.
- Retrieval and graph expansion exclude interaction nodes by default. An
  interaction node is returned by `Neighborhood` or `BoundedNeighborhood` only
  when it was an explicit seed in the request, and never by `Retrieve`. The
  exclusion lives in the retrieval path itself, not in caller convention, so a
  model cannot cite its own prior output as source evidence.
- `Interactions` and `InteractionSubgraph` are the explicit, kind-scoped entry
  points for reading interaction provenance.
- The `shoal.visibility` property of an interaction node is the conjunction of
  every visibility label of every source node the session touched, retrieved as
  well as cited. It is never derived from the asker's grant set, because that
  would turn a highly cleared asker's session into a covert channel. An
  unresolvable touched node fails the record rather than defaulting to public.
  A reviewed declassification path is deferred to a later phase.
- Capture is part of serving an inference. `EnsureInteractionSink` reports at
  setup time whether a corpus can durably record interactions; a read-only or
  offline corpus refuses `ask` outright with a clear diagnostic rather than
  failing at first write. A recording failure fails the request.
- Retention is explicit deletion only; there is no TTL. `DeleteInteraction`
  replaces a session's nodes and edges with an `interaction.tombstone` node
  carrying the original visibility, so the deletion is itself auditable. A
  deleted session ID can never be reused.
- Interaction nodes and edges are excluded from the content snapshot hash, so
  concurrent capture never invalidates a pinned snapshot.
- Interaction records are redacted by construction: they carry identities,
  digests, counts, and source node IDs, never the question, prompt, answer,
  evidence text, or model-chosen correlation strings.

### Fold summaries

Phase 2 of issue #266 adds fold-style summarization, following the public
`fold` semantics of `phrocker/sag`: a set of recorded sessions collapses into
one compact vertex that can later be unfolded back into what it replaced.

- A fold is an `interaction.fold` node with `interaction.folds` edges to the
  sessions it folds, plus `interaction.retrieved` and `interaction.cited` edges
  to the source nodes those sessions touched. Because the kind and the edge
  types are in the reserved namespace, every default-exclusion rule that
  applies to a session applies to a fold unchanged: `Retrieve` never returns
  one, expansion never discovers one, and the visibility resolver refuses to
  treat one as source evidence for a later inference. A model cannot cite its
  own prior summary as though it were a source document.
- A fold's visibility is the conjunction of every folded session's own
  visibility and every label of every source node those sessions touched,
  retrieved as well as cited. Summarizing can therefore only narrow visibility,
  never widen it. That is precisely why publishing a redacted public summary
  must be a separate, explicit, reviewed action; **no declassification
  primitive exists**, and the authority model it would need is still open in
  issue #128.
- Fold identity is content-addressed over the canonical folded provenance and
  the summary digest. The same input always folds to the same vertex, so
  refolding is idempotent rather than a conflict. Neither the fold time nor the
  derived visibility participates in identity, so a corpus whose labels moved
  under an existing fold fails closed instead of serving a stale visibility.
- `FoldInteractions` names sessions only. The provenance it folds is read from
  what those sessions actually recorded, never from the caller, so a caller
  cannot understate what a session was shown and thereby widen the fold.
- `RehydrateFold` is lossless with respect to provenance: every folded session
  comes back with its retrieved set and its cited set kept apart. Collapsing
  the two would understate what the model was shown and make the visibility
  conjunction unsound.
- A fold carries no summary text, only the SHA-256 digest of a summary held
  out-of-band. The digest shape is enforced, not trusted, so the field cannot
  be used to smuggle evidence-derived text into a node payload.
- Retention matches sessions: `DeleteFold` leaves an `interaction.tombstone`
  and drops the folded members, and a session that a live fold summarizes
  cannot be deleted until that fold is.
- Folding is an operator action, not part of serving an inference, so it never
  sits on the request latency path.

### Cross-session provenance traversal

- `InteractionsTouching` lists every session and fold that retrieved or cited a
  given source node. `RelatedInteractions` walks from one session or fold to
  every other interaction that touched at least one of the same source nodes.
- Both are explicit, kind-scoped reads, like `Interactions` and
  `InteractionSubgraph`. Neither is reachable from `Retrieve` or from expanding
  a source node's neighborhood: walking provenance is an operator and auditor
  capability, never something an inference can do to itself.
- The walk goes interaction to source node to interaction. `InteractionsTouching`
  refuses an interaction node as its subject, because treating derived content
  as the origin of a provenance walk is the same mistake as treating it as
  source evidence.
- `shoal-explore fold`, `shoal-explore unfold`, and `shoal-explore provenance`
  are the operator-facing surface for all of the above.

## Retrieval requests

### Normalization and limits

`Request.Normalize` returns cloned values and MUST NOT mutate caller-owned
slices. Normalization is idempotent:

- `TopK == 0` becomes 20; normalized `TopK` is at most 1,000.
- Empty modes become exactly lexical.
- Duplicate modes and duplicate scope IDs collapse by first occurrence.
- Query text is nonblank valid UTF-8 and at most 16 KiB.
- Combined normalized document/node scope cardinality is at most 10,000.
  Scope IDs are nonempty and individually obey the public ID bound.
- Nonzero `AsOf` represents the same instant normalized to UTC.

Equivalent normalized requests have the same request identity where an
implementation exposes one. Duplicate modes MUST NOT change scores.

### Scope and modes

- IDs are ORed within `DocumentIDs` and within `NodeIDs`.
- When both lists are nonempty, a candidate satisfies both dimensions only
  through a canonical document/graph association. Metadata, labels, string
  equality, or coincident IDs MUST NOT fabricate that association.
- Requesting vector mode requires vector support. An implementation that does
  not support it returns `unavailable`; it MUST NOT silently drop vector,
  substitute lexical retrieval, or claim an approximate result as exact.
- The fixed seed planner is lexical, then tree, then graph:
  - standalone tree requires document scope or bounded lexical seeds;
  - standalone graph requires node scope, implemented document-association
    seeds, or bounded earlier lexical/tree seeds;
  - a shape with no bounded seed source returns `unavailable` before corpus
    scanning. It MUST NOT fall back to a corpus-wide tree or graph seed scan.

### `AsOf`, completeness, and order

- `AsOf` requires a store-defined publication frontier. It MUST NOT be
  implemented by comparing `Revision.CreatedAt`.
- A store without publication-frontier semantics returns `unavailable` for an
  explicit `AsOf`. The current embedded Explorer does so.
- `Retrieve` is unary and complete-or-error. It has no partial-result envelope
  or cursor.
- Results order by score descending, then raw result-ID bytes ascending.
  Result IDs are nonempty, bounded, and unique, and result count does not
  exceed normalized `TopK`.
- Evidence orders by score descending, then
  `(document, revision, section, span, start offset, end offset)`, then the
  directed path tuple of node IDs followed by edge IDs.
- `pkg/retrieval` exposes versioned analyzer and scorer contracts. The shared
  embedded analyzer version includes its Unicode table version, and the shared
  coverage/fusion scorer fixes the lexical, tree, and graph formulas. An
  adapter claiming those versions MUST produce bit-identical terms and scores.
- A future duplicate-result merge function MUST preserve these boundaries.
  No adapter may claim a shared analyzer/scorer version while substituting
  storage-local behavior.

## Canonical logical records

- `pkg/explorer/canonical` defines the versioned deterministic logical record
  encoding used for cross-adapter comparison. It is distinct from the
  embedded persistence envelope and contains no rows, columns, visibilities,
  iterators, or tablet coordinates.
- Canonical bytes preserve opaque IDs and metadata exactly, sort metadata keys
  by unsigned bytes, retain caller-supplied slice order, and carry exact source
  bytes. The envelope includes magic, version, kind, payload length, and a
  SHA-256 checksum.
- A present publication coordinate has a positive signed-64-bit sequence and
  nonzero publication time. It is separate from `Revision.CreatedAt`.
- A canonical document-revision record is complete: graph edge endpoints must
  name nodes in that record, public values and ownership validate, IDs are
  unique within each value family, and unknown versions or malformed,
  oversized, truncated, checksum-broken, or trailing data are rejected.
- The encoding is injective: each logical record has exactly one valid byte
  representation, so equal records always have equal digests and distinct
  bytes never decode to equal records. Decoding therefore rejects any payload
  that is merely decodable rather than canonical — such as permuted metadata
  entries or an alternate long-form encoding of a zero time — even when its
  envelope checksum is consistent. `Digest` is a content identity and is safe
  to use for deduplication, idempotency, and integrity equality.

## Latest state, mutations, and retries

Current public values contain no generic epoch or tombstone field. Future
additive storage APIs use this vocabulary:

- latest selection chooses the greatest visible published generation at the
  requested frontier **including tombstones**, then interprets that winner;
- create requires expected base `ABSENT`; update/delete name the exact base;
- generic mutations carry a nonempty idempotency token of at most 128 bytes,
  scoped by operation/domain; token reuse with different canonical content is
  `conflict`;
- a lost response resolves to the committed token outcome and MUST NOT create
  new identities or duplicate artifacts.

These are requirements on future APIs, not permission to add epoch/tombstone
fields to existing public values.

`pkg/code` retains its existing typed stable-ID algorithms. `Source.Ref` is
excluded from `Source.ID` and the ingestion key because multiple refs can
resolve to one immutable revision. Retrying a committed identical ingestion
returns `IngestUnchanged` with identical artifact identities. A materializer
version change cannot reinterpret an existing key; it honors the committed
outcome or uses a future versioned operation/key contract.

`pkg/explorer/codematerializer` implements `explorer-code-v1`. It derives the
document title and source URI only from identity-bound repository/path values,
excludes mutable refs from canonical output, preserves existing code entity
and relationship IDs byte-for-byte, and returns document then graph artifact
references in stable order. Its additive association values describe the
document/graph relationships that a later storage adapter must persist.

## Authorization and non-disclosure

- `pkg/explorer/auth.Decision` is immutable trusted input. It binds subject,
  actor/client, authorization domain, allowed operations, source/policy
  grants, policy generation, expiry, request identity, and an optional
  least-privilege service role. Metadata, retrieval scope, graph labels, URI
  text, and claimed object ownership never add grants.
- Trusted context binding is capability-scoped: a binder from one
  `auth.Authority` cannot forge values accepted by another authority's
  resolver. Missing, expired, wrong-domain, or wrong-operation whole-request
  authority returns `unauthorized`; cancellation and deadlines retain their
  stable categories.
- Direct hidden objects return the same `not_found` shape as absent objects.
  Collections omit hidden values. Retrieval intersects scope with the
  authorized current-document/node projection before ranking, so hidden
  candidates cannot displace results or influence scores, explanations, or
  limits.
- Vector-space observability is projected at that same boundary.
  `authorized.Client.RetrieveWithReport` and the request-local
  `authorized.WithEmbeddingQueryObserver` callback report only distinct spaces
  reached after current policy and mosaic filtering. An authorized space is
  represented by a process-keyed opaque ID and one of `available`,
  `unavailable`, `not_attempted`, or `not_completed`; raw provider/model
  identity and metadata are never returned. A hidden-only projection performs
  no provider call and exposes no space ID. The existing `Suppressed` and
  `Restricted` document counts remain the complete permitted disclosure about
  withheld content; the embedding report adds only corresponding booleans, not
  hidden-space identities or counts. Generation is rechecked before release,
  and cancellation, expiry, revocation, or a failed generation read scrubs
  observed IDs and activity counters.
- `webapi.RetrievalResponse.embedding` carries the same report on success.
  Failed vector retrieval returns an `EmbeddingQueryError`; HTTP places its
  report beside the stable error code, and `RemoteService` reconstructs it.
  The wire encodes opaque space IDs with the standard unpadded base64url ID
  codec. Error text is generic and never substitutes raw space identity for
  the opaque report.
- Ingesting a source URI that already identifies a registered document
  requires ingest authorization under that document's current rule. A caller
  who cannot see the document is refused with the same `not_found` shape as an
  absent one, so a source URI cannot be probed, seized, or relabeled. A caller
  the current rule authorizes may reclassify the document, because the newly
  selected policy must also authorize that same caller. Shared source-URI
  claims are acquired or compare-and-swap transitioned before base mutation,
  serialize concurrent wrapped clients, and retain the selected rule when a
  base commit outlives a policy-catalog failure. Legacy registered documents
  lazily backfill claims only after current-rule authorization; an existing
  document with neither a claim nor registration is unavailable. An explicitly
  indeterminate base commit releases operation ownership but leaves a pending
  claim containing the selected rule and any previous rule. First-ingest
  recovery requires the selected rule; reclassification recovery requires both
  rules and must select the pending desired rule, preventing either side from
  seizing an ambiguous outcome. One recovery attempt reacquires exclusive
  ownership. Definite failure restores the pending state, another indeterminate
  result keeps it pending, and successful current registration commits the
  desired rule. M3 supplies durable outcome reconciliation across processes.
  Only a returned current revision may finalize a claim transition; registering
  an unchanged historical revision restores the previous source-wide claim.
  M2 claim finalization is local and atomic: commit, pend, and rollback of an
  exact acquired token are infallible and idempotent for the same outcome.
  Finalization errors therefore mean an invalid token or catalog invariant
  violation and are never discarded. Fallible durable coordination and
  recovery belong to M3.
- Structured policy labels are canonical lowercase no-padding base32 terms
  for domain, source, policy/epoch, and trusted service role. Callers cannot
  submit visibility expressions. Service-account ceilings intersect required
  labels and service roles cap allowed operations.
- Policy generation is rechecked before mutations and before response
  serialization. A changed or unreadable generation returns `unavailable`;
  old contexts cannot mutate. A newly issued decision may still authorize
  immutable objects written under an earlier physical policy epoch.
- Graph edges require their own policy plus the current policies of both
  endpoints. Neighborhood reachability is recomputed after authorization so a
  hidden intermediary cannot reveal or bridge visible nodes. Wrappers sharing
  a policy store serialize base mutations, reserve each edge ID and selected
  rule before the base write, and retain that reservation after ambiguous or
  catalog failures so another selector cannot seize or relabel the edge.
- Backend retrieval values are not trusted merely because their IDs are
  authorized. The wrapper verifies registered document digests, exact
  citations/quotes/ranges, canonical path nodes/edges, the exact canonical
  top-K prefix, and the shared analyzer/scorer explanation before returning
  them. Backend request IDs are cleared rather than exposed.
- Authorization fingerprints, cache keys, audit values, and public strings
  contain digests/categories rather than raw queries, source text, quotes,
  IDs, labels, credentials, physical coordinates, or serialized responses.
  Audit value pseudonyms use an ephemeral process HMAC key so low-entropy
  values cannot be recovered by hashing guesses and rotate on process restart.
- `authorized.MemoryPolicyStore` is an M2 reference catalog that can be reused
  across an in-process embedded restart. It is not durable process recovery;
  M3/M4 must atomically persist policy/publication state before production
  adapters claim restart or failover safety.

## Stable public error categories

Only the existing categories are used:

| Condition | Code |
|---|---|
| Invalid value, static bound, malformed range/tree/path | `invalid_argument` |
| Absent or hidden individual object; unavailable retained history | `not_found` |
| Idempotency/precondition/base mismatch | `conflict` |
| Whole-operation authentication or authorization denial | `unauthorized` |
| Unsupported requested capability/shape, transient store failure, or runtime budget exhaustion | `unavailable` |
| Caller cancellation | `canceled` |
| Deadline elapsed | `deadline_exceeded` |
| Detectable committed-data corruption or implementation fault | `internal` |

Object-level authorization, when implemented, is indistinguishable from
absence. Error text MUST NOT expose storage coordinates or hidden identities.

## Store-dependent and deferred behavior

The contract above does not claim current implementation of:

- a storage-neutral document/graph repository, generic mutation API, latest
  publication frontier, tombstone persistence, history retention, or
  citation hydration;
- durable authorization-policy persistence, canonical association
  persistence, pagination, migration, or distributed
  publication/conditional-write coordination;
- an Accumulo schema or adapter;
- vector indexing, alternative scorer implementations, or a new graph
  walk/path shape.

Adapters advertise and test only the capabilities they implement. Missing
store-dependent behavior returns the stable public error appropriate to the
condition; it is never approximated by reinterpreting source timestamps,
metadata, graph labels, or another retrieval mode.
