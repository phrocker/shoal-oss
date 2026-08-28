# Ontology-guided extraction

`pkg/extraction` is the runtime boundary between a low-level
`model.TextGenerator` and the immutable contracts in `pkg/ontology` and
`pkg/inference`.

The orchestrator:

- accepts one validated ontology version and an exact, ontology-pinned
  `inference.ContextPack`;
- constructs a deterministic bounded prompt from ontology definitions,
  caller instructions, and cited evidence anchors only (the context query and
  metadata are not copied into the prompt);
- requires one strict JSON object and rejects unknown or duplicate fields,
  malformed or oversized bodies, excessive nesting and counts, invalid value
  types, non-finite confidence, duplicate entities or relations, unknown
  ontology members, disallowed relation endpoints, ungrounded node IDs, and
  evidence IDs outside the context pack;
- validates cardinality, ownership, uniqueness, patterns, allowed values, and
  numeric bounds through `ontology.ExtractionResult.ValidateFor`;
- creates `inference.InferenceResult` claims tied to the exact context anchors;
- derives stable entity, edge, assertion, and claim identities and marks every
  planned item `inferred` and `proposed`; and
- returns a publication plan without writing storage.

An existing graph node may only be referenced when its ID occurs in a supplied
graph evidence path. Its plan action is `reference`, so it is not recreated,
overwritten, or relabeled. New entities use `create`. Consumers must submit
plans through an atomic high-level Explorer publication API when one becomes
available; model output must never be translated directly into graph-row
writes.

`HeuristicExtractor` is an explicit no-model option with `heuristic`
provenance. It is never invoked as a fallback after a model timeout,
unavailability, malformed response, or validation failure.

`internal/agentmem.StructuredEnricher` adapts validated entity proposals to the
legacy Veculo entity shape. Consolidation no longer interprets model response
substrings. It can produce a structured plan and optionally hand it to a
caller-supplied high-level publisher; without that publisher it performs no
graph mutation.
