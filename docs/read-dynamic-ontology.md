# Read-time dynamic ontology

This change implements the backend and governance portion of issue #279 as a
conservative read-time lens over immutable assertions. The source graph remains
schema-neutral and ingestion is not rejected merely because an unrelated
ontology does not describe it. Host integrations can select a lens through
`auth.DecisionConfig.SelectedOntology`; first-party workspace settings and UI
selection are intentionally tracked separately and are not claimed here.

## Source-evidence matrix

| Prerequisite from #279 | Source evidence at implementation baseline | Result |
|---|---|---|
| Version-qualified assertion meaning | `pkg/ontology/identity.go` defines schema+version identity; `Assertion` stores it; extraction persistence stores both IDs | Already shipped and preserved |
| Unknown identity is absorbing | `ReadOntologyUnder` returns `unresolved` and never substitutes an ambient version | Already shipped and preserved |
| Immutable original assertions | Assertion fields are private, constructors clone inputs, and persisted extraction records retain ontology IDs | Already shipped and preserved |
| Graph read path carries assertions | `Neighborhood.Assertions`, latent projection, extraction persistence, and authorized filtering already exist | Already shipped and preserved |
| Governed lifecycle and durable history | `GovernedProposal.Transition` plus append-only proposal transition records | Already shipped and preserved |
| Typed evolution vocabulary | No morphism type or safety classification existed | Added |
| Proposal-to-morphism linkage | Proposals carried only a proposed version | Added, immutable and persisted |
| Read-time interpretation | Reads could compare versions but could not map them | Added |
| Caller ontology selection | `auth.Decision` had no ontology field | Added |
| Cache isolation | fingerprints/cache keys omitted ontology selection | Added |
| Browser/report surface | neighborhood responses exposed originals only | Added interpretation reports |

## Product defaults

- Assertions and graph observations remain immutable; interpretations are
  separate result values.
- Only morphisms on published proposals participate in a lens.
- Published version transitions remain traversable even when an additive
  release needs no definition mappings.
- Widening is safe and monotonic. Narrowing is unsafe and resolves only when
  the assertion remains valid.
- Rename requires an explicit mapping. Split requires an explicit metadata
  discriminator. Merge is marked lossy while retaining the complete original
  assertion.
- Missing versions, missing paths, ambiguous paths, and unmatched split
  discriminators return explicit `unresolved` interpretations.
- A decision without `SelectedOntology` preserves the previous no-lens result.

## Integration API

- `ontology.NewOntologyMorphism` creates validated evidence-bearing morphisms.
- `ontology.NewOntologyLensWithTransitions` and `ontology.NewOntologyTransition`
  model governed version edges independently from definition mappings.
- `ontology.NewGovernedProposalWithMorphisms` binds them to the existing
  lifecycle.
- `auth.DecisionConfig.SelectedOntology` selects a schema+version and is part
  of `AuthorizationFingerprint` and `CacheKey`.
- `explorer.OntologyInterpreter.InterpretAssertions` applies published
  morphisms after authorization filtering.
- `webapi.NeighborhoodResponse.OntologyInterpretations` and
  `webapi.PathResponse.OntologyInterpretations` expose effective and original
  identities, safety-path IDs, and unresolved reasons.
- Proposal morphism drafts reference definitions by `namespace` plus `key`;
  evidence uses the same opaque-ID citation/path codecs as other Explorer APIs.
- Published proposal transitions form the durable active-version chain.
  Publication rejects stale bases, and `ActiveOntology` replays the chain after
  restart.
