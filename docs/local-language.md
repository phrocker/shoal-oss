# Language without an LLM

Design note; roadmap #497. This note proposes how Shoal could take text in and
give text out with no large language model and no GPU at the point of use: a
vocabulary derived from the knowledge graph, small CPU models, and templates over
Shoal's own records. Everything here is proposed; the parts it builds on are
cited, and their status is stated where it is not yet merged.

## The loop

```
 text ──► router ──┬─► action    ──► dispatch / admission / approval ─┐
                   ├─► decision  ──► registered decision task          ├─► records ──► renderer ──► text
                   ├─► lookup    ──► query template over the graph    ─┘
                   └─► abstain (with a reason)
```

Each stage is deterministic or a small model running on CPU. Each stage is
measured in shadow mode before anything relies on it, and each leaves a receipt.
An LLM, where one is configured, is an optional fallback for text the router
abstains on, and an optional rephraser of the renderer's output. It is never the
source of an answer or of authority.

## The rule everything follows

**Text selects; evidence decides.** The router's output is a *proposal*:

- which registered action, decision task or lookup the text refers to;
- the subjects and parameters it names.

The proposal never carries authority. An action still goes through admission,
effect ceilings and, where required, approval (`docs/approval.md`). A decision
is computed by a registered predictor over a measured picture of evidence
(`docs/decision-contracts.md`), not by interpreting the sentence. The router's
own target choice is itself a decision whose evidence is the text, but it only
selects; the downstream decision never sees the text. A misleading
phrasing can change which question is asked, never its answer. This is #419's
rule that model scores cannot assign policy authority, applied to language.

## Router: text to action, decision or lookup

The router has two jobs, and both are classic CPU problems once the target set is
closed.

**Choosing the target** is classification over a closed set:

- the actions registered on reachable descriptors;
- the registered decision tasks;
- the lookup templates.

It is served as a registered typed decision (a choice question with abstention)
through `pkg/decision`, on the local CPU predictor path the decision track has
prototyped (`experiments/local-ml/README.md`; #439 is a synthetic CPU-SVM
slice, #441 an offline replay of a frozen classifier; production serving is
still open).
Confidence below a threshold **abstains**.

**Filling the parameters** is slot extraction against the target's declared
input schema (an action's `input_schema`, a task's input schema, a template's
parameters). Tiers, cheapest first:

1. **Lexicon match** (below): entity mentions resolve to graph node IDs, never to
   free strings.
2. **Patterns and grammars** per target, for operational text such as "restart X
   in Y".
3. **A small encoder tagger** (below) for phrasing the first two miss.

The filled proposal must validate against the schema. Missing or ambiguous slots
abstain, naming what was missing; the router never guesses.

**Lookups** are deterministic query templates over the graph or ShoalQL, derived
from the graph schema. Each relation type yields questions in both directions
(for example a dependency edge yields "what depends on X" and "what does X depend
on"). The set of answerable questions is therefore explicit and enumerable.

## Vocabulary bundle: derived from the graph, portable with it

Everything below is built on CPU from a graph snapshot and a document corpus. It
is packaged with the snapshot it came from.

- **Lexicon.** Canonical names, aliases, abbreviations and types of graph nodes,
  compiled into a finite-state matcher. It links mentions in short text to node
  IDs in microseconds and cannot invent an entity. Exact matching misses typos
  and inflections, and an alias shared by several nodes is ambiguous; both go to
  slot abstention rather than a guess. Slice 1 is built: see
  [`lexicon.md`](lexicon.md) for the bundle format, scope-filtered matching
  and measured latency.
- **Tokenizer.** A subword vocabulary trained on the corpus (documents, guides,
  code comments), so internal identifiers and jargon tokenize sensibly.
- **Embeddings, CPU-trained.** Subword word vectors or count-based vectors for
  near-synonym matching; graph embeddings (random-walk or translational) for
  entity similarity and predictor features. The explorer's existing `lexical`
  embedding provider (`cmd/shoal-explore-web/main.go`) is the zero-training
  baseline that each of these must beat.
- **Templates.** Lookup templates from the graph schema, router targets from
  registered actions and tasks, and renderer templates (below).

## Small models: trained where compute exists, run on CPU

A bundle may carry quantized model weights, built elsewhere and run on CPU at the
destination. This follows the pinned-predictor pattern prototyped in #439 and
#441; #404 (a pinned local Laya adapter) is planned.

- **Small encoders, roughly 20–100M parameters (preferred).** Fine-tuned for
  target classification, slot tagging and semantic embeddings. Contrastive
  training on pairs derived from the graph (alias and canonical name, node and
  description, question template and relation) gives domain-aware semantic
  matching. Fine-tuning at this size is feasible on CPU for modest labelled
  sets; large contrastive runs over graph-derived pairs belong at the source.
  Inference takes milliseconds.
- **Small decoders, roughly 0.5–3B parameters (optional).** Useful only for
  rephrasing renderer output or for grounded answers with citations. Fine-tuning
  practically needs a GPU at the source. Quantized to 4 bits, a model of about
  3B parameters typically generates at single-digit to low tens of tokens per
  second on a server CPU, which bounds it to short outputs; the hardware floor is
  an open decision below. Their output is never authority: the
  template text and its receipt remain the source of truth.

**Training data** comes from three sources:

- verbalized graph facts;
- instantiated question templates with paraphrases;
- real usage as it accumulates: approvals and refusals (`docs/approval.md`),
  session-gateway observations (#447) and reviewed router abstentions.

The last is #401's loop.

**Models carry language skill; the graph carries facts.** Weights freeze a
snapshot, so current state is read from the graph at request time. A model's job
is to understand the question and rank or phrase the result. A model trained last
month must not answer from last month's topology.

## Renderer: records to text

Shoal's outputs are structured, with closed vocabularies:

- dispatch outcomes and error codes;
- approval states and conditions;
- decision results and abstention reasons;
- receipts with evidence references and coverage gaps.

Rendering them is data-to-text generation (content selection, ordering,
aggregation, surface realization). It is template-based and deterministic, and it
cannot assert anything that is not in a record.

- **Message catalog.** Templates keyed by (record kind, outcome, condition),
  with plurals, lists, durations and times handled properly and translation
  possible.
- **State machines narrate themselves.** Each transition has a sentence.
  Histories aggregate ("claimed twice; the first claim expired, the second
  succeeded"). "Why not yet?" comes from the guard blocking the next transition,
  and "what next?" from the outgoing transitions. Shoal's guards already return
  typed refusals and conditions.
- **Grounding.** Every sentence can carry the record ID and evidence references
  it came from.
- **Untrusted text is quoted and attributed, never interpolated as prose.**
  Action input, target responses and caller-asserted reasons appear as
  attributed quotations ("…asserted by alice"). Otherwise the renderer becomes an
  injection channel.

## Disclosure: the hard constraint

Derived artifacts disclose the data they were derived from.

- **A lexicon** reveals that entities exist. It must be built per authorization
  scope, or every match must be filtered against the caller's authorization
  before use, so an unauthorized entity matches as nothing. #373 gates questions
  whose entities resolve to unauthorized nodes, which is this rule; #374
  measures the related residue in grounded responses.
- **Embeddings** leak through similarity: a neighbour can hint at a hidden node.
  They are built only from data visible to the scope they serve.
- **The tokenizer** is trained on the corpus, and its learned merges can reveal
  frequent internal identifiers. **Templates** reveal relation types and
  registered actions. Both are built per scope like embeddings.
- **Model weights memorize, and cannot be filtered after training.** Deletion
  does not unlearn weights (#419). **A model's clearance is the union of
  everything it was trained on.** It may be shipped only to recipients cleared
  for all of it, or trained per scope.

The portable unit is therefore **graph snapshot + scoped vocabulary + scoped
models**, each pinned to the others.

## Provenance and promotion

Bundles will be release artifacts under the machinery the decision track plans
in #405 and #406, which is still open. The existing artifact catalog
(`docs/decision-artifacts.md`) retains request artifacts only; it neither runs
nor promotes models, and model weights need their own retained artifacts.
Requirements:

- content-addressed, with pinned build identity and reproducible builds;
- lineage to the graph snapshot and to each training source;
- poisoning tests and quarantine (#419);
- shadow evaluation before promotion;
- explicit promotion with rollback.

Automated training creates challengers only.

## Measurement

Each stage is measured in shadow mode before anything depends on it, as #409
plans for triage:

- **Router:** target accuracy, slot exact-match, abstention rate and coverage,
  with honest denominators, compared against the lexical baseline.
- **Lookups:** coverage of real questions by the template set.
- **Renderer:** a coverage test fails if any outcome, error code, condition,
  state or transition lacks a template; golden tests fix wording.

Cost and latency are measured on the target CPU class.

## Order

1. **Renderer** (#498) for approval, dispatch and decision records, with the
   coverage test. It depends on nothing new and is useful immediately.
2. **Lexicon bundle** (#499) with scope-filtered matching, plus lookup templates
   from the graph schema.
3. **Router v1** (#500): a CPU classifier over registered targets with lexicon
   and grammar slot filling, in shadow mode. The first decision target is #421's
   service-operation risk.
4. **Encoder bundle** (#501): fine-tuned target classification, tagging and
   embeddings, scoped and promoted through #405/#406.
5. **Optional decoder** (#502) for rephrasing, after the scoping rule is
   enforced.

## Non-goals

- Open-ended conversation. Text that matches no registered target abstains.
- Any model output carrying authority or bypassing admission and approval.
- Facts served from model weights instead of the graph.

## Open decisions

- **Scope granularity** for bundles: per authorization domain, per role, or per
  recipient set.
- **Abstention threshold policy:** per target or global, and who may change it.
- **Where the router runs:** as a fleet executor, or as a service in front of
  dispatch.
- **Encoder hardware floor** for the target environments, which fixes model size
  and quantization.
