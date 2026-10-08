# Lexicon bundle

Slice 1 of #499, under the vocabulary bundle in
[`local-language.md`](local-language.md). `pkg/lexicon` builds a lexicon from a
graph snapshot; `pkg/explorer/authorized` resolves mentions against it under the
caller's current authorization.

## What a bundle holds

- **Terms.** For each node: `name` (else `title`), `shoal.ontology.entity_key`,
  and every `shoal.lexicon.alias.<n>` property (a reserved key prefix; any
  non-empty suffix). With `Input.DeriveInitialisms` set (off by default), a
  `name` or `title` of three or more tokens also gives an initialism, tagged
  `origin=derived`. Labels are not used yet. Each term keeps
  its node IDs, node kinds and strongest origin
  (name < title < entity_key < alias < derived).
- **Lookup templates.** From ontology `RelationshipDefinition`s: two per directed
  relation (`out`: what does X *rel*; `in`: what *rel* X), one per undirected
  relation (`both`). Each holds the relation key, subject and answer concept IDs,
  and a phrase key. The wording belongs in a renderer catalog, not in the bundle.
- **Pin.** Snapshot `(ID, Frontier, AsOf)`, the normalization version, the
  Unicode table versions, the build flags (whether initialisms were derived),
  and the scope. Two builds that differ only in a flag have different IDs.

## Normalization

`Tokenize` applies NFKC, full case folding, then NFKC again, and splits on
maximal runs of letters, numbers and combining marks. Fullwidth forms, ligatures
and `ß` normalize (`ＡＢＣ` → `abc`, `ﬁle` → `file`, `Straße` → `strasse`);
`payments-api`, `svc_v2.prod` split at punctuation; camel case is not split.
Folding does not depend on locale, so `İ` folds to `i` plus a combining dot,
not to `i`. `NormalizationVersion` and the x/text and stdlib Unicode versions
are written into every bundle, so a change to tokenization, or a toolchain that
brings other Unicode tables, gives a new bundle ID. A bundle built under other
tables is refused at load. Typos and inflections are not matched; they abstain.

## Determinism and the codec

`Build(Input, Limits)` gives byte-identical output for the same content in any
order of nodes, properties and relationships. The encoding is fixed-width
big-endian with length-prefixed strings: magic `shoal-lexicon-v1`, versions,
build flags, pin, scope, a sorted token table, a sorted node table, terms sorted by token
sequence with sorted postings, and sorted templates. The bundle ID is the
SHA-256 of those bytes. `Load` checks every ordering and reference invariant,
re-encodes, and refuses anything that is not byte-for-byte the canonical
encoding. The matcher is then rebuilt from the decoded terms. `LoadVerified`
also checks the bytes against an expected ID. A golden ID is pinned in
`pkg/lexicon/testdata`. It depends on the Unicode tables, which x/text selects
by Go toolchain version (15.0.0 before go1.27, 17.0.0 from go1.27), so the
move to go1.27 changes it, and every bundle ID, by design.

Limits **fail the build**; nothing is dropped. A lexicon that quietly omitted
an entity would make that entity look unknown, and in a server-filtered bundle
whether something was dropped would depend on hidden nodes. Defaults: 8 tokens
per term and 8 postings per term. The hard caps are 16 and 64.

Initialisms are the exception that would make this rule fail in practice:
short names collide quickly (every "Payments API" and "Public Access" gives
`pa`), so a large graph would exceed 8 postings per term on derived terms
alone. Derivation is therefore opt-in, and when enabled it uses only names of
three or more tokens. A collision beyond the limit among those still fails the
build.

## Matching

The matcher is an Aho-Corasick automaton over token IDs, in-repo. `Candidates`
returns every term occurrence, overlapping ones included, with token and byte
spans and all node IDs and kinds. `Select(candidates, visible)` first drops
nodes that are not visible, then candidates left with none, and only then
chooses leftmost-longest spans. A span with two or more visible nodes is marked
`Ambiguous` and lists them all; it is never resolved to one.

## Scope and disclosure

`Bundle.Scope()` is one of:

- `ScopeServerFiltered`: built from nodes of any visibility. It is used only
  through `ResolveMentions`, which filters every candidate against current
  policy. Its bytes cannot be exported: the package exposes bytes only through
  `Shippable`, which only `ForShipping` on a pinned bundle returns.
- `ScopePinned{Digest}`: built from a node set already filtered for one scope.
  `Client.LexiconScopeNodes` is that filter: it reads the builder's candidate
  nodes against the caller's current rules in one batch. Filtering at match time
  cannot protect bytes someone already holds, so only this kind ships.

`Client.ResolveMentions(ctx, bundle, text)` makes a hidden node match as nothing,
the same as an unknown name:

1. It authorizes the caller for `OperationNeighborhood` (a dedicated resolve
   operation is planned).
2. It bounds the input to `MaxMentionBytes` (4096) and `MaxMentionTokens` (32).
   Every caller gets the same generic `InvalidArgument` for over-bound input.
3. It requires the bundle's worst case for 32 tokens to fit in
   `MaxCandidateIDs` (2048). That bound depends only on the bundle.
4. It reads the policy store in one `Nodes` call of exactly `MaxCandidateIDs`
   IDs, padded with sentinel IDs under a prefix `Build` refuses. The number of
   calls and the batch size do not depend on what matched or what is hidden.
5. It drops candidates the store does not know or whose current rule denies,
   then runs `Select` over the visible nodes, then the generation guard.

Tests compose the real client with memory and durable policy stores. They check
that results, errors, store-call counts and batch sizes for a hidden name equal
those for an unknown name of the same shape. They also cover a shared alias with
one hidden node (unambiguous), a hidden longer alias over a visible shorter one
(the shorter matches), a rule change or revocation after the snapshot (dropped),
and a scoped build holding no hidden bytes.

## Disclosure residuals

Two known ways a hidden node still differs from an unknown name. Both are
accepted for slice 1 and belong with the residue #374 measures.

- **Timing.** Store traffic is fixed (one call, 2048 IDs), but per-ID work is
  not. The memory store clones each registration it finds, and the client
  evaluates its rule; a sentinel or unknown ID is a miss. On the benchmark host
  a matched hidden node costs a few microseconds more than an unknown name.
  Closing this needs a store read whose cost does not depend on presence.
- **Corrupt rules.** If evaluating a found node's rule fails with anything
  other than "unauthorized", `ResolveMentions` returns the existing internal
  "inconsistent data" error, as every other authorized read does. A node the
  store does not know never reaches that check, so with a corrupt catalog a
  hidden node and an unknown name can produce different errors.

## Measurements

Go 1.26.4, linux/amd64, AMD Ryzen 9 7950X 16-Core Processor. Run with
`go test -bench . -benchtime 2s`, once.

| Benchmark | Terms | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkCandidates` (24-token question) | 10,000 | 2,152 | 3,832 | 43 |
| `BenchmarkCandidates` | 100,000 | 1,868 | 3,832 | 43 |
| `BenchmarkCandidates` | 1,000,000 | 1,729 | 3,832 | 43 |
| `BenchmarkResolveMentions` (memory store, 12-token question) | 9 | ~148,000 | 290,402 | 707 |

Matching is about 2 µs and does not grow with the lexicon. Authorized
resolution is about 150 µs. Most of that is the padded batch: 2048 lookups in
the memory store, each with its own context check. Of the rest, cloning the
registrations that matched is the main cost. Shrinking `MaxCandidateIDs`
(through a smaller token bound or tighter per-term limits) cuts the padded
batch proportionally.

## Deferred

- An authorized node-enumeration API for builds.
- Bundle retention and promotion (#405/#406).
- The #373 question gate, which should consume `ResolveMentions`.
- #374 residue.
- Typo and inflection tolerance (abstain instead).
- Renderer wording for template phrase keys.
