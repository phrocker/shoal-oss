# Router evaluation v2 (pre-registered)

Router v2 of #500. This document was committed, with the sealed test split
it describes, in a pull request of its own. That pull request changed no
router code, threshold, grammar, feature or model. Its merge comes before any
v2 grammar, lexicon, feature or model work.

The protocol, metrics, baselines and success criteria below are fixed by that
commit. Later changes to this document may only:

- append amendments before the run, each dated and justified (see Rules);
- append the run's results;
- append errata.

None of them may edit the sections above "Amendments".

v1's protocol and result are in [router-evaluation.md](router-evaluation.md),
and the router is described in [local-language.md](local-language.md). v1's
pre-registered result was negative:

- router 36 of 103 end-to-end, against the lexical baseline's 59 of 103;
- coverage 9 of 74;
- one grammar cue came from a test phrasing (v1's erratum).

## What v2 is expected to change (hypotheses)

v1 lost because the model learned to trust whole-pattern matches. The
grammars were written from the train phrasings, so on train a full pattern
match almost always marked the right target. On held-out phrasings, which
rarely match a pattern, the router abstained (v1, "Why the router lost").
v2's levers are the ones v1 named. They are exploratory, and none is built
by this commit:

- **Less reliance on whole-pattern matches:**
  - train on positives whose text no pattern covers (v1's 292 cases are spent
    and usable as v2 training data, including v1's test phrasings);
  - drop or down-weight `pattern_full` and `pattern_cover`;
  - calibrate the abstention threshold on dev.
- **Cue and paraphrase coverage** from the graph-derived lexicon (#499) and
  from cue synonyms written from train and dev only.

The encoder (#501) is the real fix for paraphrase. It is not part of v2.

Hypotheses, stated before any v2 work:

- **H1 (primary).** On the sealed v2 test split, router v2's end-to-end
  accuracy is higher than the lexical baseline's over the same catalog and
  grammars.
- **H2.** Router v2 covers more answerable cases than the frozen v1 system
  does on the same split, while its target precision stays at 95% or more.
- **H3.** Router v2 keeps abstaining on unanswerable cases: correct
  abstention of at least 48 of 51.
- **H4 (descriptive).** v2's gains, if any, are largest on plain paraphrases
  of actions and lookups, the cells where v1 abstained most (v1's per-kind
  confusion).

## The sealed test split

The split was written fresh for v2. v1's test split is spent: it informed
v1's erratum and the analysis of why v1 lost.

| Property | Value |
|---|---|
| File | `pkg/router/testdata/eval/v2/test.jsonl.gz` (gzip, `-n -9`) |
| `V2TestDigest`: SHA-256 of the decompressed split, equal to `Cases.Digest("test")` | `5510513c6c0935dff657355503167fd193ad5b0374e7c56f949bcdb83266df53` |
| `V2TestArchiveSHA256`: SHA-256 of the committed `.gz` | `a81dbc1e6b8a0cffc1da159b88eef4e52d1369b3d8b254d9659e921f3b76a0b8` |
| `V2WorldSHA256`: `pkg/router/testdata/eval/v2/world.json`, byte-equal to v1's world | `7684d173e8fe64f2513bc52e7880fccfad356eff5bbf999dfdc34b4957140748` |
| Baseline-corpus digest (`v2BaselineSHA256`, see Leak prevention) | `0c815b732417ce02f25aca477d1f4a798eb8834aebbca54d68477e45b2e7f333` |

| Cases | Answerable | action | decision | lookup | Unanswerable |
|---|---|---|---|---|---|
| 138 | 87 | 51 | 18 | 18 | 51 |

Cases by tag:

| Tag | Cases |
|---|---|
| plain (no tag) | 77 |
| unanswerable | 26 |
| hidden_target | 10 |
| missing_slot | 6 |
| hidden_node | 4 |
| ambiguous_executor | 3 |
| injection | 3 |
| ambiguous_mention | 3 |
| nonexistent_target | 2 |
| nonexistent_node | 2 |
| slot_mismatch | 2 |

Answerable and unanswerable are defined as in v1. Hidden-target and
hidden-node cases come in twins: alice abstains and bob, who sees the secret
source, gets the proposal. The labels use v1's case schema and world, so
v1's harness reads them unchanged.

**How it was written.** One author, the author of this pull request, wrote
the cases by hand. It used the world, v1's 292 cases and v1's grammars, and
did not run any router, baseline, analysis or lexicon matcher on them. The
only code that reads their texts is the checks below, which tokenize them
with `lexicon.Tokenize` and route nothing.
Labels are semantic: what a correct router should do. For example, a scale
request without a direction is labelled `missing_slot`, even where no grammar
has its cue. The author does not do v2 grammar, lexicon, feature or model
work.

Checks that read the split, but do not route it, run in CI:

- **Seal** (`TestV2TestSplitSealed`, `TestV2SealRefusesAnyChange`): every
  digest is recomputed and compared with this document
  (`TestV2DocRecordsTheSeal`). The mutation check shows that each of these is
  refused:
  - a changed label, text or split;
  - a dropped, added, duplicated or reordered case;
  - changed whitespace;
  - a changed archive byte.
- **Labels** (`TestV2TestLabelsWellFormed`), against the world alone:
  - every target exists and is visible to its caller;
  - slots match the target's schema exactly, with every required slot;
  - node slots name visible nodes of the right kind;
  - enum values are declared;
  - abstentions carry a closed reason;
  - IDs are sequential and (text, caller) pairs are unique.
- **Freshness** (`TestV2TestIsFresh`): no case repeats any of v1's 292 texts,
  verbatim or with its entities swapped. The empty-text case is exempt.
  Against the baseline corpus below, 145 word types of the split and 137 of
  its 138 templates are new. This is a descriptive count: the test names no
  word.
- **Frozen world** (`TestV2WorldFrozen`): v2's `world.json` is v1's, byte for
  byte. The labels refer to its node keys, aliases and targets, so changing
  the world voids the split.

### Why the split is committed, and sealed

v1 committed its test split in the tree, digest-gated (`cases.jsonl` with
`PreRegisteredTestDigest`). v2 does the same, with three changes, because
v1's leak came from the grammar author having the test phrasings in view:
the same file, and the same generator, held them next to the train phrasings.

1. **A file of its own, compressed.** The split is `test.jsonl.gz`, and
   `.gitattributes` marks it binary. It does not show in a diff, `git log -p`,
   a grep or a code search. Reading it takes a deliberate decompression. This
   guards against casual exposure, not a determined reader.
2. **A closed gate.** `Cases.Split` and v1's `PreRegistered` refuse it.
   `Cases.PreRegisteredV2` (`pkg/router/eval/v2gate.go`) is the only way to
   its cases. It returns `ErrV2Sealed` while `V2TestRunOpen` is false. The run's
   own pull request flips that constant and changes nothing else.
3. **No generator.** Unlike v1's `generate.py`, nothing in the tree produces or
   lists the test phrasings.

Committing only the digest and keeping the split out of the tree was
considered and rejected:

- The leak guard below needs the phrasings in CI on every change, or it
  cannot fail the build.
- Reviewers need the label checks to run against the committed labels.
- A split held outside the tree can be lost or silently regenerated. Its
  provenance would rest on one person's account, which is exactly the gap v1's
  erratum describes.

## Systems compared

All systems route every case of the sealed split once, in the single run,
through the same harness (`pkg/router/eval`). The harness gives visibility
from the fixture world, as in v1.

- **router v2**: the v2 grammars, lexicon cues, features, model and
  threshold, as committed before the run, through `routershadow.Decider`.
- **lexical baseline (primary comparator)**: `Analysis.Baseline`
  (`shoal.router.baseline/v1`). Its decision rule is unchanged, and it runs
  over the same analysis, catalog and v2 grammars as router v2. The question
  is whether the learned target choice beats the lexical rule given the same
  cue data. Better grammars help both.
- **frozen v1 (secondary comparator)**: v1's grammars and model,
  `router-pair-v1.json` at SHA-256
  `f11fa3aca2f97d33cbd72d674d26b01fe4c4dc074bf6068de312bc63c1d82a08`. It
  runs with the router code at the run commit. If v2 changes code in a way
  that keeps v1's model from loading or scoring as before, the run reports v1
  with the code of the merge commit of this pull request instead, and says
  so.
  - **lexical baseline over v1's grammars**: reported next to it, so that the
    effect of grammars and of the model can be told apart.

## Metrics (fixed)

The metric definitions are v1's, unchanged ("Metrics (fixed)" in
[router-evaluation.md](router-evaluation.md)), and so is the code that computes
them: `TargetCorrect`, `World.Correct`, `World.Measure`, `World.Compare` and
`exactMcNemar` in `pkg/router/eval/eval.go`. Every metric is an exact N of D.

| Metric | N | D |
|---|---|---|
| Target precision on non-abstained | target-correct | non-abstained proposals (all cases) |
| **End-to-end accuracy (primary)** | end-to-end correct | all 138 cases |
| Coverage | answerable cases with a non-abstained proposal | 87 answerable cases |
| Abstention rate | abstentions | all 138 cases |
| Correct abstention | unanswerable cases that abstained | 51 unanswerable cases |
| Slot exact-match | target-correct proposals with exact slots | target-correct proposals |
| Abstention reason agreement (secondary) | abstentions whose single reason is the expected one | unanswerable cases that abstained |
| Per-kind confusion | count per (expected kind, proposed kind) | — |
| End-to-end by tag (descriptive) | end-to-end correct | cases with that tag |

**Paired comparison.** For each comparison (router v2 against the lexical
baseline, and router v2 against frozen v1), the run reports:

- both-right, first-only, second-only and both-wrong counts;
- the exact two-sided McNemar p-value: a binomial test at 1/2 on the
  discordant pairs, printed to three significant figures.

For scale: with 10 discordant pairs, a 9 to 1 split gives p = 0.021 and an
8 to 2 split gives p = 0.109.

**Latency** on CPU, as v1: `BenchmarkAnalyze`, `BenchmarkRoute` and
`BenchmarkServiceRoute`, on train and dev texts only, with the host named.

## Success criteria (fixed in advance)

Router v2's safety guards, both on the sealed split:

- **G1:** target precision on non-abstained of at least 95%, as an N of D. A
  wrong proposal is the failure that matters most.
- **G2:** correct abstention of at least 48 of 51.

The primary outcome, router v2 against the lexical baseline, end-to-end, is
exactly one of:

- **Positive:** router-only exceeds baseline-only, the exact two-sided
  McNemar p is below 0.05, and both G1 and G2 hold.
- **Negative:** baseline-only exceeds router-only with p below 0.05, or G1 or
  G2 fails, whatever the accuracy. A router that wins by guessing has not won.
- **Inconclusive:** anything else. It is reported as inconclusive, not as "no
  difference" and not as a trend.

Secondary outcomes, each reported as met or not met:

- **S1:** router v2 beats frozen v1 end-to-end on the split, McNemar
  p < 0.05.
- **S2:** router v2's coverage is at least 44 of 87 (50%).
- **S3:** router v2's slot exact-match is at least 90% of its target-correct
  proposals.

Everything else (per-tag results, per-kind confusion, reason agreement,
latency) is descriptive. Any analysis not listed here is labelled
exploratory.

## Leak prevention

v1's lesson was that one author writing templates, test paraphrases and
grammars at the same time lets a test cue into a grammar ("exposed"). These
rules apply from this commit until the run.

1. **Who may see the test phrasings.**
   - Only the split's author, who does no v2 grammar, lexicon, feature, model
     or threshold work.
   - Anyone who does that work, human or agent session, must not decompress
     or otherwise read `test.jsonl.gz`, nor the author's working notes.
   - Reviewers of this pull request can check the split through its tests
     (counts, digests, label validity, freshness) without reading it.
   - A reviewer who does read it is excluded from v2 authorship, and says so
     on the pull request.
2. **Where cues may come from.** Grammar cues, patterns and enum phrases,
   lexicon entries, synonym lists, stop words, features and training examples
   are derived only from:
   - v2's train and dev splits;
   - v1's 292 cases (every split; v1's test is spent);
   - v1's grammars;
   - the world.

   v2's train and dev phrasings are written without access to the test split,
   and dev is written separately from the grammars, so that it gives a tuning
   signal (v1's dev was saturated). Every tuning decision taken on a dev
   result is recorded in the amendments.
3. **The leak guard** (`TestNoTestPhrasingInCueSources`,
   `pkg/router/eval/leak_test.go`) runs in CI on every change. Text is compared
   after `lexicon.Tokenize`, with entity mentions abstracted to a slot and
   pattern optionals expanded. A test-derived unit counts as a leak when it
   appears in a cue source and nowhere in the **baseline corpus**: v1's
   `cases.jsonl` (all splits), v1's `grammars/*.json` and the world's strings
   other than comments. That corpus is frozen at the digest above
   (`TestV2BaselineFrozen`), so it cannot absorb a test word.
   - **Checks:**
     - **phrase:** a whole test text, verbatim;
     - **template:** a whole test text with its entities abstracted;
     - **word:** a test word the baseline lacks, as a cue, pattern word, enum
       phrase, lexicon key or value, or train or dev text;
     - **bigram:** an adjacent test word pair the baseline lacks.
   - **Sources:**
     - every file under `pkg/router/testdata/eval/v2/`, except the sealed
       split, the frozen world and Markdown. Case files are audited on their
       texts.
     - files under `external/` there: general-purpose resources not written
       for this evaluation (for example a published thesaurus, pinned by
       digest in an amendment). They get the phrase and template checks only.
     - a file of any other kind fails the guard until the guard learns to
       read it.
     - every string literal of the non-test Go code under `pkg/router` and
       `pkg/lexicon`. This covers stop-word lists and any cue a harness could
       synthesize.
   - **Grandfathered units.** Go literals at this commit already hold 19 word
     and bigram units that the split also uses: function words in the
     router's stop-word and question-word lists, and words in error messages.
     They are listed as SHA-256 hashes (`goLiteralsAtRegistration`), so the
     list does not name test vocabulary. The set may only shrink.
   - **Findings** name the source, the check, a case ID and the unit as the
     source contains it. They never print a test text.
   - **Mutation check** (`TestLeakGuardCatchesLeaks`): each of these fails
     the guard, through the same file collector:
     - planting a test text in a lexicon file or an external resource;
     - planting a test template as a grammar pattern;
     - planting a test-only word as a cue, a synonym key or a train text;
     - planting a test-only pair in a pattern;
     - an unreadable file kind.

     A copy of a v1 grammar passes (`TestV2BaselineIsClean`).
4. **When the guard finds something:**
   - Remove the unit from the source. Never change the split, the baseline
     corpus or the guard to get past it.
   - A finding that reached a commit on the main branch is listed in the
     results, with its source and check (not the test text).
   - Changing the guard's rules before the run takes an amendment in the same
     pull request.
   - A guard finding tells its reader that a word occurs in the test split. A
     word removed this way is not re-added under any spelling.
5. **v1's word audit** ("A test-only cue leaked into a grammar" in v1's
   erratum) is also run before the test run, on train and dev only. Every
   grammar word must occur in some train or dev text, or be listed with a
   reason. The result is reported.

## Rules

1. This commit, the seal, merges before any v2 grammar, lexicon, feature,
   model or threshold work begins.
2. Amendments before the run are appended below, dated, each with its reason.
   They may add v2 artefacts (grammars, lexicon, training data, recipe) and
   pin them. They may not change the split, the world, the baseline corpus,
   the metrics, the comparators or the success criteria.
3. The test split is run once, in a pull request that:
   - flips `V2TestRunOpen`;
   - adds the reports;
   - appends the results here;
   - and does nothing else.

   Results are reported whatever they are. Errors found later are fixed in
   later changes and reported as errata; the run's numbers are not replaced.
4. Any rerun needs a new pre-registration and a new test split.

**What the commit history can and cannot show.**

- **What it can show:** that this digest was committed and merged before the
  v2 grammar, feature and model commits; that the guard ran on each of them;
  and that the run's pull request changed nothing but the gate and the
  results.
- **What it cannot show:** that nobody doing v2 work decompressed the split.
  That rests on the rules above and on the accounts of the people involved,
  as v1's did.

## Amendments

None yet.

## Results

Not run.
