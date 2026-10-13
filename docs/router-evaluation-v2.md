# Router evaluation v2 (pre-registered)

Router v2 of #500. This document was committed, with the sealed test split
it describes, in a pull request of its own. That pull request changed no
router code, threshold, grammar or model. Its merge comes before any v2
grammar, lexicon, feature or model work.

The protocol, metrics, baselines and success criteria below are fixed by that
commit. Everything above "## Amendments" is pinned by SHA-256
(`v2ProtocolSHA256` in `pkg/router/eval/v2seal_test.go`). Later changes may
only append to the sections from there on:

- amendments before the run, each dated and justified (see Rules);
- the run's results;
- errata.

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

Hypotheses, stated before any v2 work. Each has its own decision rule
(Success criteria).

- **H1 (primary).** On the sealed v2 test split, router v2's end-to-end
  accuracy is higher than the lexical baseline's, over the same catalog and
  grammars.
- **H2.** Router v2 covers more answerable cases than the frozen v1 system
  does on the same split, without giving up precision.
- **H3.** Router v2 keeps abstaining on unanswerable cases (guard G2).
- **H4 (descriptive).** v2's gains, if any, are largest on plain paraphrases
  of actions and lookups, the cells where v1 abstained most (v1's per-kind
  confusion).

## The sealed test split

The split was written fresh for v2. v1's test split is spent: it informed
v1's erratum and the analysis of why v1 lost.

| Property | Value |
|---|---|
| File | `pkg/router/testdata/eval/v2/test.jsonl.gz` (gzip, `-n -9`) |
| `V2TestDigest`: SHA-256 of the decompressed split, equal to `Cases.Digest("test")` | `d68f126050aa387b817f787c026cf6814cc509c5be28f36675779c4683b6030e` |
| `V2TestArchiveSHA256`: SHA-256 of the committed `.gz` | `e3ffa19b61e12f4b409af9166c8a6308a4e267b12fd8a73a98cd6f917a58895c` |
| `V2WorldSHA256`: `pkg/router/testdata/eval/v2/world.json`, byte-equal to v1's world | `7684d173e8fe64f2513bc52e7880fccfad356eff5bbf999dfdc34b4957140748` |
| Baseline-corpus digest (`v2BaselineSHA256`, see Leak prevention) | `0c815b732417ce02f25aca477d1f4a798eb8834aebbca54d68477e45b2e7f333` |

| Cases | Answerable | action | decision | lookup | Unanswerable |
|---|---|---|---|---|---|
| 303 | 191 | 108 | 40 | 43 | 112 |

Cases by tag:

| Tag | Cases |
|---|---|
| plain (no tag) | 169 |
| unanswerable | 51 |
| hidden_target | 22 |
| missing_slot | 14 |
| hidden_node | 10 |
| ambiguous_executor | 7 |
| ambiguous_mention | 7 |
| nonexistent_target | 6 |
| injection | 6 |
| slot_mismatch | 6 |
| nonexistent_node | 5 |

Answerable and unanswerable are defined as in v1. Hidden-target and
hidden-node cases come in twins: alice abstains and bob, who sees the secret
source, gets the proposal. The labels use v1's case schema and world, so
v1's harness reads them unchanged.

**How it was written.**

- **Author.** One author, the author of this pull request, wrote the cases
  by hand. It used the world, v1's 292 cases and v1's grammars.
- **Size.** The first draft had 138 cases. After review showed that size was
  underpowered (see Power), the same author extended it to 303 before merge,
  keeping the answerable-to-unanswerable balance (63% against 37%). The
  first 138 cases are unchanged.
- **Nothing was routed.** The author did not run any router, baseline,
  analysis or lexicon matcher on the cases. The only code that reads their
  texts is the checks below, which tokenize them with `lexicon.Tokenize` and
  route nothing.
- **Labels are semantic:** what a correct router should do. For example, a
  scale request without a direction is labelled `missing_slot`, even where
  no grammar has its cue.
- **Exclusion.** The author does no v2 grammar, lexicon, feature or model
  work.

Checks that read the split, but do not route it, run in CI. Their failure
messages name case IDs, never texts.

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
  verbatim or with its entities swapped. The two empty-text cases are exempt.
  Against the baseline corpus below:
  - 273 word types and 722 bigrams of the split are new;
  - 301 of its 303 templates are new.

  These are descriptive counts; no test names a word.
- **Frozen world** (`TestV2WorldFrozen`): v2's `world.json` is v1's, byte for
  byte. The labels refer to its node keys, aliases and targets, so changing
  the world voids the split.
- **Access** (`TestSealedSplitAccess`): an AST walk of every Go file under
  the audit roots (Leak prevention). Only the seal tests, the leak guard and
  the run may name `sealedV2`, `openArchive`, `v2Archive`, `v2Auditor`,
  `PreRegisteredV2`, `CheckV2Seal` or the archive's file name. A new test
  that would route, print or tune on the split fails until the allowlist
  `sealedAccess` is edited in review. This guards against accidents: code
  that builds the path by other means is not caught.

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
   its cases. It returns `ErrV2Sealed` while `V2TestRunOpen` is false.
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

All systems route every case of the sealed split once, in the single run.
The run goes through the harness committed with this seal: the
`TestEvaluationV2Test` and `v2Evaluate` functions in
`pkg/router/eval/v2run_test.go`. Three tests support it:

- `TestV2RunHarness` exercises the harness end to end on v1's dev split.
- `TestJudgeV2` pins the decision rules.
- `TestV2Readiness` loads every v2 artefact exactly as the run will, and runs
  the harness on v2's dev split, so the run is never the first time v2's
  artefacts load. Until v2's model and train/dev cases exist, it uses v1's
  model and v1's dev split as **placeholders** and logs that it did.

Visibility comes from the fixture world, as in v1.

- **router v2**: everything is loaded only from `pkg/router/testdata/eval/v2/`:
  - the world and `grammars/*.json` (through `LoadWorld`);
  - the model, `model/router-pair-v2.json`;
  - any lexicon or recipe files there.

  These must be committed before the run. Routing goes through
  `routershadow.Decider`.

  No code loads lexicon or recipe files yet. Any loader for them, and any
  other change to how v2 artefacts are loaded, lands in an amendment before
  the run, together with the matching change to `TestV2Readiness`.
- **lexical baseline (primary comparator)**: `Analysis.Baseline`
  (`shoal.router.baseline/v1`). Its decision rule is unchanged, and it runs
  over the same analysis, catalog and v2 grammars as router v2. The question
  is whether the learned target choice beats the lexical rule given the same
  cue data. Better grammars help both.
- **frozen v1 (secondary comparator)**: v1's world, grammars and model,
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
| **End-to-end accuracy (primary)** | end-to-end correct | all 303 cases |
| Coverage | answerable cases with a non-abstained proposal | 191 answerable cases |
| Abstention rate | abstentions | all 303 cases |
| Correct abstention | unanswerable cases that abstained | 112 unanswerable cases |
| Slot exact-match | target-correct proposals with exact slots | target-correct proposals |
| Abstention reason agreement (secondary) | abstentions whose single reason is the expected one | unanswerable cases that abstained |
| Per-kind confusion | count per (expected kind, proposed kind) | — |
| End-to-end by tag (descriptive) | end-to-end correct | cases with that tag |

**Paired comparisons.** Each pair reports both-right, first-only,
second-only and both-wrong counts, and the exact two-sided McNemar p-value: a
binomial test at 1/2 on the discordant pairs, to three significant figures.
The pairs are:

- router v2 against the lexical baseline, end-to-end (H1);
- router v2 against frozen v1, end-to-end (S1);
- router v2 against frozen v1 on coverage over the answerable cases (H2);
- the lexical baseline over v2's grammars against it over v1's grammars
  (descriptive).

**Latency** on CPU, as v1: `BenchmarkAnalyze`, `BenchmarkRoute` and
`BenchmarkServiceRoute`, on train and dev texts only, with the host named.

## Success criteria (fixed in advance)

Router v2's safety guards, both on the sealed split:

- **G1, precision.** Wrong proposals (non-abstained and not target-correct)
  number at most 5% of router v2's non-abstained proposals, **rounded down**.
  - Below 20 proposals, no wrong proposal is allowed, so 1 wrong of 1 fails.
  - **With no proposal at all, G1 fails:** there is no precision to guard,
    and a router that never answers has not won.
- **G2, abstention.** Correct abstention of at least 105 of 112.

The **primary outcome (H1)**, router v2 against the lexical baseline,
end-to-end, is exactly one of:

- **Positive:** router-only exceeds baseline-only, the exact two-sided
  McNemar p is below 0.05, and both G1 and G2 hold.
- **Negative:** baseline-only exceeds router-only with p below 0.05, or G1 or
  G2 fails, whatever the accuracy. A router that wins by guessing has not won.
- **Inconclusive:** anything else. **Inconclusive is not "no effect":** it
  says the split could not tell the systems apart at this size (see Power),
  and it is reported that way, not as a trend.

**H2** uses the coverage pairing: an answerable case covered by one system
and not the other is a discordant pair.

- **Supported:** router-v2-only exceeds v1-only, p is below 0.05, and G1
  holds.
- **Contradicted:** v1-only exceeds router-v2-only with p below 0.05, or G1
  fails.
- **Inconclusive:** anything else.

Secondary outcomes, each reported as met or not met:

- **S1:** router v2 beats frozen v1 end-to-end, McNemar p < 0.05.
- **S2:** router v2's coverage is at least 96 of 191 (50%).
- **S3:** router v2's slot exact-match is at least 90% of its target-correct
  proposals, rounded up; not met with no target-correct proposal.

Everything else (per-tag results, per-kind confusion, reason agreement,
latency) is descriptive. Any analysis not listed here is labelled
exploratory.

### Power

The exact McNemar test's power at two-sided α = 0.05 depends on two things:

- the share of cases that are discordant, d (v1's test split had 31 of 103,
  about 0.30);
- the true accuracy difference, Δ.

Assuming each case is discordant with probability d, the power is (computed
exactly):

| Cases | d | Δ = 5 points | Δ = 7.5 | Δ = 10 | Δ = 15 |
|---|---|---|---|---|---|
| 138 (first draft) | 0.15 | 0.25 | 0.56 | 0.85 | 1.00 |
| 138 | 0.25 | 0.17 | 0.36 | 0.60 | 0.94 |
| 138 | 0.30 | 0.15 | 0.31 | 0.52 | 0.89 |
| **303 (sealed)** | 0.15 | 0.57 | 0.92 | 1.00 | 1.00 |
| **303** | 0.25 | 0.37 | 0.71 | 0.93 | 1.00 |
| **303** | 0.30 | 0.32 | 0.63 | 0.88 | 1.00 |

What this means for the sealed split:

- **Detectable effects.** At v1-like discordance (0.30), the split detects a
  10-point gain about 88% of the time and a 15-point gain almost always. A
  5-point gain is detected only about a third of the time, so an
  inconclusive result is likely if the true gain is that small.
- **Chance guard failures.** The guards were set so that a good router fails
  them by chance only rarely:
  - G2: a router whose true correct-abstention rate is 97% fails it with
    probability 0.020, and at 98% with probability 0.002.
  - G1: a router whose true precision is 98% fails it with probability:
    - 0.078 at 50 proposals;
    - 0.015 at 100;
    - 0.005 at 190.

    At 97% precision, the probability is 0.06 to 0.19 across that range. G1
    is strict on purpose: a router that makes few proposals has to make them
    without error.

## Leak prevention

v1's lesson was that one author writing templates, test paraphrases and
grammars at the same time lets a test cue into a grammar ("exposed"). These
rules apply from this commit until the run.

1. **Who may see the test phrasings.**
   - Only the split's author, who does no v2 grammar, lexicon, feature, model
     or threshold work.
   - Anyone who does that work, human or agent session, must not decompress
     or otherwise read `test.jsonl.gz`, nor the author's working notes.
   - Reviewers can check the split through its tests (counts, digests, label
     validity, freshness) without reading it.
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

   Rule 5's provenance audit is how this is checked for train and dev, not
   the guard's word checks (rule 3).
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
     - **word:** a test word the baseline lacks;
     - **bigram:** an adjacent test word pair the baseline lacks.
   - **Which checks apply where:**
     - **Train and dev case texts** (`cases.jsonl`) get **only the phrase and
       template checks.** There are 273 test-only words and 722 test-only
       bigrams, many of the bigrams made of v1 words. A train or dev author
       writing natural paraphrases independently would trip the word and
       bigram checks by chance. Each trip would tell them a test word, and
       removing the unit would bias v2 against H1 and H2 by construction.
     - **General-purpose resources** under `external/` (for example a
       published thesaurus, pinned by digest in an amendment) also get only
       the phrase and template checks.
     - **Data cue sources**, meaning everything else under
       `pkg/router/testdata/eval/v2/`, get every check. That covers grammars,
       lexicon files, the model, recipes, and any `.json`, `.jsonl`, `.txt`,
       `.csv`, `.tsv` or `.py` file. The exceptions are the sealed split, the
       frozen world and Markdown. A file of any other kind fails the guard
       until the guard learns to read it.
     - **Every string literal of every Go file** under `pkg/router`,
       `pkg/lexicon`, `internal/routershadow` and `internal/routerwire` gets
       **only the phrase and template checks**.
       - This includes test files and testdata, so training and harness code
         is covered (v1's `trainModel` is in `eval_test.go`).
       - Code is full of ordinary words. A word or bigram finding there would
         tell a v2 code author which words the split holds.
       - Cue words therefore belong in data files under the v2 fixture tree,
         where every check applies, not in Go literals. Review holds v2 code
         to this.
   - **Only literals are seen.** Cues that code computes at run time from
     other inputs are not. Such code falls under rule 2 and review, and the
     v2 run loads its data only from the v2 fixture tree.
   - **Fail closed on other files** (`TestNoUnauditedCueFiles`): a non-Go,
     non-Markdown file under the four roots must be either in the v2 fixture
     tree or one of the 26 frozen non-Go files pinned by SHA-256 (v1's
     fixtures and model, and the lexicon's fuzz corpus and golden). A new
     file elsewhere, or a changed frozen file, fails.
   - **Grandfathered Go units.** Grandfathered Go units: 1.
     - **The entry:** a pattern in a router grammar fixture in
       `internal/routerwire/world_test.go`. It has been on main since slice 1
       (#519), before the split was written, and one of its expansions
       contains one of the split's templates.
     - **No new code is grandfathered.** No literal this pull request adds
       trips the guard.
     - **Keying and digest:** each entry is keyed by (file, check, SHA-256 of
       the unit), in `goLiteralsAtRegistration` (`leak_grandfather_test.go`;
       only that variable's own literals are skipped by the audit). The
       set's digest is
       `9f20344bf8e966241b7e8f8b791a1a33443c221639cd23eaca6fe79feadc091f`
       over sorted "file, tab, check, tab, hash" lines, and the doc test pins
       it and its size.
     - **The hash is unsalted:** hashing the file's token sequences recovers
       the unit. It guards against casual reading only.
     - **Stale entries do not fail.** That would tell whoever removed the
       literal that it was test material. The split's author lists stale
       entries at the run with `-report-stale-grandfather`, and the results
       record them. The pinned list is not edited before the run.
   - **Findings print** the source, a location in it (a line, a string index
     or a case ID), the check and the unit's SHA-256. They print no test text
     and no test word, so the CI log is not an oracle. A finding's location
     does show where test vocabulary sits in the author's own file. That is
     unavoidable if the author is to remove it.
   - **Mutation check** (`TestLeakGuardCatchesLeaks`): each of these fails
     the guard, through the same file collector:
     - planting a test text in a lexicon file, an external resource, a train
       case or a Go literal;
     - planting a test template, with the same or other entities, as a
       grammar pattern, a train case or a Go literal;
     - planting a test-only word as a cue, a synonym key or a model feature;
     - planting a test-only pair in a pattern;
     - an unreadable file kind.

     These get no word or bigram finding:
     - a train case with test-only words;
     - an external resource with test-only words;
     - a Go literal with test-only words, which the real collector marks
       phrase-only.

     No finding prints the planted unit. A copy of a v1 grammar passes
     (`TestV2BaselineIsClean`).
   - **Not caught:** paraphrases, inflections, synonyms and misspellings of a
     test phrasing or word ("restarted" for "restart", a reordered
     sentence). The guard catches copying, not knowledge.
4. **When the guard finds something:**
   - Remove the unit from the source. Never change the split, the baseline
     corpus or the guard to get past it.
   - A finding that reached a commit on the main branch is listed in the
     results, with its source, check and hash.
   - Changing the guard's rules or the grandfathered set before the run takes
     an amendment in the same pull request.
   - A removed unit is not re-added under any spelling.
5. **v1's word audit** ("A test-only cue leaked into a grammar" in v1's
   erratum) runs before the test run, on train and dev only. Every grammar
   word must occur in some train or dev text, or be listed with a reason.
   Each train and dev phrasing's provenance (who wrote it, and when) is also
   recorded. The result is reported.

## Rules

1. This commit, the seal, merges before any v2 grammar, lexicon, feature,
   model or threshold work begins.
2. Amendments before the run are appended below, dated, each with its reason.
   They may add v2 artefacts (grammars, lexicon, training data, recipe,
   model) under `pkg/router/testdata/eval/v2/` and pin them. They may not
   change any of these:
   - the split or the world;
   - the baseline corpus;
   - the metrics, the comparators or the success criteria;
   - the run harness's decision rules.
3. The test split is run once, in a pull request that does only these
   things:
   - flips `V2TestRunOpen`;
   - writes `report-test.md` with `go test -run TestEvaluationV2Test -update`;
   - appends the results here, including the stale grandfathered entries
     that the split's author lists with `-report-stale-grandfather`.

   Results are reported whatever they are. Errors found later are fixed in
   later changes and reported as errata; the run's numbers are not replaced.
4. Any rerun needs a new pre-registration and a new test split.

**What the commit history can and cannot show.**

- **What it can show:** that this digest was committed and merged before the
  v2 grammar, feature and model commits; that the guard ran on each of them;
  and that the run's pull request changed nothing but the gate, the report
  and the results.
- **What it cannot show:** that nobody doing v2 work decompressed the split.
  That rests on the rules above and on the accounts of the people involved,
  as v1's did.

## Amendments

### A1 (2026-10-13): router v2's artefacts, features and dev tuning

**Reason.** Rule 2 allows amendments to add v2 artefacts under
`pkg/router/testdata/eval/v2/` and pin them. This one adds router v2. Nothing
above "## Amendments" changes. The split, the world, the baseline corpus, the
metrics code, the comparators, the success criteria, the judge and the run
harness (`v2run_test.go`) are unchanged. The leak guard and its grandfathered
set are unchanged too. **No session doing this work opened the sealed
split.**

**Artefacts.**

| File | Contents | SHA-256 |
|---|---|---|
| `cases.jsonl` | 503 train and 143 dev cases, no test case | `33d57fa04b4f4c3a1132282b6c48aa295f6233f7de2ee85d6d9480b0072cb7de` |
| `grammars/*.json` | 18 grammars, one per target, in `shoal.router.grammar/v1` | — |
| `model/router-pair-v2.json` | the trained model, `router.pair/v2` | `6e7b0ab6c01ad938f8666aadeb214217ab59ab928af108541b7afba0476b82f3` |
| `report-dev.md` | the harness's dev report (a golden, `TestV2DevReport`) | — |

- **Train split.** v1's 292 cases, every split, relabelled train (v1's test
  is spent), and 211 new cases `v2r-*`.
- **Dev split.** 143 new cases `v2d-*`.
- **Model.** It is trained on the train split alone (`TestV2ModelIsReproducible`
  retrains it byte for byte). The recipe is v1's averaged perceptron, 30
  epochs, 4 positive repeats, with an intercept offset of −1. The recipe
  records the offset in a new `intercept_offset` field, omitted when zero, so
  v1's recipe digest is unchanged.

**Provenance (rule 5).** Each set was written once, on 2026-10-13, by its
own agent session. Each session was told not to open the sealed split, and
reports that it did not.

- **Train (`v2r-*`).** Written by one session that read only `world.json`,
  v1's `cases.jsonl` and v1's grammars.
- **Dev (`v2d-*`).** Written by a second session that read only `world.json`
  and v1's `cases.jsonl`. It saw no grammar, v1's or v2's, and no train case.
- **Grammars and code.** Written by a third session, the author of this
  amendment, from v1's grammars, v1's 292 cases, the new train texts and the
  world. That session did not read dev texts until the first dev run, and
  added no cue, pattern or enum phrase from dev afterwards.
- **Overlap.** 11 train cases whose text repeated a dev text were dropped from
  train. `TestV2CasesWellFormed` now requires the two splits' texts to be
  disjoint, and checks every train and dev label against the world as
  `TestV2TestLabelsWellFormed` checks the sealed one.
- **Word audit (`TestV2GrammarWordsHaveProvenance`).** Every grammar word
  occurs in some train or dev text, except six. Each of the six is an enum
  phrase word kept from v1's grammars, and v1's erratum lists them as
  occurring in no fixture text: `sev`, `1`, `2`, `3`, `forcefully` and
  `graceful`.

**Leak-guard findings (rule 4), before any commit.** Each flagged unit was
removed from its source by a script that read only the guard's output
(source, location, check). No flagged unit was looked at or re-added.

- 48 train and dev cases were removed for phrase or template findings: 25
  from dev and 23 from train.
- 99 grammar strings were removed for word or bigram findings, each a cue, a
  pattern or an enum phrase.
- Removing test vocabulary from the grammars biases v2 against H1 and H2 by
  construction. The dev numbers below are after these removals.

**Code (what is evaluated).**

- **`router.pair/v2` features** (`pkg/router/featuresv2.go`). The vector is
  v1's nine features, unchanged and in the same positions, followed by twelve
  more:
  - stemmed cue and vocabulary recall;
  - cue scoring: evidence weighted by 1/df over the visible candidates,
    and the candidate's share, top flag, lead and deficit;
  - partial, slot-anchored pattern matching: in order, gaps allowed, slots
    only over whole fitting mentions;
  - slot satisfiability from the lexicon (lookups use the graph-derived
    templates' subject concepts);
  - mention fit;
  - the share of words no candidate knows.

  Every feature is a scope-free number in [0, 1]. Comparisons are made within
  the caller's visible catalog only.
- **Schema selection.** `routershadow.Decider` takes the feature schema from
  the served predictor's preprocessing ID. It builds the task
  (`router.TaskSpecFor`) and the feature artifact for that schema. The task
  differs per schema only in its input schema ID. A v1 model gets exactly
  v1's task, artifact and receipt.
- **Loading is unchanged.** No loader was added: `LoadWorld` and
  `TestV2Readiness` are as sealed, and the readiness run now uses the real v2
  artefacts, with no placeholder.
- **Slot filling.** A node slot that the pattern tier filled now abstains as
  `ambiguous_mention` when an unused mention of the same kind could have
  filled it ("restart ledger and inventory in staging"). Before, the first
  service won silently. This is the documented contract of `fill`. It applies
  to the baseline too. v1's train, dev and test reports are unchanged by it,
  so frozen v1 scores as before.

**Dev tuning decisions.**

1. The intercept offset of −1 was chosen from a sweep on dev: epochs {30,
   60} × positive repeats {2, 4, 8} × offset {0, −0.25, −0.5, −1, −1.5, −2}.
   The pick was 30, 4 and −1, the smallest offset that left at most 2 wrong
   proposals at no loss of end-to-end accuracy. The sweep ran before the
   leak-guard removals, and was not rerun after them.
2. The slot-filling rule above was added after a dev case named two services
   for one slot and got a proposal.
3. Three enum inflections without train or dev provenance were removed.

**Dev results** (`TestV2DevReport`, 143 cases, 88 answerable and 55
unanswerable):

| Metric | router v2 | baseline (v2 grammars) | frozen v1 | baseline (v1 grammars) |
|---|---|---|---|---|
| Target precision | 60 of 61 (98.4%) | 36 of 36 | 8 of 8 | 29 of 29 |
| End-to-end | 115 of 143 (80.4%) | 91 of 143 (63.6%) | 63 of 143 (44.1%) | 84 of 143 (58.7%) |
| Coverage | 61 of 88 (69.3%) | 36 of 88 (40.9%) | 8 of 88 (9.1%) | 29 of 88 (33.0%) |
| Abstention | 82 of 143 | 107 of 143 | 135 of 143 | 114 of 143 |
| Correct abstention | 55 of 55 | 55 of 55 | 55 of 55 | 55 of 55 |
| Slot exact-match | 60 of 60 | 36 of 36 | 8 of 8 | 29 of 29 |

Paired comparisons on dev:

| Pair | First only | Second only | Exact McNemar p |
|---|---|---|---|
| router v2 against the baseline, end-to-end | 27 | 3 | 8.4 × 10⁻⁶ |
| router v2 against frozen v1, end-to-end | 52 | 0 | 4.4 × 10⁻¹⁶ |
| router v2 against frozen v1, coverage | 53 | 0 | 2.2 × 10⁻¹⁶ |

The harness prints "Primary outcome: negative" on dev. That verdict is not
about the router: `judgeV2` requires G2's denominator to be the sealed
split's 112 unanswerable cases, and dev has 55, so G2 cannot hold on dev. Dev
numbers are a tuning signal, not a result. They are optimistic for the test
split because the recipe was chosen on dev.

**Open (needs a decision before the run).** The guard
(`TestNoTestPhrasingInCueSources`) reports two word findings in
`model/router-pair-v2.json`. Both are at fixed strings of the
`internal/decisionlinear` model format: string 10, a label name, and string
14, the `recipe_sha256` key. The harness's model path and the provider's
strict format fix those strings, so rule 4's remedy (remove the unit) is not
available. This amendment does not change the guard, which rule 4 forbids
doing to get past a finding. Resolving it, for example by auditing the
format's fixed keys and labels with the phrase and template checks only, is
for the protocol's owner to decide in its own amendment.

## Results

Not run.
