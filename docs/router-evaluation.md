# Router evaluation (pre-registered)

Router v1, slice 1 of #500. This note was committed before the held-out test
split was run for the first time; that run's results were then appended
below, unchanged. The protocol, metrics and denominators in this section were
fixed at that commit and must not be edited after it.
[local-language.md](local-language.md) describes the router.

## What is measured

Two systems route the same fixture texts:

- **router**: `pkg/router` analysis, the target-choice decision served by
  `internal/decisionlinear` with the checked-in model, aggregation, then slot
  filling and validation. It runs through `routershadow.Decider`, the code
  the shadow service uses.
- **baseline**: `Analysis.Baseline` (`shoal.router.baseline/v1`). If a
  pattern covers the whole text, that target wins. Otherwise the target is
  the one with the highest token-overlap recall, if that recall is at least
  0.5 and leads the next target by at least 0.2. If neither holds, the
  baseline abstains. It fills and validates slots with the router's code.

Visibility comes from the fixture. A caller sees nodes, descriptors, profiles
and the ontology whose source it holds. The authorized composition is tested
separately (`internal/routershadow`), not measured here.

## Fixtures

All fixtures are in `pkg/router/testdata/eval/v1`.

- `world.json` is a synthetic world:
  - 18 nodes: services, teams and environments, including a shared `core`
    alias and one node under a secret source;
  - 6 descriptors, two of which offer the same action;
  - 3 decision profiles. One is `service-operation-risk`, modelled on #421;
    another is secret;
  - an ontology with 3 directed relations, which gives 6 lookup templates.
- `grammars/*.json` holds 18 grammars, one per target.
- `cases.jsonl` holds 292 labelled cases. `generate.py` regenerates it, with
  a fixed seed.

Each paraphrase template belongs to exactly one split, so no test-split
phrasing appears in train or dev. A (text, caller) pair occurs once.

**Grammars were written from the train and dev phrasings only.** The test
split therefore measures paraphrase generalization, not recall of
memorized phrasings. One author wrote the templates, test paraphrases and
grammars at the same time. That is a known limitation: the test paraphrases
are not independent of the grammar author.

| Split | Cases | Answerable | Unanswerable | SplitDigest (SHA-256 of its lines) |
|---|---|---|---|---|
| train | 139 | 101 | 38 | `8d06b7e562a1b665ffab26d12071cb2a7ed3dc901174a9ed1632f9b6665c5ef1` |
| dev | 50 | 37 | 13 | `34bf7f1a941604280c2f8940cedd9d1b07071ff395ecaba5cab8c2c3488d5bfd` |
| test | 103 | 74 | 29 | `83b9411380288a69baf5cb5ffd97c4862c760921d96ecabee6a901d2c94ffab6` |

`eval.PreRegisteredTestDigest` pins the test digest above. `eval.PreRegistered`
refuses to release a test split with any other digest, and refused to run
the test split at all before this commit.

A case is **answerable** when its expected kind is action, decision or
lookup. It is **unanswerable** when the expected outcome is an abstention:

- no visible target, including hidden-target twins;
- an empty text;
- an ambiguous executor;
- an ambiguous mention;
- a missing slot, including hidden-node and nonexistent-node twins;
- a slot that does not fit.

## Model

- Trained by `router.Train`, an averaged perceptron over 30 epochs with each
  positive repeated 4 times, in fixed order, on the **train split only**.
- `pkg/router/testdata/model/router-pair-v1.json`:
  - SHA-256: `f11fa3aca2f97d33cbd72d674d26b01fe4c4dc074bf6068de312bc63c1d82a08`
  - `dataset_sha256`: the train SplitDigest
  - `recipe_sha256`: `f980ade09baeca799f961069942255665dd9fe65f64ce189e3b5db705e4040a6`
- Predictor identity on go1.26.4/linux/amd64:
  `decision:predictor:v1:b17d76b7609525501b7adce5fcd62cbab6de6e58217ad992634487ef486d3127`
  (`TestGoldenPredictorID`).
- `TestModelArtifactIsReproducible` retrains and requires identical bytes.

## Metrics (fixed)

Every metric is reported as an exact N of D, with the percentage only as a
gloss. Correctness:

- **Target-correct**: a non-abstained proposal whose kind and target key
  (`TargetRef.Key()`) equal the expected ones, on an answerable case.
- **Slots exact**: the proposal's slots, as fixture node keys or enum values,
  equal the expected slot map exactly: no missing, extra or different slot.
- **End-to-end correct**:
  - answerable case: target-correct and slots exact. **An abstention on an
    answerable case is wrong.**
  - unanswerable case: an abstention, whatever its reason.

| Metric | N | D |
|---|---|---|
| Target precision on non-abstained | target-correct | proposals that did not abstain (all cases) |
| End-to-end accuracy | end-to-end correct | all cases |
| Coverage | answerable cases with a non-abstained proposal | answerable cases |
| Abstention rate | abstentions | all cases |
| Correct abstention | unanswerable cases that abstained | unanswerable cases |
| Slot exact-match | target-correct proposals with exact slots | target-correct proposals |
| Abstention reason agreement (secondary) | abstentions whose single reason is the expected one | unanswerable cases that abstained |
| Per-kind confusion | count per (expected kind, proposed kind) | — |

**Paired comparison** with the baseline uses end-to-end correctness:

- report both-right, router-only, baseline-only and both-wrong counts;
- the discordant pairs are router-only and baseline-only;
- the exact two-sided McNemar p-value is a binomial test on the discordant
  pairs at 1/2.

The primary outcome is router-versus-baseline end-to-end accuracy on the test
split. Secondary outcomes are everything else. End-to-end correctness is also
broken down by case tag (plain, injection, hidden_target, hidden_node,
nonexistent_target, nonexistent_node, missing_slot, ambiguous_mention,
ambiguous_executor, slot_mismatch, unanswerable). These breakdowns are
descriptive; most have fewer than ten cases.

**Latency** is measured on CPU on the host named with the results, by:

- `BenchmarkAnalyze` (pure analysis);
- `BenchmarkRoute` (analysis, the decision, aggregation and the baseline);
- `BenchmarkServiceRoute` (the whole shadow path over the real authorized
  composition with a memory policy store).

These use train and dev texts only.

## Rules

1. The test split is run once, at the commit after this one. Nothing under
   `pkg/router`, its fixtures, grammars or model, or `internal/routershadow`
   is changed between this commit and that run.
2. Results are reported whatever they are, in the section below. Errors found
   later are fixed in later changes and reported as such; this run's numbers
   are not replaced.
3. Any analysis not listed here is labelled exploratory.

## Before the test run

Train and dev are saturated: on both, router and baseline are end-to-end
correct on every case except two train cases, where the baseline missed an
injected decision text. This is expected, since the grammars were written
from those phrasings. Dev therefore gave no tuning signal. No threshold,
feature or grammar was tuned after any dev result. The reports are
`report-train.md` and `report-dev.md`.

**Expectation, stated before the run:** test accuracy will be clearly lower
than dev, for both systems. Test phrasings were held out of the grammars. Some
use cue words no grammar has ("cycle", "kick", "evacuate", "resize").

## Results

Not yet run.
