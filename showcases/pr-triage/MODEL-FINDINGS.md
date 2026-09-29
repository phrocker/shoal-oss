# Local authorization decision models: measured findings

Laya now has a credible **shadow candidate**: low-rank encoder adaptation
retained 33/34 adjudicated relevant functions on a new 189-function sample,
while proposing 19.1% of function-diff bytes for reduced review. This is a
retrospective relevance result, not verified defect recall or permission to
exclude source. Full review remains mandatory.

## Latest result: adapted Laya on changed-code evidence

The frozen candidate uses the pinned English checkpoint with rank-8,
alpha-16 encoder adapters and a trained decision head. Base encoder weights
stay frozen; 29,846,529 parameters are trainable. The input is a five-context-line
function diff with explicit missing caller/dependency context. Four supervised
epochs use three whole-PR development folds to choose an operating threshold;
no evaluation labels are used for fitting or threshold selection.

Training uses 459 adjudicated functions from 20 code-bearing PRs, with all
earlier evaluation sets explicitly retired to development. The next twelve
earlier PRs were frozen separately: 1,312 functions, of which 189 were sampled
uniformly within PR, at most 20 per PR, before scores or labels. The candidate,
weights, renderer and threshold were frozen before opening those labels.

| Reference for the new sample | Laya relevant retained | CPU comparator relevant retained |
| --- | ---: | ---: |
| Blinded frontier | 31/32 (96.9%) | 30/32 (93.8%) |
| Blinded Claude | 29/31 (93.5%) | 29/31 (93.5%) |
| Post-evaluation blinded-to-scores adjudication | 33/34 (97.1%) | 32/34 (94.1%) |

Laya proposes 56/189 functions, containing 41,441/217,271 function-diff bytes
(19.1%) or 40,657/392,928 complete before/after source bytes (10.3%), for reduced
review. The CPU comparator proposes 21.3% of diff bytes but misses two
adjudicated relevant functions. Its five-fold development estimate is not
directly comparable to Laya's three-fold estimate; the final comparison uses
the same reserved sample and frozen thresholds.

The Laya miss is `configureIdentity` in PR #317, an actual identity policy
configuration function. It remains a failure in the record. Claude also marked
the constant `entraDenied` error helper relevant and left three proposed
reductions unknown. Adjudication inspected their pinned callers, preserved
both original judgments, and applied the fixed distinction between policy or
disclosure decisions and generic constant error helpers. This resolves proposed
task relevance; it does not establish a defect oracle or erase reviewer dissent.

187 inputs fit the exact 2,048-token budget; two oversized inputs retain full
review. Median GPU inference was 20.66 ms on the local RTX 4070 Ti. Training
used a low-rank configuration after a full-parameter memory preflight refused
to start; its allocator was capped at half the GPU. The threshold is not a
calibrated probability or an approved exclusion loss budget.

The evaluation scores come from the exported adapter reloaded on its pinned
base. Zero-initialized adapter parity was checked before training. The original
training process did not save final-fit reference logits, so trained in-memory
versus exported/reloaded parity is **not separately established**. Quality
claims here refer to the actual reloaded checkpoint used for evaluation.
An independent replay from the preserved local checkpoint reproduced all 189
scores and proposals exactly; its [verification receipt](runs/reserved-v5/checkpoint-replay-verification.json)
does not substitute for the missing training-process comparison.

Latest evidence:

- [Development recipe](runs/encoder-v4/config.json),
  [PR-separated results](runs/encoder-v4/report.json), and
  [checkpoint identity](runs/encoder-v4/model-identity.json).
- [Frozen Laya selection](runs/reserved-v5/laya-selection.json),
  [sampled manifest](runs/reserved-v5/sampled-manifest.json),
  [actual predictions](runs/reserved-v5/laya-predictions.json).
- [Frontier comparison](runs/reserved-v5/frontier-report.json),
  [Claude comparison](runs/reserved-v5/claude-report.json), and
  [adjudicated comparison](runs/reserved-v5/adjudicated-report.json).

The 114 MiB adapter is preserved in this workspace at
`showcases/pr-triage/local-artifacts/laya-v4/`, outside Git. Its digest and
recipe are versioned; it has not been published to a model registry. With the
pinned GPU runtime and cached base checkpoint, replay the actual candidate:

```sh
python showcases/pr-triage/predict_adapter.py \
  --selection showcases/pr-triage/runs/reserved-v5/laya-selection.json \
  --training showcases/pr-triage/local-artifacts/laya-v4 \
  --manifest showcases/pr-triage/runs/reserved-v5/sampled-manifest.json \
  --cache /tmp/shoal-laya-model-cache \
  --output /tmp/shoal-laya-replay-new.json
```

No generic Shoal production API or PR-review product feature is added. The
next promotion gate is prospective shadow data plus paired downstream reviews
that measure actual inspection cost and missed findings. Small, correlated,
retrospective samples and agent references cannot authorize automatic exclusion.

## Earlier candidates and what changed

The earlier small CPU classifier met the provisional relevance target,
but its proposed reductions contain only **1.4% of before/after source bytes**
or **3.0% of function-diff bytes**. It was a useful baseline, not a viable
review-cost reduction model. Earlier Laya configurations did not meet the
provisional 95% authorization-relevance recall target on separate PRs. No model may exclude
source from review. These measurements concern
agent-proposed task relevance, not verified vulnerabilities, general fault
coverage, or downstream review savings.

## Frozen comparisons

| Candidate and evaluation | Relevant functions retained | Proposed lower-priority functions |
| --- | ---: | ---: |
| Best zero-shot Laya, separate eight-PR cohort, frontier reference | 60/72 (83.3%) | 29/156 (18.6%) |
| Same Laya, Claude reference | 50/58 (86.2%) | 29/156 (18.6%) |
| Lexical baseline, exploratory eight-PR cohort, frontier reference | 70/72 (97.2%) | 29/156 (18.6%) |
| Frozen lexical model, new twelve-PR sample, frontier reference | 69/70 (98.6%) | 31/210 (14.8%) |
| Same frozen lexical model, Claude reference | 45/46 (97.8%) | 31/210 (14.8%) |
| Adapted Laya scorer, new twelve-PR sample, frontier reference | 62/70 (88.6%) | 51/210 (24.3%) |
| Same adapted Laya scorer, Claude reference | 42/46 (91.3%) | 51/210 (24.3%) |
| Frozen lexical model, post-evaluation adjudication | 57/58 (98.3%) | 31/210 (14.8%) |
| Adapted Laya scorer, post-evaluation adjudication | 52/58 (89.7%) | 51/210 (24.3%) |

Both reviewers identify the lexical model's same missed helper,
`loopbackHost` in PR #349. Its caller uses it to decide whether an endpoint
may receive credentials over plain HTTP. The model sees the helper's changed
body but not that caller. Neither reviewer marks any of the lexical model's
31 proposed reductions as unknown. Claude marks two of Laya's proposed
reductions unknown; those are reported separately, never counted as negatives.

The lexical model is character-ngram TF-IDF plus logistic regression. It was
trained on the original 93 functions (three code-bearing PRs within the
four-PR pilot). Its threshold comes from leave-one-PR-out development
predictions. Vocabulary fitting happens inside each fold. The subsequent
twelve-PR assessment changed neither weights nor threshold.

Function counts overstate its utility: most lower-priority functions are tiny
helpers. The [source-volume audit](runs/reserved-v3/source-volume-v2.json) measures
the actual bytes represented by those proposals. The next development round
must rank candidates by meaningful code-volume reduction at high relevance
recall. These byte counts still exclude whole-PR context and are not measured
LLM token savings.

The adapted Laya candidate trains only its existing option scorer, with its
encoder and transformer head frozen. It uses 249 adjudicated development
functions from eight code-bearing PRs, including the retired eight-PR
validation cohort. Ten epochs, learning rate, seed and representation were
fixed before the new sample's labels were opened. Its threshold comes from
leave-one-PR-out development predictions. This is supervised cross entropy,
not the upstream RLCD recipe or full encoder fine-tuning. The negative result
does not rule out a better-trained Laya model.

The CPU model's exported JSON bundle reproduces every one of its 210 measured
scores exactly. Median per-function CPU inference was **0.86 ms**, with about
518 ms to import dependencies and load the bundle in that process. These are
local measurements, not service throughput or an LLM token-savings estimate.

Post-evaluation adjudication preserved both original reviewer judgments and
remained blinded to model scores. It retained the same missed helper for the
lexical candidate. The adjudicated labels supplement the independent results;
they do not replace them with verified defect truth.

## Cohorts, evidence and limitations

The original four-PR pilot remains development data. Five zero-shot
checkpoint/input/task combinations were compared there. Increasing the input
budget to 2,048 tokens and using diffs improved inference coverage from 55/93
to 92/93; better coverage did not imply better ranking. The best development
ranking used complete before/after bodies and the original question.

That candidate and threshold were frozen before inference on eight earlier
PRs. After it failed, those PRs were explicitly retired to development. A
scorer trained on the original pilot alone produced zero reduction on that
cohort. The lexical baseline was introduced after the failure, so its
eight-PR result is exploratory.

For the next test, twelve earlier PRs were frozen by chronological rule.
They contain 1,146 changed functions. To bound assessment work, a seeded
uniform sample of at most 20 functions per PR was selected **before labels
or predictions**, giving 210 functions. Reported percentages describe this
sample; they are not population-weighted estimates or whole-PR savings.
Unsampled and nonfunction units retain full review. The two assessors worked
without candidate scores or each other's labels; Claude had complete sampled
function bodies and whole-PR diffs only where the explicit byte budget allowed.

There are no exact rendered-state duplicates across the three sets. Shared
paths, related change families, repository-specific vocabulary, and overlap
with public model pretraining remain possible. Functions within a PR are
correlated. Small retrospective cohorts and agent judgments do not establish
a statistical lower bound on future defect recall. Score thresholds are
operating points; neither model's scores are calibrated probabilities.

Evidence:

- [Development candidate selection](runs/candidates-v2/selection.json),
  [adjudication](runs/candidates-v2/adjudication.json), and
  [original lexical baseline](runs/candidates-v2/lexical-v1.json).
- Eight-PR [frontier comparison](runs/validation-v2-predictions/frontier-report.json)
  and [Claude comparison](runs/validation-v2-predictions/claude-report.json).
- Twelve-PR [sampling manifest](runs/reserved-v3/sampled-manifest.json),
  [frozen lexical selection](runs/reserved-v3/lexical-selection.json),
  [frontier comparison](runs/reserved-v3/frontier-report.json),
  [Claude comparison](runs/reserved-v3/claude-report.json),
  [post-evaluation adjudication](runs/reserved-v3/adjudicated-report.json), and
  [replay verification](runs/reserved-v3/replay-verification.json).
- [Adapted Laya recipe](runs/scorer-v2/config.json) and
  [PR-separated predictions and final scores](runs/scorer-v2/result.json).

## Run the frozen candidate locally

Use Python 3.11 and the pinned
[CPU dependencies](experiments/lexical-requirements.txt). No GPU or hosted
model is required for replay:

```sh
python showcases/pr-triage/local_model.py \
  --bundle showcases/pr-triage/runs/reserved-v3/lexical-model-v2.json \
  --manifest showcases/pr-triage/runs/reserved-v3/sampled-manifest.json \
  --output /tmp/shoal-local-replay-new.json
```

The bundle contains numerical parameters and vocabulary, not pickle or
executable code. The loader validates content identity, model contract,
training recipe and renderer identity, parameter shapes and finite values.
Every output retains `action: full_review`
and `optimization_eligible: false`; `lower_priority` is only a proposal.
Use a new output path for each observation.

Adversarial code review found that the original frozen selection did not
mechanically enforce its training-recipe identity. The schema-2 bundle and
replay selection add that enforcement. The
[post-run attestation](runs/reserved-v3/recipe-binding-audit.json) records this
limitation and exact score parity; it does not claim enforcement existed
before the original evaluation. Historical selections and outputs remain
unchanged. Initializer-order coverage was also strengthened; recollecting all
three populations confirmed unchanged case contents for these actual cohorts.

The full population manifest is archived as deterministic gzip alongside its
uncompressed digest. Training configs preserve original experiment paths;
the archived records identify the exact source manifests, labels and inputs.
Binary Laya scorer weights remain local experiment artifacts, identified by
hash in each result. They are reproducible from the pinned training recipe,
but are not the shipped CPU candidate.

## Implications for Shoal

Keep the generic decision contract independent of Laya. A model bundle needs
its task/rubric, input representation, training and calibration lineage,
evaluation evidence, operating threshold and limitations. CPU classifiers and
Laya should use the same observation/proposal/action separation.

The operational picture should carry pinned caller and policy-consumer
evidence, plus explicit missing edges. The missed transport helper is a
concrete context-acquisition case. Do not hide incomplete context behind a
high model score or treat a graph snapshot as complete authorization evidence.

The next promotion gate is a prospective shadow cohort and paired downstream
reviews measuring actual inspection cost and missed findings. Keep reference
adjudication separate from model outputs; retraining consumes retired
development data and reserves new evaluation data. PR triage stays an external
showcase of these generic capabilities.
