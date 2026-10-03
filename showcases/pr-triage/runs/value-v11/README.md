# V11: whole-review value experiment, awaiting future PRs

The protocol in `experiments/value-v11.json` was committed at `75441df` before
collecting new source or labels. At registration there were no eligible open
PRs, only three previously observed drafts. The user expects a new PR soon.
No background polling service has been installed; collection is operator/agent
invoked when the new PR is available.

The value question is whether the frozen local classifier reduces actual LLM
input while preserving actionable review findings. This uses entire changed
populations and isolated full/candidate review pairs, not function samples or
relevance labels as a proxy for review quality. The operational picture remains
partial source evidence; no complete repository review or authenticated Shoal
graph projection is claimed.

## Frozen choices

- First four future-created, non-draft PR families with Go changes, in PR-number
  order. Pin the first ready head and merge base from a retained metadata inventory.
  Exclude prior families and experiment branches. Do not replace a case based on
  classifier output, oversized packets or reviewer findings.
- Same V9 code-only SVM and threshold. No fitting or calibration on these cases.
  Unsupported units stay in both arms; retain-all PRs still count toward utility.
- One full and one candidate Claude call per eligible case, no tools, isolated
  sessions. Deterministic hash order varies which arm goes first. Reviewer
  findings require source adjudication; model agreement is not defect truth.
- Aggregate target: at least 10% fewer input tokens, four evaluable families,
  and no confirmed omission-caused actionable finding loss. These are pilot
  targets, not evidence sufficient for production source exclusion.
- Record exact training overlap, non-evaluable cases, proposed omissions,
  actual input tokens including cache, output tokens, cost and timing. Compare
  full-only and candidate-only findings against pinned source and omission
  evidence. Preserve unresolved claims and stochastic/cache confounding.

## Budget continuation

The original **$25 total limit is unchanged**. Four V10 calls cost $1.046898,
leaving $23.953102. This newly registered protocol explicitly replaces the old
8-total-call cap with 12 total calls: four carried calls plus eight new calls
for four pairs. It does not reset spending or silently amend V10.

`value_pair.py carry-budget` verifies the exact parent ledger digest, parent
protocol, four settled calls and total charge, then copies those calls into a
new protocol-bound ledger. The existing paid runner accounts for all charges
and reservations. An import cannot overwrite an existing ledger. Failed or
unpriced calls retain reservations; no automatic paid retries are configured.
Use one canonical continuation ledger for every pair.

## Executable preparation and run

`value_pair.py prepare` builds code-only features from all changed functions,
replays the frozen classifier locally, retains unsupported changes, and uses
the existing complete-source paired packet builder. Operation features are not
constructed or used. Both complete packets must fit the 180,000-byte cap;
oversized cases are recorded and make no paid calls. Feature construction,
model batch scoring and process/load time are measured separately.

`run` requires the pinned PR metadata, source manifest, model, repository and
extractor. It rechecks future/open/non-draft eligibility and reconstructs the
packets from source and model before spending. A rehearsal receipt cannot run.
The call cap and cumulative spending remain enforced by the existing ledger.
Calls execute sequentially; report generation remains `awaiting_source_adjudication`.

In the pinned CPU environment, after retaining PR metadata and collecting its
full manifest using the existing source collector:

```sh
python showcases/pr-triage/value_pair.py carry-budget \
  --protocol showcases/pr-triage/experiments/value-v11.json \
  --parent /path/to/v10/budget.sqlite --budget /path/to/v11/budget.sqlite
python showcases/pr-triage/value_pair.py prepare \
  --protocol showcases/pr-triage/experiments/value-v11.json \
  --repo . --manifest /path/to/collected/manifest.json \
  --pr-metadata /path/to/pinned-pr.json \
  --model /path/to/v9/trained/model.json --extractor /path/to/shadow-extract \
  --output /path/to/v11/pr-N
python showcases/pr-triage/value_pair.py run \
  --protocol showcases/pr-triage/experiments/value-v11.json \
  --run /path/to/v11/pr-N --budget /path/to/v11/budget.sqlite \
  --repo . --model /path/to/v9/trained/model.json \
  --extractor /path/to/shadow-extract --claude /path/to/claude
```

The agent must retain the complete ordered inventory and cohort-selection
receipt before preparing each case; the per-case runner checks eligibility and
pins but is not a GitHub discovery or whole-cohort ordering service. Local
metadata/hashes are not authenticated remote attestations.

## Preparation validation

A no-payment rehearsal on the already assessed PR415 prepared both arms from
all 43 changed functions and 52 changed units. It is explicitly excluded from
V11 value evidence and cannot trigger reviewer calls. Budget transfer retained
all four settled charges. Tests cover carryover, overwrite prevention, future
eligibility and refusal to pay for rehearsals. No new paid calls have occurred.
