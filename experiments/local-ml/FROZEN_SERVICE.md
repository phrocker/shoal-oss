# Frozen code: authorized service inquiry and durable replay

This local integration registers the retained V9 cached source representations,
numeric inputs and model, then runs the complete 188-subject cohort through the
artifact catalog, decision service and engine-backed receipt store. It extends
[frozen provider conformance](FROZEN_CODE.md) to retained source bytes, current
access checks and durable replay. The service remains general; source-review
semantics live in this command and exporter.

The Python wrapper produces one exclusively published bundle containing the
existing numeric export, all cached code-text source files, and an independently
pinned source manifest. It verifies the retained archive and binds source order,
subject identities, byte lengths and digests to the numeric manifest. It records
its own builder hash. No model fitting or paid inference occurs.

These sources are the original model's cached `text.code` representations. They
are not complete repository files and have no independently verified historical
observation or receipt timestamps. Pictures use the current local import time;
revision identifiers bind cached content. The task is historical priority replay,
not a current assessment of those repositories. Every subject remains full review.

The Go command checks external model, numeric-manifest and source-manifest pins.
A trusted local registration authority admits only the exact registered record
and checks current task/source access before artifact disclosure and inference.
The example binds a fixed local principal through the real authentication resolver;
it is not a network login, a multi-tenant registration endpoint or a production
source-permission exporter. Manifest hashes establish integrity for this trusted
producer, not authorization or authenticated historical provenance.

Each subject has a source-backed picture, request and durable receipt. Immediate
retry and process restart return the original stored prediction without another
provider call. State metadata binds all three external pins and the import time;
missing/corrupt metadata or changed pins cannot silently create new request
identities. Publication errors leave explicit uncertainty rather than claiming
that a completed filesystem rename was rolled back.

The numeric provider and persisted core record formats are unchanged. This slice
does not implement outcomes/adjudication, training rights, poisoning-resistant
label governance, promotion or rollback, source-to-model withdrawal, or HTTP/SDK
transport. Those remain separate roadmap work. Frozen V11 is unchanged.

## Run

Use the same pinned Python environment as [the numeric training example](README.md).
From the repository root:

```sh
go build -buildvcs=false -o /tmp/shoal-frozen-code-replay ./cmd/shoal-frozen-code-replay
/tmp/shoal-frozen-code-replay describe-service
"$PYTHON" experiments/local-ml/frozen_service_bundle.py \
  --task-id TASK_ID_FROM_DESCRIBE_SERVICE --output-dir /tmp/my-code-service-bundle
/tmp/shoal-frozen-code-replay inquire \
  --bundle /tmp/my-code-service-bundle \
  --model-sha256 MODEL_SHA256_FROM_EXPORT \
  --manifest-sha256 MANIFEST_SHA256_FROM_EXPORT \
  --source-manifest-sha256 SOURCE_MANIFEST_SHA256_FROM_EXPORT \
  --state-dir /tmp/my-code-service-state
```

Repeat the inquiry command with the same pins and state directory. The first
process should report 188 provider calls; the second should report zero, with
identical per-subject picture/request/receipt/prediction identities and labels.
Each inquiry also checks immediate retry and authorized receipt retrieval.
The original `describe` and `replay` commands continue to support the provider-only
experiment; `describe-service` defines a distinct task with actual evidence-policy
and ranking-plan identities. Bundles for the two tasks are not interchangeable.

Generated source/numeric bundles and the engine state live outside the repository.
The original compressed evidence and compact run reports are retained. State is a
single local registration session, not a shared multi-user service deployment.
