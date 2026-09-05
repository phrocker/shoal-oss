# Distributed IVF-PQ lifecycle

`internal/vectorindex` owns the derived index lifecycle independently of its
physical storage. A generation is an immutable `Snapshot` containing:

- a manifest with generation/parent/lineage, the exact embedding-space
  identity, deterministic codebook version, source and indexed watermarks,
  dimensions, shard layout, counts, and optional benchmark evidence;
- veculo-compatible coarse-centroid and PQ blobs;
- sharded postings carrying document identity/metadata, visibility, timestamp,
  tombstone, cluster, code, and codebook version.

`EncodeSnapshot` maps that state to sorted `Record` values. Their
row/CF/CQ/visibility/timestamp/delete fields map directly to Accumulo keys and
also form stable rows for RFile or Parquet writers. `DecodeSnapshot` permits
the same generation to be replayed in any order. Publication is atomic and
generation-parent checked, so failed or racing builders cannot replace a newer
active generation.

## Lifecycle

- `Build` sorts identifiers, selects a deterministic hash-ranked training
  sample, trains normalized cosine IVF-PQ codebooks, hashes all training
  inputs/configuration into the version, and publishes generation 1.
- `Update` reuses the active codebook and publishes timestamped vector changes
  or tombstones as a new generation.
- `Rebuild` retrains from a supplied authoritative live corpus and records
  lineage.
- `Compact` removes obsolete pre-fence versions while retaining newer versions
  and active-generation metadata.

Readers pin one immutable generation. Accumulo visibility expressions are
evaluated with the public compatibility evaluator. `AS OF` ignores newer
versions and tombstones; latest reads choose the newest authorized version,
with a same-timestamp tombstone winning.

## Query and freshness contract

Callers explicitly select approximate mode and provide `topK`, `nprobe`, and
optional requirements:

- minimum generation;
- minimum indexed watermark;
- source watermark plus maximum permitted lag.

Every build/update record and every query vector carries an embedding-space
identity. Training, updates, and comparisons fail with a typed mismatch even
when two models have the same dimensions. A legacy manifest without an identity
fails closed rather than treating dimension as identity. Recall evidence is
therefore evidence for the manifest's one recorded space, not a global claim.

If any requirement fails, search returns `ErrStale`, or
`ErrExactFallback` only when exact fallback was explicitly enabled. ShoalQL
then runs its existing exhaustive vector iterator for graph rows. Document
semantic results carry shard, datatype, UID, source row, and arbitrary metadata
so ShoalQL can hydrate the exact document cells.

Selected clusters fan out by persisted shard. Every shard computes a bounded
partial top-k. The global merge is deterministic: score descending, then
document identifier ascending. Context cancellation stops fan-out and prevents
partial results from being returned.

Explorer's mixed-space fallback groups the eligible snapshot by the full
embedding-space identity. Query vectors use a bounded cache keyed by that
identity and trimmed query text. `MaxEmbeddingSpaceFanout` refuses excess
spaces before any provider call, while `EmbeddingQueryObserver` reports the
sorted participating identities, cache hits, provider calls, unavailable
spaces, and fan-out refusals without exposing query text or vector bytes.
Single-space retrieval keeps score ordering; mixed-space retrieval uses
deterministic rank fusion and never compares raw scores across models.

## Recall contract

Configuration is not a recall claim. `BenchmarkRecall` compares approximate
results with exhaustive cosine top-k over a named deterministic corpus.
`SetRecallContract` accepts evidence only when corpus, query count, and
benchmark reference are present. `EXPLAIN` otherwise states
`unbenchmarked; no recall claim`.

Tests declare a measurable minimum recall, compare approximate and exact
results, and cover stale generations, visibility, `AS OF`, tombstones,
RFile/Parquet/mixed/Accumulo record replay, cancellation, publication faults,
and generation races. A live Accumulo run remains optional and unsupported
without Docker or an externally configured endpoint.

Compaction convergence applies classification policy before provider work
through `embedconverge.EgressGuard`. A denied cell never reaches the rewriter,
does not consume cell budget, and causes the deterministic unconverged fallback
to retain the original file identity, source bytes, and visibility. Rewriters
receive a cloned key so they cannot widen visibility. Migration epochs persist
per-file span counts, and progress reports both file and span counts per space.
