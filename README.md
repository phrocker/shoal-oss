# shoal

Shoal is an Accumulo-compatible sorted key-value engine and replacement
runtime written in Go. It runs as a standalone embedded database, exposes Go,
C, and Python client APIs, and implements Accumulo-compatible scan,
tablet-server, and compaction roles. Production replacement remains gated by
the live conformance verdicts tracked in
[issue #74](https://github.com/phrocker/shoal-oss/issues/74). RFile and
Parquet are native storage formats; ShoalQL runs SQL, graph, document,
exact-vector, and distributed IVF-PQ queries across local and Accumulo-backed
data.

See [`FEATURES.md`](FEATURES.md) for the complete capability matrix, current
validation state, and the few remaining platform and infrastructure gates.

Shoal's north star is a local-to-Accumulo knowledge plane for agent systems;
see the accepted direction in
[`docs/platform-product-plan.md`](docs/platform-product-plan.md).

## Choose a path

| Goal | Start here |
|---|---|
| Run a local database with RFile or Parquet | [Embedded engine](#embedded-engine-standalone-no-zookeeper) |
| Use Sharkbite import-compatible Python APIs | [`python/README.md`](python/README.md) |
| Embed the Go client or stable C ABI | [`accumulo/`](accumulo/) · [`capi/README.md`](capi/README.md) |
| Evaluate Shoal replacement roles with Accumulo | [`FEATURES.md`](FEATURES.md#accumulo-replacement-roles) · [`docs/tserver-hosting-lifecycle.md`](docs/tserver-hosting-lifecycle.md) |
| Run graph, document, SQL, or vector search | [`FEATURES.md`](FEATURES.md#query-and-indexing) · [`docs/graph-schema.md`](docs/graph-schema.md) · [`docs/shoalql-accumulo.md`](docs/shoalql-accumulo.md) |
| Validate against an exact Accumulo 4 cluster | [`test/accumulo/README.md`](test/accumulo/README.md) |

Go consumers should use the canonical repository-backed module path:

```bash
go get github.com/phrocker/shoal-oss/accumulo
```

## Quick start

Prerequisites for local development are Go 1.25+, Python 3.9+, `make`, and a
native C toolchain. Docker with Compose v2 is required for the live HDFS
integration suite and live Accumulo conformance.

```bash
git clone https://github.com/phrocker/shoal-oss.git
cd shoal-oss
make build
make test
```

Run the embedded engine without ZooKeeper or Accumulo:

```bash
go run ./cmd/shoal-embed init --table events --workload analytical \
  --data ~/.shoal/data
printf '%s\n' \
  '{"row":"event:1","entries":[{"cf":"meta","cq":"type","value":"login"}]}' |
  go run ./cmd/shoal-embed write --table events --data ~/.shoal/data
go run ./cmd/shoal-embed scan --table events --data ~/.shoal/data
```

Build and install the Sharkbite import-compatible Python package:

```bash
make capi
python -m pip install ./python
export SHOAL_LIBRARY="$PWD/bin/capi/libshoal.so"
# macOS: export SHOAL_LIBRARY="$PWD/bin/capi/libshoal.dylib"
```

On Windows PowerShell, use
`$env:SHOAL_LIBRARY = "$PWD\bin\capi\shoal.dll"` instead.

On a Linux Docker host, validate the exact Accumulo 4 harness:

```bash
make test-accumulo-static
make test-accumulo
make conformance-live
```

The live harness is opt-in, cleans up containers and volumes, and emits
machine-readable release verdicts. Docker absence returns exit status 2
(`unsupported`), never a passing result.

## Operating modes

- **Embedded / standalone** (`shoal-embed`, `internal/engine`) owns its WAL,
  memtable, splits, and compaction. It stores RFile, Parquet, or mixed-format
  tablets on local, memory, GCS, S3, Azure Blob, or HDFS backends.
- **Read fleet** (`cmd/shoal`) serves stateless and stateful Thrift scans over
  RFiles with locator, file, and block caches.
- **Tablet server** (`shoal-tserver`) acquires an Accumulo ServiceLock, hosts
  tablets, serves scans and ingest, writes fenced WALs, and commits native
  RFile minor compactions.
- **Compactor** (`shoal-compactor`) executes coordinator jobs, publishes
  outputs durably, reports progress, and completes jobs through Accumulo's
  existing completion RPC.

The standalone engine does not need ZooKeeper. Accumulo replacement roles use
the exact Accumulo 4 wire and metadata contracts described in
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## Embedded engine (standalone, no ZooKeeper)

The embedded engine is a self-contained sorted KV store. It owns its own
write-ahead log, in-memory memtable, RFile flush + compaction, and tablet
split policy — there is no manager, no tablet server, and no ZooKeeper in
the loop. Point it at a data directory and go:

```bash
make build   # builds cmd/shoal-embed (and everything else) via go build ./...

# create an operational table (auto selects RFile), optionally pre-split
shoal-embed init   --table graph --splits "entity:,event:,knowledge:" --data ~/.shoal/data

# create a scan/aggregate-heavy SQL table (auto selects Parquet)
shoal-embed init   --table events_analytics --workload analytical --data ~/.shoal/data

# write mutations (JSON lines on stdin)
shoal-embed write  --table graph --data ~/.shoal/data < mutations.jsonl

# scan back out as JSON lines
shoal-embed scan   --table graph --row-prefix "entity:" --data ~/.shoal/data

# flush + compact, or print status
shoal-embed compact --table graph --data ~/.shoal/data
shoal-embed status  --data ~/.shoal/data

# migrate an existing RFile table to Parquet (mixed files remain readable
# until compaction replaces them)
shoal-embed compact --table graph --format parquet --data ~/.shoal/data

# or serve the ShoalEmbed gRPC API for external consumers
shoal-embed serve  --data ~/.shoal/data --port 9876
```

The server is also published as a non-root, multi-architecture container at
`ghcr.io/phrocker/shoal-oss/shoal-embed`. It starts the gRPC and observability
listeners with container-safe defaults and includes `proto/embed.proto` for
non-Go client generation. See
[`docs/shoal-embed-container.md`](docs/shoal-embed-container.md) for runtime,
versioning, proto extraction, and local smoke-test instructions.

Programmatic use mirrors the CLI:

```go
eng, _ := engine.Open("~/.shoal/data", engine.Options{})
eng.CreateTable("graph", engine.TableOptions{
    Splits: engine.PrefixSplit("entity:", "event:", "knowledge:"),
})
eng.Write("graph", mutations)
sc, _ := eng.Scan("graph", iterrt.InfiniteRange(), engine.ScanOptions{})
for sc.Next() { /* sc.Key(), sc.Value() */ sc.Advance() }
sc.Close()
eng.Close()
```

### Conditional gRPC writes

`ShoalEmbed.ConditionalWrite` supports compare-and-set conditions on each
mutation. It is deliberately separate from unconditional `Write`: an older
server returns `UNIMPLEMENTED` instead of ignoring unknown condition fields
and applying entries unconditionally during a rolling upgrade. Conditions
target the mutation row plus an exact
`column_family` / `column_qualifier` / `column_visibility` coordinate and
require either `absent` or `value_equals`. With no condition timestamp, the
newest version is checked and a newest tombstone counts as absent. Setting the
optional timestamp checks that exact version instead.

All conditions on one mutation are evaluated atomically with its WAL-backed
write under the owning tablet's writer lock. Concurrent ordinary and
conditional writers therefore cannot interleave between comparison and write.
`WriteResponse.results` contains one accepted/rejected status per mutation in
request order; `written` remains the accepted count and is unchanged for
legacy unconditional requests.

```go
resp, err := client.ConditionalWrite(ctx, &embedpb.WriteRequest{
    Table: "leases",
    Mutations: []*embedpb.Mutation{{
        Row: []byte("service-a"),
        Conditions: []*embedpb.Condition{{
            ColumnFamily: []byte("lease"),
            ColumnQualifier: []byte("owner"),
            Predicate: &embedpb.Condition_Absent{Absent: true},
        }},
        Entries: []*embedpb.Entry{{
            ColumnFamily: []byte("lease"),
            ColumnQualifier: []byte("owner"),
            Value: []byte("worker-7"),
        }},
    }},
})
```

**Local and at scale.** Durable RFile or Parquet files flush through a pluggable
`storage.Backend`. The default is the local filesystem; an in-memory,
GCS, or S3 backend keeps each tablet's WAL local while flushing immutable
files elsewhere — a locally-resident, cloud-durable store with the same
engine and iterators in both cases. WAL durability is tunable
(`SyncFull` / `SyncNormal` + group-commit interval).

**Choosing a format.** Use the default operational profile (RFile) for
point/range lookups, adjacency-index graph traversal, or Accumulo
interoperability. Use `--workload analytical` (Parquet) for SQL scans,
aggregations, and external analytics tools. ShoalQL runs above the engine and
returns the same results for RFile, Parquet, and mixed migration tables.
Parquet files are sorted into row groups with row statistics and bloom filters,
so bounded SQL predicates prune unrelated groups instead of decoding the whole
file.

ShoalQL also has an Accumulo client backend with the same scalar, graph,
document, and exact-vector semantics. It pushes native scan constraints and
uses an explicit deterministic local fallback for Shoal-only iterators.
Both embedded and Accumulo backends can attach the format-neutral distributed
IVF-PQ lifecycle for explicitly selected approximate graph or document
semantic queries. Freshness failures never silently use a stale generation:
the query fails or takes an explicitly enabled exact fallback. See
[`docs/shoalql-accumulo.md`](docs/shoalql-accumulo.md) and
[`docs/distributed-vector-index.md`](docs/distributed-vector-index.md).

Use `shoal-sql --explain --query 'SELECT ...'` to print the physical plan and
the table's configured write format, authoritative read formats, and mixed
migration state without executing the query. Reproduce the pruning benchmark
with `go test -run '^$' -bench BenchmarkSourcePruning ./internal/parquetfile`.

**HDFS.** Select the `hdfs` backend and use `hdfs:/path` or
`hdfs://namenode:port/path` object paths. The native Go client loads
`core-site.xml` and `hdfs-site.xml` from `HADOOP_CONF_DIR` or
`HADOOP_HOME`. Set `SHOAL_HDFS_NAMENODE=host:port` when commands discover
qualified HDFS paths indirectly (for example, through Accumulo metadata);
otherwise authority-less paths use the Hadoop default cluster. Simple
authentication uses `HADOOP_USER_NAME` when set.

**External compactor.** `shoal-compactor` discovers the active coordinator,
serves Accumulo's multiplexed `CompactorService` on `-listen` (default:
`-advertise`), and executes capability-gated HDFS jobs. Configure
`-hdfs-namenode`/`SHOAL_HDFS_NAMENODE` and persist `-state-file` across
restarts so an ambiguous `compactionCompleted` response can be reconciled
without duplicate completion or premature output cleanup. Optional
`-metrics-address` exposes `/healthz`, `/readyz`, and `/metrics`.

**Complex graph & vector operations, pushed down.** Rather than streaming
whole row ranges to the client, the engine runs server-side iterators next
to the data and returns only what the query needs:

- `EdgeExpandIterator` — one Seek returns a node's one-hop neighborhood
  (walks edge cells, resolves neighbor ids, emits neighbor rows).
- `LatentEdgeDiscoveryIterator` — derives latent links during compaction.
- `TermIndexIterator` — keyword/term-index lookups bounded by content.
- `VectorKNNIterator` / `IvfPqDistanceIterator` — brute-force and IVF-PQ
  approximate nearest-neighbor over vector cells.

These are schema-agnostic mechanisms — the consumer supplies the
vocabulary per request; no graph schema is baked into the engine. See
`docs/ai-knowledge-graph.md` for the design direction.

## Distributed read fleet

`cmd/shoal` is the read-optimized serving role. It exposes Accumulo-compatible
scan RPCs without hosting or writing tablets and can be fanned out across
pods. The separate `shoal-tserver` binary is the stateful replacement role
that acquires ServiceLocks, hosts tablets, and serves scans and ingest.

```
       ┌──────────────────┐    Thrift scan()  ┌─────────────┐
       │ Client / SDK     │ ─── HEDGE ──────▶ │ shoal pod   │── ◀ GCS/S3 RFiles
       │ ShoalScanRouter  │                   │  (Go)       │── ◀ metadata cache
       │ HedgedScan       │ ──────────────▶   └─────────────┘
       │ Coordinator      │   (parallel)      ┌─────────────┐
       │                  │ ─────────────────▶│ shoal pod   │── ◀ GCS/S3 RFiles
       └──────────────────┘    Thrift scan()  │  (Go)       │── ◀ metadata cache
                                              └─────────────┘
```

A shoal pod's read path:

1. **Bootstrap**: resolve the tablet→RFile map (standalone from shoal's own
   metadata, or — for Accumulo interop — via ZK
   `/accumulo/<uuid>/root_tablet` → `accumulo.metadata` walk). Exception-
   driven cache invalidation (sharkbite-pattern) instead of TTL.
2. **Per scan**: locator-cache lookup → fan-out to one `fileIter` per
   (RFile, locality-group) → heap-merge by Key → visibility filter (alloc-
   free) → optional CF/iterator pushdown → emit results.
3. **Caches**: file-bytes LRU (default 1 GB), decompressed-block LRU,
   tablet-locator cache. Block-level CRC + zone-map skip when the RFile
   carries the `RFile.blockmeta` extension.

See `ARCHITECTURE.md` for design rationale and `REFERENCES.md` for the
Apache Accumulo + sharkbite source pointers consulted while building the
format-compatible reader.

## Build

```bash
make build       # go build ./... (builds shoal-embed and all binaries)
make test        # full test suite (race-clean)
make capi        # stable C connector ABI shared library + headers
make test-accumulo-static  # validate the exact Accumulo 4 harness, no Docker
make test-accumulo         # live ZooKeeper/HDFS/Accumulo 4 Java smoke + cleanup

# only the distributed-serving mode needs generated Thrift bindings:
make thrift-gen     # regenerate internal Go bindings from the vendored IDLs
make thrift-verify  # regenerate and fail if the checked-in bindings drift
```

The disposable Docker harness under `test/accumulo` is the integration oracle
for client, scan, iterator, RFile, compaction, and replacement-role
conformance. It builds the exact pinned Accumulo 4 source revision; the image
is deliberately not environment-overridable so a successful verdict always
targets the vendored wire contract. See
[`test/accumulo/README.md`](test/accumulo/README.md).

The embedded engine builds with a plain `go build ./...` and has no Thrift
dependency. The required Accumulo 4 IDLs are vendored under
`internal/thrift/idl`; regeneration does not require an Accumulo checkout or
`ACCUMULO_SRC`. They are pinned to Accumulo 4 source revision
`1a716b2c1bb5762ead4b46d2bc4f53e13873b314`, whose root POM pins the
compiler to **Apache Thrift exactly 0.17.0**. Install that compiler, verify
`thrift --version`, or set
`THRIFT=/path/to/thrift-0.17.0` when invoking make. Windows users can use the
ASF binary whose SHA-256 is
`e2406226921e8d2822ec20a199060342398084f130e85fbe1dba0cb1f060e592`.
The matching local cluster harness is documented in
[`test/accumulo/README.md`](test/accumulo/README.md). Its live target is
intentionally opt-in and reports Docker absence as a skipped, unexecuted test
with a nonzero status.
Go 1.25+ (transitively from `cloud.google.com/go/storage`).

Platform Docker image (multi-stage, distroless static):
```bash
docker build -t shoal:dev .
```

Build and smoke-test the minimal standalone `shoal-embed` image:

```bash
make container-build
make container-smoke
```

## Layout

```
cmd/
  shoal-embed/          embedded standalone engine — CLI + ShoalEmbed gRPC server (no ZK/Accumulo)
  shoal/                distributed serving daemon — metadata + Thrift listener
  shoal-bootstrap/      diagnostic CLI: walks ZK → root → metadata → tablets
  shoal-compactor/      external compaction worker — discovers the manager's CompactionCoordinator in ZK
  shoal-offline-compact/  offline (OFFLINE-fenced) full major compaction of a table's tablets, off-cluster
  shoal-compactor-shadow/  shadow-compaction harness
  shoal-probe/          one-shot RFile probe (version + LG summary + walk count)
  shoal-rfile-pull/     gs://… → local copy
  shoal-rfile-write/    synthetic RFile writer (test fixtures)
  shoal-scan-client/    Thrift StartScan from CLI
  shoal-count-row/      row-count micro-bench against a tablet

internal/
  engine/               embedded engine API — tables, split routing, parallel scan
  tablet/               tablet runtime: memtable + WAL + flush + compaction
  memtable/             in-memory sorted cell buffer
  localwal/             local write-ahead log (durability tiers, group-commit)
  qwal/                 quorum WAL
  embedpb/              generated ShoalEmbed gRPC bindings (proto/embed.proto)
  iterrt/               SortedKeyValueIterator runtime: merge, versioning,
                        deleting, visibility + edge-expand / latent-edge /
                        term-index / vector-knn graph & vector pushdown
  protocol/             AccumuloProtocol — magic + version + instance-id header
  zk/                   ZooKeeper client + root-tablet locator (Accumulo interop)
  cred/                 Hadoop-Writable PasswordToken encoding
  metadata/             metadata-table walker — tablet→file map bootstrap
  offlinecompact/       offline-compaction orchestrator: OFFLINE fence + guarded
                        commit (plan/direct) + byte-exact verify (see docs/offline-compaction-design.md)
  cclient/              cooked Go types (KeyExtent, Range, Authorizations, …)
  scanclient/           Thrift client wrapper (TSocket → framed → AccumuloProtocol → MUX)
  cache/                LRU caches: tablet locator, decompressed blocks, RFile bytes
  storage/              backend interface + local / memory / gcs implementations
  rfile/                RFile reader (block-level seek, multi-LG, multi-level index)
    bcfile/             BCFile container (footer, meta-index, block layout)
      block/            decompressor + sharkbite-style async prefetcher
    relkey/             relative-key decoder (cursor-based, zero-copy views)
    index/              RFile.index parsing + multi-level walker
    blockmeta/          RFile.blockmeta optional meta-block — zone-map + skip predicate
    wire/               Java DataInput primitives (UTF, varint, key codec)
  visfilter/            CV expression parser + Authorizations + alloc-free evaluator
  ivfpq/                IvfPqDistanceIterator Go port (V1)
  scanserver/           Thrift TabletScanClientService implementation
  thrift/
    idl/                pinned Apache Accumulo 4 IDLs + provenance
    gen/                checked-in internal Go bindings (run thrift-gen)
```

## Custom iterators

The hedge coordinator can route through shoal whenever the underlying scan
is iterator-free OR uses one of shoal's natively-recognized iterators.
Currently recognized:

- **`org.apache.accumulo.core.graph.ann.IvfPqDistanceIterator`** — full
  ADC-distance + top-K + threshold replicated in `internal/ivfpq/`.
  Wire-compatible with the Java side's `VectorPQ.toBytes()` and
  `IvfPqDistanceIterator.encodeQuery`. When this iterator appears in the
  `ssiList` of a multi-scan, shoal runs it natively and returns the same
  top-K output a server-side iterator would.

Anything else in `ssiList` errors out server-side rather than silently
producing wrong answers — for Accumulo interop, callers can fall back to a
tserver in that case.

## Offline compaction

`shoal-offline-compact` runs a full major compaction of an **OFFLINE**
Accumulo table's tablets from a standalone process — no tserver or compaction
coordinator in the compaction work. It reads each tablet's input RFiles,
applies the resolved `table.iterator.majc.*` stack, writes one compacted output
RFile per tablet, verifies it byte-for-byte, and emits a machine-readable
metadata commit plan under an OFFLINE continuity fence. Only plan generation
is release-approved today: do not apply it with a standalone Ample/shell
writer or use direct mode. Application awaits a supported
manager/coordinator/FATE operation carrying current authority proof.

- [Design & safety model](docs/offline-compaction-design.md)
- [Operator runbook](docs/offline-compaction.md)

## Operational notes

- Standalone, shoal resolves tablets from its own metadata; for Accumulo
  interop it can use a ZK watch lookup → `/accumulo/<uuid>/root_tablet` +
  metadata table walk, with exception-driven cache invalidation (sharkbite
  pattern) instead of TTL.
- Block-level CRC check via the `RFile.blockmeta` extension when present;
  zone-map skip predicate avoids decompressing blocks that can't match.
- Visibility filtering pushed down into the relkey decoder; reject path
  doesn't allocate or copy values.
- One Server instance per pod; goroutine-safe across concurrent scans.
- Default file cache 1 GB, decompressed-block cache configurable.
- Pre-warm walks user-table tablets at startup; first scan is warm-fast.
- Storage backends intentionally hide exact reserved staging/backup
  artifacts from normal `List` results. Operators or background maintenance
  code that need to reap stale internals should call
  `storage.CleanupStaleArtifacts(ctx, backend, prefix, cutoff)` against one
  managed subtree/object prefix at a time, with `cutoff <= time.Now().Add(-
  storage.RecommendedArtifactCleanupAge)` (15 minutes by default). Cleanup
  deletes only exact reserved artifacts older than that cutoff; any backup
  artifacts that cannot be mapped back to one safe target are reported in
  `ArtifactCleanupResult.Recoverable` for manual recovery instead of being
  deleted automatically. Cloud cleanup verifies Shoal ownership metadata and
  uses generation/ETag/version-conditional deletes. Cloud bucket/container
  roots are valid cleanup prefixes so root-level artifacts are reachable. S3
  cleanup requires `s3:GetBucketVersioning`, `s3:ListBucketVersions` for
  versioned or suspended buckets, `s3:ListBucket` for never-versioned buckets,
  `s3:GetObject`/HeadObject inspection, and conditional `s3:DeleteObject` or
  `s3:DeleteObjectVersion` permissions. Azure cleanup enumerates blob versions
  and deletes the exact owned stage version so versioned containers do not
  retain hidden staging data. Azure writes larger than the 5,000 MiB
  `Put Blob From URL` promotion limit are rejected before staging. Local
  replacement on portable
  rename-fallback paths can leave a reserved backup as the only surviving copy
  after a crash or ambiguous publish failure, so janitor cleanup preserves such
  backups for explicit recovery instead of deleting them automatically. Local
  `Open` follows symlinks. Local `Create` rejects a final-component symlink
  because portable path-based replacement cannot atomically verify its
  referent and publish without risking a concurrent retarget.

## License

Apache License, Version 2.0.
