# Shoal

**No authority without evidence.**

Shoal is an evidence plane for AI systems. Claims, retrieval, and actions stay
bound to the sources, identity, authorization, and system state that justify
them. When that binding cannot be established, Shoal refuses instead of
degrading quietly.

Most systems in this space offer citations. Citations are presentation: a
footnote is only as good as the discipline of whoever checks it. Shoal enforces
the binding in the architecture, so the unverified path is not available to
callers, applications, or agents.

## What Shoal refuses to do

| It will not | Because |
|---|---|
| Assert a claim it cannot cite | Only claims backed by verified citations become output. The rest are recorded as unresolved issues, so a thin answer is visible as thin rather than dressed up |
| Persist a citation that was not derived from the source | Anchor identity is recomputed from the citation and its resolved quote on write. An invented or edited anchor is rejected, not stored |
| Search what you may not read | Documents outside a caller's authorization are removed from the candidate set before scoring, so unauthorized content cannot influence a result it can never appear in |
| Let an empty result and a withheld one look different | A term whose matches exist but are withheld returns exactly what a term matching nothing returns, so a caller cannot probe for the existence of content it may not read |
| Substitute a retrieval strategy you did not ask for | An unconfigured vector mode fails explicitly. It never silently falls back to lexical and calls it a match |
| Serve without an identity | The local development authenticator refuses any listener another host can reach; the remote backend is closed because it cannot yet carry the caller's decision upstream |

That last row is the pattern, not an exception. Where Shoal cannot preserve the
evidence-to-authority chain, the feature is closed rather than left permissive.

## Sixty seconds

```bash
# 1. Ingest a source into a durable local corpus.
go run ./cmd/shoal-explore ingest \
  -data .shoal/explorer -file docs/platform-product-plan.md

# 2. Ask a grounded question. The default provider is deterministic and
#    offline, so this works with no API key and no network.
go run ./cmd/shoal-explore ask \
  -data .shoal/explorer \
  -question "what gates local to cluster promotion?"
```

You get verified claims with exact document, revision, and byte-range
citations, the graph paths behind them, the snapshot the answer was pinned to,
and a trace of how it was reached. Ask about something the corpus does not
cover and you get a grounded no-answer with an unresolved issue, not an
invention.

Swap the provider for a local Ollama model or an OpenAI-compatible endpoint
with `-provider`; nothing above changes.

One honest note about this path. `shoal-explore` is a single-user local tool
and reports `AuthorizationEnforced: false` in its own output, because there is
no principal to enforce against. The authorization rows in the table above are
enforced by the authorized client that backs the web workspace, the MCP server,
and anything else serving more than one identity. Shoal says which of the two
you are running rather than letting you assume.

## Why this rather than a bigger context window

Models, tool protocols, and agent frameworks are converging fast. An agent that
can call your ticket system is not scarce, and will be less scarce next year.

What stays scarce is the ability to establish, at the moment a decision is
made: what is known, why it is believed, whether it is still valid, whether
this actor may rely on it, and whether the resulting action is permitted.

Shoal keeps that chain intact and checkable. Because the knowledge plane is
separate from model execution, the model, the harness, and the tool protocol
can all be replaced without discarding the accumulated, attributable
understanding underneath.

Two honest limits. Provenance proves where a belief came from, not that it
still holds, which is why snapshot identity, revisions, `as_of`, and ontology
evolution matter as much as citation does. And a verified, authorized,
correctly cited claim can still be wrong if the world moved; the record is
designed to make that detectable, not impossible.

## Where Shoal stops

Shoal governs the epistemic and authorization boundary around execution. It
does not compete with the execution ecosystem.

It registers agents, narrows their capabilities, leases and dispatches work,
records execution evidence, and publishes lifecycle events. It will run work
whose only effect is on its own evidence record, such as answering a grounded
question about its corpus. Anything with an effect outside that boundary is
dispatched to an external executor and recorded, never performed by Shoal.

That keeps model and harness commoditization working in your favour: whatever
executes the work, the warrant for why it was allowed lives here.


## Is this for you?

- **Document intelligence** — navigate long technical, legal, financial, and
  operational sources by structure, then retrieve exact cited passages.
- **Code exploration** — connect files, symbols, revisions, diagnostics, and
  source ranges without coupling Shoal to one parser.
- **Cross-document knowledge graphs** — relate evidence across sources and
  inspect the path behind a result.
- **Private knowledge for agents** — keep source content and indexes inside
  your own infrastructure, with authorization enforced on every read.
- **Grounded inference contracts** — assemble immutable evidence packs from
  verified results and validate provenance-bearing claims without coupling to
  a model vendor or transport.
- **Local-to-cluster** — prototype against an embedded corpus and keep the
  same graph, document, and retrieval contracts as storage moves toward
  Accumulo scale.

## Choose a path

| Goal | Start here |
|---|---|
| Ingest and explore a cited document corpus | [Explorer alpha](#explorer-alpha-ingest-explore-retrieve) |
| Build with the public knowledge contracts | [`pkg/document`](pkg/document) · [`pkg/graph`](pkg/graph) · [`pkg/retrieval`](pkg/retrieval) · [`pkg/ontology`](pkg/ontology) · [`pkg/inference`](pkg/inference) · [`pkg/explorer`](pkg/explorer) |
| Define grounded generation boundaries | [`docs/inference-contracts.md`](docs/inference-contracts.md) |
| Run a local database with RFile or Parquet | [Embedded engine](#embedded-engine-standalone-no-zookeeper) |
| Use Sharkbite import-compatible Python APIs | [`python/README.md`](python/README.md) |
| Embed the Accumulo client or stable C ABI | [`accumulo/`](accumulo/) · [`capi/README.md`](capi/README.md) |
| Evaluate Shoal replacement roles with Accumulo | [`FEATURES.md`](FEATURES.md#accumulo-replacement-roles) · [`docs/tserver-hosting-lifecycle.md`](docs/tserver-hosting-lifecycle.md) |
| Validate against an exact Accumulo 4 cluster | [`test/accumulo/README.md`](test/accumulo/README.md) |

See [`FEATURES.md`](FEATURES.md) for the complete capability and validation
matrix, and [`docs/platform-product-plan.md`](docs/platform-product-plan.md)
for the accepted local-to-Accumulo product direction. Production replacement
roles remain gated by the live conformance verdicts tracked in
[issue #74](https://github.com/phrocker/shoal-oss/issues/74).

`pkg/inference` provides public, provider-neutral contracts, and
`pkg/contextpack` deterministically builds bounded packs from Explorer
retrieval and hydration APIs. `pkg/inference/harness` can run a bounded
model-guided loop over an already authorized Explorer client and returns an
inspectable trace. Shoal does not ship a Copilot/SDK hosted execution backend
by default.


## Explorer alpha: ingest, explore, retrieve

The alpha accepts UTF-8 Markdown and plain text, persists the corpus in
Shoal's embedded engine, builds deterministic document and graph structure,
and returns revision-specific span citations.

For a clean-checkout, command-by-command demo that also starts the optional
web workspace, see
[`docs/explorer-demo-walkthrough.md`](docs/explorer-demo-walkthrough.md).

```bash
# List documents, then inspect one document or graph neighborhood.
go run ./cmd/shoal-explore list -data .shoal/explorer
go run ./cmd/shoal-explore outline \
  -data .shoal/explorer -document <document-id>
go run ./cmd/shoal-explore neighbors \
  -data .shoal/explorer -node <section-or-document-id> -depth 2

# Retrieve evidence with exact source ranges and an explanation path.
go run ./cmd/shoal-explore query \
  -data .shoal/explorer \
  -text "what gates local to cluster promotion?"
```

Explorer currently provides deterministic lexical, tree, and hierarchy-graph
ranking. Vector mode fails explicitly until a vector strategy is configured;
it never silently substitutes a different retrieval plan. PDF, source-code,
embedding, and remote adapters can use the same public contracts while
remaining separate from the product workflow.

`shoal-explore ask` builds a snapshot-pinned context pack from retrieved
evidence, runs the bounded `pkg/inference/harness` exploration loop, and emits
only verified claims plus supporting evidence. Output includes exact document
citations, quotes, byte ranges, any graph paths, redacted provenance, local
execution semantics, budget limits, and a concise trace summary; add `-trace`
for per-iteration details or `-format markdown` for a readable report. When no
evidence matches, the command returns a grounded no-answer response with an
unresolved issue instead of inventing a claim. Opaque Shoal IDs in ask output
and graph metadata keys/values are base64-encoded so document, graph, and
evidence identifiers round-trip losslessly. The default provider is the
deterministic fake so the command works without network access; select
`-provider ollama` for a local Ollama model.

For embeddings, the recommended local path is a real embedding model through
`OllamaEmbedder` (for example `nomic-embed-text`, `mxbai-embed-large`, or
`all-minilm`) or `OpenAIEmbedder` pointed at a loopback OpenAI-compatible
server such as llama.cpp or vLLM with no API key. The `pkg/model` package also
includes a dependency-free local lexical embedder strictly as a zero-dependency
offline and CI fallback; it produces lexical/statistical vectors only and makes
no semantic quality claim. Anthropic publishes no embeddings API, so hosted
embedding paths are OpenAI-compatible endpoints or Voyage. Credentials for
hosted providers are resolved at request time and are never printed or stored.

### Local web workspace

`shoal-explore-web` serves an evidence-first browser workspace over an existing
corpus: paged documents with their authored hierarchy, retrieval with exact
revision and span citations and score explanations, and a bounded interactive
graph canvas with cursor-based expansion and directed path finding.

```bash
go run ./cmd/shoal-explore-web \
  -data .shoal/explorer -listen 127.0.0.1:8080 -dev-auth
```

Every request is authorized. The transport binds one trusted decision per
request and serves through the decision-enforcing client, so a caller sees only
the documents, spans, edges, and evidence its decision permits; a request that
cannot be authenticated is answered `401` and never reaches the service.

`-dev-auth` is a loopback-only development authenticator and refuses any
listener another host can reach. Exposing the workspace requires the
provider-neutral OIDC authenticator, and the `-backend remote` mode stays
closed until it can carry the caller's decision upstream. Deployment,
authentication, and the startup policy backfill are covered in
[`docs/shoal-explore-web-deploy.md`](docs/shoal-explore-web-deploy.md).

### MCP stdio workspace

`shoal-mcp` serves the same authorized embedded workspace over
newline-delimited JSON-RPC on stdin and stdout, so an MCP-capable agent gets
retrieval, documents, graph neighborhoods, grounded ask, provenance, and
context compression under the same authorization and recording rules as the
browser.

```bash
go run ./cmd/shoal-mcp -data .shoal/explorer
```

The launcher configuration is the trusted identity source: the command grants
nothing implicitly and refuses to serve rather than assume an identity. Tool
surface, recording, and configuration are documented in
[`docs/mcp-stdio.md`](docs/mcp-stdio.md).

### Trees, graphs, and vectors are complementary

A document tree is an excellent way to preserve a source's authored
structure, but it is not the whole knowledge model. Shoal keeps tree
navigation as one retrieval strategy alongside lexical search, vector search,
typed cross-document graph traversal, temporal state, and authorization.
Explorer is therefore an opinionated view over general document, graph, and
retrieval APIs—not a tree-only storage system.


## Build and platform quick start

Local development requires Go 1.25+, Python 3.9+, `make`, and a native C
toolchain. Docker with Compose v2 is required only for the live HDFS and
Accumulo conformance suites.

```bash
git clone https://github.com/phrocker/shoal-oss.git
cd shoal-oss
make build
make test
```

Go Accumulo-client consumers can use:

```bash
go get github.com/phrocker/shoal-oss/accumulo
```

Run the lower-level embedded engine without ZooKeeper or Accumulo:

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
Exact vector queries additionally require
`--embedding-space '<stable provider/model/version/dimension/normalization identity>'`;
legacy or mixed files are refused rather than compared by raw score.

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
