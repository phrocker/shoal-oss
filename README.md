# shoal

A Go sorted key-value store with graph and vector query pushdown.
Independent project — inspired by [Apache Accumulo](https://accumulo.apache.org)'s
data model and RFile format, but its own engine that runs **standalone** with
no Accumulo cluster and no ZooKeeper required.

Shoal stores arbitrary keys and values as cells
(`row → (cf, cq, cv, ts) → value`) and runs a minimal in-process iterator
stack that pushes graph traversal, keyword, and vector queries down next to
the data. It is RFile-compatible end to end, so the same data and iterators
work whether you run it embedded on a laptop or fan out across a fleet.

Two ways to run it:

- **Embedded / standalone** (`shoal-embed`, `internal/engine`) — a
  self-contained storage engine with its own write-ahead log, memtable,
  compaction, and split policy. No external coordinator: open a data
  directory, create tables, write and scan. Durable RFiles land on the
  local filesystem or any pluggable backend (in-memory, GCS, S3), so the
  same engine scales from a single machine to cloud-durable storage. Use it
  via the CLI or the `ShoalEmbed` gRPC service.
- **Distributed serving** (`cmd/shoal`) — a shoal pod opens RFiles directly
  from object storage and serves single-shot Thrift `scan()` / `multiScan()`
  calls, with a client-side hedge coordinator racing N pods in parallel and
  taking the first response. Because shoal shares Accumulo's RFile and Thrift
  formats, this mode can also read an existing Accumulo cluster's data for
  interop — but it doesn't require one.

The rest of this README covers both modes; the embedded engine needs none of
the ZooKeeper machinery described in the distributed-serving sections.

## Status

V1 + IVF-PQ iterator port shipped. Embedded standalone engine shipped.

| Capability | Status |
|---|---|
| **Embedded engine** (`shoal-embed`, `internal/engine`) — no ZooKeeper | shipped — WAL + memtable + compaction + split policy |
| Embedded CLI (`init` / `write` / `scan` / `compact` / `status` / `serve`) | shipped |
| `ShoalEmbed` gRPC service (`proto/embed.proto`) | shipped |
| Pluggable durable backend (local FS / in-memory / GCS / S3) | shipped — local scale to cloud-durable |
| Graph pushdown: `EdgeExpandIterator` (one-hop neighborhood) | shipped |
| Graph pushdown: `LatentEdgeDiscoveryIterator` | shipped |
| Keyword pushdown: `TermIndexIterator` | shipped |
| Vector pushdown: `VectorKNNIterator` (brute-force k-NN) | shipped |
| Vector pushdown: `IvfPqDistanceIterator` (ADC + top-K) | shipped — wire-compatible with Java `VectorPQ.toBytes()` |
| `startScan` (single tablet, single range) | shipped |
| `startMultiScan` (BatchScanner shape) | shipped — auto-bins ranges across tablets |
| Multi-locality-group merge + CF pushdown (LG-skip) | shipped |
| RFile reader (block-level CRC, prefetch, snappy/gz/zlib) | shipped |
| `RFile.blockmeta` zone-map block-skip extension | shipped — forward-compatible with Java `RFile.Reader` |
| VisibilityFilter pushdown | shipped — alloc-free warm cache |
| File / locator / block caches | shipped |
| Startup pre-warm (`-prewarm-tables=auto`) | shipped — distributed-serving mode |
| Client-side hedge coordinator | shipped — `scanRow*` + `scanBatch*` overloads |

## Embedded engine (standalone, no ZooKeeper)

The embedded engine is a self-contained sorted KV store. It owns its own
write-ahead log, in-memory memtable, RFile flush + compaction, and tablet
split policy — there is no manager, no tablet server, and no ZooKeeper in
the loop. Point it at a data directory and go:

```bash
make build   # builds cmd/shoal-embed (and everything else) via go build ./...

# create a table, optionally pre-split by row prefix
shoal-embed init   --table graph --splits "entity:,event:,knowledge:" --data ~/.shoal/data

# write mutations (JSON lines on stdin)
shoal-embed write  --table graph --data ~/.shoal/data < mutations.jsonl

# scan back out as JSON lines
shoal-embed scan   --table graph --row-prefix "entity:" --data ~/.shoal/data

# flush + compact, or print status
shoal-embed compact --table graph --data ~/.shoal/data
shoal-embed status  --data ~/.shoal/data

# or serve the ShoalEmbed gRPC API for external consumers
shoal-embed serve  --data ~/.shoal/data --port 9876
```

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

**Local and at scale.** Durable RFiles flush through a pluggable
`storage.Backend`. The default is the local filesystem; an in-memory,
GCS, or S3 backend keeps each tablet's WAL local while flushing immutable
RFiles elsewhere — a locally-resident, cloud-durable store with the same
engine and iterators in both cases. WAL durability is tunable
(`SyncFull` / `SyncNormal` + group-commit interval).

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

## Distributed serving mode

In serving mode a shoal pod exposes a Thrift scan surface (mirroring
Accumulo's `TabletScanClientService` for interop) but is **not** a tablet
server — it doesn't write, doesn't host tablets, doesn't coordinate with any
manager. It's strictly a reader fanned out across pods:

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

# only the distributed-serving mode needs generated Thrift bindings:
make thrift-gen  # generate Go bindings from Accumulo-compatible .thrift IDL
```

The embedded engine builds with a plain `go build ./...` and has no Thrift
dependency. Generating bindings (serving mode) requires the Apache Thrift
compiler **exactly 0.17.0** (matches Accumulo's `version.thrift` in its root
`pom.xml`). Go 1.25+ (transitively from `cloud.google.com/go/storage`).

Docker image (multi-stage, distroless static):
```bash
docker build -t shoal:dev .
```

## Layout

```
cmd/
  shoal-embed/          embedded standalone engine — CLI + ShoalEmbed gRPC server (no ZK/Accumulo)
  shoal/                distributed serving daemon — metadata + Thrift listener
  shoal-bootstrap/      diagnostic CLI: walks ZK → root → metadata → tablets
  shoal-compactor/      standalone compaction worker
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
  thrift/gen/           generated Thrift bindings (gitignored, run thrift-gen)
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

## License

Apache License, Version 2.0.
