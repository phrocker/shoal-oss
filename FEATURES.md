# Shoal features

This document is the feature and validation index for Shoal. The
[`README.md`](README.md) is the task-oriented entry point; detailed design and
operational contracts remain in the linked runbooks and ADRs.

## Status legend

- **Shipped**: implemented, tested, and available on `main`.
- **Partial**: useful implementation exists, but a named production adapter or
  contract remains unimplemented.
- **Code complete; live validation pending**: the production path and
  fail-closed conformance gate are implemented, but the exact Accumulo 4
  Docker verdict still needs to run on a Docker-capable host.
- **External validation pending**: implementation is complete, but native
  platform or release-infrastructure evidence is unavailable in the current
  environment.
- **Infrastructure blocked**: repository code or scripts exist, but completing
  the release path requires credentials or infrastructure unavailable to the
  current maintainers.

## Core storage engine

| Capability | Status |
|---|---|
| Embedded engine with WAL, memtable, splits, flush, and compaction | Shipped |
| RFile read/write, locality groups, CRC, compression, and block metadata | Shipped |
| Native Parquet persistence, restart, import/export, migration, and compaction | Shipped |
| Indexed Parquet reads with row-group statistics, bloom filters, and page indexes | Shipped |
| Mixed RFile/Parquet tablet scans and ShoalQL execution | Shipped |
| Local, memory, GCS, S3, Azure Blob, and HDFS storage | Shipped |
| Atomic publication, cancellation, recovery, and stale-artifact cleanup | Shipped |
| Format policy and authoritative-RFile promotion boundary | Shipped |

## Query and indexing

| Capability | Status |
|---|---|
| ShoalQL parser, planner, capability contracts, and text/JSON `EXPLAIN` | Shipped |
| SQL predicates, ranges, ordering, and aggregation | Shipped |
| Graph traversal and latent-edge discovery | Shipped |
| Document predicates and term-index queries | Shipped |
| Exact vector search | Shipped |
| Distributed IVF-PQ lifecycle, tombstones, freshness, and recall contracts | Shipped |
| Local RFile, local Parquet, mixed, and Accumulo-backed corpus parity | Shipped |
| Provider-neutral model I/O with deterministic fake and bounded Ollama adapters | Shipped |

## Accumulo replacement roles

| Role or capability | Status |
|---|---|
| Stateful scan and multi-scan sessions, continuation, expiry, and close | Shipped |
| Fail-closed Accumulo 4 iterator capability registry | Shipped |
| Tablet ServiceLock, manager RPCs, metadata/config/file/WAL loading | Shipped |
| Tablet ingest routing, fenced WAL authority, recovery, and minor compaction | Shipped |
| `shoal-tserver` process, readiness, metrics, TLS, drain, and deployment | Code complete; live validation pending |
| External compaction executor and `shoal-compactor` worker | Code complete; live validation pending |
| Offline full major compaction | Shipped |
| Local-to-Accumulo promotion, split reconciliation, and cutover state | Partial — durable state and FATE integration are shipped; public engine/manager authority and cutover adapters remain |

Replacement-role live evidence is tracked by
[issue #74](https://github.com/phrocker/shoal-oss/issues/74). The checked-in
verdict runner reports `unsupported`, not `pass`, when Docker or a required
live adapter is unavailable.

## Client APIs and Sharkbite compatibility

| Surface | Status |
|---|---|
| Go Accumulo 4 client, topology, scanner, writer, and administration | Shipped |
| Stable C ABI with capability negotiation and owned lifetimes | Shipped |
| `shoal-sharkbite` wheel/sdist with `sharkbite` and `pysharkbite` imports | Shipped |
| Python scanner, batch scanner, mutation, writer, and administration APIs | Shipped |
| Python RFile and HDFS APIs | Shipped |
| Structured errors, logging, deadlines, retries, GIL, and fork-safety contracts | Shipped |
| Windows shared/static ABI and wheel artifacts | Shipped |
| Linux manylinux distribution | Shipped |
| Native macOS dylib/wheel runtime and Mach-O verification | Optional/deferred by @phrocker on 2026-08-20 |
| Controlled Linux manylinux release workflow | Shipped and main-verified in run [32493148857](https://github.com/phrocker/shoal-oss/actions/runs/32493148857); macOS publication is optional |

The normative matrix is
[`docs/sharkbite-compatibility.md`](docs/sharkbite-compatibility.md), with
scope defined by
[`docs/sharkbite-client-scope.md`](docs/sharkbite-client-scope.md). Revision
56 satisfies 390 of 394 required rows. The four remaining rows require live
Accumulo 4 evidence:

- `SB-SCAN-022`, `SB-SCAN-026`, `SB-XCUT-015`,
  `SB-XCUT-016`.

Native macOS publication/runtime rows `SB-PKG-008`, `SB-XCUT-014`, and
`SB-XCUT-019` remain visible but are optional/deferred under the named,
dated owner decision.

## Operations and conformance

| Capability | Status |
|---|---|
| Semantic readiness, metrics, alerts, TLS/mTLS, and bounded drain | Shipped |
| Kubernetes/Helm manifests, PDBs, secure contexts, and rollout policy | Shipped |
| Exact Accumulo 4 ZooKeeper/HDFS/Accumulo Docker harness | Shipped |
| Deterministic replay verdicts with source hashes | Shipped |
| Live tserver, scanserver, client, compactor, and promotion verdict adapters | Shipped |
| Live Docker execution and retained verdict/log artifacts | External validation pending |

Run the static and live gates from a Linux Docker host:

```bash
make test-accumulo-static
make test-accumulo
make conformance-replay
make conformance-live
```

See [`test/accumulo/README.md`](test/accumulo/README.md) for prerequisites,
expected evidence, debugging, and cleanup commands.

## Detailed documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md): coordination and serving architecture
- [`docs/tserver-hosting-lifecycle.md`](docs/tserver-hosting-lifecycle.md):
  tablet-server lifecycle and fencing
- [`docs/tablet-ingest-service.md`](docs/tablet-ingest-service.md): ingest,
  WAL, and minor-compaction authority
- [`docs/write-tier-operations.md`](docs/write-tier-operations.md):
  production operations
- [`docs/promotion.md`](docs/promotion.md): promotion and cutover
- [`docs/offline-compaction.md`](docs/offline-compaction.md): offline
  compaction runbook
- [`python/README.md`](python/README.md): Python install and API usage
- [`test/accumulo/README.md`](test/accumulo/README.md): exact Accumulo 4
  conformance harness
