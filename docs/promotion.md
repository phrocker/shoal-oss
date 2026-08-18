<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Promoting a local Shoal table into Accumulo — design

Status: **partial**. This document covers the first slice of #70: the
client-side half of Accumulo's Bulk Import V2 protocol
(`internal/promotion`, `accumulo.Connector.BulkImport`,
`managerclient.TableBulkImport`). Cutover/fan-in semantics, promotion-state
APIs, and live-cluster verification are not yet implemented — see
[§5](#5-whats-deferred) for the full list against #70's acceptance
criteria.

## 1. Goal and authority invariant

#70 asks for a way to move an already-exported local Shoal table into an
existing Accumulo cluster "through supported Accumulo primitives (bulk
import/manager authority), never direct uncoordinated metadata edits."
That invariant drives every design choice here:

- Promotion **never writes to `accumulo.metadata` or ZooKeeper**, and never
  invents its own notion of a tablet, split, or load state.
- The only Accumulo-facing call this package makes is submitting the
  standard `TABLE_BULK_IMPORT2` **FATE** operation through the table's
  **manager**, the same operation Accumulo's own `TableOperations.importDirectory()`
  submits. The manager's `PrepBulkImport`/`CopyFiles`/`LoadFiles`/`RefreshTablets`
  FATE steps remain the sole authority over how the destination table's
  tablets and file set change as a result.
- Shoal's promotion code is a **producer of a load mapping**, not a
  participant in tablet assignment or metadata mutation. If Accumulo's FATE
  operation fails, rejects, or reconciles splits differently than
  requested, that decision stands unchallenged.

## 2. Wire contract (verified against upstream Java, not reinvented)

Accumulo's Bulk Import V2 client side is Gson-serialized JSON, not Thrift,
so the contract had to be reverse-engineered from the exact upstream
sources listed in `REFERENCES.md` rather than assumed. The load mapping
(`loadmap.json`, written at the bulk directory root —
`Constants.BULK_LOAD_MAPPING`) is:

- A **top-level JSON array**, not a single object
  (`clientImpl.bulk.LoadMappingIterator` streams it via
  `JsonReader.beginArray()`).
- Each element: `{"tablet": {"endRow": "<base64url>", "prevEndRow":
  "<base64url>"}, "files": [{"name": "...", "estSize": N, "estEntries":
  N}, ...]}` — field names are the private Java field names verbatim (no
  Gson naming policy is applied).
- `endRow`/`prevEndRow` are encoded with `Base64.getUrlEncoder()` —
  URL-safe, padded — matching Go's `base64.URLEncoding`
  (`clientImpl.bulk.ByteArrayToBase64TypeAdapter`).
- A nil/unbounded `endRow` or `prevEndRow` **omits the JSON key entirely**
  (Gson's default null-suppression), rather than serializing `null`.
- Entries must appear in **ascending order by `endRow`**, with a nil
  (unbounded) `endRow` sorting **last** — `KeyExtent.compareTo`'s
  `nullsLast` ordering. Violating this throws on the read side.
- The destination table ID is **not** part of the per-entry JSON; it is
  supplied as a separate FATE argument (mirrored by
  `managerclient.TableBulkImport`'s 3-argument contract:
  `[tableID, bulkDir, setTime]`), matching
  `FateServiceHandler`/`BulkImport.java`'s own call shape.

## 3. Why no RFile-index reading or live destination lookup is needed

Accumulo's own bulk-import client normally must open each RFile's index to
discover its row range and query the *destination* table's live tablet
metadata to compute a load mapping
(`clientImpl.bulk.BulkImport.computeMappingFromFiles`) — because it is
importing arbitrary externally-produced files of unknown provenance.

Accumulo also exposes a caller-supplied-partition mode
(`LoadPlan`/`RangeType.TABLE`): callers may supply `(startRow, endRow)`
pairs directly as destination `KeyExtent`s, and the server's
`PrepBulkImport` FATE step creates matching splits if the destination table
doesn't already have them.

Shoal's own `RFileExportManifest` already records, per RFile, exactly
which of the *source* table's own tablet ranges it came from
(`RFileExportTablet.{StartRow,EndRow}` + `RFileExportFile.TabletIndex`),
and those ranges are already a valid, gapless, ordered partition of the
source table's keyspace. `internal/promotion.BuildLoadMapping` uses that
existing partition directly as the destination `KeyExtent` set — this is
the `LoadPlan`-style path, fully supported by Accumulo, and it avoids
opening RFile indexes or querying a live destination cluster. It also means
the destination table's split points end up matching the source table's,
which is what "cell-equivalent results" (acceptance criterion 1) requires
in the first place.

## 4. What's implemented

```
RFileExportManifest (existing)  →  promotion.BuildLoadMapping
                                          │
                                          ▼
                                  promotion.StageBulkDir
                    (flattens nested export files into bulkDir,
                     writes bulkDir/loadmap.json)
                                          │
                                          ▼
                                  promotion.Promote
                                          │
                                          ▼
                        accumulo.Connector.BulkImport(tableName, bulkDir)
                                          │
                                          ▼
                  managerclient: FATE TABLE_BULK_IMPORT2 [tableID, bulkDir, setTime]
                                          │
                                          ▼
                     Accumulo manager's own bulk-import FATE operation
                (sole authority over destination tablets/file set from here)
```

- `internal/promotion.BuildLoadMapping` — pure function, manifest → load
  mapping, per §3.
- `internal/promotion.WriteLoadMapping` / `ReadLoadMapping` — the exact
  JSON shape from §2.
- `internal/promotion.StageBulkDir` — flattens an export manifest's nested
  `t-NNNN/` files into a flat bulk directory via `storage.Copy` (copies,
  never moves, so a failed stage never loses the source export; re-running
  with the same inputs reproduces byte-identical output) and writes
  `loadmap.json`. Detects basename collisions across the whole manifest
  before copying anything, so a collision never leaves a half-staged
  directory.
- `internal/promotion.Promote` — composes `StageBulkDir` with a
  `BulkImporter` (satisfied by `*accumulo.Connector`) to submit the FATE
  call.
- `accumulo.Connector.BulkImport` — resolves the destination table name to
  its stable ID, then submits `TABLE_BULK_IMPORT2` through the same
  `executeTableMutation` path (manager resolution, discovery-cache
  invalidation, error mapping) used by `FlushTable`/`CreateTable`/etc.
- `managerclient.TableBulkImport` — the FATE operation itself:
  `manager.TFateOperation_TABLE_BULK_IMPORT2`, 3 arguments
  (`tableID`, `bulkDir`, `setTime`).

## 5. What's deferred

Mapped against #70's five acceptance criteria:

1. **"a local table can be promoted into an existing Accumulo instance and
   queried with cell-equivalent results"** — the mechanism above is
   protocol-correct against upstream Java source and unit-tested end to
   end with fakes, but has **not been verified against a live Accumulo
   cluster** (none is available in this environment). Split points are
   carried over exactly (§3), which is the strongest available argument
   for cell-equivalence short of an actual query.
2. **"rerunning any interrupted transfer is safe"** — `StageBulkDir` is
   idempotent (deterministic bulk dir contents, copy-based, no partial
   writes on error), so re-running `Promote` before the FATE call is
   submitted is safe. Detecting and skipping re-submission of a bulk
   import that Accumulo already accepted (i.e. FATE-level idempotency) is
   not implemented — that's inherent to Accumulo's own FATE operation and
   hasn't been exercised against a live manager here.
3. **"split changes and partial uploads recover without manual metadata
   repair"** — split reconciliation is delegated entirely to Accumulo's
   own `PrepBulkImport` FATE step (§3); this package does not add any
   split-repair logic of its own, and that delegation is unverified
   against a live cluster.
4. **"a documented cutover protocol"** — not implemented. No
   promotion-state or cutover API surface is exposed yet; this slice is
   promotion of a single point-in-time export, not the ongoing fan-in /
   cutover semantics #70 also asks for.
5. **"graph, document, and vector fixtures pass before and after
   promotion"** — not exercised; requires a live cluster.

Also explicitly out of scope for this slice: RFile-index-based load-mapping
computation for externally-produced files (not needed given §3, but also
not implemented — this package only handles Shoal's own manifests),
automatic basename-collision resolution (collisions are reported as
errors, not renamed), and encryption/authentication of the transfer itself
(inherited from whatever `storage.Backend`/`accumulo.Connector` are already
configured with; nothing new added here).

## 6. Testing

All tests use in-memory storage backends and fake FATE/manager RPC layers
(mirroring the existing patterns in `accumulo/*_test.go` and
`internal/managerclient/managerclient_test.go`) — there is no live
Accumulo cluster or Java toolchain in this environment. See the pull
request for the exact commands and pass/fail output.
