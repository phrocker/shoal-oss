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

Status: **partial**. This document covers the current safe slice of #70:
client-side staging plus submission of Accumulo Bulk Import V2
(`internal/promotion`, `accumulo.Connector.BulkImport`,
`managerclient.TableBulkImport`) for **unambiguous single-tablet exports
only**. Split-bearing exports, cutover/fan-in semantics, promotion-state
APIs, and live-cluster verification are not yet implemented — see
[§5](#5-whats-deferred).

## 1. Goal and authority invariant

#70 asks for a way to move an already-exported local Shoal table into an
existing Accumulo cluster "through supported Accumulo primitives (bulk
import/manager authority), never direct uncoordinated metadata edits."
That invariant drives every design choice here:

- Promotion **never writes to `accumulo.metadata` or ZooKeeper**, and never
  invents its own notion of a tablet, split, or load state.
- The only Accumulo-facing call this package makes is submitting the
  standard `TABLE_BULK_IMPORT2` **FATE** operation through the table's
  **manager**, the same operation Accumulo's own
  `TableOperations.importDirectory()` submits.
- Shoal's promotion code is a **producer of a staged bulk directory plus
  load mapping**, not a participant in tablet assignment or metadata
  mutation. If Accumulo's FATE operation fails or rejects the request,
  that decision stands unchallenged.

## 2. Wire contract (verified against upstream Java, not reinvented)

Accumulo's Bulk Import V2 client side is Gson-serialized JSON, not Thrift,
so the contract had to be reverse-engineered from the exact upstream
sources listed in `REFERENCES.md`. The load mapping (`loadmap.json`,
written at the bulk directory root — `Constants.BULK_LOAD_MAPPING`) is:

- A **top-level JSON array**, not a single object
  (`clientImpl.bulk.LoadMappingIterator` streams it via
  `JsonReader.beginArray()`).
- Each element:
  `{"tablet":{"endRow":"<base64url>","prevEndRow":"<base64url>"},"files":[{"name":"...","estSize":N,"estEntries":N},...]}`.
- `endRow`/`prevEndRow` are encoded with `Base64.getUrlEncoder()` —
  URL-safe, padded — matching Go's `base64.URLEncoding`
  (`clientImpl.bulk.ByteArrayToBase64TypeAdapter`).
- A nil/unbounded `endRow` or `prevEndRow` **omits the JSON key entirely**
  (Gson's default null-suppression), rather than serializing `null`.
- Entries must appear in **ascending order by `endRow`**, with a nil
  (unbounded) `endRow` sorting **last**.
- The destination table ID is **not** part of the per-entry JSON; it is
  supplied as a separate FATE argument, mirrored by
  `managerclient.TableBulkImport`'s 3-argument contract:
  `[tableID, bulkDir, setTime]`.

## 3. Why this slice only supports unambiguous single-tablet exports

Shoal's local tablets are `[StartRow, EndRow)` (inclusive start,
exclusive end — see `internal/engine/table.go`'s `routeTablet`), while
Accumulo's `KeyExtent` is `(PrevEndRow, EndRow]` (exclusive start,
inclusive end). Copying split boundaries from a multi-tablet Shoal
manifest into Accumulo extents therefore makes rows whose value exactly
equals a split point land on opposite sides of the boundary in the two
systems:

- in Shoal, a row equal to split `S` belongs to the tablet that **starts**
  at `S`;
- in Accumulo, the same row belongs to the tablet that **ends** at `S`.

That mismatch means a multi-tablet manifest cannot safely be promoted by
simply reusing its tablet boundaries. An exact split-point row can become
invisible unless the export is rewritten/materialized differently around
the boundary or a more destination-aware mapping strategy is designed.
This slice does **not** attempt either of those.

So the implementation now fails closed:

- legacy manifests with no `Tablets` entries are accepted only when every
  `RFiles[*].TabletIndex == 0`;
- explicit manifests are accepted only when they declare **exactly one**
  tablet and that tablet has `StartRow == nil` and `EndRow == nil`;
- anything else — multiple tablets, any non-nil boundary, undeclared
  tablet references, or one `DestinationPath` assigned to multiple tablet
  indexes — is rejected locally before staging writes or FATE submission.

For the supported case, `BuildLoadMapping` always produces a **single
fully unbounded** mapping entry: one `KeyExtent{PrevEndRow:nil,
EndRow:nil}` containing every exported file. This still avoids RFile-index
reading, and it avoids claiming any split carryover at all. Accumulo can
load a file that spans many destination tablets under one unbounded
mapping entry, so the destination split layout does not need to mirror the
source for this safe slice. That behavior is still not verified against a
live cluster here.

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
  mapping. Accepts only unambiguous single-tablet exports and returns one
  fully unbounded mapping entry. Rejects split/multi-tablet manifests,
  ambiguous legacy manifests, undeclared tablet references, and conflicting
  duplicate `DestinationPath` assignments.
- `internal/promotion.WriteLoadMapping` / `ReadLoadMapping` — the exact
  JSON shape from §2.
- `internal/promotion.StageBulkDir` — flattens an export manifest's nested
  `t-NNNN/` files into a flat bulk directory via `storage.Copy` (copies,
  never moves) and writes `loadmap.json`. Preflight-validates `bulkDir`
  plus the manifest's single-tablet shape before copying anything, so
  invalid destinations and rejected split manifests never leave a
  half-staged directory. It also verifies every referenced RFile against
  the manifest's recorded size/SHA256 (`engine.VerifyRFileExport`) before
  copying, and rejects any RFile whose destination path would alias its
  own source path — including via an absolute-vs-relative path
  difference or a symlink/hard link, not just an identical path string
  (`bulkDir` coinciding with the export's own tablet directory) — before
  copying it. `storage.Copy` truncates the destination for writing
  before streaming the source, so an aliased copy would otherwise
  destroy the source and report a false success.
- `internal/promotion.Promote` — composes `StageBulkDir` with a
  `BulkImporter` (satisfied by `*accumulo.Connector`) to submit the FATE
  call. Submits nothing when the derived mapping is empty (nothing to
  import).
- `accumulo.Connector.BulkImport` — resolves the destination table name to
  its stable ID, then submits `TABLE_BULK_IMPORT2` through the same
  `executeTableMutation` path used by `FlushTable`/`CreateTable`/etc.
- `managerclient.TableBulkImport` — the FATE operation itself:
  `manager.TFateOperation_TABLE_BULK_IMPORT2`, 3 arguments
  (`tableID`, `bulkDir`, `setTime`).

## 5. What's deferred

Mapped against #70's five acceptance criteria:

1. **"a local table can be promoted into an existing Accumulo instance and
   queried with cell-equivalent results"** — only the safe
   single-tablet-export case is supported. Split-bearing exports are
   explicitly rejected until a rewrite/materialization strategy exists for
   the boundary mismatch in §3, and none of this has been verified against
   a live Accumulo cluster in this environment.
2. **"rerunning any interrupted transfer is safe"** — `StageBulkDir` is
   deterministic and copy-based, so rerunning it with the same manifest
   and `bulkDir` safely reproduces the same bytes. A persistent I/O failure
   partway through copying *multiple* files can still leave a partially
   staged bulk directory, but retrying the same call completes it without
   manual repair. That guarantee covers staging only: once
   `Promote` has called `BulkImporter.BulkImport`, retry safety ends,
   because `TABLE_BULK_IMPORT2` FATE submission has no dedup/idempotency
   of its own. An ambiguous failure at or after that call (e.g. a
   timeout after the manager received the request but before the client
   observed a response) leaves the caller unable to tell whether the
   import already happened, so blindly calling `Promote` again in that
   window risks a duplicate bulk import. Closing that gap needs a
   promotion-state or idempotency-token API this slice does not yet
   have (see item 4 below).
3. **"split changes and partial uploads recover without manual metadata
   repair"** — not implemented for split-bearing exports, because those
   manifests are rejected before submission. A future rewrite or
   destination-aware materialization strategy would be needed before this
   criterion can be met.
4. **"a documented cutover protocol"** — not implemented. No
   promotion-state or cutover API surface is exposed yet; this slice is
   promotion of one point-in-time export, not the ongoing fan-in/cutover
   semantics #70 also asks for.
5. **"graph, document, and vector fixtures pass before and after
   promotion"** — not exercised; requires a live cluster.

Also explicitly out of scope for this slice: RFile-index-based mapping or
rewrite logic for split-bearing exports, automatic basename-collision
resolution (collisions are reported as errors), and
encryption/authentication of the transfer itself (inherited from whatever
`storage.Backend`/`accumulo.Connector` are already configured with).

## 6. Testing

All tests use in-memory storage backends and fake FATE/manager RPC layers
(mirroring the existing patterns in `accumulo/*_test.go` and
`internal/managerclient/managerclient_test.go`) — there is no live
Accumulo cluster or Java toolchain in this environment. See the pull
request for the exact commands and pass/fail output.
