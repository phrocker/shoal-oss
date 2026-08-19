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
`managerclient.TableBulkImport`) for both single-tablet exports and
**multi-tablet/split-bearing exports**, the latter via destination split
reconciliation (`accumulo.Connector.AddTableSplits`,
`RequiredDestinationSplits`) ahead of staging. Durable/resumable
promotion-state tracking, duplicate-safe FATE resubmission, an explicit
cutover/fan-in state machine, and live-cluster verification are not yet
implemented — see [§5](#5-whats-deferred).

## 1. Goal and authority invariant

#70 asks for a way to move an already-exported local Shoal table into an
existing Accumulo cluster "through supported Accumulo primitives (bulk
import/manager authority), never direct uncoordinated metadata edits."
That invariant drives every design choice here:

- Promotion **never writes to `accumulo.metadata` or ZooKeeper**, and never
  invents its own notion of a tablet, split, or load state.
- The only *mutating* Accumulo-facing calls this package makes are two standard
  **FATE** operations submitted through the table's **manager**: a
  `TABLE_SPLIT` operation (`accumulo.Connector.AddTableSplits`, submitted
  only when a multi-tablet manifest's widened load mapping requires
  destination splits that don't already exist — see §3) and the
  `TABLE_BULK_IMPORT2` operation itself — the same two operations
  Accumulo's own `TableOperations.addSplits()` and
  `.importDirectory()` submit. `Promote` also makes one read-only
  metadata query, `accumulo.Connector.ListTableSplits`, immediately
  after `AddTableSplits`, to positively verify the destination's
  resulting splits rather than merely assume them (see §3.3) — this
  resolves tablet locations by walking the `accumulo.root`/
  `accumulo.metadata` system tables through this connector's normal
  scan-based metadata path (`ListTableSplits` → `Connector.Tablets` →
  `internal/cache.LocatorCache` → `internal/metadata.Walker`), the same
  mechanism this connector already uses to locate tablets for every
  scan or write, and the same one any Accumulo client (including the
  shell's `getsplits`) relies on. It is not a manager RPC/FATE
  operation, but it is a metadata-table read — through the standard
  client scan protocol against whichever tablet server hosts the
  relevant metadata range, not a raw or unmediated ZooKeeper/file
  access.
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

## 3. Multi-tablet exports: destination split reconciliation

Shoal's local tablets are `[StartRow, EndRow)` (inclusive start,
exclusive end — see `internal/engine/table.go`'s `routeTablet`), while
Accumulo's `KeyExtent` is `(PrevEndRow, EndRow]` (exclusive start,
inclusive end). Copying split boundaries from a multi-tablet Shoal
manifest into Accumulo extents verbatim would make rows whose value
exactly equals a split point land on opposite sides of the boundary in
the two systems:

- in Shoal, a row equal to split `S` belongs to the tablet that **starts**
  at `S`;
- in Accumulo, the same row belongs to the tablet that **ends** at `S`.

A naive "reuse the source boundaries verbatim" mapping is therefore never
safe: an exact split-point row would silently land in the wrong tablet
relative to the tablet its RFile actually belongs to.

### 3.1 The widening rule

Instead of reusing a tablet's own `[StartRow, EndRow)`, `BuildLoadMapping`
widens each destination `KeyExtent` so it can never exclude a row that
genuinely belongs to that tablet: for chain entry `i`, the resolved
extent's `PrevEndRow` is set to the **previous** tablet's own `StartRow`
(one tablet further back in the source chain), not tablet `i`'s own
`StartRow`. Concretely, for an `N`-tablet chain (0-indexed):

| index `i`   | `PrevEndRow`             | `EndRow`          |
|-------------|--------------------------|-------------------|
| `0`         | `nil` (always)           | `chain[0].EndRow` |
| `1`         | `nil` (always — `chain[0].StartRow` is always nil) | `chain[1].EndRow` |
| `i >= 2`    | `chain[i-2].EndRow`      | `chain[i].EndRow` |

Every resolved extent therefore spans **at most two** of the manifest's
own source tablets, regardless of overall chain length, and the last
entry's `EndRow` is always `nil` (unbounded) by chain-validation. A
2-tablet manifest is the smallest possible case and its second (last)
entry always collapses to fully unbounded, since there is no tablet
before index 0 to widen against — see `twoTabletManifest` in
`loadmapping_test.go`.

### 3.2 Why widening is provably correct — and where that proof's limits are

This is not a design choice taken on faith: it was checked line-by-line
against the actual upstream `PrepBulkImport.validateLoadMapping` (see
REFERENCES.md for the exact source URL). That method walks a
`PeekingIterator` over the destination's real tablets, sorted ascending,
whose "current tablet" position is created **once** and **persists across
every load-mapping entry** — it only ever advances forward via
`pi.next()`, and a tablet the iterator is currently resting on is not
force-advanced past before the next entry is checked. For each entry, the
algorithm advances the current tablet forward only while it does not
match the entry's declared `prevEndRow`, checks the match, then repeats
for `endRow`, leaving the iterator resting exactly on the real tablet
matching that entry's `endRow` once both checks succeed.

**When the destination's real splits, at or before the mapping's last
boundary row, are exactly the manifest's own chain boundaries** — no
fewer and no more — induction over the chain shows this always
validates: after entry `k` is validated, the iterator is resting on the
real destination tablet ending at `chain[k].EndRow`; because the
destination has no *other* split before that point, that real tablet's
own `prevEndRow` is exactly `chain[k-1].EndRow` (or `nil` for `k=0`) —
which is *exactly* what this package's widening rule sets
`resolved[k+1].PrevEndRow` to. So every entry's declared `PrevEndRow`
is matched by the real tablet the validation iterator is already
resting on, and the widened, seemingly-overlapping extents (e.g.
`(nil,"d"]` immediately followed by `(nil,"m"]`) validate correctly.

**That induction's premise — no extra splits before the last required
boundary — is doing real work, and does not hold for free.** An earlier
version of this document (and of `RequiredDestinationSplits`'s doc
comment) claimed the opposite: that "extra, unrelated pre-existing
destination splits beyond the required boundary rows are harmless." That
claim is false, and was corrected after Copilot's automated review of
this PR caught it. Concretely: if the destination already has a split at
row `c` with `c < d` (`d` being the first required boundary), validating
entry 0 `(nil,d]` leaves the iterator resting on the real tablet
`(c,d]`, not `(nil,d]` — its own `prevEndRow` is `c`, not `nil` — so
entry 1's widened `prevEndRow=nil` can never match, the iterator cannot
rewind to look further back, and the whole submission is rejected with a
spurious `BULK_CONCURRENT_MERGE` ("Concurrent merge happened") even
though nothing actually merged. The same failure occurs for an extra
split anywhere else before the last required boundary row, not only
before the first one. An extra split strictly *after* the last required
boundary row is harmless with respect to *this specific* validation
walk: it falls entirely inside the final, always-unbounded entry's own
`endRow` search, which the iterator simply walks past on its way to
matching `nil`, so it can never reproduce the `prevEndRow` mismatch
described above. That is not the same as harmless in every sense a
bulk import can fail, though — see the `table.bulk.max.tablets` caveat
in [§5](#5-whats-deferred) item 3.

Neither `BuildLoadMapping` nor `RequiredDestinationSplits` can detect an
unsafe pre-existing split on their own: both are pure functions over the
manifest alone, with no view of the destination's actual state. §3.3
describes how `Promote` positively verifies the "no more" half of the
destination's splits instead of merely assuming it.

Separately, if the destination is **not** reconciled at all (still only
its original single unbounded tablet), a multi-tablet promotion attempt
is rejected outright via `validateLoadMapping`'s `!pi.hasNext()`
special-case branch — the same "concurrent merge"-style rejection
Accumulo itself uses; it does not corrupt data or silently drop rows.

### 3.3 Reconciling the destination before staging

`RequiredDestinationSplits(manifest)` returns exactly the `N-1` boundary
rows an `N`-tablet manifest's widened load mapping references (`nil` for
a single-tablet/legacy manifest, which needs no destination splits at
all). It is a pure, network-free function — it performs the same chain
validation `BuildLoadMapping` does, but never touches storage or
Accumulo.

`Promote` calls it before doing anything else, and — only when it
reports at least one row — submits those rows through
`accumulo.Connector.AddTableSplits` (a manager `TABLE_SPLIT` FATE
operation, never a direct metadata/ZooKeeper edit) before staging or
submitting the bulk import. That guarantees the destination has *at
least* the required rows, but — as §3.2 explains — the widening rule
also requires the destination to have *no other* split at or before the
last required row, and `AddTableSplits` cannot itself remove a
pre-existing or concurrently-added split that doesn't belong to this
promotion (it is, correctly, additive-only). So immediately after
`AddTableSplits` succeeds, `Promote` also calls
`accumulo.Connector.ListTableSplits` and runs
`verifyNoUnexpectedDestinationSplits`: if the destination's splits at or
before the last required row are not exactly the required set, `Promote`
fails closed with a specific, actionable error naming the offending
row(s), before `StageBulkDir` or `BulkImport` ever run — turning what
would otherwise be a confusing, unexplained `BULK_CONCURRENT_MERGE` deep
inside Accumulo into an explicit, Shoal-side failure.

`BuildLoadMapping` and `StageBulkDir` themselves never call Accumulo: a
caller invoking them directly for a multi-tablet manifest outside of
`Promote` is responsible for that same two-part reconciliation itself —
both adding the required splits and confirming no unrelated one exists
in range — or the subsequent `BulkImport` call may fail closed (not
silently) as described above.

**Residual race, stated plainly:** this verification narrows, but does
not close, the window for a concurrent structural change on the
destination. A split or merge racing between
`verifyNoUnexpectedDestinationSplits` succeeding and the later
`BulkImport` FATE call's own server-side validation running could still
invalidate the mapping `Promote` just staged. Accumulo's own defense
against that is to reject the bulk import cleanly (the same
`!pi.hasNext()`/concurrent-merge-style failure from §3.2), not to
corrupt data — so this is a safe-failure gap, not a correctness one.
Closing it fully would require either a promotion-state API that
re-validates immediately before submission or an Accumulo-side
split-then-import atomicity guarantee this package does not control; see
[§5](#5-whats-deferred).

### 3.4 Guarding against destination table identity drift

A table name is not a stable handle across the same staging window §3.3
discusses: Accumulo lets a table be deleted and a different, unrelated
table created under the identical name, and `AddTableSplits`,
`ListTableSplits`, and `BulkImport` each independently resolve
`tableName` to a table ID on their own, through
`internal/tablenames.Resolver`'s cache — which has no enforced TTL and
is only refreshed by an explicit `Invalidate()` call. Without a check
for this, a destination table deleted and recreated under the same name
partway through `StageBulkDir`'s wall-clock copying could pass
`verifyNoUnexpectedDestinationSplits` by coincidence — an empty
replacement table has no splits to conflict with, for instance, if the
manifest only required one — and still receive `tableName`'s bulk
import despite being a different table than the one `Promote`
reconciled splits against moments earlier.

`accumulo.Connector.ResolveTableID` (new alongside this check) forces
exactly that: it invalidates the discovery cache before resolving, so
it always observes `tableName`'s real, current table ID rather than a
cached one from before a delete-and-recreate. `Promote` calls it once,
before `AddTableSplits`, to pin the destination's identity as
`pinnedTableID` — but only for a multi-tablet manifest (`len(splits) >
0`); a single-tablet or legacy manifest has no split-reconciliation step
to pin against in the first place. It calls `ResolveTableID` again,
through `verifyDestinationTableIdentity`, immediately before
`BulkImport`, and fails closed if `tableName` now resolves to a
different ID than the one it pinned — naming both the expected and the
actual table ID in the error.

This is a narrower, independent check from
`verifyNoUnexpectedDestinationSplits`: it does not inspect splits at
all, so it catches an identity change even in the case where the
replacement table happens to end up with the same splits, and it
covers the *entire* staging window end to end, not only the
`AddTableSplits`/`ListTableSplits` round-trip §3.3 already guards. The
two checks are complementary, not redundant: one confirms the
destination's *shape*, the other confirms it is still the same
*table*.

**What this trusts, and cannot verify for itself:** the check relies on
one guarantee only Accumulo's manager can actually provide — that a
table ID, once assigned, is never reused for a different table. Real
table IDs are assigned from a ZooKeeper sequential counter as a side
effect of a real `CreateTable`/`DeleteTable` FATE operation (verified
against `accumulo/table_admin.go`, not assumed), never fabricated
client-side, so an ID mismatch observed here can only mean the table
actually changed identity — never a false positive from ID reuse. But
that is the manager's guarantee, not something `ResolveTableID` proves
on its own; this package has no way to independently audit it.

**Residual race, stated plainly (same shape as §3.3's):** pinning
narrows, but cannot close, the very last sliver of the window: a
delete-and-recreate landing strictly between this final
`ResolveTableID` call and `BulkImport`'s own separate internal
resolution remains possible in principle. Closing that sliver fully
would need the same promotion-state or idempotency-token API
[§5](#5-whats-deferred) already identifies as missing for the
`BulkImport` step itself — a single check immediately beforehand cannot
be made atomic with the FATE submission that follows it using only the
`Promoter` interface this slice has today.

**Deliberately out of scope:** single-tablet ("safe single-tablet
staging") promotion is not covered by this check at all, even though
the same wall-clock staging window exists for it too — it has no
`AddTableSplits` reconciliation step, and so nothing to pin a table ID
across. That gap predates this slice and is not silently ignored; it is
named here as a known, deliberately out-of-scope boundary rather than
folded into a multi-tablet-specific fix this late in this PR's review
cycle.

## 4. What's implemented

```
promotion.Promote(src, manifest, dst, bulkDir, conn, tableName, opts)
                                          │
                                          ▼
              validatePromotionDestination(dst, tableName, bulkDir)
                (tableName/bulkDir shape, plus a writability preflight --
                 dst must implement storage.WritableBackend before
                 anything below can mutate it -- see §4's Promote bullet)
                                          │
                                          ▼
                            promotion.BuildLoadMapping
                (preflight: validates the manifest's tablet-chain
                 shape and every RFile's reference into it; result
                 discarded here -- see the Promote bullet below)
                                          │
                                          ▼
                            promotion.stagingPreflight
                (verifies every RFile's declared size/SHA256 against
                 its actual bytes via engine.VerifyRFileExport, plus
                 the source-alias dedup and flattened-basename/path-
                 safety checks StageBulkDir itself repeats below)
                                          │
                                          ▼
                     promotion.RequiredDestinationSplits
                       (nil unless manifest is multi-tablet)
                                          │
                                          ▼
                  accumulo.Connector.ResolveTableID(tableName)
                     pins pinnedTableID              [only if splits != nil]
                (forces a fresh, uncached resolve of the destination's
                 table ID -- see §3.4)
                                          │
                                          ▼
                     accumulo.Connector.AddTableSplits(tableName, splits)
                        managerclient: FATE TABLE_SPLIT  [only if splits != nil]
                                          │
                                          ▼
                     accumulo.Connector.ListTableSplits(tableName)
                        + verifyNoUnexpectedDestinationSplits    [only if splits != nil]
                (fails closed if the destination has any split other than
                 exactly `splits`, at or before the last required row --
                 see §3.3)
                                          │
                                          ▼
                                  promotion.StageBulkDir
                (re-verifies engine.VerifyRFileExport -- closes the
                 AddTableSplits/ListTableSplits TOCTOU window above,
                 see §6 -- then calls BuildLoadMapping again, for the
                 real widened per-tablet KeyExtents from §3.1, flattens
                 nested export files into bulkDir, re-verifying each
                 file's *destination* bytes against the manifest
                 immediately after its own copy -- closes the separate
                 per-file copy-loop TOCTOU window, see §6 -- and only
                 then writes bulkDir/loadmap.json)
                                          │
                                          ▼
          mapping empty (nothing to import)? return here --
          BulkImport and the identity re-check below are never called
                                          │
                                          ▼ mapping non-empty
                  accumulo.Connector.ResolveTableID(tableName) again
                     via verifyDestinationTableIdentity [only if pinnedTableID != ""]
                (fails closed if tableName no longer resolves to
                 pinnedTableID -- see §3.4)
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

`Promote` is the only entry point that performs the full sequence above.
Calling `BuildLoadMapping`/`StageBulkDir` directly for a multi-tablet
manifest skips the `AddTableSplits` step — the caller must reconcile
destination splits itself, or the eventual `BulkImport` fails closed (see
§3.3).

```
(single-tablet / legacy manifest: RequiredDestinationSplits reports nil,
 AddTableSplits is never called, and BuildLoadMapping always returns one
 fully unbounded KeyExtent — unchanged from the original single-tablet
 slice.)
```

- `internal/promotion.BuildLoadMapping` — pure function, manifest → load
  mapping. Accepts a legacy single-implicit-tablet manifest, an explicit
  single-tablet manifest, or any number of explicitly declared tablets
  forming one gapless, non-overlapping chain (validated by
  `resolveManifestTablets`/`resolveTabletChain`). Produces one `Mapping`
  per non-empty tablet, using the widened `KeyExtent`s from §3.1 for a
  multi-tablet chain, or a single fully unbounded `KeyExtent` for the
  single-tablet/legacy case. Rejects ambiguous legacy manifests,
  undeclared or duplicate tablet indexes, gaps/overlaps in a declared
  chain, degenerate/inverted tablet ranges, and conflicting duplicate
  `DestinationPath` assignments across tablets.
- `internal/promotion.RequiredDestinationSplits` — pure function, manifest
  → the destination split rows `AddTableSplits` must reconcile before a
  multi-tablet load mapping can pass Accumulo's own validation; `nil` for
  single-tablet/legacy manifests. See §3.3.
- `internal/promotion.WriteLoadMapping` / `ReadLoadMapping` — the exact
  JSON shape from §2.
- `internal/promotion.StageBulkDir` — flattens an export manifest's nested
  `t-NNNN/` files into a flat bulk directory via `storage.Copy` (copies,
  never moves) and writes `loadmap.json`. Preflight-validates `bulkDir`
  plus the manifest's tablet chain (single-tablet or multi-tablet alike)
  before copying anything, so invalid destinations and malformed chains
  never leave a half-staged directory. It also verifies every referenced
  RFile against
  the manifest's recorded size/SHA256 (`engine.VerifyRFileExport`) before
  copying, and preflights every RFile's computed destination path
  against **every unique source path in the manifest** — not just its
  own — rejecting any alias before any copying starts. An alias includes
  an absolute-vs-relative path difference or a symlink/hard link, not
  just an identical path string, and covers both `bulkDir` coinciding
  with the export's own tablet directory *and* one RFile's destination
  coinciding with a *different* RFile's source. `storage.Copy` truncates
  the destination for writing before streaming the source, so an aliased
  copy would otherwise destroy a source file (possibly one not yet
  copied, whose earlier verification would not be repeated) and report a
  false success.
  Before any destination path is even computed, manifest entries whose
  *source* resolves to the same already-exported object are collapsed
  into one (`dedupeStageSources`/`canonicalStageSource`): a source
  reached through a symlink/hard link, or an equivalent
  backend-canonicalized spelling (e.g. an `s3://bucket/key` entry and a
  scheme-less `bucket/key` entry naming the same object), no longer has
  to flatten and verify as two independent files. Collapsing is
  conservative — it only happens when every aliased entry declares the
  *same* tablet index and would flatten to the *same* basename; if the
  same physical source is declared under two different flattened
  filenames, `StageBulkDir` fails closed instead of guessing which name
  to keep, since silently discarding one would otherwise drop a named
  file the load mapping still expects to exist. This complements rather
  than replaces the alias checks below, which still run against the
  deduplicated set to catch purely lexical risks (case/Unicode
  aliasing) that don't imply the same confirmed physical identity a
  dedupe decision requires.
  The same preflight also covers `bulkDir/loadmap.json` itself (written
  by `WriteLoadMapping`, which truncates exactly like `storage.Copy`
  does, but runs after every RFile copy — an aliased `loadmap.json`
  would otherwise destroy an already-staged source), and checks every
  write target against every *other* write target, not only against
  manifest sources — catching two flattened destinations that are
  hard-linked to each other, which would otherwise let the second copy
  silently overwrite the first while the load mapping still lists both
  names as independent files.
  Alias detection also treats two local publication paths that collapse
  to the same filesystem-visible spelling as a potential alias, purely
  lexically, even when neither final path exists yet: Windows and macOS
  (by default) resolve paths case-insensitively, macOS normalizes
  filenames to NFC, and Windows ignores trailing dots/spaces. So two
  basenames that differ only by case, Unicode normalization (e.g. NFC vs
  NFD `é`), or a trailing `.`/space are rejected before either write can
  silently overwrite the other.
  That same case/Unicode fold applies to a Windows volume prefix, not
  only to the path components after it: both the standard UNC form
  (`\\server\share\...`) and the Win32 extended-length form
  (`\\?\UNC\server\share\...`, which bypasses ordinary path processing
  and would otherwise keep its own distinct literal spelling) have
  their server and share segments case-folded and NFC-normalized
  identically to ordinary components, so `\\SERVER\Share\B.rf` and
  `\\server\share\B.rf` (or their `\\?\UNC\...` equivalents) collapse
  to the same publication key instead of aliasing past this check. A
  drive-letter prefix (`C:\...`) needs no separate folding here --
  it's already uppercased upstream before the path is split into a
  prefix and components.
  Those extended-length forms don't just fold their own case
  independently of each other -- they fold to the exact same
  publication key as their ordinary-form equivalent.
  `\\?\UNC\server\share\...` normalizes identically to
  `\\server\share\...` (both collapse marker, server, and share down to
  the same shared prefix shape), and `\\?\C:\...` normalizes
  identically to `C:\...` (its drive letter is uppercased the same way
  the ordinary form's already is). Without that convergence, a source
  published under one spelling and a not-yet-created write target
  expressed under the other would look like two distinct locations
  right up until the second write silently overwrote the first.
  A drive-*relative* spelling is a separate wrinkle from the rooted
  drive-letter prefix above: a drive letter and colon with no separator
  immediately after it (`C:bulk\A.rf`, meaning "`bulk\A.rf` relative to
  drive C's own current directory", as distinct from the rooted
  `C:\bulk\A.rf`) doesn't go through that upstream folding, since it
  only fires once a separator follows the colon. This same
  volume-prefix folding step now recognizes a bare, separator-less
  drive-letter prefix directly and uppercases it, so `c:bulk\A.rf` and
  `C:bulk\A.rf` converge on the same publication key too -- while
  still staying distinct from the rooted `C:\bulk\A.rf` spelling of the
  same drive and path, which names a different location whenever that
  drive's current directory isn't its root.
  Every unique manifest source path is additionally checked against
  every *other* unique source path, not only against write targets: two
  different `DestinationPath` values that are physically the same file
  (e.g. reached via a symlink/hard link, or differing only by case or
  Unicode normalization) are deduped when they flatten to the same
  basename, but rejected when they would flatten to different basenames:
  staging the same underlying file twice under two bulk-import names
  would silently duplicate it once Accumulo imports both flattened
  copies.
  Built-in remote/object-store paths (S3, GCS, Azure, HDFS) are
  canonicalized through each backend's own path parser before
  comparison, so a qualified spelling like `s3://bucket/key` and its
  scheme-less `bucket/key` equivalent are recognized as the same
  destination even though they're different strings. That same backend
  identity still applies when the backend is wrapped (for example by
  `diskcache.Backend`). Generic custom `scheme://...` backends keep
  URL-style path joining, but unless they use one of those built-in
  parsers they are otherwise compared lexically. Local paths get
  Windows-drive normalization first (so `C://data/F.rf` is recognized
  as local rather than mistaken for a remote URL), then an explicit
  symlink-target resolution step alongside the case/Unicode-insensitive
  lexical check and `os.Stat` + `os.SameFile` fallback, so a symlink
  aliasing a path that itself doesn't exist yet (nothing for
  `os.SameFile` to compare) is still caught by comparing the symlink's
  resolved target lexically. Symlink-target resolution also walks a
  path's ancestor directory components, not only its own final
  component: a relative symlink
  reached through a *symlinked parent directory* (for example
  `bulk/alias -> "."` with `bulk/A.rf -> alias/B.rf`, where
  `bulk/B.rf` doesn't exist yet) is resolved to the same
  not-yet-existing `bulk/B.rf` target either path would actually reach,
  rather than being compared as if `alias/B.rf` were itself the final,
  non-symlink spelling. This ancestor walk deliberately avoids
  `filepath.EvalSymlinks`, which on Windows also silently expands 8.3
  short names (e.g. `MARCPA~1` to a real user's long name) even when no
  symlink is present anywhere in the path, which would otherwise break
  lexical comparison against a path that was never resolved; instead it
  substitutes a resolved spelling only where `os.Lstat` actually
  confirms a real symlink, leaving every other component untouched.
  On Windows specifically, the lexical comparison also strips trailing
  dots and spaces from every path component before comparing: Win32's
  own path resolution silently discards them, so `A.rf`, `A.rf.`, and
  `A.rf ` can all name the same not-yet-created file there even though
  they are three distinct strings. This is gated to `runtime.GOOS ==
  "windows"` rather than to a path's apparent syntax, since the quirk
  depends on the filesystem the write actually lands on (always this
  process's own OS for the local backend) — unconditionally stripping
  trailing dots/spaces would misreport genuinely distinct filenames as
  aliases on Linux/macOS, where a trailing dot or space is ordinary,
  significant content. On local Windows destinations, `StageBulkDir`
  also rejects any additional `:` inside a target path component (for
  example `A.rf:$DATA`, `A.rf::$DATA`, or named streams such as
  `A.rf:meta:$DATA`) instead of trying to canonicalize NTFS alternate
  data stream aliases; the only allowed colon is the drive-letter
  separator itself.
  A Win32 extended-length `\\?\` prefix is stripped before this check
  runs (recognizing both `\\?\C:\...` and `\\?\UNC\server\share\...`),
  so a bulk directory expressed in extended-length form -- typically
  chosen specifically to bypass `MAX_PATH` for a long staging path --
  isn't rejected merely for the drive letter's own colon surviving as
  a separate path component under the naive `\\?\` split.
  `StageBulkDir` also conservatively treats a literal Windows DOS 8.3
  short-name spelling (for example `LONGFI~1.RF`) as a possible alias of
  any not-yet-created long-name write target sharing the same extension
  in the same directory, even when the two stems don't lexically match.
  NTFS only derives a short name's stem from the long name's own leading
  characters under its simple, non-colliding scheme; once a directory
  accumulates enough short-name collisions, NTFS instead assigns a
  hashed stem with no predictable relationship to the long name's
  characters. Because both write targets are still absent at this point
  (`os.Stat` can't disambiguate them), and NTFS's own hashing scheme
  can't be reliably replicated here, requiring an exact stem match would
  risk silently missing that hash-based alias. The extension isn't
  subject to this hash substitution, so it remains a reliable narrowing
  signal instead: only same-extension pairs are treated as ambiguous. A
  long-name component that already fits within 8.3 using its
  definitely-safe character set is exempt, since NTFS never generates a
  distinct short name for it.
  `joinBulkPath` and the `bulkDir` root-validation preflight both
  recognize a non-local write target the same, deliberately generic
  way `internal/engine`'s own backend-path joining does: any
  `scheme://...`-shaped path (plus HDFS's authorityless `hdfs:/` form)
  is treated as backend-style, not only the four schemes
  (`s3/gs/az/hdfs`) this package knows how to canonicalize for alias
  comparison — so a custom or future backend with its own URI scheme
  still joins with `/` and validates correctly instead of silently
  falling through to a native, OS-specific `filepath.Join`. A backend
  can override this scheme-shaped heuristic entirely by declaring
  itself local (`storage.LocalPathSemanticProvider`, which the local
  backend implements and which still applies through a wrapper such as
  `diskcache.Backend`, since scheme/identity checks unwrap to the
  innermost backend first): a `bulkDir` that is itself a local
  directory literally named e.g. `hdfs:/bulk` is then still recognized
  and preflighted as local rather than mistaken for a remote HDFS root
  purely because of its spelling.
  All of the above are preflight checks: every one of them runs, and
  either passes or fails closed, before any file is copied. They cannot,
  by construction, protect the copy loop itself -- `engine.VerifyRFileExport`
  reads and hashes every RFile once, in a single pass, before the loop
  starts, and `storage.Copy` then separately re-opens and re-reads each
  source file when it is actually copied, an arbitrary (if normally
  short) amount of time later. A source file replaced in that window --
  the same file changing between being verified and being copied, or a
  later file in the manifest changing while an earlier one is still
  being copied -- would be silently staged with the wrong bytes and
  written into `loadmap.json` as trustworthy, with nothing above ever
  re-checking it. `StageBulkDir` closes this by verifying each file's
  *destination* object immediately after its own `storage.Copy` call
  returns: it re-opens and re-hashes the object just written at `dst`
  and compares that against the manifest's recorded size/SHA256 for
  that file, before continuing to the next file or writing
  `loadmap.json`. Checking the destination (not a second read of the
  source) also catches corruption introduced by the copy/backend path
  itself, not only source mutation -- `storage.Copy`'s own doc comment
  notes that some `WritableBackend` implementations buffer and upload
  on `Close`, so a copy can still fail or corrupt data after every prior
  `Write` appeared to succeed. A mismatch fails the whole call closed
  immediately, but -- consistent with the cancellation behavior in §5 --
  does not roll back that one file's already-written (incorrect) bytes;
  `loadmap.json` is only ever written after every file has both copied
  and re-verified successfully, so its presence remains a reliable
  signal that every listed file's bytes were confirmed at `dst`, not
  merely assumed from an earlier check.
- `internal/promotion.Promote` — orchestrates the full sequence: calls
  `RequiredDestinationSplits`, conditionally submits `AddTableSplits` for
  a multi-tablet manifest, then calls `StageBulkDir`, then submits the
  bulk import through a `Promoter` (satisfied by `*accumulo.Connector`).
  Validates `tableName`/`bulkDir`, that `dst` implements
  `storage.WritableBackend` (via `validatePromotionDestination`'s
  `validateDestinationWritable` check), the tablet chain (via a
  `RequiredDestinationSplits` preflight), every RFile's own reference
  into that chain (via a `BuildLoadMapping` preflight), and everything
  `StageBulkDir` itself would reject before writing a single byte (via a
  `stagingPreflight` call covering source-alias dedup, flattened-basename
  validity/uniqueness, staging write-target aliasing, *and* a full
  `engine.VerifyRFileExport` content check — every RFile's declared
  size/SHA256 against its actual source bytes) — all before any
  Accumulo call or destination write — so a malformed manifest, a
  corrupt or mismatched export, a read-only destination, or an invalid
  bulk directory never adds a split, stages a file, or submits a bulk
  import. The dedup/flatten/alias
  checks inside `stagingPreflight` are redundant with validation
  `StageBulkDir` performs again further on, and cheaply so: each is a
  pure or local-probe-only function of the manifest and the backends
  alone, so recomputing costs nothing and never observes a different
  destination state. `VerifyRFileExport` is not cheap the same way — it
  streams and hashes every RFile's full content — but `Promote` still
  lets `StageBulkDir` run it again below, deliberately paying that cost
  twice: `AddTableSplits`/`verifyNoUnexpectedDestinationSplits` (via
  `ListTableSplits`) run in between, a real manager round-trip taking
  arbitrary time, and `src` is not guaranteed unchanged across it (a
  local path or an object-store key can be overwritten while that call
  is in flight). Skipping `StageBulkDir`'s own verification to avoid
  the duplicate cost would leave that window unchecked — a source
  object replaced during the split round-trip would be staged and
  bulk-imported without ever being checked against the manifest it
  actually matches at copy time. So the two verify calls guard
  different, non-overlapping windows: `stagingPreflight`'s protects
  `AddTableSplits` from ever mutating the destination's splits for an
  export that was already corrupt beforehand; `StageBulkDir`'s own
  protects the copy itself against one that became corrupt, or was
  swapped out, during the calls in between (see
  `TestPromoteRejectsSourceMutatedDuringAddTableSplits` in §6).

  Submits nothing when the derived mapping is empty (nothing to
  import) — but still reconciles splits first if the manifest declares
  a multi-tablet chain, even when every tablet in it ends up empty, so
  the destination's tablet boundaries don't depend on which tablets
  happened to carry files.

  For a multi-tablet manifest, also pins the destination's real table
  ID via `conn.ResolveTableID` before `AddTableSplits`, and re-checks it
  via `verifyDestinationTableIdentity` immediately before `BulkImport`
  — see §3.4 for the full mechanism, its trust assumption, and its
  residual race. Neither call happens for a single-tablet/legacy
  manifest, which has no split-reconciliation step to pin against.
- `accumulo.Connector.ResolveTableID` — invalidates the discovery cache
  and re-resolves `tableName` to its current table ID, unlike
  `AddTableSplits`/`ListTableSplits`/`BulkImport` below, each of which
  may resolve `tableName` from a cache with no enforced TTL. New in this
  slice specifically so `Promote` can pin and re-check the destination's
  identity across the staging window — see §3.4.
- `accumulo.Connector.AddTableSplits` — resolves the destination table
  name to its stable ID, then submits the split rows through the manager
  `TABLE_SPLIT` FATE operation, the same protocol
  `TableOperations.addSplits` uses. Pre-existing, this slice's new code
  only adds a caller (`Promote`), not the primitive itself.
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
   single-tablet **and** multi-tablet/split-bearing exports are now
   supported client-side (§3), but none of this has been verified against
   a live Accumulo cluster in this environment — no cell-equivalent-results
   check has ever actually run. The multi-tablet path also carries the
   residual concurrent-merge race from §3.3: a split or merge landing on
   the destination between `verifyNoUnexpectedDestinationSplits`
   succeeding and `BulkImport`'s own server-side validation running is
   not fully closed by this slice, only turned into a clean, safe
   FATE-level rejection instead of a silent correctness problem.
   A related but independent gap — the destination table being deleted
   and recreated under the same name during that same window — is
   narrowed by §3.4's identity pinning, which fails closed on a
   resolvable identity change but, by the same reasoning, cannot close
   the very last sliver of that race either (a delete-and-recreate
   landing strictly between its own final check and `BulkImport`'s
   internal resolution), and does not cover single-tablet/legacy
   manifests at all, which have no split-reconciliation step to pin
   against in the first place.
2. **"rerunning any interrupted transfer is safe"** — true up through
   staging, not true across the whole `Promote` call once `BulkImport` has
   been invoked. `StageBulkDir` is deterministic and copy-based, so
   rerunning it with the same manifest and `bulkDir` safely reproduces the
   same bytes; a persistent I/O failure partway through copying *multiple*
   files can still leave a partially staged bulk directory, but retrying
   the same call completes it without manual repair (see
   `TestStageBulkDirCancellationLeavesNoPartialObjectAndRetrySucceeds` in
   §6). `AddTableSplits` is also safe to retry, but that safety comes from
   Shoal's own client-side logic, not from any inherent idempotency
   `TABLE_SPLIT` FATE submission guarantees: each retry round re-queries
   the destination's *real* current tablet metadata before deciding what
   to submit, and a requested split row that already matches an existing
   tablet's end row is treated as already satisfied (only its
   mergeability metadata is refreshed) rather than resubmitted as a new
   split. That re-observation step is what makes a retry converge safely
   after an ambiguous `AddTableSplits` failure — it is not a property of
   the FATE protocol itself.

   `BulkImport` has no equivalent re-observation step: there is no
   client-visible way to ask "was this exact bulk import directory
   already loaded into this table," so `TABLE_BULK_IMPORT2` FATE
   submission has no dedup/idempotency of its own. An ambiguous failure
   at or after that call (e.g. a timeout after the manager received the
   request but before the client observed a response) leaves the caller
   unable to tell whether the import already happened, and blindly
   calling `Promote` again in that window risks a duplicate bulk import.
   This slice deliberately does **not** claim idempotency for the whole
   `Promote` call — only for its pre-`BulkImport` steps (split
   reconciliation and staging). Closing the `BulkImport` gap needs a
   promotion-state or idempotency-token API this slice does not yet have
   (see item 4 below).
3. **"split changes and partial uploads recover without manual metadata
   repair"** — partial-upload recovery (retrying an interrupted
   `StageBulkDir`/pre-`BulkImport` `Promote` call) is implemented and
   tested, per item 2. "Split changes" recovery is only partial: this
   slice reconciles the destination's splits to match a multi-tablet
   manifest *before* staging, actively verifying (not merely assuming)
   that no unrelated extra split exists in the range the widened mapping
   depends on (§3.2/§3.3) — so an already-wrong destination layout is
   caught and reported before any file is staged. Accumulo itself
   separately, safely rejects (rather than corrupts) a bulk import if a
   concurrent split or merge invalidates a reconciled destination
   *after* that check but before `BulkImport`'s own validation runs —
   but Shoal does not automatically detect that later rejection and
   re-reconcile/retry on the caller's behalf. That would need the same
   promotion-state machinery as item 4.

   Separately, this reconciliation deliberately tolerates any number of
   *trailing* splits strictly after the manifest's last required
   boundary row (§3.2), since they cannot reproduce the `prevEndRow`
   mismatch `verifyNoUnexpectedDestinationSplits` exists to catch. But
   Accumulo's own `PrepBulkImport.validateLoadMapping` separately
   enforces `table.bulk.max.tablets` (`Property.TABLE_BULK_MAX_TABLETS`,
   default `100`, admin-configurable per table, since Accumulo 2.1.0):
   for the load mapping's final, always-unbounded entry, it counts how
   many real destination tablets that entry's files overlap — a count
   that trailing splits inflate directly. Enough of them (past the
   configured limit) can make `BulkImport` reject the load mapping for
   a reason this package neither detects nor explains up front. Promote
   does not enforce that limit itself: doing so accurately would
   require reading the destination table's actual (possibly
   non-default) property value, a capability the `Promoter` interface
   does not expose today, and guessing the compiled-in default here
   risks rejecting imports a differently-configured table would
   legitimately accept. Accumulo's manager-side validation is the safe
   backstop either way — a rejection here is data-safe, only less
   informative than the errors this package already raises for the
   cases it does check — but closing it client-side (checking before
   `AddTableSplits`/`BulkImport`, with an accurate, table-specific
   error) is deferred to a future slice that gives `Promoter` a way to
   read destination table properties.
4. **"a documented cutover protocol"** — not implemented. No
   promotion-state or cutover API surface is exposed yet; this slice is
   promotion of one point-in-time export, not the ongoing fan-in/cutover
   semantics #70 also asks for.
5. **"graph, document, and vector fixtures pass before and after
   promotion"** — not exercised; requires a live cluster.

**A manifest's `TabletIndex` is trusted, not verified against the
RFile's actual content.** Every check in §3/§4 confirms that a
manifest's tablet chain is well-formed (§3.1/§3.2) and that each
`RFileExportFile` genuinely exists and matches its own declared
`Size`/`SHA256` (`engine.VerifyRFileExport` against the *source*, run
twice — see §3.3 and §4's `stagingPreflight`/`StageBulkDir` description
— specifically to close the window a manager round-trip opens — plus a
third, per-file check of the freshly staged *destination* immediately
after each copy, closing the separate copy-loop window described
there). None of that checks
whether an RFile's *actual* embedded key range genuinely falls inside
the boundaries `Tablets[TabletIndex]` declares. A manifest that
correctly hashes a byte-for-byte real RFile, but assigns it to the
*wrong* `TabletIndex` — for example a file whose real keys belong under
tablet 0's range, declared instead as tablet 3 in a four-tablet chain —
passes every check this package performs, including
`RequiredDestinationSplits`, `BuildLoadMapping`, `stagingPreflight`, and
`StageBulkDir`, then loads successfully via `BulkImport`: Accumulo's own
server-side `PrepBulkImport.validateLoadMapping` validates the load
mapping's structure (extents, file counts — see item 3 above) and does
not open RFiles to check their content against the extent they were
declared under either. The practical effect is silent, not a rejection:
the file's rows outside the extent it actually lands under become
unreachable through any range-scoped read that trusts tablet
boundaries, even though the bytes that were loaded are completely
intact and pass every hash check that exists.

Closing this needs one of two capabilities this slice does not have:
reading each RFile's real first/last key at validation time and
cross-checking it against its declared tablet's boundaries (the
`internal/rfile` reader is a low-level streaming API today, with no
cheap "just the key range" accessor, and no code in this package opens
an RFile's content at all — every existing check is either a hash
comparison against a manifest-declared value or a local/path-only
probe), or authenticating the manifest itself from a trusted origin so
`TabletIndex` does not need re-deriving from content at all. Both are
substantial, separate features, not a gap in this slice's own new
logic. What bounds the real-world exposure today: the one production
code path that builds a manifest, `engine.ExportRFiles`, assigns
`TabletIndex` directly from Shoal's own local per-tablet RFile
bookkeeping, not from any input a caller supplies — so it is correct by
construction for a manifest that has never left that path. The gap
only matters once a manifest crosses a boundary where it could be
altered before reaching `Promote` (written to disk, transferred over a
network, hand-edited) — which is precisely the threat model every
other check in §3/§4 already treats as real, so this is a genuine,
currently-unaddressed blind spot in that same model, not a merely
theoretical one.

Also explicitly out of scope for this slice: an RFile-index-based
per-key-range rewrite/materialization strategy for split-bearing exports
(a different approach from the destination-split-widening one
implemented here — see §3 — and not needed now that widening covers the
same case), automatic basename-collision resolution (collisions,
including cross-tablet ones, are reported as errors — see
`TestStageBulkDirRejectsCrossTabletBasenameCollisionBeforeCopying` in
§6), and encryption/authentication of the transfer itself (inherited
from whatever `storage.Backend`/`accumulo.Connector` are already
configured with).

## 6. Testing

All tests use in-memory storage backends and fake FATE/manager RPC layers
(mirroring the existing patterns in `accumulo/*_test.go` and
`internal/managerclient/managerclient_test.go`) — there is no live
Accumulo cluster or Java toolchain in this environment. See the pull
request for the exact commands and pass/fail output.

Coverage added for multi-tablet split reconciliation, on top of the
existing single-tablet suite:

- **Widened-extent correctness** — `twoTabletManifest`/`threeTabletManifest`/
  `fourTabletManifest` fixtures with hand-computed boundary rows, asserting
  `BuildLoadMapping`'s exact `KeyExtent` for every chain position (including
  the always-collapses-to-unbounded index-1 case and a genuinely bounded
  `PrevEndRow` at index ≥2), plus a JSON round-trip test for a multi-tablet
  mapping and an exact `RequiredDestinationSplits` split-row assertion for
  each fixture.
- **Malformed-chain rejection, before any write** — duplicate/out-of-range
  tablet index, non-nil first `StartRow`/last `EndRow`, missing middle
  boundaries, chain gaps, chain overlaps, degenerate/inverted ranges,
  undeclared tablet references, and duplicate `DestinationPath` across
  tablets — every case covered for both `BuildLoadMapping` and (via
  `StageBulkDir`'s early validation call) the staging path, so a malformed
  multi-tablet manifest never reaches a partial copy.
- **Robustness** — an out-of-order tablet slice (chain validity does not
  depend on `Tablets` appearing in index order) and empty-tablet skipping
  (a declared tablet with zero files contributes no mapping entry, but
  still contributes to `RequiredDestinationSplits` — proved directly by
  `TestRequiredDestinationSplitsIgnoresWhichTabletsHaveFiles`, not just
  inferred from `BuildLoadMapping`'s behavior — matching `Promote`'s doc'd
  "reconcile splits even when a tablet ends up empty" behavior).
- **`Promote` orchestration** — `AddTableSplits` called with the exact
  expected split rows before `StageBulkDir`/`BulkImport` for a multi-tablet
  manifest; `AddTableSplits` skipped entirely for a single-tablet manifest;
  zero staged writes and zero `BulkImport` calls when `AddTableSplits`
  fails (proving the fail-before-any-write ordering); and a full
  three-tablet success path exercising every step end to end. The
  `stagingPreflight` ordering guarantee itself is proved by four
  `Promote`-level tests, each a multi-tablet manifest that reaches
  `AddTableSplits` unless the fix holds:
  `TestPromoteRejectsCrossTabletBasenameCollisionBeforeAddTableSplits`
  and `TestPromoteRejectsNonLeafBasenameBeforeAddTableSplits` cover
  `flattenNames`'s two rejections (a shared basename across tablets, and
  a basename that collapses to a non-leaf `.`/`..`), and
  `TestPromoteRejectsLoadMapAliasBeforeAddTableSplits` covers
  `checkNoStagingAliases` specifically — a real hard link between
  `bulkDir/loadmap.json` and one of the manifest's own source files, on
  the local backend, since alias detection for local paths depends on
  `os.SameFile`, which the in-memory backend used by the other two tests
  cannot exercise. All three assert zero `AddTableSplits` and zero
  `BulkImport` calls, not just a non-nil error, so a regression that
  moved the check back to only firing inside `StageBulkDir` would fail
  them even if `Promote` still ultimately returned an error.
  `TestPromoteRejectsCorruptExportBeforeAddTableSplits` covers
  `engine.VerifyRFileExport` itself — a manifest RFile whose declared
  SHA256 does not match its real source bytes — proving
  `stagingPreflight`'s verification specifically (the manifest is wrong
  from the start, before `AddTableSplits` ever runs).
  `TestPromoteRejectsSourceMutatedDuringAddTableSplits` proves the
  companion half described above: it starts from an accurate manifest
  (so `stagingPreflight` passes) and overwrites the source object as a
  side effect of the fake `Promoter`'s `AddTableSplits` call, simulating
  a real actor mutating `src` during that round-trip; asserts
  `AddTableSplits` *was* called (`splitCalls == 1`, confirming the test
  reaches the window after the preflight rather than being rejected
  there) but `BulkImport` was not, and that `dst` stays empty. Verified
  this one similarly: with `StageBulkDir`'s own re-verification call
  temporarily removed (while `stagingPreflight`'s separate call stays
  intact), this test — and only this one, not
  `TestPromoteRejectsCorruptExportBeforeAddTableSplits` — fails, showing
  each test isolates its own distinct window.
- **Cross-tablet alias/path safety** —
  `TestStageBulkDirRejectsCrossTabletBasenameCollisionBeforeCopying` proves
  the flatten-collision check (previously only exercised within a single
  tablet) rejects two different tablets' files sharing a basename, before
  any copy starts.
- **Cancellation, partial uploads, and resumable retry** —
  `TestStageBulkDirCancellationLeavesNoPartialObjectAndRetrySucceeds` uses
  a wrapper backend that cancels the call's own context the instant a
  later file's `Create` is issued, proving: the file whose `Create` raced
  the cancellation is aborted (no partial/corrupt object left behind); an
  earlier file that had already finished copying is **not** rolled back
  (staging is not atomic across a multi-file manifest — stated honestly,
  not hidden); `loadmap.json` is never written on the failed attempt; and
  retrying with a fresh context against the same destination converges to
  the full, correct staged set.
- **Copy-loop TOCTOU (destination re-verification)** —
  `TestStageBulkDirRejectsSourceMutatedBetweenPreflightVerifyAndItsOwnCopy`
  covers the window `TestPromoteRejectsSourceMutatedDuringAddTableSplits`
  does not: a source file changing *after* `StageBulkDir`'s own
  whole-manifest `engine.VerifyRFileExport` preflight has already
  approved it, but *before* `storage.Copy` performs its own, separate
  read of that same file inside the copy loop. A wrapper backend
  mutates the tracked file's content on its second `Open` call
  specifically (the first is the preflight verify; the second is
  `storage.Copy`'s own read), to equal-length-but-different bytes, so
  neither the preflight nor `storage.Copy`'s own length bookkeeping can
  incidentally catch the swap — isolating the new per-file, post-copy
  destination verification as the only mechanism that can. Asserts
  `StageBulkDir` returns an error, that the second (later, unrelated)
  file was never copied (fails closed before continuing the loop), and
  that `loadmap.json` is absent. Verified this test against a temporary
  revert of the new post-copy check (with every other check left
  intact): only this test fails, confirming it isolates the specific
  window the new check closes rather than incidentally passing for an
  unrelated reason.
- **Destination writability preflight** —
  `TestStageBulkDirRejectsReadOnlyDestinationBeforeAnyRead` proves
  `validateDestinationWritable` runs before any read: it wraps a
  writable in-memory backend in a `readOnlyBackend` (implementing only
  `Open`, mirroring `internal/storage/storage_test.go`'s own same-named
  type — test doubles aren't shared cross-package) and points the
  manifest at nonexistent `src` files, so observing `storage.ErrReadOnly`
  — rather than a "not found" error from the first RFile lookup — proves
  the writability check runs first.
  `TestPromoteRejectsReadOnlyDestinationBeforeAddTableSplits` proves the
  same ordering one level up, through `Promote` itself, for a
  multi-tablet manifest: it asserts `splitCalls == 0` and
  `resolveTableIDCalls == 0`, so a regression that let `AddTableSplits`
  or the identity pin run against a read-only destination before the
  writability check would fail it even if `Promote` still ultimately
  returned an error.
- **Destination table identity drift (§3.4)** —
  `TestResolveTableIDForcesFreshLookupUnlikeTableByName`
  (`accumulo/discovery_test.go`) proves `ResolveTableID` actually forces
  a fresh lookup where `TableByName` would not: it introduces a new
  `staleCachingTableNames` fake with a genuine snapshot/live-map split,
  only synced on `Invalidate()` — unlike the package's existing
  `fakeTableNames`, which proxies its map live on every call and so has
  no caching to be stale in the first place, and could not have caught a
  regression here. It mutates the live map without invalidating and
  asserts `TableByName` still returns the stale ID while `ResolveTableID`
  observes the change; `TestResolveTableIDMissingTableAndEmptyName`
  covers the same not-found/empty-name error mapping `TableByName`
  already has.
  `TestPromoteRejectsWhenDestinationTableChangesIdentityDuringStaging`
  proves `verifyDestinationTableIdentity`'s regression case end to end:
  a fake `Promoter`'s `onResolveTableID` hook changes the destination's
  reported table ID on its second call (simulating a delete-and-recreate
  that happened during `StageBulkDir`'s staging), and the test asserts
  the resulting error names both the pinned and the current table ID,
  `resolveTableIDCalls == 2` (one pin before `AddTableSplits`, one
  re-check before `BulkImport`), that `BulkImport` was never called, and
  that the files `StageBulkDir` already staged are left in place
  (consistent with the non-atomic, no-rollback staging behavior the
  cancellation bullet above documents).
  `TestPromoteAbortsBeforeAddTableSplitsWhenResolveTableIDFails` proves
  the simpler propagation case: a `ResolveTableID` failure aborts before
  `AddTableSplits` ever runs rather than being swallowed or retried
  silently.

No test in this slice submits a real FATE operation or talks to a live
Accumulo manager; `AddTableSplits`/`BulkImport` are exercised only through
the package's own fake (`fakePromoter`), so the concurrent-merge race and
FATE-submission ambiguity described in §3.3/§5 are documented and
reasoned about, not reproduced end to end here.
