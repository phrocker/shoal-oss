# Offline Compaction — Operator Runbook

`shoal-offline-compact` performs a full major compaction of an **OFFLINE**
Accumulo table's tablets from a standalone process — no tserver, manager,
or compaction coordinator in the loop. It reads each tablet's input
RFiles, applies the table's resolved `table.iterator.majc.*` stack, writes
one compacted output RFile per tablet, verifies it, and hands off the
metadata delta.

This is the operator guide. For the safety model and rationale see
[`offline-compaction-design.md`](./offline-compaction-design.md).

> **Golden rule:** the table must be **OFFLINE** for the entire run, and it
> stays that way until you have applied the commit and re-onlined it. The
> tool fails closed if the table is (or becomes) ONLINE.

---

## 1. When to use this

- A table is offline (or can be taken offline during a maintenance window)
  and has accumulated many small RFiles per tablet you want merged.
- You want compaction work done off-cluster (on a batch host) without
  loading tservers.
- Every iterator in the table's `majc` stack has a Go port (see
  §7 — Failure modes). If any don't, the run aborts and names them; that
  gap is the iterator-forge track.

Do **not** use it on an ONLINE table — it will refuse at the fence.

---

## 2. Prerequisites

- Network access to the ZooKeeper quorum and the RFile storage backend
  (`gs`, `s3`, `azure`, `local`, or `memory`).
- The metadata-read principal + password. The password doubles as the ZK
  instance secret used for digest auth on `/config` znodes; prefer the
  `SHOAL_PASSWORD` env var over `-password`.
- The server major.minor you are reading must match `-accumulo-version`.
- **The table must already be OFFLINE.** Take it offline in the Accumulo
  shell first:

  ```
  offline -t mytable -w
  ```

  `-w` waits for the offline to complete before returning.

---

## 3. Flag reference

| Flag | Default | Meaning |
|------|---------|---------|
| `-zk` | *(required)* | Comma-separated ZK quorum. |
| `-instance` | `accumulo` | Accumulo instance name. |
| `-table` | *(required)* | Table **name or id** to compact. |
| `-range` | *(empty)* | Restrict to tablets intersecting `startRow:endRow` (inclusive; either side may be empty for unbounded). See §6. |
| `-dry-run` | `true` | Plan + verify only. **Pass `--dry-run=false` to actually commit — this is the ONLY commit gate.** |
| `-commit-mode` | `plan` | `plan` or `direct`. Always validated; only acted on when `--dry-run=false`. See §5. |
| `-verify` | `true` | Run the design §5 verification on every compacted tablet; a failure aborts the run. |
| `-out` | `.` | Directory to write the commit-plan JSON into. |
| `-storage` | `gs` | RFile backend: `gs`, `s3`, `azure`, `local`, `memory`. |
| `-user` | `root` | Principal for metadata reads. |
| `-password` | *(empty)* | Password; prefer `SHOAL_PASSWORD` env. Also the ZK instance secret. |
| `-accumulo-version` | `4.0.0-SNAPSHOT` | Server major.minor must match. |
| `-zk-timeout` | `30s` | ZK session timeout. |
| `-itercfg-ttl` | `30s` | TTL for the resolved iterator-stack cache. |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error`. |
| `-version` | | Print version and exit. |

The process exits non-zero on any failure (fence refusal, unported
iterator, read/write error, verification failure, or commit error) and
logs the cause.

---

## 4. The standard procedure

The safe workflow is **always dry-run first, review the plan, then
commit.**

### Step 1 — take the table offline

```
# Accumulo shell
offline -t mytable -w
```

### Step 2 — dry run (plan + verify, no writes)

```bash
export SHOAL_PASSWORD=…
shoal-offline-compact \
  -zk zk1:2181,zk2:2181 \
  -instance accumulo \
  -table mytable \
  -storage gs \
  -out ./plans
# -dry-run defaults to true; nothing is committed.
```

This establishes the OFFLINE fence, compacts every tablet **in memory /
to unreferenced output files**, verifies each, and writes the commit-plan
JSON to `./plans/offline-compact-<table>-<UTC timestamp>.json`. Because no
metadata is mutated, any output files written are unreferenced orphans
that Accumulo GC reclaims — a dry run is always safe to abort.

Review the log summary (`compacted` / `no_op` tablet counts) and inspect
the commit plan (§8) before proceeding.

### Step 3 — commit

Pick a commit mode (§5). The default **Mode P** emits the plan for an
Ample-based applier to apply inside Accumulo:

```bash
shoal-offline-compact … -dry-run=false -commit-mode=plan -out ./plans
```

The tool re-verifies the fence, then writes the final commit plan. Apply
it with your Ample-based applier (the conditional mutation runs *inside*
Accumulo, preserving metadata-write authority there).

For **Mode D** (direct), the tool applies the metadata delta itself —
opt-in and requires a wired `MetadataCommitter` (see §5):

```bash
shoal-offline-compact … -dry-run=false -commit-mode=direct
```

### Step 4 — bring the table back online

```
# Accumulo shell
online -t mytable -w
```

### Step 5 — validate (§9)

Scan and confirm cell counts / spot-check content; each compacted tablet
should now reference a single `file:` entry.

---

## 5. Commit modes (decision D-1: **Mode P default, Mode D opt-in**)

**Mode P (`plan`, default) — conservative.** The tool emits a
machine-readable `CommitPlan` (§8) and makes **no** metadata writes. An
operator or an Ample-based applier consumes it and performs the
conditional mutation inside Accumulo. Metadata-write authority stays in
Accumulo. This is the path shipped first and the recommended default.

**Mode D (`direct`) — opt-in.** The tool writes `accumulo.metadata`
itself via a supplied `MetadataCommitter`, as a conditional mutation
guarded on the current file set. shoal ships **no** default committer (it
is a read fleet), so Mode D errors with
`direct commit mode requires a MetadataCommitter` until one is wired.

The commit-plan JSON is written to `-out` whenever `Commit` returns a
plan — including when Mode D fails with `ErrDirectCommitUnavailable` (the
plan is still produced) — so you can inspect or resume from it. A
pre-commit **fence trip** is different: it fails *before* a plan is built,
returns no plan, and writes no artifact — because nothing was safe to
commit (see §10).

---

## 6. Range compaction (decision D-2: **supported**)

`-range startRow:endRow` restricts the run to whole tablets that intersect
the given row range. Selection is **tablet-granular** — never sub-tablet.
A tablet is included if its extent overlaps `[startRow, endRow]`.

- `-range h:p` — tablets covering rows `h` through `p`.
- `-range h:` — from `h` to the end of the table.
- `-range :p` — from the start of the table through `p`.
- *(omitted / empty)* — the whole table.

A `-range` value without a `:` is rejected as ambiguous.

---

## 7. Verification (`-verify`, on by default)

When `-verify` is set (default), each tablet's output is checked before it
is eligible to commit (design §5):

1. **Self-consistency (primary gate).** An independent second pipeline
   re-derives the compaction stream through the same iterator stack and
   asserts every output cell is byte-identical, the cell count matches,
   and there are no trailing cells. Any divergence aborts the run and
   pins the first mismatching cell.
2. **T4 summary (informational).** A per-file summary (cell count, per-CF
   histogram, first/last key, size) computed over the actual written
   bytes.
3. **T1 Java cross-read (env-gated).** When `SHOAL_JAVA_RFILE_VALIDATE`
   is configured, the real Java RFile reader is run against the output to
   prove Java can consume shoal's bytes. Only fails the gate when it was
   actually attempted and rejected the output.

A failed verification aborts the whole run before any commit.

---

## 8. The commit-plan JSON

Written to `-out/offline-compact-<table>-<UTC timestamp>.json`. Shape:

```json
{
  "tableId": "2",
  "mode": "plan",
  "tablets": [
    {
      "endRow": "bQ==",
      "prevRow": null,
      "delete": ["eyJwYXRoIjoi…"],
      "add": { "path": "hdfs:/…/Aout.rf", "size": 4096, "numEntries": 120 }
    }
  ]
}
```

- `endRow` / `prevRow` identify the tablet extent (base64 of the raw row
  bytes; `null` = the first/last tablet).
- `delete` is the exact set of `file:` column qualifiers to remove
  (base64 of the byte-exact `StoredTabletFile` JSON, as it appears in
  metadata). Accumulo enforces byte-exact qualifier match on mutation.
- `add` is the single output file to reference (path + size + entry
  count).

**GC-safe:** the plan never deletes input RFiles from storage — it only
drops their metadata refs and lets Accumulo GC reclaim the bytes. An
applier must run each tablet's mutation as a **conditional** mutation
guarded on the current file set, so even a (fence-precluded) concurrent
change cannot corrupt metadata.

---

## 9. Post-run validation

After onlining the table:

- Full scan (or targeted range scan) and confirm the data is intact.
  Post-compaction, versioning/age-off iterators in the `majc` stack will
  have collapsed old versions — expect the compacted view, not the raw
  pre-compaction cell count.
- Confirm each compacted tablet now references exactly one `file:` entry
  (Accumulo shell: `scan -t accumulo.metadata -c file` for the tablet
  rows, or the monitor).

---

## 10. Failure modes & recovery

| Symptom | Cause | Recovery |
|---------|-------|----------|
| `table … is "ONLINE", must be OFFLINE` at startup | Table not offline. | `offline -t <table> -w`, then re-run. |
| `offline fence tripped: state znode version changed …` at commit | Table went ONLINE (round-trip) during the run, or its state changed. | Nothing was committed. Re-offline and re-run from the dry run. |
| `offline fence tripped: zk session changed …` | ZK connection dropped mid-run (watches lost). | Nothing was committed. Re-run. |
| Run aborts naming an iterator class (e.g. `com.example.MyIterator`) | An iterator in the `majc` stack has no Go port. | Port it (iterator-forge track) or remove it from the table config; do not silently drop it. Nothing was written. |
| `verification failed for tablet …` | Output diverged from the independent re-derivation. | Nothing was committed. File a bug with the pinned cell mismatch; do **not** force-commit. |
| Mode D: `direct commit mode requires a MetadataCommitter` | Mode D requested with no committer wired. | Use `-commit-mode=plan` (default) and apply via Ample, or wire a committer. The plan JSON was still written. |
| Mode D partial failure naming a tablet | A per-tablet conditional mutation was rejected mid-run. | Already-committed tablets are durable and independent; the error names the failed tablet so you can inspect and resume. Re-run for the remaining tablets. |

Because the only dangerous step is the metadata mutation (gated behind the
fence and `--dry-run=false`), a crash or abort before commit leaves only
unreferenced orphan RFiles, which Accumulo GC reclaims.

---

## 11. Environment variables

| Variable | Purpose |
|----------|---------|
| `SHOAL_PASSWORD` | Password / ZK instance secret (preferred over `-password`). |
| `SHOAL_JAVA_RFILE_VALIDATE` | When set, enables the T1 Java cross-read in verification (see §7). |
