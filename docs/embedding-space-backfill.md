# Embedding-space backfill

`shoalctl embedding-backfill` records an explicit `file.embedding` column
for tablet files that carry none. It is the migration path for issue
#274.

## Why it is needed

`internal/embeddingspace` models three states per file:
`has_embeddings:<id>`, `no_embeddings`, and `unknown`. `unknown` is
absorbing — it refuses to merge — because absence of information must
never be mistaken for information about absence.

Before #274, several layers filled in `no_embeddings` when they simply
had no data. `no_embeddings` merges freely with every space, so a file
carrying foreign vectors could be merged into an output labelled with
the target space. Those layers now report `unknown`, and only an
explicitly decoded column or footer produces `no_embeddings`.

The operator-visible consequence is that files written before the
`file.embedding` column existed now read as `unknown`. Compaction and
convergence fail closed on them until their state is recorded once.

## What it does

For each file entry in a table:

| Situation | Outcome |
| --- | --- |
| The entry already has a definite `file.embedding` column | left alone, counted as *already labelled* |
| The file's footer declares a known state | that state is written to `file.embedding`, counted as *resolved* |
| The footer is absent, unreadable, or explicitly `unknown` | left `unknown` and listed individually as *unresolvable* |
| The entry changed mid-run | not written, listed individually as *raced*; re-run picks it up |

An entry whose column is explicitly `unknown` is treated exactly like one
with no column: neither establishes anything, and both are repaired by
the same run.

The authority is the file's own footer meta block
(`embeddingspace.RFileMetaBlockName`), read through
`internal/rfile`. It travels with the file and was written by whoever
produced it. Nothing else is consulted: the backfill never inspects cell
values to guess whether something looks like a vector, because a guess
written into durable metadata is the exact failure #274 removes.

A file whose footer does not establish a known state cannot be resolved
from metadata alone, so it is reported by name with the reason rather
than guessed at.

## Running it

```
shoalctl embedding-backfill --zk zk1:2181,zk2:2181 --instance accumulo \
    --table 2k --storage gs --user root
```

It defaults to `--dry-run=true`, which reports the size and shape of the
migration without writing anything. Pass `--dry-run=false` to apply.
Supply the password through `SHOAL_PASSWORD` rather than `--password`.

The command exits non-zero when the run left anything outstanding
(unresolvable or raced files), so a script cannot treat a partial
migration as done.

## Safety and idempotence

Every write is conditional on:

* the tablet's `~tab:~pr` being unchanged, so the row still describes the
  tablet that was examined and has not split;
* the `file:` entry still holding exactly the bytes that were read, so
  the file has not been replaced by a compaction; and
* the `file.embedding` column still holding exactly what it held when the
  file was examined — absent, or an explicit `unknown` — so a concurrent
  writer that established the state first wins. An established column is
  never replaced.

A second run therefore writes nothing and reports every file as already
labelled. The run is safe to interrupt and safe to repeat.

The root tablet is refused (`metadatacas.ErrRootBackfillUnsupported`):
its metadata lives in ZooKeeper behind a different mutation path owned by
whichever server hosts it. A table id that locates no tablets is also
refused, so a typo cannot report a completed zero-file migration.

## Declaring a default instead

An operator who knows an ingest pipeline emits no vectors can say so
rather than have it inferred. `shoal-tserver --default-embedding` takes
`no_embeddings`, `unknown`, or `has_embeddings:<identity>`, and is the
state recorded for a minor-compaction snapshot that declares none. When
it is unset the recorded state is `unknown`. The value is validated at
startup.

Internally that is `hostedingest.Config.DefaultEmbedding` →
`mincauthority.Config.DefaultEmbedding`. `WriterOptions.EmbeddingSpace`
is honoured as a fallback default for the same reason, and the
coordinator now forces the RFile footer to carry the same state it
records in metadata — previously a configured writer option could reach
the footer while the metadata column said something else, which the next
integrity check rejects.

## Diagnosing a refusal

When a compaction refuses because an input's embedding space was never
established, the error wraps
`compaction.ErrEmbeddingBackfillRequired`, names the offending inputs,
and names this command. That distinguishes "nobody ever recorded what
this file holds" from "these files genuinely hold different embedding
spaces" — the second is a real data condition resolved by convergence,
not by a backfill.
