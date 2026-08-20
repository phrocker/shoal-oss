# Tablet ingest service integration

`internal/ingestservice` implements the Accumulo 4
`TabletIngestClientService` update-session surface over
`internal/ingestrouter`. It decodes Accumulo's compact `TMutation` format,
authenticates sessions, checks table WRITE permission per extent, preserves
partial success, and returns stale/retry, authorization, and constraint
failures through `UpdateErrors`.

Supported durability requests are `DEFAULT`, `SYNC`, `FLUSH`, and `LOG`.
The hosted WAL authority currently satisfies all four with the stronger
append-and-sync contract. `NONE` is rejected explicitly. Conditional mutation
sessions are also rejected explicitly with `UNSUPPORTED_OPERATION`; the
service never returns a success-shaped result for semantics it does not
implement.

`internal/hostedingest` composes a manager assignment attempt with:

- exact ServiceLock, manager-lock, and assignment fencing;
- referenced-WAL recovery before a tablet is published;
- deterministic server timestamps before WAL append;
- idempotent memtable application;
- threshold/manual minor compaction through `internal/mincauthority`;
- serialized commit, flush, drain, and unload.

`tserverprocess.NewWritableStore` publishes a tablet to scans and ingest only
after that composition opens successfully. `tserverprocess.Services` registers
and advertises `TABLET_INGEST` only when a live ingest service is supplied and
accepting. The current `cmd/shoal-tserver` intentionally supplies no ingest
service because a production Accumulo conditional metadata writer has not yet
landed. Therefore the command remains fail-closed and does not advertise
`TABLET_INGEST`; tests use a fenced fake metadata authority to exercise WAL,
replay, ambiguous responses, compaction, stale ownership, and drain races.

Live unmodified-Java-client evidence remains part of issue #74 and requires
Docker or another Accumulo 4 cluster environment.
