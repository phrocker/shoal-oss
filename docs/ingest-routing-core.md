# Ingest routing core

`internal/ingestrouter` is the validation, session, idempotency, and routing
core for a future Accumulo `TabletIngestClientService` implementation.

The boundary accepts table-scoped mutation batches, validates every batch
before contacting a tablet, enforces extent membership, visibility syntax,
timestamp policy, and configured byte/count limits, then routes each extent to
a `Directory`. A resolved `HostedTablet` carries an opaque hosting `Fence`;
the tablet implementation must re-check that fence atomically with commit.

Requests have stable idempotency keys. A duplicate key with different bytes is
rejected. A duplicate identical request preserves applied/rejected extents and
retries only retryable extents. Results are per extent, so stale assignments,
replacement extents, cancellation, unknown outcomes, and partial commits are
never collapsed into a success-shaped response.

## Deliberate boundary for issue #69

This slice does **not** implement the Accumulo-authoritative WAL, metadata log
references, recovery, minor-compaction commit, authorization/constraint RPC
mapping, or the Thrift service. It does not adapt the manager host, tablet
loader, embedded tablet, or scan server. Those components can implement the
interfaces later.

`HostedTablet.Authority()` must report `AuthorityAccumuloWAL` before the router
will acknowledge a commit. The zero/default `AuthorityUnsupported` returns
`ErrWALAuthorityUnsupported`; there is no local-WAL, in-memory, or
success-shaped fallback. Consequently this package is independently usable for
validation and routing integration, but it does not yet satisfy issue #69's
end-to-end Java BatchWriter durability acceptance criteria.
