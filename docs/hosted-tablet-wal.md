# Hosted-tablet WAL authority

`internal/walauthority` is the Accumulo-authoritative write boundary used by a
hosted tablet. It is deliberately separate from `internal/localwal`, which is
the embedded single-process WAL and is not valid authority for a tablet
assigned by an Accumulo manager.

The commit order is:

1. verify the tablet-server ServiceLock, manager generation, and assignment;
2. create a uniquely named WAL under `<root>/<host+port>/<uuid>`;
3. conditionally install the tablet's exact `log:` reference through
   `MetadataAuthority`;
4. append and sync a checksummed mutation frame carrying the operation,
   session, request, sequence, extent, and fence identities;
5. re-verify the fence, install the batch in the memtable, and acknowledge.

Recovery uses `AssignedOwnerVerifier` while the Host attempt is loading. Normal
commits use `HostedOwnerVerifier`, so the tablet cannot accept routed writes
until `LoadComplete` publishes the same assignment attempt.

Lost metadata or append responses are reconciled by reading the authoritative
state. Retries use the stable operation identity and cannot append a conflicting
batch. Recovery reads only metadata-referenced WALs, validates every frame
before applying any record from a WAL, repairs an incomplete trailing frame,
rejects checksum/interior corruption, sorts by sequence, and deduplicates by
operation identity.

Rollover leaves old references in metadata. The next minor-compaction slice
must atomically publish its RFile and delete the corresponding WAL references;
only after that commit may it call `Tablet.Retire`. This package intentionally
does not claim that metadata transaction.
