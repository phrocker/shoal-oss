# Shoal C ABI

`capi/include/shoal.h` is the stable, versioned C surface for external
language bindings. Build the shared library and installable header with:

```text
make capi
```

The output is placed in `bin/capi/`. On Windows without `make`, the equivalent
library build is:

```text
go build -buildmode=c-shared -o bin\capi\shoal.dll .\cmd\shoal-capi
```

Use the checked-in `capi/include/shoal.h`, not the implementation header that
Go emits next to a `c-shared` library.

## Ownership and lifetime

- `shoal_connector_config` and all memory it references are borrowed only
  during `shoal_connector_create`; Shoal copies retained strings and bytes.
- `shoal_connector_create` returns an opaque, library-owned connector handle.
  Call `shoal_connector_close` to observe shutdown errors, then
  `shoal_connector_free` exactly once with the address of the handle variable.
  Freeing also performs best-effort close and sets the variable to `NULL`.
- A connector owns the bootstrap instance created for it. Callers do not
  separately close ZooKeeper resources.
- Table administration calls (`list_tables`, `table_exists`, `create`,
  `delete`, `rename`, `flush`, property mutation, and effective property
  reads) accept per-call deadlines. Connector close prevents new calls,
  cancels and joins active table-administration calls, and remains idempotent
  while the handle stays alive.
- Scanner configuration, ranges, and all nested arrays/bytes are borrowed only
  for the creating or scan call. Shoal copies every value it retains.
- Scanner and batch-scanner handles support concurrent scan calls. Close
  cancels and joins in-flight calls and is idempotent while the handle remains
  alive; free performs best-effort close and sets the handle variable to
  `NULL`.
- Table list and effective-property results own all returned strings. Views
  from `shoal_table_list_get` and `shoal_table_properties_get` remain valid
  until the matching result is freed. Effective properties preserve explicit
  empty string values.
- Scan results own binary-safe key/value storage. Views returned by
  `shoal_scan_result_get` remain valid until `shoal_scan_result_free`.
- Mutations copy every row, column, visibility, and value input. BatchWriter
  add snapshots the mutation, so callers may immediately reuse or free it.
- BatchWriter operations accept per-call deadlines. Close prevents new calls,
  cancels and joins active calls, and flushes the remaining buffer; free uses a
  bounded best-effort close.
- Write failures optionally return an owned `shoal_write_failure` containing
  ambiguous-commit/retry flags plus failed extents, constraint violations,
  authorization failures, and cleanup failures. Borrowed views remain valid
  until `shoal_write_failure_free`.
- Failed calls can return an owned `shoal_error`. Its message is borrowed from
  that object and remains valid until `shoal_error_free`.
- Status mapping is stable across the ABI: duplicate table names return
  `SHOAL_STATUS_ALREADY_EXISTS`, invalid table/property inputs return
  `SHOAL_STATUS_INVALID_ARGUMENT`, and missing manager/client-service
  endpoints return `SHOAL_STATUS_UNAVAILABLE`.
- Do not copy opaque handles, access them after free, or free/use the same
  handle concurrently. No Go pointer or Go-managed buffer is exposed through
  the ABI.

## Bootstrap modes

- `SHOAL_BOOTSTRAP_STATIC` uses `instance_name` and `instance_id`. It is useful
  for connector lifecycle and operations that do not require ZooKeeper
  discovery.
- `SHOAL_BOOTSTRAP_ZOOKEEPER` uses `instance_name` and a comma-separated
  `zookeeper_servers` list. A zero session or bootstrap timeout selects the
  30-second default. `instance_secret` is optional.

The ABI currently exposes connector bootstrap/lifecycle, synchronous Scanner
and BatchScanner reads, Mutation/BatchWriter writes with owned structured
failures, and table administration for listing, existence checks, create/
delete/rename, full-table flush, table property mutation, and effective
property reads. Namespace/security/status, split/compaction, bulk import/
export, local-only property surfaces, and Python wheel APIs remain deferred.
