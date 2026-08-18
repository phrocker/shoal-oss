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

## ABI version and capability discovery

Shoal separates **ABI compatibility** from **feature availability**:

- `SHOAL_ABI_VERSION` and `shoal_abi_version()` remain the legacy
  compatibility-major for existing callers. They stay at `1` for every
  backward-compatible ABI update.
- `SHOAL_ABI_VERSION_MAJOR`, `_MINOR`, `_PATCH`,
  `SHOAL_ABI_VERSION_PACKED`, and the matching runtime queries provide a
  stable allocation-free version tuple that works before connector creation.
  `SHOAL_ABI_VERSION_PACKED` uses
  `SHOAL_ABI_PACK_VERSION(major, minor, patch)` with an `0x00MMmmpp` layout,
  so ABI `1.0.0` is `0x00010000`.
- Capability identifiers are append-only. Existing IDs and bits never change
  meaning. `shoal_abi_capability_word_count()` reports how many 64-bit words
  the current library uses, `shoal_abi_capability_word(i)` returns `0` for
  `i >= word_count`, and `shoal_abi_has_capability(id)` returns `0` for both
  unsupported and unknown IDs.

Current capability assignments (`word 0 == 0x00000000000003ff`):

| ID | Mask | Surface |
| --- | --- | --- |
| `SHOAL_ABI_CAPABILITY_CONNECTOR` | `0x0001` | connector handle lifecycle |
| `SHOAL_ABI_CAPABILITY_BOOTSTRAP` | `0x0002` | static/ZooKeeper bootstrap configuration |
| `SHOAL_ABI_CAPABILITY_ERROR` | `0x0004` | owned `shoal_error` objects and stable code/message access |
| `SHOAL_ABI_CAPABILITY_SCANNER` | `0x0008` | single-range scanner creation, close, and scan |
| `SHOAL_ABI_CAPABILITY_BATCH_SCANNER` | `0x0010` | multi-range batch scanner creation, close, and scan |
| `SHOAL_ABI_CAPABILITY_OWNED_SCAN_RESULT` | `0x0020` | owned scan results and borrowed key/value views |
| `SHOAL_ABI_CAPABILITY_MUTATION` | `0x0040` | mutation creation, updates, size, and free |
| `SHOAL_ABI_CAPABILITY_BATCH_WRITER` | `0x0080` | batch writer creation, add, flush, close, and free |
| `SHOAL_ABI_CAPABILITY_STRUCTURED_WRITE_FAILURE` | `0x0100` | owned structured write failure details |
| `SHOAL_ABI_CAPABILITY_TABLE_ADMIN` | `0x0200` | list/exists/create/delete/rename/flush/property administration |

Shoal does **not** advertise namespace/security/status, split creation/listing,
compaction/import/export, Python/wheel, or any other unimplemented surface
until the API exists and has coverage.

Compatibility rules:

- Older headers with newer libraries are safe: additive symbols, capability
  IDs, and trailing struct fields do not change existing numeric values or
  layouts.
- Newer headers with older **post-discovery** libraries are safe when callers
  check the reported capability words/IDs and treat missing words or IDs as
  unavailable.
- Libraries that predate this discovery surface do not export the new version
  and capability symbols. Load those symbols dynamically when mixing library
  vintages; a missing symbol means "pre-discovery library", not "feature
  supported".

All forward-compatible input structs start with `uint32_t struct_size`. Call
the matching `*_init` helper or set `struct_size = sizeof(struct)` from the
header you compiled against. Future ABI revisions may append fields only at
the end. Libraries must continue accepting the smallest prefix they actually
read (today the `*_V1_SIZE` constants) and must ignore trailing bytes from
newer callers. Existing fields must never be reordered, removed, or assigned
new meanings.

Version numbers change only when the public ABI contract changes:

- bump **major** (and therefore `SHOAL_ABI_VERSION`) for breaking ABI changes
  such as symbol removal/rename, signature or ownership contract changes,
  reordered/shrunk structs, or renumbered status/enum values
- bump **minor** for backward-compatible ABI growth such as new exported
  functions, appended struct fields guarded by `struct_size`, or new capability
  IDs/bits
- bump **patch** for compatible fixes and clarifications that do not change
  layout, numeric assignments, or advertised capability coverage

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
