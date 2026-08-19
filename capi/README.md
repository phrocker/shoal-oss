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
  `SHOAL_ABI_PACK_VERSION(major, minor, patch)` with a hexadecimal
  `0x00MMmmpp` layout, so ABI `1.8.0` is `0x00010800`.
- Capability identifiers are append-only. Existing IDs and bits never change
  meaning. `shoal_abi_capability_word_count()` reports how many 64-bit words
  the current library uses, `shoal_abi_capability_word(i)` returns `0` for
  `i >= word_count`, and `shoal_abi_has_capability(id)` returns `0` for both
  unsupported and unknown IDs.

Current capability assignments (`word 0 == 0x00000000000fffff`):

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
| `SHOAL_ABI_CAPABILITY_NAMESPACE_ADMIN` | `0x0400` | namespace discovery, lifecycle, and property administration |
| `SHOAL_ABI_CAPABILITY_SECURITY_ADMIN` | `0x0800` | users, authorizations, and permission administration |
| `SHOAL_ABI_CAPABILITY_TABLE_SPLITS` | `0x1000` | binary-safe split listing and creation |
| `SHOAL_ABI_CAPABILITY_CONNECTOR_IDENTITY` | `0x2000` | owned connector instance-name, instance-ID, and principal discovery |
| `SHOAL_ABI_CAPABILITY_DATA_DESCRIPTORS` | `0x4000` | owned range and iterator-setting descriptors with versioned borrowed views |
| `SHOAL_ABI_CAPABILITY_CONFIGURATION_TOPOLOGY` | `0x8000` | binary-safe configuration handles and owned instance-topology snapshots |
| `SHOAL_ABI_CAPABILITY_RFILE` | `0x10000` | owned standalone RFile readers, writers, seekable relocations, and copied results |
| `SHOAL_ABI_CAPABILITY_DATA_VALUES` | `0x20000` | copied key/range/authorization operations and owned key/value results |
| `SHOAL_ABI_CAPABILITY_BUFFERED_WRITER` | `0x40000` | owned lazy row-buffered high-level writer with close coordination |
| `SHOAL_ABI_CAPABILITY_TABLE_MAINTENANCE` | `0x80000` | row-bounded table flush and owned constraint administration |

Shoal does **not** advertise instance status, compaction/import/export,
Python/wheel, or any other unimplemented surface until the API exists and has
coverage.

Compatibility rules:

- Older headers with newer libraries are safe: additive symbols, capability
  IDs, and trailing struct fields do not change existing numeric values or
  layouts.
- Newer headers with older libraries require dynamic resolution of every
  optional additive symbol before use, followed by the corresponding
  capability check where one exists. A capability check alone cannot make a
  hard symbol reference load-safe on Windows or with eager ELF binding.
- Libraries that predate this discovery surface do not export the new version
  and capability symbols, so those queries must also be resolved dynamically
  when mixing library vintages. A missing discovery symbol means
  "pre-discovery library", not "feature supported".

All forward-compatible input structs start with `uint32_t struct_size`. The
existing `*_init` helpers initialize and advertise only their V1 prefix, so a
newer library never writes beyond an older caller's allocation. Callers using
fields appended after V1 must initialize their full structure and set
`struct_size = sizeof(struct)` from the header they compiled against. Future
ABI revisions may append fields only at the end. Libraries must continue
accepting the smallest prefix they actually read (today the `*_V1_SIZE`
constants) and must ignore trailing bytes from newer callers. Existing fields
must never be reordered, removed, or assigned new meanings.

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
- Connector identity and administration calls (`get_identity`, `list_tables`, `table_exists`, `create`,
  `delete`, `rename`, `flush`, property mutation, and effective property
  reads; namespace discovery/lifecycle/property reads; security operations;
  and table split listing/creation) accept per-call deadlines. Connector close
  prevents new calls, cancels and joins active administration calls, waits for any
  already-started scanner or batch-scanner calls to finish before final
  teardown, and remains idempotent while the handle stays alive.
- `shoal_connector_get_identity` returns one owned result containing copied
  instance-name, instance-ID, and principal strings. Initialize
  `shoal_connector_identity_view` with its init helper; the view borrows its
  pointers only until `shoal_connector_identity_free`, which is idempotent
  when called with the address of a `NULL` result variable.
- `shoal_range_create` and `shoal_iterator_setting_create` validate and copy
  every nested byte/string input into owned results. Their versioned views
  borrow result memory and expose range bound kinds, keys, inclusivity, and
  unboundedness plus iterator name/class/priority/options. The bound kind keeps
  empty row bounds distinct from unbounded bounds and preserves ROW versus KEY
  semantics for lossless reuse. These are local, non-blocking
  descriptor operations, so connector close, deadlines, and cancellation do
  not apply. Concurrent getters are safe while free is externally serialized.
- Scanner configuration, ranges, and all nested arrays/bytes are borrowed only
  for the creating or scan call. Shoal copies every value it retains.
- Configuration names, values, and defaults are length-delimited and copied,
  preserving embedded NUL bytes. Configuration handles and topology results
  are owned and released with NULL-safe, idempotent free functions.
- Root-tablet, manager, and server discovery use per-call deadlines and are
  cancelled by connector close. Root, ZooKeeper, and configuration getters
  are coordinated with close and return owned handles or snapshots.
- RFile readers, writers, and seekable relocations are owned opaque handles.
  Path, key, value, and column-family inputs are copied; top entries, values,
  ranges, and family accessors return owned results. Operations accept
  per-call deadlines, close cancels and joins active work, and every free is
  NULL-safe, idempotent, and clears the caller's handle.
- Key and range formatting/predicates are local copied-value operations.
  Key/value results own every nested byte slice. Authorization handles copy,
  sort, and deduplicate labels at construction and are immutable, making
  concurrent getters safe while free remains externally serialized.
- Scanner and batch-scanner handles support concurrent scan calls. Close
  cancels and joins in-flight calls and is idempotent while the handle remains
  alive; free performs best-effort close and sets the handle variable to
  `NULL`. Once connector close/free starts, new scan calls fail with
  `SHOAL_STATUS_CLOSED`, but already-started scans are allowed to finish
  before connector teardown.
- Table list and effective-property results own all returned strings. Views
  from `shoal_table_list_get` and `shoal_table_properties_get` remain valid
  until the matching result is freed. Effective properties preserve explicit
  empty string values.
- Row-bounded flush distinguishes a `NULL` bound pointer (unbounded) from a
  non-NULL zero-length bound (the empty row) and copies binary bounds before
  invoking Go. Constraint listing returns an owned, stably ordered snapshot;
  its caller-sized views borrow class names until list free. All live
  maintenance calls accept deadlines and participate in connector-close
  cancellation and active-call joining.
- Namespace lists and property results own all returned strings. Versioned
  property results additionally preserve the Accumulo property-store version.
- Authorization and split results own binary-safe byte arrays. Their borrowed
  views remain valid until `shoal_bytes_list_free`.
- Passwords, authorization arrays, and split arrays are copied before calls
  return. Empty passwords are represented by a non-NULL `shoal_bytes` input
  whose length is zero.
- Security failures retain the stable Accumulo user and code on the owned
  `shoal_error`; `shoal_error_security_user` and
  `shoal_error_security_code` return borrowed strings or `NULL`.
- Scan results own binary-safe key/value storage. Views returned by
  `shoal_scan_result_get` remain valid until `shoal_scan_result_free`.
- Mutations copy every row, column, visibility, and value input. BatchWriter
  add snapshots the mutation, so callers may immediately reuse or free it.
- BatchWriter operations accept per-call deadlines. Close prevents new calls,
  cancels and joins active calls, and flushes the remaining buffer; free uses a
  bounded best-effort close.
- The owned buffered writer copies its batch-writer configuration and every
  binary update, creates the underlying BatchWriter lazily on the first
  update, combines adjacent updates for one row, and submits the pending
  mutation on row change or close. `put` replaces timestamp zero with the
  current Unix time in milliseconds; `put_delete` preserves an explicit zero.
  Calls are serialized, connector close cancels active work, and writer close
  cancels, joins, flushes, and closes with the caller's deadline.
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
failures, table and namespace administration, complete merged security
administration, and binary-safe table split listing/creation. Instance status,
compaction, bulk import/export, and Python wheel APIs remain deferred.

`docs/sharkbite-compatibility.md` enumerates the full Sharkbite compatibility
contract, maps every element to this ABI, and defines the implementation-entry
and release gates for any Sharkbite-compatible Python wheel.
