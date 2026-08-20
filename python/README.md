# Shoal Python binding

This is an independently usable **incremental binding**, not a complete
Sharkbite replacement and not a release of the `sharkbite` distribution.
It installs the import-compatible modules `sharkbite` and `pysharkbite`.

Supported now:

- deterministic Shoal shared-library discovery and ABI/capability negotiation;
- stable status-to-exception mapping, including Sharkbite `ClientException`;
- owned handle/result cleanup and idempotent context-manager close;
- `Connector`, `Client`, `Scanner`, `Key`, and streaming-free bounded scans
  through the high-level C ABI;
- binary-safe `Mutation` and `BatchWriter` APIs with deterministic
  flush/close and structured write-failure status mapping;
- table, namespace, and security administration through both direct
  connector helpers and Sharkbite-shaped `tableOps`, `namespaceOps`, and
  `securityOps` objects.

Unsupported legacy entry points are present only where useful for discovery and
raise `NotImplementedError` with a stable message. They never fabricate data.

## Install and load

```text
python -m pip install ./python
set SHOAL_LIBRARY=C:\path\to\shoal.dll
```

On Linux/macOS set `SHOAL_LIBRARY` to `libshoal.so`/`libshoal.dylib`.
Without it, the loader checks a packaged `.libs` directory, the platform
loader search path, and common repository build locations.

```python
from sharkbite import Client

with Client("accumulo", "zk1:2181", "root", "secret", table="events") as client:
    with client.scanner() as scanner:
        for key, value in scanner.scan(b"a", b"z"):
            print(key.row, value)
```

The library must expose ABI major 1. APIs check their exact capability set:
5/21/22 for scans, 6/7/8 for writes, and 9/10/11/12/19 for administration.

```python
from sharkbite import Connector, Mutation, TablePermissions

with Connector("accumulo", "zk1:2181", "root", "secret") as connector:
    table = connector.tableOps("events")
    table.create(recreate=True)
    with table.createWriter([], 4) as writer:
        with Mutation(b"row", _api=connector._api) as mutation:
            mutation.put(b"cf", b"cq", b"A", 7, b"value")
            writer.addMutation(mutation)
    connector.securityOps().grant_table_permission(
        "analyst", "events", TablePermissions.READ
    )
```

Online compaction, legacy chunked iterators, and other surfaces without an
equivalent stable C ABI remain explicit `NotImplementedError` paths.
