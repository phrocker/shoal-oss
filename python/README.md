# Shoal Python foundation

This is an independently usable **early binding slice**, not a complete
Sharkbite replacement and not a release of the `sharkbite` distribution.
It installs the import-compatible modules `sharkbite` and `pysharkbite`.

Supported now:

- deterministic Shoal shared-library discovery and ABI/capability negotiation;
- stable status-to-exception mapping, including Sharkbite `ClientException`;
- owned handle/result cleanup and idempotent context-manager close;
- `Connector`, `Client`, `Scanner`, `Key`, and streaming-free bounded scans
  through the high-level C ABI.

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

The library must expose ABI major 1 and capabilities 21 (high-level client),
22 (high-level scanner), and 5 (owned scan result).
