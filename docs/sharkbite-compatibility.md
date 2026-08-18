# Sharkbite → Shoal compatibility matrix

Exhaustive enumeration of the public Sharkbite compatibility contract and its
mapping onto Shoal. This document is the normative compatibility matrix and
release gate for
[phrocker/sharkbite#108](https://github.com/phrocker/sharkbite/issues/108) and
Shoal issue [#81](https://github.com/phrocker/shoal-oss/issues/81) (umbrella
[#59](https://github.com/phrocker/shoal-oss/issues/59)).

<a id="sec-1"></a>

## 1. Status of this document

| Field | Value |
| --- | --- |
| Document status | Normative gate. Binding on all Sharkbite-compatibility work. |
| Tracking issue | Shoal [#81](https://github.com/phrocker/shoal-oss/issues/81) — "docs: define and audit complete Sharkbite compatibility matrix" (parent [#59](https://github.com/phrocker/shoal-oss/issues/59); upstream target [phrocker/sharkbite#108](https://github.com/phrocker/sharkbite/issues/108)) |
| Sharkbite reference | `phrocker/sharkbite` @ `7f2625f74331b0cd4a75dc0484949c40f1409686` ("Bump accumulo-core from 2.0.0 to 2.0.1 in /native-iterators-jni (#100)", 2022-07-22) |
| Sharkbite release line | `sharkbite` 1.2.0.3 on PyPI (`setup.py:34-35`) |
| Shoal reference | `phrocker/shoal-oss` `main` @ `1c2944798faf5a5deb659065dfea0bee23593df0` ("platform: make shoal-embed serve reachable, observable, and safely drainable (#79)") |
| Shoal C ABI version | `SHOAL_ABI_VERSION 1u` (`capi/include/shoal_types.h:19`) |
| Rows | 350 |
| Covered rows | 51 (14.6 percent) |

This document is the normative backlog and release gate for Sharkbite
compatibility work; it does **not** by itself authorize publishing a
Sharkbite-compatible Python surface. See
[§2 Release gate](#sec-2).

<a id="sec-2"></a>

## 2. Release gate (normative)

This gate is the deliverable of Shoal issue
[#81](https://github.com/phrocker/shoal-oss/issues/81); that issue is where
gate status, the independent omission/classification audit, and any approval
decisions are recorded. Closing #81 requires this document plus that audit —
not the document alone.

> **Implementation-entry gate.** After this matrix and the independent
> omission/classification audit are recorded on issue
> [#81](https://github.com/phrocker/shoal-oss/issues/81), implementation may
> proceed row-by-row. The matrix is the backlog: some rows may still be
> `Missing Go`, `Missing C ABI`, or `Behavior mismatch` while other rows are
> being implemented.

> **Release gate.** No package named `sharkbite`, no wheel or sdist, and no
> "Sharkbite compatible"/drop-in-replacement claim may be published until
> every in-scope row in [§5](#sec-5) through [§20](#sec-20) is either
> `Covered` with at least one **named** automated test on each layer it claims
> (Go and, where applicable, C ABI), `Not required (rationale required)` with
> an explicit scope decision in Notes, or `Intentional divergence` **and** has
> recorded explicit human approval in [§26](#sec-26).

Corollaries, all binding:

1. **No partial public API.** A Python module that exposes a subset of the
   Sharkbite surface must not be published under the name `sharkbite`, must not
   be described as "Sharkbite compatible", and must not be advertised as a
   drop-in replacement. Sharkbite users import one module and expect the whole
   1.2.x surface; a subset silently converts a compile-time port into a
   runtime `AttributeError` for every consumer.
2. **No name-based coverage claims.** A row may only be marked `Covered` when
   the Shoal symbol has been read and its semantics compared against the
   Sharkbite behavior. Matching identifiers (`Scanner`, `Mutation`, `flush`,
   `close`) are not evidence.
3. **Ordered prerequisites.** For any given row, work proceeds
   Go parity → C ABI parity → compatibility tests → Python/wheels
   ([§23](#sec-23)). The order is per-row, not a global freeze; no release
   occurs until the final gate above is shut.
4. **Named tests only.** "Covered by the existing suite" is not evidence. Each
   `Covered` row names test functions or C assertion blocks.
5. **Scope exclusions must be explicit.** `Not required` means "outside the
   compatibility contract", not "to be reviewed later". The Notes column must
   state why the row is excluded.
6. **Divergence requires approval.** `Intentional divergence` rows are
   proposals until a named approver signs [§26](#sec-26). Unapproved
   divergences block the release gate exactly like `Missing Go`.
7. **Live-cluster conformance is part of the gate.** Unit tests alone cannot
   close rows whose semantics are defined by a real tablet server
   ([§24](#sec-24)).

<a id="sec-3"></a>

## 3. Pinned sources and validation method

### 3.1 Sharkbite inputs actually inspected

| Input | Path | What was extracted |
| --- | --- | --- |
| Python binding | `src/python/pysharkbite.cpp` (560 lines) | Every `pybind11::class_`, `enum_`, `def`, `def_static`, `def_readonly`, `init`, default argument, dunder, and exception registration. Read in full. |
| Python package | `sharkbite/__init__.py` (171 lines) | `AccumuloBase`, `AccumuloWriter`, `AccumuloScanner`, `AccumuloIterator`, `from .pysharkbite import *` re-export. |
| Optional data helpers | `sharkbite/torch.py`, `pandashark/__init__.py`, `pandashark/pandassharkbite.py` | `AccumuloDataset`, `AccumuloValueDataset`, `AccumuloCluster`, `read_accumulo`, `read_accumulo_nex`, `DataFrameIterator`. |
| Packaging | `setup.py`, `PYTHONREADME.md`, `CMakeLists.txt`, `bootstrapper.sh`, `.github/workflows/ccpp.yml` | Distribution name/version, `python_requires`, ext-module wiring, module target `pysharkbite`, CI matrix. |
| Public headers | `include/data/constructs/**`, `include/interconnect/**`, `include/scanner/**`, `include/writer/**`, `include/data/exceptions/**`, `include/extern/accumulo.h` | Signatures and defaults behind each binding, exception hierarchy, ownership annotations. Vendored `include/extern/libhdfs3/**` and generated Thrift excluded. |
| Python tests | `test/python/*.py` (10 files), `test/python/testmodule/__init__.py`, `test/MainExecutor.py`, `test/performance/QuarterMillionWrites.py` | Behavioral contracts asserted against a live cluster. |
| C++/integration tests | `test/vandv/*.h`, `test/vandv/IntegrationTest.cpp`, `test/constructs/*.cpp`, `test/zookeeper/*.cpp` | Error-code assertions, tablet-move behavior, security matrix. |
| Examples | `examples/*.py`, `examples/CppExample.cpp`, `c-library-examples/python/**` | User-facing call sequences, including flat-C-API consumers. |

### 3.2 Shoal inputs actually inspected

`accumulo/*.go` (16 non-test files, all exported declarations extracted),
`accumulo/*_test.go` (test function names extracted),
`capi/include/shoal.h`, `capi/include/shoal_types.h`, `capi/README.md`,
`capi/tests/{lifecycle.c,result_bridge.c,header_cpp_test.cpp,test_seam.h}`,
`cmd/shoal-capi/*.go`, `Makefile`, `.github/workflows/ci.yml`,
`.github/workflows/hdfs-integration.yml`, `README.md`, `ARCHITECTURE.md`,
and the `internal/` packages named in matrix rows.

### 3.3 Evidence-validation method

Every Shoal symbol, path, and test name cited below was validated against the
Shoal reference commit above, not against memory or a prior summary:

1. **Path existence** — each cited file was confirmed present with
   `Test-Path` or a direct read.
2. **Symbol existence** — exported Go declarations were extracted mechanically
   (`^(func|type|const|var)` over `accumulo/*.go`) and cross-checked against
   file/line before citation. C ABI declarations were extracted from
   `capi/include/shoal.h` and `capi/include/shoal_types.h` by full read.
3. **Test names** — extracted mechanically (`^func Test`) from
   `accumulo/*_test.go` and `cmd/shoal-capi/*_test.go`; C assertion blocks were
   read from `capi/tests/*.c`.
4. **Absence claims** — every "absent" claim was produced by a negative search
   (for example, no `Namespace`, `Permission`, `GrantAuthorizations`,
   `AddSplits`, `ListSplits`, `Compact`, `ImportTable`, `ExportTable`,
   `CloneTable`, `Online`/`Offline`, `Merge`, or `DeleteRows` symbol exists in
   `accumulo/`), not by omission from a summary.
5. **Sharkbite claims** — validated against the pinned clone. Where an
   intermediate summary disagreed with the source (for example a claimed
   three-argument `Range(start, end, bool)` call in
   `examples/pythonexample.py`), the source won: that file uses the four-argument
   form at `examples/pythonexample.py:92`.

<a id="sec-4"></a>

## 4. How to read the matrix

### 4.1 Columns

| Column | Meaning |
| --- | --- |
| ID | Stable row anchor. Never renumber; retire with a tombstone instead. |
| Sharkbite | Python-visible symbol (or C++ symbol for [§19](#sec-19)) with its pinned signature/defaults. |
| Shoal Go | Exact Go symbol in `accumulo/` (or an internal package, marked as such) or `—` when absent. |
| Shoal C ABI | Exact declaration in `capi/include/shoal.h` / `shoal_types.h`, or `—` when absent. |
| Evidence | Named tests, on both layers where both are claimed. `—` means no test exists. |
| Status | One of the six values below. |
| Notes | Semantics, defaults, ownership, and shim requirements. |

### 4.2 Status decision procedure

Applied top-down; the first matching rule wins.

1. **`Missing Go`** — no Go implementation exists in `accumulo/` or a reachable
   internal package. This is the bottom-most gap and always blocks first.
2. **`Missing C ABI`** — a Go implementation exists but the capability is not
   reachable through `capi/include/shoal.h`. This bucket also covers rows whose
   only remaining work is above the ABI (wheels, packaging, Python shims),
   because such rows are still unreachable from Python today.
3. **`Behavior mismatch`** — reachable on both layers, but user-visible
   semantics differ (types, defaults, ordering, lifetime, error class,
   binary handling, streaming vs materialized).
4. **`Intentional divergence (approval required)`** — deliberately not
   reproduced; requires a signed entry in [§26](#sec-26).
5. **`Not required (rationale required)`** — out of the compatibility contract;
   requires a stated rationale in the Notes column.
6. **`Covered`** — reachable with equivalent semantics on every layer the row
   claims, with named tests on each of those layers.

A row only owes evidence for the layers its `Shoal Go` and `Shoal C ABI`
columns actually name. Where a column reads `n/a`, `implicit`, or `—`, no
separate test is owed for that layer, and the row must say why in Notes.
`cmd/shoal-capi/*_test.go` counts as C ABI evidence: those tests exercise the
exported ABI entry points directly.

### 4.3 Layer legend

`Go` = `accumulo/` package. `ABI` = `capi/include/shoal.h`.
`internal` = present in Shoal but not exported (`internal/...`); an internal
implementation never satisfies a row on its own, because Sharkbite consumers
cannot reach it — it only shortens the Go-parity work.

<a id="sec-5"></a>

## 5. Matrix: packaging, imports, and wheels (`SB-PKG`)

Sharkbite's import contract is part of its public API: `PYTHONREADME.md:10`
states "package import is now **sharkbite** not **pysharkbite**", while
`sharkbite/__init__.py:5` re-exports the native module, so **both** names are
importable and both are used by the pinned tests and examples.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-PKG-001 | Distribution `sharkbite`, version `1.2.0.3` (`setup.py:34-35`) | — | — | — | Missing C ABI | No Python packaging exists in Shoal. `Makefile` has `build`, `capi`, `test`, `test-hdfs`, `vet`, `clean` only — no wheel/sdist target. |
| SB-PKG-002 | `import sharkbite` (`examples/hdfswrite.py`, `PYTHONREADME.md:10`) | — | — | — | Missing C ABI | Top-level package must exist and export the full native surface. |
| SB-PKG-003 | `import pysharkbite` (native module; `test/python/testmodule/__init__.py:64`) | — | — | — | Missing C ABI | Legacy module name still used by every pinned Python test. Must remain importable. |
| SB-PKG-004 | `from .pysharkbite import *` (`sharkbite/__init__.py:5`) | — | — | — | Missing C ABI | Star-export means every native symbol is reachable as `sharkbite.X`. Shim must reproduce the exact export set. |
| SB-PKG-005 | `pkgutil.extend_path` namespace extension (`sharkbite/__init__.py:1-3`) | — | — | — | Not required (rationale required) | Rationale: namespace-package extension has no Shoal analogue and no pinned consumer; reproducing it risks import-shadowing. Record as divergence if any downstream relies on it. |
| SB-PKG-006 | `ext_modules=[CMakeExtension('sharkbite.pysharkbite')]` (`setup.py:42`) | — | — | — | Not required (rationale required) | Rationale: build mechanism, not API. Shoal will ship a `cffi`/`ctypes` binding over `libshoal`, not a CMake extension. Note the pinned `setup.py` never sets `cmdclass`, so the committed packaging path cannot actually build the extension. |
| SB-PKG-007 | `python_requires='>=3.6'` (`setup.py:49`) | — | — | — | Missing C ABI | Shoal must decide and record a floor (3.9+ recommended); 3.6 is EOL. Divergence must be approved if the floor is raised. |
| SB-PKG-008 | Platform classifiers: Linux or macOS only, chosen at build time (`setup.py:27-31,44-48`) | — | — | — | Missing C ABI | No Windows wheel in Sharkbite. Shoal's ABI declares `__declspec(dllexport)`/`__cdecl` for `_WIN32` (`capi/include/shoal_types.h:7-13`), so Windows support is possible and must be an explicit decision. |
| SB-PKG-009 | `zip_safe=False`, `packages=['sharkbite']` (`setup.py:43,50`) | — | — | — | Missing C ABI | Wheel layout contract. |
| SB-PKG-010 | Shared object must be `ctypes.cdll.LoadLibrary`-able before `import pysharkbite` (`test/python/testmodule/__init__.py:63-64`, `test/MainExecutor.py`) | — | — | — | Behavior mismatch | Sharkbite tests preload the `.so` explicitly. A Shoal binding over `libshoal` must not require preloading; the shim should make the `-s/--solocation` argument a no-op so pinned test drivers still run. |
| SB-PKG-011 | CI builds one Ubuntu configuration, no wheel job (`.github/workflows/ccpp.yml`) | — | — | Shoal CI: `.github/workflows/ci.yml` build/vet/race jobs | Missing C ABI | Shoal has no wheel or `manylinux` job. Wheel matrix (CPython × platform) must be defined before any release. |
| SB-PKG-012 | `sharkbite.torch` submodule import (`examples/torchexample.py`, `sharkbite/torch.py`) | — | — | — | Missing C ABI | See [SB-RFILE-024](#sec-15) family; PyTorch dataset surface. |
| SB-PKG-013 | Top-level `pandashark` package boundary (`pandashark/__init__.py:1-4`, `examples/dataframe.py:26`, `setup.py:39-50`) | — | — | — | Not required (rationale required) | Rationale: the pinned repository contains `pandashark`, but the shipped `sharkbite` 1.2.0.3 wheel packages only `sharkbite` (`packages=['sharkbite']`). Track the helper rows below for exhaustiveness, but do not treat `pandashark` as part of the `sharkbite` import contract unless packaging scope changes. |
| SB-PKG-014 | Accumulo version support: 1.6.x–2.x (`PYTHONREADME.md:9`) | `accumulo.DefaultAccumuloVersion = "4.0.0-SNAPSHOT"`; `normalizeConnectorOptions` rejects non-`4.` (`accumulo/config.go:13,55-61`) | `shoal_connector_config.accumulo_version` (`capi/include/shoal_types.h:125`) | `TestNewConnectorRejectsUnsupportedVersion` (`accumulo/connector_test.go:42`); `capi/tests/lifecycle.c:49-52` | Intentional divergence (approval required) | Shoal targets Accumulo 4 only. Every Sharkbite user on 1.x/2.x is unsupported. This is the single largest divergence in the document and must be approved explicitly, with a documented migration statement. |

<a id="sec-6"></a>

## 6. Matrix: configuration, instance, and credentials (`SB-CFG`)

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-CFG-001 | `Configuration()` (`pysharkbite.cpp:57-58`) | `accumulo.ConnectorOptions{}` struct (`accumulo/config.go:30`) | `shoal_connector_config` + `shoal_connector_config_init` (`capi/include/shoal.h:12-13`) | `TestNewConnectorCanDisablePooling` (`accumulo/connector_test.go:51`); `capi/tests/lifecycle.c:36-38` | Behavior mismatch | Sharkbite is an untyped string bag; Shoal is a typed struct. Shim must accept arbitrary keys and either map or reject them explicitly — silently dropping keys such as `FILE_SYSTEM_ROOT` changes behavior. |
| SB-CFG-002 | `Configuration.set(key, value)` (`pysharkbite.cpp:59`) | — | — | — | Missing Go | No key/value configuration API. `FILE_SYSTEM_ROOT` is set by every pinned test (`test/python/testmodule/__init__.py:66`). |
| SB-CFG-003 | `Configuration.get(key)` (`pysharkbite.cpp:60`) | — | — | — | Missing Go | |
| SB-CFG-004 | `Configuration.get(key, default)` (`pysharkbite.cpp:61`) | — | — | — | Missing Go | Overload on arity. |
| SB-CFG-005 | `Configuration.getLong(key, default) -> uint32` (`pysharkbite.cpp:62`) | — | — | — | Missing Go | Name says long, type is `uint32_t`. |
| SB-CFG-006 | `Instance` opaque base class (`pysharkbite.cpp:64`) | `accumulo.Instance` interface (`accumulo/instance.go:23`) | Folded into `shoal_connector_config.bootstrap` (`capi/include/shoal_types.h:118`) | `TestNewStaticInstance` (`accumulo/instance_test.go:12`); `capi/tests/lifecycle.c:54-57` | Behavior mismatch | Sharkbite exposes `Instance` as a Python type used for isinstance-style polymorphism; Shoal's ABI has no instance handle, only a bootstrap discriminator. Shim must synthesize an `Instance` object. |
| SB-CFG-007 | `ZookeeperInstance(instance, zookeepers, timeoutMs, Configuration)` (`pysharkbite.cpp:69-70`) | `accumulo.NewZooKeeperInstance(ctx, ZooKeeperConfig{Servers, InstanceName, SessionTimeout, InstanceSecret})` (`accumulo/instance.go:52`, `accumulo/config.go:22`) | `SHOAL_BOOTSTRAP_ZOOKEEPER` + `zookeeper_servers`, `zookeeper_session_timeout_ms` (`capi/include/shoal_types.h:51,121,126`) | `TestNewZooKeeperInstanceResolvesAndClosesOnce` (`accumulo/instance_test.go:22`); `capi/tests/lifecycle.c:66-75` | Behavior mismatch | Sharkbite takes a comma-separated string and a millisecond `uint32`; Shoal takes `[]string` + `time.Duration`, and requires a `context.Context` the Python signature has no slot for. Shim must split the string and own the context. |
| SB-CFG-008 | `ZookeeperInstance(None, None, 1000, None)` raises (`test/python/TestBadOperations.py:67-71`) | `newZooKeeperInstance` validation (`accumulo/instance.go:63`) | `SHOAL_STATUS_INVALID_ARGUMENT` (`capi/include/shoal_types.h:25`) | `TestNewZooKeeperInstanceValidatesConfig` (`accumulo/instance_test.go:54`); `capi/tests/lifecycle.c:30-33` | Behavior mismatch | Both reject, but Sharkbite raises `ClientException`/`TypeError`; Shoal returns an error value. Exception class mapping is [SB-ERR-002](#sec-18). |
| SB-CFG-009 | `ZookeeperInstance.getInstanceName()` / alias `instance_name()` (`pysharkbite.cpp:71-72`) | `InstanceInfo` via `Instance.Info()` (`accumulo/instance.go:16,23`) | — | `TestNewStaticInstance` (`accumulo/instance_test.go:12`) | Missing C ABI | Both alias spellings must exist on the shim. |
| SB-CFG-010 | `ZookeeperInstance.getInstanceId(retry=False)` / alias `instance_id(retry=False)` (`pysharkbite.cpp:73-74`) | `InstanceInfo.ID` via `Instance.Info()` / `Connector.Instance()` (`accumulo/instance.go:16`, `accumulo/connector.go:124`) | — | `TestNewZooKeeperInstanceResolvesAndClosesOnce` (`accumulo/instance_test.go:22`) | Missing C ABI | Shoal has no `retry` parameter; ZooKeeper resolution happens once at construction. Shim should accept and ignore `retry` only after approval, otherwise re-resolve. |
| SB-CFG-011 | Static/standalone instance (no direct Sharkbite equivalent) | `accumulo.NewStaticInstance(name, id)` (`accumulo/instance.go:153`) | `SHOAL_BOOTSTRAP_STATIC` (`capi/include/shoal_types.h:50`) | `TestNewStaticInstance` (`accumulo/instance_test.go:12`); `capi/tests/lifecycle.c:54-57` | Not required (rationale required) | Rationale: Shoal-only capability with no Sharkbite counterpart. Listed so the matrix is complete in both directions; it adds no compatibility obligation. |
| SB-CFG-012 | `AuthInfo(username, password, instanceId)` (`pysharkbite.cpp:76-77`) | `accumulo.PasswordCredentials(principal string, password []byte)` (`accumulo/credentials.go:21`) | `shoal_connector_config.principal` / `password` / `password_length` (`capi/include/shoal_types.h:122-124`) | `TestPasswordCredentialsCopiesAndRedacts` (`accumulo/credentials_test.go:11`); `capi/tests/lifecycle.c:54-57` | Behavior mismatch | Sharkbite binds the instance ID into the credential object; Shoal binds it at `NewConnector`. Sharkbite takes `str` passwords, Shoal takes `[]byte` — the shim must accept `str` and `bytes` and must not transcode `bytes`. |
| SB-CFG-013 | `AuthInfo.getUserName()` / alias `username()` (`pysharkbite.cpp:78-79`) | `Credentials.Principal()` (`accumulo/credentials.go:34`), `Connector.Principal()` (`accumulo/connector.go:131`) | — | `TestPasswordCredentialsCopiesAndRedacts` (`accumulo/credentials_test.go:11`) | Missing C ABI | |
| SB-CFG-014 | `AuthInfo.getPassword()` / alias `password()` (`pysharkbite.cpp:80-81`) | — (deliberately unreadable; `Credentials.String()`/`GoString()` redact, `accumulo/credentials.go:37,42`) | — | `TestPasswordCredentialsCopiesAndRedacts` (`accumulo/credentials_test.go:11`) | Intentional divergence (approval required) | Sharkbite hands the plaintext password back to the caller. Shoal deliberately redacts. Reproducing a password getter re-introduces a credential-leak path (it lands in logs via `repr`). Proposal: raise `AttributeError` with a migration message, or return the value only when the caller passed it in-process. Requires approval. |
| SB-CFG-015 | `AuthInfo.getInstanceId()` / alias `instance_id()` (`pysharkbite.cpp:82-83`) | `Connector.Instance().ID` (`accumulo/connector.go:124`) | — | `TestNewConnectorLifecycle` (`accumulo/connector_test.go:9`) | Missing C ABI | |
| SB-CFG-016 | `AuthInfo` copy assignment silently drops the password (`include/data/constructs/security/AuthInfo.h:60-67`) | `Credentials.clone()` copies all fields (`accumulo/credentials.go:44`) | n/a | `TestPasswordCredentialsCopiesAndRedacts` (`accumulo/credentials_test.go:11`) | Not required (rationale required) | Rationale: this is an upstream defect, not a contract. See [§21](#sec-21). Do not reproduce. |
| SB-CFG-017 | ZooKeeper session timeout is the only timeout in the Sharkbite public API (`include/data/constructs/client/zookeeperinstance.h:89-104`) | `ZooKeeperConfig.SessionTimeout`, `ConnectorOptions.DialTimeout`, `IdleTimeout` (`accumulo/config.go:22-42`) | `zookeeper_session_timeout_ms`, `bootstrap_timeout_ms`, `dial_timeout_ms` (`capi/include/shoal_types.h:126-129`) | `TestNewZooKeeperInstanceResolvesAndClosesOnce` (`accumulo/instance_test.go:19`); `cmd/shoal-capi/export.go:203-252` | Missing C ABI | Go proves the default `SessionTimeout`; the ABI exposes the three timeout knobs, but no named C ABI test yet proves the `zookeeper_session_timeout_ms` mapping/default end to end. |
| SB-CFG-018 | Instance secret (not exposed in Sharkbite Python) | `ZooKeeperConfig.InstanceSecret` (`accumulo/config.go:26`) | `shoal_connector_config.instance_secret` (`capi/include/shoal_types.h:128`) | `TestNewZooKeeperInstanceValidatesConfig` (`accumulo/instance_test.go:54`) | Not required (rationale required) | Rationale: Shoal-only; no Sharkbite obligation. |

<a id="sec-7"></a>

## 7. Matrix: connector and session (`SB-CONN`)

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-CONN-001 | `AccumuloConnector(AuthInfo, Instance)` (`pysharkbite.cpp:259`) | `accumulo.NewConnector(instance, credentials, ConnectorOptions)` (`accumulo/connector.go:36`) | `shoal_connector_create` (`capi/include/shoal.h:30-33`) | `TestNewConnectorLifecycle` (`accumulo/connector_test.go:9`); `capi/tests/lifecycle.c:54-57` | Covered | Argument order and types differ; the capability, lifetime, and failure modes match. Shim must construct `ConnectorOptions` from the `Configuration` bag ([SB-CFG-001](#sec-6)). |
| SB-CONN-002 | `AccumuloConnector(instance, zookeepers, username, password)` convenience ctor (`pysharkbite.cpp:260-265`) | Compose `NewZooKeeperInstance` + `PasswordCredentials` + `NewConnector` | Compose `shoal_connector_config` with `SHOAL_BOOTSTRAP_ZOOKEEPER` | `TestNewConnectorLifecycle` (`accumulo/connector_test.go:9`); `capi/tests/lifecycle.c:54-57` | Covered | The pinned ctor hardcodes a 1000 ms ZK timeout (`pysharkbite.cpp:262`); the shim must reproduce that default, not Shoal's 30 s default (`accumulo/instance.go:13`). |
| SB-CONN-003 | `AccumuloConnector.tableOps(table) -> AccumuloTableOperations` (`pysharkbite.cpp:269`) | No handle type; ops are `Connector` methods taking a table name (`accumulo/table_admin.go:76`) | No handle type; scanner/writer configs take `table_name`/`table_id` (`capi/include/shoal_types.h:143-144,202-203`) | `TestTableAdministrationListingAndExistence` (`accumulo/table_admin_test.go:13`) | Behavior mismatch | Sharkbite binds a table to a stateful handle whose methods take no table argument. Shim must provide the handle object and hold the table name. |
| SB-CONN-004 | `AccumuloConnector.securityOps() -> SecurityOperations` (`pysharkbite.cpp:266`) | — | — | — | Missing Go | Entire security surface absent. See [§13](#sec-13). |
| SB-CONN-005 | `AccumuloConnector.namespaceOps() -> AccumuloNamespaceOperations` (`pysharkbite.cpp:267`) | — | — | — | Missing Go | Entire namespace surface absent. See [§12](#sec-12). `internal/managerclient` defines `ErrorNamespaceExists`/`ErrorNamespaceNotFound` kinds but exposes no namespace operation. |
| SB-CONN-006 | `AccumuloConnector.tableInfo() -> AccumuloTableInfo` (`pysharkbite.cpp:270`) | `Connector.Tables`, `TableByName`, `TableByID` (`accumulo/table_admin.go:29`, `accumulo/discovery.go:65,87`) | — | `TestTableAdministrationListingAndExistence` (`accumulo/table_admin_test.go:13`); `TestDiscoveryTableLookupAndRouting` (`accumulo/discovery_test.go:108`) | Missing C ABI | Capability exists in Go; no ABI entry point. Blocked behind issue [#82](https://github.com/phrocker/shoal-oss/issues/82) / PR [#84](https://github.com/phrocker/shoal-oss/pull/84). |
| SB-CONN-007 | `AccumuloConnector.getStatistics() -> AccumuloInfo` (`pysharkbite.cpp:268`) | — | — | — | Missing Go | Cluster status RPC absent. See [§14](#sec-14). |
| SB-CONN-008 | Connector has no explicit `close()` in Python; lifetime ends with refcount | `Connector.Close() error` (`accumulo/connector.go:138`) | `shoal_connector_close` / `shoal_connector_free` (`capi/include/shoal.h:39-47`) | `TestNewConnectorLifecycle` (`accumulo/connector_test.go:9`); `capi/tests/lifecycle.c:257-264` | Behavior mismatch | Shoal requires explicit close for deterministic shutdown. Shim must close on `__del__` **and** support `with`; it must not rely on GC ordering at interpreter shutdown. |
| SB-CONN-009 | Copy-assigning a connector shares pooled transports (`include/interconnect/Accumulo.h:126`) | `Connector` is a pointer type; copying the pointer shares state | Handles must not be copied (`capi/README.md`, "Do not copy opaque handles") | `TestNewConnectorLifecycle` (`accumulo/connector_test.go:9`); `TestConnectorRegistryLifecycle` (`cmd/shoal-capi/state_test.go:13`) | Covered | Both share state on copy; Shoal documents the rule and enforces handle validity. |
| SB-CONN-010 | Process-wide singleton transport pool `ACCUMULO_COORDINATOR` (`include/interconnect/Accumulo.h:50`) | Per-connector pool (`internal/transportpool.Pool`, wired in `accumulo/connector.go:36`) | Per-connector, implicit | `TestNewConnectorCanDisablePooling` (`accumulo/connector_test.go:51`) | Intentional divergence (approval required) | Shoal scopes pooling per connector; Sharkbite shares one process-wide pool. Observable difference: connection count and failure blast radius. Requires approval as a divergence. |
| SB-CONN-011 | Process-wide table name/ID cache `Tables::getInstance()` (`include/interconnect/tableOps/TableOperations.h:30,40`) | Per-connector resolver `internal/tablenames.Resolver`; invalidation via `Connector.InvalidateTable` / `InvalidateDiscovery` (`accumulo/discovery.go:170,183`) | — | `TestDiscoveryInvalidationAndDefensiveCopies` (`accumulo/discovery_test.go:148`) | Missing C ABI | Per-connector scoping is safer and should be approved as part of SB-CONN-010. |
| SB-CONN-012 | Reconnect with bad credentials raises `ClientException` (`test/python/TestBadOperations.py:85-94`, `test/python/TestSecurityOperations.py:110-116`) | `NewConnector` error path (`accumulo/connector.go:36`), `ErrPermissionDenied` (`accumulo/errors.go:32`) | `SHOAL_STATUS_PERMISSION_DENIED` (`capi/include/shoal_types.h:34`) | — | Behavior mismatch | No Shoal test asserts an authentication failure against a live or faked security-exception path. This row cannot close without a live-cluster conformance test ([§24](#sec-24)). |
| SB-CONN-013 | No connection-level cancellation or deadline in the Python API | Every `Connector` method takes `context.Context` (`accumulo/connector.go`, `accumulo/discovery.go`, `accumulo/table_admin.go`) | Per-call `timeout_ms` on scan/write only (`capi/include/shoal.h:90,161,167,177`) | `TestDiscoveryErrorsAndCancellation` (`accumulo/discovery_test.go:212`); `TestOwnedScannerDeadline` (`cmd/shoal-capi/state_test.go:89`) | Behavior mismatch | Shoal is strictly better, but the ABI exposes no cancellation handle (only deadlines), so a Python `KeyboardInterrupt` cannot interrupt an in-flight scan. See [SB-XCUT-008](#sec-20). |
| SB-CONN-014 | High-level `AccumuloBase(instance, zookeepers, username, password, table=None, auths=None)` (`sharkbite/__init__.py:24-36`) | Compose `NewZooKeeperInstance`, `PasswordCredentials`, and `NewConnector` | Compose `shoal_connector_create` plus later scanner/writer/table wrappers | `sharkbite/__init__.py:24-36`; `TestNewConnectorLifecycle` (`accumulo/connector_test.go:9`); `capi/tests/lifecycle.c:54-75` | Missing C ABI | Public convenience layer absent. The pinned helper hardcodes a 1000 ms ZooKeeper timeout (`sharkbite/__init__.py:30`) and eagerly binds `tableOps` when `table` is not `None`. |

<a id="sec-8"></a>

## 8. Matrix: data model (`SB-DATA`)

### 8.1 `Value`

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-DATA-001 | `Value()` (`pysharkbite.cpp:273`) | `KeyValue.Value []byte` (`accumulo/scanner.go:31-34`) | `shoal_key_value_view.value` (`capi/include/shoal_types.h:187`) | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/result_bridge.c:25-38` | Behavior mismatch | Sharkbite has a mutable, shared, refcounted `Value` object; Shoal has a plain byte slice. Shim must wrap bytes in a `Value` class. |
| SB-DATA-002 | `Value.get() -> str` (`pysharkbite.cpp:274`) | `KeyValue.Value []byte` | `shoal_key_value_view.value` | `capi/tests/result_bridge.c:25-38` | Behavior mismatch | **Lossy.** `getValueAsString` transcodes to a Python `str`; non-UTF-8 values raise `UnicodeDecodeError`. Shoal is byte-exact. See [§21](#sec-21) for the shim rule (`errors='surrogateescape'`). |
| SB-DATA-003 | `Value.get_bytes() -> bytes` (`pysharkbite.cpp:275-279`) | `KeyValue.Value []byte` | `shoal_key_value_view.value` (borrowed until `shoal_scan_result_free`) | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/result_bridge.c:25-38,40-43` | Covered | Binary-safe path exists on both layers, including empty and NUL-containing values (`result_bridge.c:21-23`). |
| SB-DATA-004 | `Value.__repr__` / `__str__` return `""`/`"[]"` for a null holder (`pysharkbite.cpp:280-295`) | — | — | — | Behavior mismatch | Shim must reproduce the exact null-holder strings; Python code in the wild prints values directly (`test/python/TestWithFx.py:69`). |

### 8.2 `Key`

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-DATA-005 | `Key()` (`pysharkbite.cpp:298`) | `accumulo.Key{}` (`accumulo/scanner.go:22-29`) | `shoal_key` (`capi/include/shoal_types.h:87-93`) | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`); `capi/tests/lifecycle.c:98-101` | Covered | Default-constructed key with empty components exists on both layers. |
| SB-DATA-006 | `Key(row, cf=None, cq=None, cv=None, timestamp=9223372036854775807)` (`pysharkbite.cpp:299-300`) | `accumulo.Key{Row, ColumnFamily, ColumnQualifier, ColumnVisibility, Timestamp}` | `shoal_key` fields | `TestKeyRangeUsesAccumuloTimestampOrdering` (`accumulo/range_test.go:61`) | Behavior mismatch | Sharkbite's default timestamp is `INT64_MAX`; Shoal's zero value is `0`. Shim must inject `INT64_MAX` when unset or key ordering changes. Parameters are `const char *`, so embedded NULs truncate — see [§21](#sec-21). |
| SB-DATA-007 | `Key.setRow(row="")` (`pysharkbite.cpp:301`) | Field assignment | `shoal_key.row` | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`) | Behavior mismatch | Setter mutates in place; Shoal's `Key` is a value struct. Shim must keep mutability (`test/python/TestWrites.py:59-65` builds keys by mutation). |
| SB-DATA-008 | `Key.setColumnFamily(cf="")` (`pysharkbite.cpp:317-318`) | Field assignment | `shoal_key.column_family` | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`) | Behavior mismatch | As above. |
| SB-DATA-009 | `Key.setColumnQualifier(cq="")` (`pysharkbite.cpp:319-320`) | Field assignment | `shoal_key.column_qualifier` | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`) | Behavior mismatch | As above. No `setColumnVisibility` or `setTimestamp` exists in the pinned binding — a genuine Sharkbite gap the shim need not reproduce. |
| SB-DATA-010 | `Key.getRow() -> str` (`pysharkbite.cpp:321`) | `Key.Row []byte` | `shoal_key_value_view.row` | `TestBatchScannerSupportsUnboundedAndMultipleRanges` (`accumulo/scanner_test.go:517`); `capi/tests/result_bridge.c:25-38` | Behavior mismatch | Binary-unsafe: returns `str`. Sharkbite has **no** bytes accessor for key components, so binary rows are unreachable from Python. Shim must add `get_row_bytes()` and keep `getRow()` for compatibility. |
| SB-DATA-011 | `Key.getColumnFamily() -> str` (`pysharkbite.cpp:322`) | `Key.ColumnFamily []byte` | `shoal_key_value_view.column_family` | `capi/tests/result_bridge.c:25-38` | Behavior mismatch | As above. |
| SB-DATA-012 | `Key.getColumnQualifier() -> str` (`pysharkbite.cpp:325`) | `Key.ColumnQualifier []byte` | `shoal_key_value_view.column_qualifier` | `capi/tests/result_bridge.c:25-38` | Behavior mismatch | As above. |
| SB-DATA-013 | `Key.getColumnVisibility() -> str` (`pysharkbite.cpp:323`) | `Key.ColumnVisibility []byte` | `shoal_key_value_view.column_visibility` | `capi/tests/result_bridge.c:25-38` | Behavior mismatch | As above. |
| SB-DATA-014 | `Key.getTimestamp() -> int64` (`pysharkbite.cpp:324`) | `Key.Timestamp int64` | `shoal_key_value_view.timestamp` | `TestKeyRangeUsesAccumuloTimestampOrdering` (`accumulo/range_test.go:61`); `capi/tests/result_bridge.c:40-43` (negative timestamp round trip) | Covered | Signed 64-bit on all layers, including negative values. |
| SB-DATA-015 | `Key.__str__` / `__repr__` -> `toString()`, `" : []"` when null (`pysharkbite.cpp:302-316`) | — | — | — | Behavior mismatch | Format is user-visible (`test/python/TestWithFx.py:69` prints `%r`). Shim must reproduce Sharkbite's exact `Key::toString()` format. |

### 8.3 `KeyValue`

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-DATA-016 | `KeyValue()` (`pysharkbite.cpp:329`) | `accumulo.KeyValue{}` (`accumulo/scanner.go:31`) | `shoal_key_value_view` | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/result_bridge.c:14-16` | Covered | |
| SB-DATA-017 | `KeyValue(Key, Value)` (`pysharkbite.cpp:330`) | `accumulo.KeyValue{Key: ..., Value: ...}` | `shoal_bridge_scan_result_set` (internal) | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/result_bridge.c:17-23` | Covered | Used by RFile writes (`test/python/TestRFileWrites.py:38-40`) and by Python iterators returning `KeyValue`. |
| SB-DATA-018 | `KeyValue.getKey() -> Key` (`pysharkbite.cpp:331`) | `KeyValue.Key` | `shoal_key_value_view` fields | `TestBatchScannerSupportsUnboundedAndMultipleRanges` (`accumulo/scanner_test.go:517`); `capi/tests/result_bridge.c:25-38` | Covered | |
| SB-DATA-019 | `KeyValue.getValue() -> Value` (`pysharkbite.cpp:332`) | `KeyValue.Value` | `shoal_key_value_view.value` | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/result_bridge.c:25-38` | Covered | |

### 8.4 `Range`

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-DATA-020 | `Range()` — infinite range (`pysharkbite.cpp:335`) | `accumulo.InfiniteRange()` (`accumulo/range.go:63`) | `SHOAL_RANGE_BOUND_UNBOUNDED` on both bounds (`capi/include/shoal_types.h:98`) | `TestBatchScannerSupportsUnboundedAndMultipleRanges` (`accumulo/scanner_test.go:517`); `capi/tests/lifecycle.c:98-101` | Covered | Used by `test/python/TestRFileWrites.py:56`. |
| SB-DATA-021 | `Range(row)` — single row (`pysharkbite.cpp:336`) | `accumulo.NewRangeRow(row []byte)` (`accumulo/range.go:55`) | `SHOAL_RANGE_BOUND_ROW` both sides (`capi/include/shoal_types.h:99`) | `TestRowRangeInclusiveEndStillIncludesWholeRow` (`accumulo/range_test.go:72`); `capi/tests/lifecycle.c:98-101` | Covered | Semantics match: the whole row is included. |
| SB-DATA-022 | `Range(startKey, startInclusive, endKey, endInclusive, update=False)` (`pysharkbite.cpp:337-338`) | `accumulo.NewKeyRange(start *Key, startInclusive bool, end *Key, endInclusive bool)` (`accumulo/range.go:40`) | `SHOAL_RANGE_BOUND_KEY` (`capi/include/shoal_types.h:100`) | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`); `TestRangeWireBoundaries` (`accumulo/scanner_test.go:194`) | Behavior mismatch | Shoal has no `update` parameter. In Sharkbite `update=True` rewrites the supplied keys in place. Shim must reject or emulate `update=True`; silently ignoring it changes caller-visible key state. |
| SB-DATA-023 | `Range(key, inclusive)` — 2-arg form (`pysharkbite.cpp:339`) | Compose `NewKeyRange(key, inclusive, nil, false)` | `shoal_range` with one `UNBOUNDED` bound | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`); `capi/tests/lifecycle.c:98-101` | Covered | |
| SB-DATA-024 | `Range(startRow, startInclusive, endRow, endInclusive, update=False)` (`pysharkbite.cpp:340-341`) | `accumulo.NewRange(startRow []byte, startInclusive bool, endRow []byte, endInclusive bool)` (`accumulo/range.go:22`) | `SHOAL_RANGE_BOUND_ROW` (`capi/include/shoal_types.h:99`) | `TestRangeWireBoundaries` (`accumulo/scanner_test.go:194`); `TestBatchScannerValidatesRanges` (`accumulo/scanner_test.go:855`) | Behavior mismatch | Same `update` gap as SB-DATA-022. Also: Sharkbite accepts `""` as a bound meaning "unbounded" (`test/python/TestRanges.py:120`), and `None` for key bounds (`test/python/TestRanges.py:147`); Shoal distinguishes empty from unbounded (`TestDiscoveryPreservesEmptyRowBoundaries`, `accumulo/discovery_test.go:185`). The shim must translate `""`/`None` to `SHOAL_RANGE_BOUND_UNBOUNDED` to preserve pinned test behavior. |
| SB-DATA-025 | `Range.get_start_key()` (`pysharkbite.cpp:342`) | `Range.StartKey()` (`accumulo/range.go:91`) | — | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`) | Missing C ABI | The ABI accepts ranges but exposes no range accessors. |
| SB-DATA-026 | `Range.get_stop_key()` (`pysharkbite.cpp:343`) | `Range.EndKey()` (`accumulo/range.go:100`) | — | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`) | Missing C ABI | Note the Sharkbite spelling is `stop`, Shoal's is `End`. |
| SB-DATA-027 | `Range.start_key_inclusive()` (`pysharkbite.cpp:344`) | `Range.StartInclusive()` (`accumulo/range.go:108`) | — | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`) | Missing C ABI | |
| SB-DATA-028 | `Range.stop_key_inclusive()` (`pysharkbite.cpp:345`) | `Range.EndInclusive()` (`accumulo/range.go:113`) | — | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`) | Missing C ABI | |
| SB-DATA-029 | `Range.inifinite_start_key()` — note the upstream typo (`pysharkbite.cpp:346`) | `Range.StartRow()` returns nil for unbounded (`accumulo/range.go:68`) | — | `TestBatchScannerSupportsUnboundedAndMultipleRanges` (`accumulo/scanner_test.go:517`) | Missing C ABI | Shim must keep the misspelled name (it is the published API) and may add a corrected alias. |
| SB-DATA-030 | `Range.inifinite_stop_key()` — upstream typo (`pysharkbite.cpp:347`) | `Range.EndRow()` (`accumulo/range.go:79`) | — | `TestBatchScannerSupportsUnboundedAndMultipleRanges` (`accumulo/scanner_test.go:517`) | Missing C ABI | |
| SB-DATA-031 | `Range.after_end_key(key)` (`pysharkbite.cpp:348`) | — (unexported `fitsTablet`, `compareKeys`; `accumulo/range.go:129,197`) | — | — | Missing Go | No exported range/key predicate. `internal/iterrt.Range` has `AfterEnd`/`BeforeStart` but is not public. |
| SB-DATA-032 | `Range.before_start_key(key)` (`pysharkbite.cpp:349`) | — | — | — | Missing Go | As above. |
| SB-DATA-033 | `Range.__str__` / `__repr__` via `operator<<` (`pysharkbite.cpp:350-359`) | — | — | — | Missing Go | Format is user-visible. |
| SB-DATA-034 | Reversed/invalid ranges | `NewKeyRange` rejects reversed bounds (`accumulo/range.go:40`) | Validated during scan (`capi/include/shoal.h:90`) | `TestKeyRangeRejectsReversedFields` (`accumulo/range_test.go:86`); `TestBatchScannerValidatesRanges` (`accumulo/scanner_test.go:855`) | Behavior mismatch | Sharkbite constructs reversed ranges without complaint and returns no rows. Shoal errors at construction. Divergence is safer but observable; shim must decide and record. |

### 8.5 `Mutation`

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-DATA-035 | `Mutation(row: str)` (`pysharkbite.cpp:362`) | `accumulo.NewMutation(row []byte) (*Mutation, error)` (`accumulo/mutation.go:21`) | `shoal_mutation_create(shoal_bytes row, ...)` (`capi/include/shoal.h:115-117`) | `TestMutationPublicModel` (`accumulo/mutation_test.go:8`); `capi/tests/lifecycle.c:144-171` | Behavior mismatch | Shoal rejects an empty row (`accumulo/mutation.go:22`, `TestNewMutationRejectsEmptyRow`, `accumulo/mutation_test.go:27`); Sharkbite accepts it. Shim must map the new failure to `ClientException`, not `ValueError`. |
| SB-DATA-036 | `Mutation.put(cf="", cq="", cv="", timestamp=0, value="")` (`pysharkbite.cpp:363-364`) | `Mutation.Put(cf, cq, cv []byte, ts int64, value []byte)` (`accumulo/mutation.go:43`) | `shoal_mutation_put` (`capi/include/shoal.h:119-123`) | `TestBatchWriterRoutesBatchesAndCopiesMutation` (`accumulo/batch_writer_test.go:194`); `capi/tests/lifecycle.c:144-171` | Behavior mismatch | Timestamp default differs in meaning: Sharkbite writes literal `0`, Shoal has `MutationLatestTimestamp` (`accumulo/mutation.go:11`) for server assignment. The shim must write literal `0` to preserve pinned behavior (`test/python/TestWrites.py:31-36` writes `1569786960`), and must not silently promote `0` to "latest". |
| SB-DATA-037 | `Mutation.put(cf, cq, cv, timestamp)` 4-arg overload (`pysharkbite.cpp:365-366`) | `Mutation.Put(..., value=nil)` | `shoal_mutation_put` with empty value | `TestBatchWriterRoutesBatchesAndCopiesMutation` (`accumulo/batch_writer_test.go:194`); `capi/tests/lifecycle.c:144-171` | Covered | Empty value is explicitly exercised (`test/python/TestWrites.py:36`). |
| SB-DATA-038 | `Mutation.put(cf, cq, cv)` 3-arg overload (`pysharkbite.cpp:367-368`) | `Mutation.Put(cf, cq, cv, 0, nil)` | `shoal_mutation_put` | `TestMutationPublicModel` (`accumulo/mutation_test.go:8`); `capi/tests/lifecycle.c:144-171` | Covered | |
| SB-DATA-039 | `Mutation.put(cf, cq)` 2-arg overload (`pysharkbite.cpp:369-370`) | `Mutation.Put(cf, cq, nil, 0, nil)` | `shoal_mutation_put` | `TestMutationPublicModel` (`accumulo/mutation_test.go:8`); `capi/tests/lifecycle.c:144-171` | Covered | Used with keywords by `examples/lambdaexample.py` and `sharkbite/__init__.py:87-95`. |
| SB-DATA-040 | Keyword-argument call form `put(cf=..., cq=..., cv=..., timestamp=..., value=...)` (`sharkbite/__init__.py:87-95`) | n/a (Go is positional) | n/a | — | Missing C ABI | Binding-layer obligation: the shim must accept exactly these keyword names. |
| SB-DATA-041 | `Mutation.putDelete(cf="", cq="", cv="", timestamp=0)` (`pysharkbite.cpp:371-372`) | `Mutation.Delete(cf, cq, cv []byte, ts int64)` (`accumulo/mutation.go:59`) | `shoal_mutation_delete` (`capi/include/shoal.h:131-135`) | `TestMutationPublicModel` (`accumulo/mutation_test.go:8`); `capi/tests/lifecycle.c:144-171` | Covered | |
| SB-DATA-042 | `Mutation.putDelete(cf, cq, cv)` 3-arg overload (`pysharkbite.cpp:373-374`) | `Mutation.DeleteLatest(cf, cq, cv)` (`accumulo/mutation.go:67`) | `shoal_mutation_delete_latest` (`capi/include/shoal.h:137-142`) | `capi/tests/lifecycle.c:144-171` (size reaches 2 after put+delete_latest) | Behavior mismatch | Sharkbite's 3-arg overload writes timestamp `0`; Shoal's `DeleteLatest` requests a **server-assigned** timestamp. Not equivalent. The shim must call `Delete(..., 0)`. |
| SB-DATA-043 | Server-assigned timestamps | `Mutation.PutLatest` (`accumulo/mutation.go:52`), `MutationLatestTimestamp` (`accumulo/mutation.go:11`) | `shoal_mutation_put_latest` (`capi/include/shoal.h:125-129`) | `capi/tests/lifecycle.c:144-171,173-176` | Not required (rationale required) | Rationale: no Sharkbite Python equivalent; Shoal superset. Exposing it in the shim is optional and additive. |
| SB-DATA-044 | Mutation has no size accessor in Python | `Mutation.Size() int` (`accumulo/mutation.go:38`) | `shoal_mutation_size` (`capi/include/shoal.h:144-146`) | `TestMutationPublicModel` (`accumulo/mutation_test.go:8`); `capi/tests/lifecycle.c:144-171` | Not required (rationale required) | Rationale: Shoal superset; `BatchWriter.size()` is the Sharkbite analogue ([SB-WRITE-008](#sec-10)). |
| SB-DATA-045 | Mutation row accessor | `Mutation.Row() []byte` returns a copy (`accumulo/mutation.go:33`) | — | `TestMutationPublicModel` (`accumulo/mutation_test.go:8`) | Not required (rationale required) | Rationale: Shoal superset. |
| SB-DATA-046 | Mutation reuse after `addMutation` (Sharkbite enqueues a `shared_ptr`; the caller must not mutate afterwards) | `BatchWriter.Add` encodes and copies (`accumulo/batch_writer.go:268`) | "BatchWriter add snapshots the mutation, so callers may immediately reuse or free it" (`capi/README.md`) | `TestBatchWriterRoutesBatchesAndCopiesMutation` (`accumulo/batch_writer_test.go:194`); `capi/tests/lifecycle.c:191-199` | Covered | Shoal's snapshot semantics are strictly safer and preserve every pinned usage. |

### 8.6 `Authorizations`

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-DATA-047 | `Authorizations()` (`pysharkbite.cpp:454`) | `ScannerOptions.Authorizations [][]byte` (`accumulo/scanner.go:103`) | `shoal_scanner_config.authorizations` + `authorization_count` (`capi/include/shoal_types.h:146-147`) | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`); `capi/tests/lifecycle.c:84-89` | Behavior mismatch | Sharkbite has an `Authorizations` **object** with identity and validation; Shoal has a slice on the scanner config. Shim must provide the class. |
| SB-DATA-048 | `Authorizations(list[str])` (`pysharkbite.cpp:455`) | `[][]byte` | as above | `capi/tests/lifecycle.c:84-89` | Behavior mismatch | |
| SB-DATA-049 | `Authorizations(str)` single-auth convenience (`pysharkbite.cpp:456-460`) | `[][]byte{[]byte(s)}` | as above | — | Behavior mismatch | |
| SB-DATA-050 | `Authorizations.addAuthorization(auth)` (`pysharkbite.cpp:461`) | — (slice append by the caller) | — | `test/python/TestAuthorizations.py:23-24` | Behavior mismatch | Sharkbite validates characters and raises `ClientException("Invalid authorization character")` (`include/data/constructs/security/Authorizations.h:88`). Shoal performs **no** validation. Shim must validate to preserve the contract. |
| SB-DATA-051 | `Authorizations.contains(auth) -> bool` (`pysharkbite.cpp:462`) | — | — | — | Missing Go | |
| SB-DATA-052 | `Authorizations.get_authorizations() -> iterable[str]` (`pysharkbite.cpp:463`) | — | — | `examples/pythonexample.py` | Missing Go | Also reachable through `SecurityOperations.get_auths` ([SB-SEC-004](#sec-13)). |
| SB-DATA-053 | `Authorizations.__str__` / `__repr__` (`pysharkbite.cpp:464-484`) | — | — | — | Not required (rationale required) | Rationale: the pinned implementation dereferences `vec.end()-1` and `vec.back()` on an **empty** vector, so `str(Authorizations())` is undefined behavior. Do not reproduce; emit `[ ]`. See [§21](#sec-21). |
| SB-DATA-054 | Empty `Authorizations()` accepted by scanners and writers (`test/python/TestWrites.py:23,29`) | `ScannerOptions.Authorizations` nil | `authorization_count = 0` | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/lifecycle.c:84-89` | Covered | Empty auths must remain legal on both layers. |

### 8.7 Iterator configuration objects

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-DATA-055 | `IterInfo(name, class, priority)` (`pysharkbite.cpp:86`) | `accumulo.NewIteratorSetting(name, className string, priority int32, options map[string]string)` (`accumulo/scanner.go:71`) | `shoal_iterator_setting` (`capi/include/shoal_types.h:79-85`) | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`) | Behavior mismatch | Shoal requires an options map argument and validates name/class/priority (`accumulo/scanner.go:394`); Sharkbite has no options on `IterInfo`. Shim must default options to `{}`. |
| SB-DATA-056 | `IterInfo(script, iteratorName, priority, type="Python")` (`pysharkbite.cpp:87-88`) | — | — | — | Missing Go | Script-carrying iterator; requires server-side Python iterator support ([SB-SCAN-016](#sec-9)). |
| SB-DATA-057 | `IterInfo.getPriority()` / alias `priority()` (`pysharkbite.cpp:89-90`) | `IteratorSetting.Priority()` (`accumulo/scanner.go:95`) | `shoal_iterator_setting.priority` | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`) | Missing C ABI | Accessor not exposed by the ABI (write-only config struct). |
| SB-DATA-058 | `IterInfo.getName()` / alias `name()` (`pysharkbite.cpp:91-92`) | `IteratorSetting.Name()` (`accumulo/scanner.go:89`) | `shoal_iterator_setting.name` | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`) | Missing C ABI | |
| SB-DATA-059 | `IterInfo.getClass()` / alias `class()` (`pysharkbite.cpp:93-94`) | `IteratorSetting.ClassName()` (`accumulo/scanner.go:92`) | `shoal_iterator_setting.class_name` | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`) | Missing C ABI | The `class` alias is unreachable via attribute syntax in Python (reserved word); only `getattr(obj, "class")()` works. Keep both, document the wart. |
| SB-DATA-060 | Iterator options map | `IteratorSetting.Options()` returns a defensive copy (`accumulo/scanner.go:98`) | `shoal_iterator_option` (`capi/include/shoal_types.h:74-77`) | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`) | Not required (rationale required) | Rationale: Shoal superset; Sharkbite `IterInfo` carries no options. |
| SB-DATA-061 | `PythonIterator(name, script, priority)` (`pysharkbite.cpp:97`) | — | — | — | Missing Go | See [SB-SCAN-016](#sec-9). |
| SB-DATA-062 | `PythonIterator(name, priority)` (`pysharkbite.cpp:98`) | — | — | — | Missing Go | Two-argument form used by `test/python/TestIterator.py:63` and `examples/lambdaexample.py`. |
| SB-DATA-063 | `PythonIterator.onNext(lambda_source) -> self` (`pysharkbite.cpp:103`) | — | — | — | Missing Go | Takes a **Python lambda as a source string**, evaluated server-side. Chainable (returns the iterator). |
| SB-DATA-064 | `PythonIterator.getPriority()` / `priority()` / `getName()` / `name()` / `getClass()` (`pysharkbite.cpp:99-104`) | — | — | — | Missing Go | |

<a id="sec-9"></a>

## 9. Matrix: scanners and result iteration (`SB-SCAN`)

Sharkbite exposes exactly one scanner type to Python — `scanners::BatchScanner`
bound as `BatchScanner` (`pysharkbite.cpp:434`) — returned by
`AccumuloTableOperations.createScanner(auths, threads)`, which calls
`createSharedScanner` (`include/interconnect/python/PythonStructures.h:172-175`).

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-SCAN-001 | `tableOps.createScanner(auths, threads) -> BatchScanner` (`pysharkbite.cpp:248`, `PythonStructures.h:172`) | `Connector.NewScanner(table, ScannerOptions)` (`accumulo/scanner.go:136`), `Connector.NewBatchScanner` (`accumulo/batch_scanner.go:47`) | `shoal_connector_create_scanner`, `shoal_connector_create_batch_scanner` (`capi/include/shoal.h:54-64`) | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `TestBatchScannerSplitsRangeAcrossTablets` (`accumulo/scanner_test.go:456`); `capi/tests/lifecycle.c:66-75` | Behavior mismatch | Sharkbite's `threads` argument becomes Shoal's `ScannerOptions.Parallelism` (`accumulo/scanner.go:107`). Sharkbite creates a scanner without a range and configures it afterwards; Shoal freezes options at construction. Shim must defer construction to first `getResultSet()`. Passing `None` auths must raise `ClientException` (`test/python/TestBadOperations.py:75-79`). |
| SB-SCAN-002 | `BatchScanner.addRange(Range)` (`pysharkbite.cpp:451`) | `Scanner.Scan(ctx, *Range)` / `BatchScanner.Scan(ctx, []*Range)` (`accumulo/scanner.go:168`, `accumulo/batch_scanner.go:58`) | `shoal_scanner_scan(range)`, `shoal_batch_scanner_scan(ranges, count)` (`capi/include/shoal.h:89-98`) | `TestBatchScannerSupportsUnboundedAndMultipleRanges` (`accumulo/scanner_test.go:517`) | Behavior mismatch | Sharkbite accumulates ranges on the scanner; Shoal passes them per call. Also: calling `addRange` twice and re-reading `getResultSet()` in the pinned tests **replaces** the result set (`test/python/TestRanges.py:64-86`); the shim must reproduce accumulate-then-scan semantics exactly, including the "second `getResultSet()` re-executes" behavior. |
| SB-SCAN-003 | `BatchScanner.withRange(Range) -> self` fluent form (`pysharkbite.cpp:444-446`) | — (compose in Go) | — | `test/python/TestWithFx.py:50,63` | Missing C ABI | Pure binding-layer sugar over SB-SCAN-002; must return `self`. |
| SB-SCAN-004 | `BatchScanner.getResultSet() -> Results` (`pysharkbite.cpp:435`) | `Scanner.Scan(ctx, r) ([]KeyValue, error)` (`accumulo/scanner.go:168`) | `shoal_scanner_scan(..., shoal_scan_result **out)` (`capi/include/shoal.h:89-92`) | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/lifecycle.c:103-106` | Behavior mismatch | **Structural difference.** Sharkbite streams (`Results` pulls batches lazily, `include/scanner/constructs/Results.h:93,115`); Shoal materializes the entire result into a slice, and the ABI buffers the whole result before returning (`shoal_bridge_scan_result_alloc`). Large scans that stream today will OOM. A streaming/cursor ABI is a prerequisite; see [SB-GAP-C-005](#sec-23). |
| SB-SCAN-005 | `BatchScanner.fetchColumn(cf, cq)` (`pysharkbite.cpp:436`; C++ `fetchColumn(col, colqual="")`, `include/scanner/Source.h:85`) | `ScannerOptions.Columns` with `NewColumnFamily` / `NewColumn` (`accumulo/scanner.go:104,43,48`) | `shoal_scanner_config.columns` + `shoal_column` (`capi/include/shoal_types.h:148-149,68-72`) | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`) | Behavior mismatch | The C++ default `colqual=""` is **not** declared to pybind11, so the pinned `fetchColumn(cf)` one-argument call raises `TypeError`. See [§21](#sec-21): the shim must accept one or two arguments. |
| SB-SCAN-006 | `BatchScanner.addIterator(IterInfo)` (`pysharkbite.cpp:448`) | `ScannerOptions.Iterators []IteratorSetting` (`accumulo/scanner.go:105`) | `shoal_scanner_config.iterators` (`capi/include/shoal_types.h:150-151`) | `TestPublicScannerAPICompiles` (`accumulo/external_test.go:52`) | Behavior mismatch | Configuration timing only (post-construction vs construction-time); capability present on both layers. No test exercises a real server-side iterator end-to-end — see [§24](#sec-24). |
| SB-SCAN-007 | `BatchScanner.addIterator(PythonIterator)` (`pysharkbite.cpp:449`) | — | — | `test/python/TestIterator.py:63-65,84-86` | Missing Go | Overload dispatch on argument type. See SB-SCAN-016. |
| SB-SCAN-008 | `BatchScanner.setOption(ScannerOptions)` (`pysharkbite.cpp:437`) | — | — | `PYTHONREADME.md:55` | Missing Go | Neither option value has a Shoal equivalent (SB-SCAN-014/015). |
| SB-SCAN-009 | `BatchScanner.removeOption(ScannerOptions)` (`pysharkbite.cpp:447`) | — | — | — | Missing Go | |
| SB-SCAN-010 | `BatchScanner.close()` (`pysharkbite.cpp:450`) | Scanner has no `Close`; server-side scan sessions are closed inside `Scan` (`accumulo/scanner.go:254`, `scannerCloseTimeout`, `accumulo/scanner.go:15`) | `shoal_scanner_close` / `shoal_scanner_free` (`capi/include/shoal.h:71-81`) | `TestScannerCancellationStillClosesServerScan` (`accumulo/scanner_test.go:396`); `TestOwnedScannerCloseIsConcurrentAndIdempotent` (`cmd/shoal-capi/state_test.go:75`); `capi/tests/lifecycle.c:109-114` | Behavior mismatch | Go has no scanner-level close because scans are self-contained; the ABI does. Shim maps `close()` to `shoal_scanner_close`. |
| SB-SCAN-011 | `BatchScanner.__enter__` / `__exit__` — context-manager protocol closing on exit (`pysharkbite.cpp:438-443`) | n/a | `shoal_scanner_close` | `test/python/TestWithFx.py:50,63`; `capi/tests/lifecycle.c:109-114` | Missing C ABI | Binding-layer obligation. `__exit__` must swallow nothing and always close. |
| SB-SCAN-012 | Scanner `threads` argument (parallel tablet fan-out) (`test/python/TestWrites.py:57` uses `2`) | `ScannerOptions.Parallelism` (`accumulo/scanner.go:107`) | `shoal_scanner_config.parallelism` (`capi/include/shoal_types.h:152`) | `TestBatchScannerBoundsParallelismAndPreservesOrder` (`accumulo/scanner_test.go:559`); `capi/tests/lifecycle.c:66-75` | Covered | Shoal bounds parallelism and preserves result order; Sharkbite does not document ordering. |
| SB-SCAN-013 | Multi-scan RPC batching (implicit in Sharkbite) | `ScannerOptions.UseMultiScan` (`accumulo/scanner.go:110`) | `shoal_scanner_config.use_multi_scan` (`capi/include/shoal_types.h:153`) | `TestBatchScannerGroupsMultiScansByServer` (`accumulo/scanner_test.go:632`); `capi/tests/lifecycle.c:91-96` | Not required (rationale required) | Rationale: Shoal-only tuning knob; result order becomes server-defined when enabled, so the shim must leave it off by default to preserve Sharkbite ordering. |
| SB-SCAN-014 | `ScannerOptions.HedgedReads` enum value (`pysharkbite.cpp:400`, `ScannerOptions::ENABLE_HEDGED_READS = 0x1`) | — | — | `PYTHONREADME.md:31-63` (documented beta feature) | Missing Go | Concurrent RFile + RPC scan racing. Shoal has RFile reading (`internal/rfile`) and RPC scanning (`internal/scanclient`) but no hedging layer. Commented out in every pinned test (`test/python/TestHedgedReads.py:55`), so a divergence request is plausible — but it is a published, documented option and cannot be silently dropped. |
| SB-SCAN-015 | `ScannerOptions.RFileScanOnly` enum value (`pysharkbite.cpp:401`, `ENABLE_RFILE_SCANNER`) | — (`internal/rfile`, `internal/offlinecompact` are internal) | — | — | Missing Go | Direct RFile scanning bypassing tablet servers. |
| SB-SCAN-016 | Server-side Python iterators (`PYTHONREADME.md:65-116`, `test/python/TestIterator.py`) | — (`internal/iterrt` is a Go iterator runtime, not a Python one) | — | `test/python/TestIterator.py:63-88` | Missing Go | Two forms: full class text with `seek`/`onNext`, and a single lambda string. Requires the Accumulo-side `pysharkbite-iterators` JNI build (`CMakeLists.txt`, `PYTHON_ITERATOR_SUPPORT`). This is the single largest functional gap after security/namespace ops; it needs a design decision (server-side execution is not something Shoal can provide against a stock Accumulo tserver without the same JNI shim). |
| SB-SCAN-017 | `Results.__iter__` (`pysharkbite.cpp:383-386`) | `[]KeyValue` (natively iterable in Go) | `shoal_scan_result_count` + `shoal_scan_result_get` (`capi/include/shoal.h:100-106`) | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/result_bridge.c:14-48` | Behavior mismatch | Sharkbite's `__iter__` calls `begin()`, which **restarts** the scan. Shim must reproduce restart-on-iter. |
| SB-SCAN-018 | `Results.__next__` -> `KeyValue`, raising `StopIteration` at end (`pysharkbite.cpp:397`, `include/scanner/constructs/Results.h:280-288`) | slice iteration | `shoal_scan_result_get` returns `SHOAL_STATUS_INVALID_ARGUMENT` out of range (`capi/tests/result_bridge.c:44-46`) | `capi/tests/result_bridge.c:44-46` | Behavior mismatch | Sharkbite throws `pybind11::stop_iteration` from C++. Shim must convert index exhaustion into `StopIteration`. |
| SB-SCAN-019 | `Results.__aiter__` (`pysharkbite.cpp:381-383`) | — | — | `examples/asyncexample.py` (`async for keyvalue in iter`) | Missing Go | Async iteration protocol. |
| SB-SCAN-020 | `Results.__anext__` returning an `asyncio` future (`pysharkbite.cpp:387-396`) | — | — | `examples/asyncexample.py` | Missing Go | Implementation detail matters: it grabs `asyncio.events.get_event_loop()` and resolves a future immediately (no real concurrency). A shim can satisfy the contract with `asyncio.to_thread`, which is strictly better; record as a divergence if observable. |
| SB-SCAN-021 | `Results.__await__` (`pysharkbite.cpp:378-380`) | — | — | — | Missing Go | Awaiting the result set restarts iteration and returns `self`. |
| SB-SCAN-022 | `asyncio.run` over multiple concurrent scans (`test/python/TestAsyncOperations.py:180-188`) | `context.Context` per call; goroutine-safe scanners | Scanner handles support concurrent scan calls (`capi/README.md`) | `TestOwnedScannerCloseCancelsAndJoinsActiveCalls` (`cmd/shoal-capi/state_test.go:37`) | Behavior mismatch | Sharkbite's async is cooperative-only; Shoal is genuinely concurrent. Must be validated by a live-cluster async conformance test. |
| SB-SCAN-023 | Scan batch size | `ScannerOptions.BatchSize` (`accumulo/scanner.go:102`), default 1024 (`accumulo/scanner.go:14`) | `shoal_scanner_config.batch_size` (`capi/include/shoal_types.h:145`) | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`) | Not required (rationale required) | Rationale: not exposed by Sharkbite Python (`AccumuloIterator` chunking is client-side only, `sharkbite/__init__.py:190-194`). Shoal superset. |
| SB-SCAN-024 | Client-side chunking `AccumuloIterator(scanner, chunkSize)` (`sharkbite/__init__.py:166-194`) | — | — | `sharkbite/__init__.py:190-194` | Not required (rationale required) | Rationale: the pinned implementation is defective — it raises `StopIteration` when the chunk counter reaches `chunkSize`, silently truncating results instead of yielding the next batch (`nextBatch()` is never called). Reproducing it would reproduce data loss. See [§21](#sec-21). |
| SB-SCAN-025 | Scan cleanup failures | `CleanupError{ScanID, Err}` (`accumulo/scanner.go:116`) | `SHOAL_STATUS_CLEANUP_FAILED` (`capi/include/shoal_types.h:38`) | `TestScannerRangeAndCleanupErrors` (`accumulo/scanner_test.go:365`); `TestBatchScannerContinuesAfterCleanupError` (`accumulo/scanner_test.go:491`) | Not required (rationale required) | Rationale: Sharkbite has no equivalent (it silently ignores close failures). Shoal superset; shim should surface it as a warning, not an exception, to preserve behavior. |
| SB-SCAN-026 | Tablet relocation during a scan (`test/vandv/testScanLocationMove.h`) | `Scanner` retries `NotServingTablet` once (`accumulo/scanner.go:227,350`) | Implicit — no separate ABI surface; `shoal_scanner_scan` inherits the Go behavior | `TestScannerRetriesNotServingAssignmentOnce` (`accumulo/scanner_test.go:294`) | Covered | User-visible outcome is identical (the scan completes and the exception never reaches the caller), although the retry shape differs: Sharkbite loops on `NotServingException`, Shoal retries once and then invalidates its locator cache. Live conformance still validates the end-to-end shim ([§24](#sec-24)). |
| SB-SCAN-027 | Isolation, `waitForWrites`, sampler, execution hints, busy timeout | — (present in `internal/scanclient.StartRequest`, never populated by `accumulo`) | — | — | Not required (rationale required) | Rationale: not exposed by Sharkbite either. Listed to record that the internal plumbing exists if a future row needs it. |
| SB-SCAN-028 | High-level `AccumuloScanner(instance, zookeepers, username, password, table=None, auths=None)` (`sharkbite/__init__.py:136-138`) | Compose [SB-CONN-014](#sec-7), then `Connector.NewScanner` / `Connector.NewBatchScanner` semantics | Compose `shoal_connector_create_scanner` / `shoal_connector_create_batch_scanner` | `sharkbite/__init__.py:136-138`; `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`); `capi/tests/lifecycle.c:59-114` | Missing C ABI | Public convenience layer absent. The pinned helper eagerly creates the underlying scanner in `__init__`, unlike the compatibility-safe deferral called out in SB-SCAN-001. |
| SB-SCAN-029 | `AccumuloScanner.get(begin_row, end_row=None, chunksize=1000)` (`sharkbite/__init__.py:146-154`) | Compose `Range`, `addRange`, and `AccumuloIterator(scanner, chunkSize)` | Compose scan calls plus a client-side iterator shim | `sharkbite/__init__.py:146-154`; `examples/torchexample.py:75,82` | Missing C ABI | Returns the defective client-side `AccumuloIterator` from SB-SCAN-024; `chunksize` is a helper-layer batching hint, not a server batch-size control. |
| SB-SCAN-030 | `AccumuloScanner.fetch_range(range)` (`sharkbite/__init__.py:156-157`) | Compose SB-SCAN-002 | Compose SB-SCAN-002 | `examples/dataframe.py:67` | Missing C ABI | Thin one-range convenience wrapper over `addRange`. |
| SB-SCAN-031 | `AccumuloScanner.fetch_ranges(ranges)` (`sharkbite/__init__.py:158-160`) | Compose SB-SCAN-002 repeatedly | Compose SB-SCAN-002 repeatedly | `pandashark/__init__.py:59-61` | Missing C ABI | Loops over the caller's list; no bulk validation beyond the underlying scanner. |
| SB-SCAN-032 | `AccumuloScanner.__del__` best-effort close (`sharkbite/__init__.py:162-164`) | n/a | n/a | — | Not required (rationale required) | Rationale: like SB-WRITE-020, this performs network I/O during interpreter teardown and is an upstream defect, not a compatibility requirement. Keep only a best-effort finalizer and document `with`/`close()` as the contract. See [§21](#sec-21). |

<a id="sec-10"></a>

## 10. Matrix: writers (`SB-WRITE`)

Sharkbite's Python `BatchWriter` is `writer::Sink<KeyValue>`
(`pysharkbite.cpp:486`), created by
`tableOps.createWriter(auths, threads)` → `createSharedWriter`
(`include/interconnect/python/PythonStructures.h:177-180`).

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-WRITE-001 | `tableOps.createWriter(auths, threads) -> BatchWriter` (`pysharkbite.cpp:249`) | `Connector.NewBatchWriter(ctx, table, BatchWriterOptions)` (`accumulo/batch_writer.go:225`) | `shoal_connector_create_batch_writer` (`capi/include/shoal.h:150-153`) | `TestBatchWriterRoutesBatchesAndCopiesMutation` (`accumulo/batch_writer_test.go:194`); `capi/tests/lifecycle.c:127-132` | Behavior mismatch | Sharkbite's `threads` maps to `MaxWriteThreads` (`accumulo/batch_writer.go:76`). Sharkbite takes authorizations at writer creation (they are unused for writes); Shoal's writer config has no auths field. Passing `None` auths must raise `ClientException` (`test/python/TestBadOperations.py:75-79`). |
| SB-WRITE-002 | `BatchWriter.addMutation(Mutation) -> bool` (`pysharkbite.cpp:488`) | `BatchWriter.Add(ctx, *Mutation) error` (`accumulo/batch_writer.go:268`) | `shoal_batch_writer_add(writer, mutation, timeout_ms, out_failure, out_error)` (`capi/include/shoal.h:160-164`) | `TestBatchWriterRoutesBatchesAndCopiesMutation` (`accumulo/batch_writer_test.go:194`); `capi/tests/lifecycle.c:178-199` | Behavior mismatch | Return type differs: Sharkbite returns `bool` (enqueued), Shoal returns an error. The pinned tests ignore the return value, but `sharkbite/__init__.py` does too, so the shim should return `True` on success and raise on error. Empty mutations are rejected by Shoal (`capi/tests/lifecycle.c:178-190`) and accepted by Sharkbite. |
| SB-WRITE-003 | `BatchWriter.flush(override)` — C++ default `override=false` is **not** exposed to Python (`pysharkbite.cpp:487`, `include/writer/Sink.h:62`) | `BatchWriter.Flush(ctx) error` (`accumulo/batch_writer.go:337`) | `shoal_batch_writer_flush` (`capi/include/shoal.h:166-169`) | `TestBatchWriterExplicitFlushResetsAutomaticDeadline` (`accumulo/batch_writer_test.go:329`); `capi/tests/lifecycle.c:191-199` | Behavior mismatch | Because pybind11 does not inherit C++ defaults, `writer.flush()` raises `TypeError` in Sharkbite 1.2.0.3; only `flush(True)`/`flush(False)` work. The shim must accept zero arguments (and tolerate the legacy boolean). See [§21](#sec-21). |
| SB-WRITE-004 | `BatchWriter.close()` (`pysharkbite.cpp:495`, `Sink::close()` calls `flush(true)`) | `BatchWriter.Close(ctx) error` (`accumulo/batch_writer.go:356`) | `shoal_batch_writer_close` (`capi/include/shoal.h:176-179`) | `TestBatchWriterCloseLifecycle` (`accumulo/batch_writer_test.go:852`); `capi/tests/lifecycle.c:191-199,225-231` | Covered | Close flushes on both layers; Shoal's close is idempotent (`capi/tests/lifecycle.c:196-199`). Repeated `createWriter`/`close` cycles in `test/python/TestWrites.py:42-51` are preserved. |
| SB-WRITE-005 | `BatchWriter.__enter__` / `__exit__` closing on exit (`pysharkbite.cpp:489-495`) | n/a | `shoal_batch_writer_close` | `test/python/TestSecurityOperations.py:55-61`; `test/python/TestWithFx.py:30-42`; `capi/tests/lifecycle.c:191-199` | Missing C ABI | Binding-layer obligation. |
| SB-WRITE-006 | `BatchWriter.size()` — approximate queue depth (`pysharkbite.cpp:496`, `Sink::size()` → `sinkQueue.size_approx()`) | — (internal buffer accounting only, `accumulo/batch_writer.go:203-223`) | — | — | Missing Go | No exported buffered-size accessor. |
| SB-WRITE-007 | Write durability | `BatchWriterOptions.Durability` + `Durability{Default,Sync,Flush,Log,None}` (`accumulo/batch_writer.go:44-53,78`) | `shoal_durability` (`capi/include/shoal_types.h:190-198`) | `capi/tests/lifecycle.c:134-137` | Not required (rationale required) | Rationale: Sharkbite exposes no durability control. Shoal superset. |
| SB-WRITE-008 | Writer memory bound | `BatchWriterOptions.MaxMemoryBytes` (default 50 MiB, `accumulo/batch_writer.go:17,57`) | `shoal_batch_writer_config.max_memory_bytes` (`capi/include/shoal_types.h:204`) | `TestBatchWriterMemoryBoundFlushesSynchronously` (`accumulo/batch_writer_test.go:256`); `capi/tests/lifecycle.c:116-125` | Not required (rationale required) | Rationale: Sharkbite bounds by queue length (`Sink(uint16_t maxQueue)`), not bytes. The shim must map Sharkbite's `threads`-shaped constructor without exposing new knobs. |
| SB-WRITE-009 | Writer latency flush | `BatchWriterOptions.MaxLatency` (`accumulo/batch_writer.go:66`) | `max_latency_ms` (`capi/include/shoal_types.h:206`) | `TestBatchWriterAutomaticFlushesAtDeadline` (`accumulo/batch_writer_test.go:273`); `capi/tests/lifecycle.c:139-142` | Not required (rationale required) | Rationale: Shoal-only (issue [#53](https://github.com/phrocker/shoal-oss/issues/53) / PR [#55](https://github.com/phrocker/shoal-oss/pull/55)); Sharkbite flushes on queue pressure and close only. Defaults to disabled, preserving Sharkbite behavior. |
| SB-WRITE-010 | Per-mutation failures surfaced to the caller | `MutationRejectionError{Server, FailedExtents, ConstraintViolations, AuthorizationFailures}` (`accumulo/batch_writer.go:116`) | `shoal_write_failure` + `shoal_failed_extent_view`, `shoal_constraint_violation_view`, `shoal_authorization_failure_view` (`capi/include/shoal_types.h:225-257`) | `TestBatchWriterSurfacesAccumuloUpdateErrors` (`accumulo/batch_writer_test.go:942`); `TestFlattenWriteFailurePreservesStructuredDetails` (`cmd/shoal-capi/writer_export_test.go:11`); `capi/tests/lifecycle.c:201-223` | Behavior mismatch | Sharkbite drops server-side update errors on the floor; there is no Python-visible `MutationsRejectedException`. Shoal surfaces structured failures. This is a **safety-improving** divergence but changes control flow: code that never saw write failures will now see exceptions. Must be recorded and approved. |
| SB-WRITE-011 | Ambiguous commit ("may have partially committed") | `ErrBatchWriterFailed` (`accumulo/batch_writer.go:33`), sticky terminal state | `SHOAL_STATUS_AMBIGUOUS_WRITE` (18), `SHOAL_WRITE_FAILURE_AMBIGUOUS_COMMIT` (`capi/include/shoal_types.h:42,220`) | `TestBatchWriterDoesNotRetryAmbiguousApplyFailure` (`accumulo/batch_writer_test.go:1401`); `TestBatchWriterAmbiguousAutomaticFlushErrorIsSticky` (`accumulo/batch_writer_test.go:473`); `capi/tests/lifecycle.c:201-223` | Not required (rationale required) | Rationale: Sharkbite has no concept of ambiguity; it retries blindly. Shoal's behavior is strictly safer and must be preserved through the shim rather than hidden. |
| SB-WRITE-012 | Retry semantics | `MaxRetries` (default 3) + `RetryBackoff` (default 100 ms) (`accumulo/batch_writer.go:20-21,70,74`), retries only provably safe failures (`accumulo/batch_writer.go:743`) | `max_retries`, `retry_backoff_ms` (`capi/include/shoal_types.h:208-209`); `SHOAL_STATUS_RETRY_EXHAUSTED` (16) | `TestBatchWriterRetryOptionsValidation` (`accumulo/batch_writer_test.go:526`); `TestBatchWriterExplicitRetryExhaustionIsSticky` (`accumulo/batch_writer_test.go:1360`); `TestBatchWriterRetryBackoffHonorsCancellation` (`accumulo/batch_writer_test.go:1434`) | Behavior mismatch | Sharkbite retries at the transport layer only (protocol-version fallback, `include/interconnect/transport/BaseTransport.h:57`) and reshuffles servers up to the server-list length (`TransportPool.h:222`). Semantics are not equivalent; document the new failure modes. |
| SB-WRITE-013 | Writer cleanup failure | `BatchWriterCleanupError` (`accumulo/batch_writer.go:135`) | `shoal_cleanup_failure_view` (`capi/include/shoal_types.h:254-257`) | `TestBatchWriterCloseCleanupErrorIsStable` (`accumulo/batch_writer_test.go:1479`); `capi/tests/result_bridge.c:56-101` | Not required (rationale required) | Rationale: Shoal superset. |
| SB-WRITE-014 | Concurrent `addMutation` from multiple Python threads | `BatchWriter` is mutex-guarded (`accumulo/batch_writer.go:380-394`) | Writer handles are close-safe and join in-flight calls (`capi/README.md`) | `TestBatchWriterConcurrentAdds` (`accumulo/batch_writer_test.go:595`); `TestOwnedBatchWriterCloseCancelsAndJoinsActiveCalls` (`cmd/shoal-capi/state_test.go:124`) | Covered | Sharkbite relies on an internal concurrent queue with no documented user-thread guarantee; Shoal is explicitly safe. |
| SB-WRITE-015 | Writer after connector close | `ErrConnectorClosed` (`accumulo/errors.go:51`) | `SHOAL_STATUS_CLOSED` (6) (`capi/include/shoal_types.h:30`) | `TestOwnedBatchWriterRejectsOperationsAfterConnectorClose` (`cmd/shoal-capi/state_test.go:196`); `capi/tests/lifecycle.c:233-243` | Not required (rationale required) | Rationale: undefined in Sharkbite (use-after-free). Shoal's defined error is required, not optional. |
| SB-WRITE-016 | `AccumuloWriter.put(row, cf, cq, cv=None, timestamp=0, value=None)` high-level helper (`sharkbite/__init__.py:73-95`) | Compose `NewMutation` + `Put` + `Add` | Compose `shoal_mutation_*` + `shoal_batch_writer_add` | `capi/tests/lifecycle.c:144-199` | Missing C ABI | Row-change-triggered mutation flushing, lazy writer creation, and `timestamp == 0 → time.time()*1000` substitution must be reproduced exactly (`sharkbite/__init__.py:84-85`). |
| SB-WRITE-017 | `AccumuloWriter.putDelete(row, cf, cq, cv="", timestamp=0)` (`sharkbite/__init__.py:97-111`) | Compose `Mutation.Delete` | `shoal_mutation_delete` | `capi/tests/lifecycle.c:144-171` | Missing C ABI | Note the asymmetry with `put`: `putDelete` does not substitute the current time when `timestamp == 0`. |
| SB-WRITE-018 | `AccumuloWriter.delete(key: Key)` (`sharkbite/__init__.py:113-115`) | Compose | `shoal_mutation_delete` | — | Missing C ABI | Uses `key.getTimestamp()`, so it inherits the `INT64_MAX` default from SB-DATA-006. |
| SB-WRITE-019 | `AccumuloWriter.close()` flushing the pending mutation (`sharkbite/__init__.py:117-126`) | `BatchWriter.Close` | `shoal_batch_writer_close` | `TestBatchWriterCloseLifecycle` (`accumulo/batch_writer_test.go:852`) | Missing C ABI | |
| SB-WRITE-020 | `AccumuloWriter.__del__` calling `close()` (`sharkbite/__init__.py:128-129`) | n/a | n/a | — | Not required (rationale required) | Rationale: `__del__`-driven network I/O at interpreter shutdown is unsafe (exceptions are swallowed, module globals may already be `None`). Shim should keep a best-effort `__del__` but must document `with`/`close()` as the contract. |
| SB-WRITE-021 | `AccumuloBase.set_threads(n)` (`sharkbite/__init__.py:52-53`) | `BatchWriterOptions.MaxWriteThreads` / `ScannerOptions.Parallelism` | `max_write_threads` / `parallelism` | `TestBatchWriterBoundsParallelServerSubmission` (`accumulo/batch_writer_test.go:643`) | Missing C ABI | Default is 10 in Sharkbite (`sharkbite/__init__.py:19`) versus 3 in Shoal (`accumulo/batch_writer.go:19`). Shim must preserve 10. |
| SB-WRITE-022 | `AccumuloBase.set_table` / `set_authorizations` / `list_tables` / `to_scanner` (`sharkbite/__init__.py:39-58`) | `Connector.Tables` (`accumulo/table_admin.go:29`) | — | `TestTableAdministrationListingAndExistence` (`accumulo/table_admin_test.go:13`) | Missing C ABI | `set_authorizations` accepts an `Authorizations` object, a list, or `None` (`sharkbite/__init__.py:43-50`); all three must work. |
| SB-WRITE-023 | High-level `AccumuloWriter(instance, zookeepers, username, password, table=None, auths=None)` (`sharkbite/__init__.py:70-71`) | Compose [SB-CONN-014](#sec-7) plus lazy `NewBatchWriter` on first write | Compose `shoal_connector_create` plus lazy `shoal_connector_create_batch_writer` | `sharkbite/__init__.py:70-71`; `TestBatchWriterCloseLifecycle` (`accumulo/batch_writer_test.go:852`); `capi/tests/lifecycle.c:127-199` | Missing C ABI | Thin convenience subclass only; all writer semantics live in SB-WRITE-016…SB-WRITE-022. The constructor nevertheless needs its own row because it is public and imported directly from `sharkbite`. |

<a id="sec-11"></a>

## 11. Matrix: table operations (`SB-TABLE`)

All rows below are methods of `AccumuloTableOperations`
(`pysharkbite.cpp:238`), whose implementation is
`interconnect::PythonTableOperations`
(`include/interconnect/python/PythonStructures.h:63`). The handle is bound to
one table at construction, so no method takes a table name.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-TABLE-001 | `create(recreate=False) -> bool` (`pysharkbite.cpp:250`, `PythonStructures.h:74`) | `Connector.CreateTable(ctx, name) error` (`accumulo/table_admin.go:76`) | `shoal_connector_create_table` (`capi/include/shoal.h:109`) | `TestTableAdministrationLifecycleAndCancellation` (`accumulo/table_admin_test.go:62`); `TestTableMutationsUseAccumulo4FATEArguments` (`accumulo/table_admin_test.go:207`); `capi/tests/lifecycle.c:196-206` | Behavior mismatch | `recreate=True` means "drop then create"; Shoal has no recreate. Return type differs (`bool` vs status/error), and the duplicate-create case in [SB-TABLE-022](#sec-11) must return `False`, not raise. |
| SB-TABLE-002 | `exists(createIfNot=False) -> bool` (`pysharkbite.cpp:240`, `PythonStructures.h:87`) | `Connector.TableExists(ctx, name) (bool, error)` (`accumulo/table_admin.go:53`) | `shoal_connector_table_exists` (`capi/include/shoal.h:104`) | `TestTableAdministrationListingAndExistence` (`accumulo/table_admin_test.go:13`); `capi/tests/lifecycle.c:185-192` | Behavior mismatch | The `createIfNot` side effect must be reproduced; every pinned Python test calls `exists(False)` first (`test/python/TestWrites.py:15`). |
| SB-TABLE-003 | `remove() -> bool` (`pysharkbite.cpp:239`, `PythonStructures.h:80`) | `Connector.DeleteTable(ctx, name) error` (`accumulo/table_admin.go:96`) | `shoal_connector_delete_table` (`capi/include/shoal.h:114`) | `TestTableAdministrationLifecycleAndCancellation` (`accumulo/table_admin_test.go:62`); `capi/tests/lifecycle.c:212-217` | Covered | Every pinned Python test ends with `tableOperations.remove()`. The shim can return `True` on `SHOAL_STATUS_OK`; the missing-table path is [SB-TABLE-021](#sec-11). |
| SB-TABLE-004 | Table rename (not bound in Python) | `Connector.RenameTable(ctx, old, new) error` (`accumulo/table_admin.go:110`) | `shoal_connector_rename_table` (`capi/include/shoal.h:119`) | `TestTableMutationsUseAccumulo4FATEArguments` (`accumulo/table_admin_test.go:207`); `capi/tests/lifecycle.c:204-209` | Not required (rationale required) | Rationale: Shoal superset; `AccumuloTableOperations` has no rename binding. |
| SB-TABLE-005 | `flush(startRow, endRow, wait) -> int8` (`pysharkbite.cpp:242`, `PythonStructures.h:111`) | `Connector.FlushTable(ctx, tableName string, wait bool) error` (`accumulo/table_flush.go:15`) | `shoal_connector_flush_table` (`capi/include/shoal.h:129`) | `TestFlushTableUsesStableIDAndAccumulo4WaitModes` (`accumulo/table_flush_test.go:12`); `capi/tests/lifecycle.c:221-224` | Behavior mismatch | Shoal flushes the **whole table**; Sharkbite flushes a row range. Row-bounded flush is missing. Also `int8` status vs status/error. |
| SB-TABLE-006 | `compact(startRow, endRow, wait) -> int8` (`pysharkbite.cpp:243`, `PythonStructures.h:124`) | — | — | — | Missing Go | No online compaction trigger. `internal/offlinecompact` is an out-of-band OFFLINE-fenced tool and is not reachable from `Connector`; issue [#65](https://github.com/phrocker/shoal-oss/issues/65) tracks online compaction. |
| SB-TABLE-007 | `setProperty(property, value) -> int8` (`pysharkbite.cpp:244`, `PythonStructures.h:135`) | `Connector.SetTableProperty(ctx, tableName, property, value) error` (`accumulo/table_properties.go:14`) | `shoal_connector_set_table_property` (`capi/include/shoal.h:138`) | `TestTablePropertyMutationsUseAccumulo4ManagerRPCs` (`accumulo/table_properties_test.go:12`); `capi/tests/lifecycle.c:227-233` | Covered | |
| SB-TABLE-008 | `removeProperty(property) -> int8` (`pysharkbite.cpp:245`, `PythonStructures.h:145`) | `Connector.RemoveTableProperty(ctx, tableName, property) error` (`accumulo/table_properties.go:30`) | `shoal_connector_remove_table_property` (`capi/include/shoal.h:146`) | `TestTablePropertyMutationsUseAccumulo4ManagerRPCs` (`accumulo/table_properties_test.go:12`); `capi/tests/lifecycle.c:236` | Covered | |
| SB-TABLE-009 | Property reads (not bound in Python) | `Connector.EffectiveTableProperties(ctx, tableName)` (`accumulo/table_property_reads.go:31`) | `shoal_connector_effective_table_properties` + `shoal_table_properties_get` (`capi/include/shoal.h:158,166`) | `TestEffectiveTablePropertiesPreservesValuesAndCopyIsolation` (`accumulo/table_property_reads_test.go:29`); `capi/tests/lifecycle.c:241-270` | Not required (rationale required) | Rationale: Shoal superset (issue [#54](https://github.com/phrocker/shoal-oss/issues/54) / PR [#56](https://github.com/phrocker/shoal-oss/pull/56)). |
| SB-TABLE-010 | `addSplits(set[str])` (`pysharkbite.cpp:246`, `PythonStructures.h:153`) | — | — | — | Missing Go | Issue [#80](https://github.com/phrocker/shoal-oss/issues/80) / PR [#83](https://github.com/phrocker/shoal-oss/pull/83) add split **listing**, not split addition. Splitting remains unimplemented. |
| SB-TABLE-011 | Split listing (not bound in Python) | — (in flight: PR [#83](https://github.com/phrocker/shoal-oss/pull/83)) | — | — | Not required (rationale required) | Rationale: no Sharkbite equivalent; recorded because it is the sibling of SB-TABLE-010 and lands first. |
| SB-TABLE-012 | `addConstraint(className) -> int` (`pysharkbite.cpp:247`, `PythonStructures.h:162`) | — | — | — | Missing Go | Constraint violations are surfaced on write (`accumulo/batch_writer.go:101`) but constraints cannot be installed. |
| SB-TABLE-013 | `import(dir, fail_path, setTime=False) -> bool` (`pysharkbite.cpp:241`, `PythonStructures.h:98`) | `Connector.BulkImport(ctx, tableName, bulkDir, BulkImportOptions{SetTime})` (`accumulo/table_bulk_import.go`); `TestBulkImportUsesTableIDAndFateArguments`, `TestBulkImportResolvesTableNameNotFound`, `TestBulkImportMapsManagerErrors`, `TestBulkImportValidationCancellationAndLifecycle` | — | — | Behavior mismatch | Shoal exposes Accumulo 4 Bulk Import V2 on `Connector`, but requires a pre-staged `loadmap.json` and has no Sharkbite `fail_path` behavior or compatibility adapter. Reachable only via `getattr(tableOps, "import")(...)` because `import` is a Python keyword; the shim must expose both `import` and a usable alias such as `import_directory`. |
| SB-TABLE-014 | Table export | — (no Sharkbite Python binding either) | — | — | Not required (rationale required) | Rationale: not in the Sharkbite Python surface. Listed so future readers do not re-derive it. |
| SB-TABLE-015 | Table clone / online / offline / merge / delete-rows | — (no Sharkbite Python binding) | — | — | Not required (rationale required) | Rationale: absent from the Sharkbite Python contract; confirmed absent from `accumulo/` as well. |
| SB-TABLE-016 | `AccumuloTableInfo.table_id(table) -> str` (`pysharkbite.cpp:253`, `PythonStructures.h:40`) | `Connector.TableByName(ctx, name) (Table, error)`; `Table.ID` (`accumulo/discovery.go:65`, `accumulo/discovery.go:14`) | `shoal_connector_list_tables` + `shoal_table_list_get` (`capi/include/shoal.h:85,93`) | `TestDiscoveryTableLookupAndRouting` (`accumulo/discovery_test.go:108`); `capi/tests/lifecycle.c:160-175` | Covered | The ABI returns `{name,id}` pairs, so the shim can project the matching `id` by name. |
| SB-TABLE-017 | `AccumuloTableInfo.table_name(tableid) -> str` (`pysharkbite.cpp:254`, `PythonStructures.h:49`) | `Connector.TableByID(ctx, id) (Table, error)` (`accumulo/discovery.go:87`) | `shoal_connector_list_tables` + `shoal_table_list_get` (`capi/include/shoal.h:85,93`) | `TestDiscoveryTableLookupAndRouting` (`accumulo/discovery_test.go:108`); `capi/tests/lifecycle.c:160-175` | Covered | The ABI returns `{name,id}` pairs, so the shim can project the matching `name` by id. |
| SB-TABLE-018 | `AccumuloTableInfo.list_tables() -> list[str]` (`pysharkbite.cpp:255`) | `Connector.Tables(ctx) ([]Table, error)` (`accumulo/table_admin.go:29`) | `shoal_connector_list_tables` + `shoal_table_list_get` (`capi/include/shoal.h:85,93`) | `TestTableAdministrationListingAndExistence` (`accumulo/table_admin_test.go:13`); `capi/tests/lifecycle.c:160-175` | Covered | Sharkbite returns names; Shoal returns `Table{ID, Name}` pairs. The shim must project to names. |
| SB-TABLE-019 | `AccumuloTableInfo.exists(table) -> bool` (`pysharkbite.cpp:256`, `PythonStructures.h:34`) | `Connector.TableExists(ctx, name)` (`accumulo/table_admin.go:53`) | `shoal_connector_table_exists` (`capi/include/shoal.h:104`) | `TestTableAdministrationListingAndExistence` (`accumulo/table_admin_test.go:13`); `capi/tests/lifecycle.c:185-192` | Covered | Distinct from SB-TABLE-002: takes an explicit table name and has no create side effect. |
| SB-TABLE-020 | Tablet discovery (not bound in Python) | `Connector.Tablets`, `LocateTablet`, `InvalidateTablet`, `InvalidateTable`, `InvalidateDiscovery` (`accumulo/discovery.go:112,135,157,170,183`) | — | `TestDiscoveryInvalidationAndDefensiveCopies` (`accumulo/discovery_test.go:148`) | Not required (rationale required) | Rationale: Shoal superset; Sharkbite hides locator state entirely. |
| SB-TABLE-021 | Table-operation errors: table does not exist after `remove()` (`test/python/TestBadOperations.py:33-62`) | `ErrTableNotFound` (`accumulo/errors.go:15`), `mapManagerError` (`accumulo/table_admin.go:171`) | `SHOAL_STATUS_NOT_FOUND` (9) via `shoal_connector_delete_table` (`capi/include/shoal_types.h:107`; `capi/include/shoal.h:114`) | `TestTableMutationsMapErrorsAndLifecycle` (`accumulo/table_admin_test.go:263`); `TestMapManagerErrorUsesServerTableName` (`accumulo/table_admin_test.go:328`); `TestStatusForTableAdministrationErrors` (`cmd/shoal-capi/table_admin_export_test.go:10`); `capi/tests/lifecycle.c:212-215` | Covered | Sharkbite raises `ClientException` with code `TABLE_NOT_FOUND` (`test/vandv/invalidscans.h`). Mapping is [SB-ERR-004](#sec-18). |
| SB-TABLE-022 | Creating an existing table | `ErrTableExists` (`accumulo/errors.go:18`) | `SHOAL_STATUS_ALREADY_EXISTS` (19) via `shoal_connector_create_table` (`capi/include/shoal_types.h:117`; `capi/include/shoal.h:109`) | `TestTableMutationsMapErrorsAndLifecycle` (`accumulo/table_admin_test.go:263`); `TestStatusForTableAdministrationErrors` (`cmd/shoal-capi/table_admin_export_test.go:10`); `capi/tests/lifecycle.c:196-203` | Behavior mismatch | Sharkbite's `create(False)` returns `False` rather than raising (`test/python/TestWrites.py:17-19`). The shim must return `False`, not raise. |

<a id="sec-12"></a>

## 12. Matrix: namespace operations (`SB-NS`)

`AccumuloNamespaceOperations` (`pysharkbite.cpp:229`,
`include/interconnect/python/PythonStructures.h:186`). **Every row is
`Missing Go`.** `internal/managerclient` defines the error kinds
`ErrorNamespaceExists` and `ErrorNamespaceNotFound` and `accumulo/errors.go:28`
defines `ErrNamespaceNotFound`, but no namespace-mutating `Operation` exists in
`internal/managerclient.Operation` (only `TableCreate`, `TableDelete`,
`TableRename`), so the capability is absent at every layer.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-NS-001 | `list() -> list[str]` (`pysharkbite.cpp:230`, `PythonStructures.h:196`) | — | — | — | Missing Go | |
| SB-NS-002 | `create(name="")` (`pysharkbite.cpp:236`, `PythonStructures.h:215`) | — | — | — | Missing Go | Empty name means "the namespace this handle was constructed with". |
| SB-NS-003 | `remove(name="") -> bool` (`pysharkbite.cpp:231`, `PythonStructures.h:202`) | — | — | — | Missing Go | |
| SB-NS-004 | `exists(name="") -> bool` (`pysharkbite.cpp:232`, `PythonStructures.h:209`) | — | — | — | Missing Go | |
| SB-NS-005 | `rename(newName, oldName="")` (`pysharkbite.cpp:233`, `PythonStructures.h:222`) | — | — | — | Missing Go | Argument order is (new, old) — unusual and must be preserved. |
| SB-NS-006 | `setProperty(property, value, nm="")` (`pysharkbite.cpp:234`, `PythonStructures.h:231`) | — | — | — | Missing Go | |
| SB-NS-007 | `removeProperty(property, nm="")` (`pysharkbite.cpp:235`, `PythonStructures.h:240`) | — | — | — | Missing Go | |
| SB-NS-008 | Qualified `namespace.table` name resolution | `internal/tablenames.Resolver.ResolveID` handles qualified names | — | `TestDiscoveryTableLookupAndRouting` (`accumulo/discovery_test.go:108`) | Behavior mismatch | Reading qualified names works; managing namespaces does not. |

<a id="sec-13"></a>

## 13. Matrix: security operations and permissions (`SB-SEC`)

`SecurityOperations` (`pysharkbite.cpp:594`,
`include/interconnect/python/PythonStructures.h:248`). **Every operation row is
`Missing Go`**: a repository-wide search for `Permission`, `GrantAuthorizations`,
`CreateUser`, `DropUser`, and `ChangePassword` symbols in `accumulo/` returns
nothing, and `internal/managerclient.Adapter` exposes only `Execute`,
`FlushTable`, `GetTableConfiguration`, `SetTableProperty`,
`RemoveTableProperty`, and `Close`.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-SEC-001 | `create_user(user, password) -> int8` (`pysharkbite.cpp:595`, `PythonStructures.h:263`) | — | — | `test/python/TestSecurityOperations.py:15` | Missing Go | |
| SB-SEC-002 | `change_password(user, password) -> int8` (`pysharkbite.cpp:596`, `PythonStructures.h:276`) | — | — | — | Missing Go | |
| SB-SEC-003 | `remove_user(user) -> int8` (`pysharkbite.cpp:597`, `PythonStructures.h:288`) | — | — | `test/python/TestSecurityOperations.py:107` | Missing Go | |
| SB-SEC-004 | `get_auths(user) -> Authorizations` (`pysharkbite.cpp:598`, `PythonStructures.h:295`) | — | — | `examples/pythonexample.py` | Missing Go | Returns `unique_ptr<Authorizations>`; Python takes ownership. |
| SB-SEC-005 | `grantAuthorizations(auths, user) -> int8` (`pysharkbite.cpp:608`, `PythonStructures.h:310`) | — | — | `test/python/TestAuthorizations.py:28`; `test/python/TestRanges.py:29`; `test/python/TestWithFx.py:19` | Missing Go | Used by four of the ten pinned Python tests; without it the visibility tests cannot run at all. |
| SB-SEC-006 | `has_system_permission(user, SystemPermissions) -> bool` (`pysharkbite.cpp:599`, `PythonStructures.h:321`) | — | — | `test/python/TestSecurityOperations.py:18,23` | Missing Go | |
| SB-SEC-007 | `has_table_permission(user, table, TablePermissions) -> bool` (`pysharkbite.cpp:600`, `PythonStructures.h:333`) | — | — | `test/vandv/testSecurityOperations.h` | Missing Go | |
| SB-SEC-008 | `has_namespace_permission(user, ns, NamespacePermissions) -> bool` (`pysharkbite.cpp:601`, `PythonStructures.h:345`) | — | — | — | Missing Go | |
| SB-SEC-009 | `grant_system_permission(user, perm) -> int8` (`pysharkbite.cpp:602`, `PythonStructures.h:358`) | — | — | `test/python/TestSecurityOperations.py:19-20` | Missing Go | |
| SB-SEC-010 | `revoke_system_permission(user, perm) -> int8` (`pysharkbite.cpp:603`, `PythonStructures.h:370`) | — | — | `test/vandv/testSecurityOperations.h` | Missing Go | |
| SB-SEC-011 | `grant_table_permission(user, table, perm) -> int8` (`pysharkbite.cpp:604`, `PythonStructures.h:384`) | — | — | `test/python/TestSecurityOperations.py:38-40` | Missing Go | |
| SB-SEC-012 | `revoke_table_permission(user, table, perm) -> int8` (`pysharkbite.cpp:605`, `PythonStructures.h:397`) | — | — | `test/vandv/testSecurityOperations.h` | Missing Go | |
| SB-SEC-013 | `grant_namespace_permission(user, ns, perm) -> int8` (`pysharkbite.cpp:606`, `PythonStructures.h:412`) | — | — | — | Missing Go | |
| SB-SEC-014 | `revoke_namespace_permission(user, ns, perm) -> int8` (`pysharkbite.cpp:607`, `PythonStructures.h:426`) | — | — | — | Missing Go | |
| SB-SEC-015 | `SystemPermissions` enum: `GRANT`, `CREATE_TABLE`, `DROP_TABLE`, `ALTER_TABLE`, `CREATE_USER`, `ALTER_USER`, `SYSTEM`, `CREATE_NAMESPACE`, `DROP_NAMESPACE`, `ALTER_NAMESPACE` (`pysharkbite.cpp:403-413`) | — | — | `test/python/TestSecurityOperations.py:18-23` | Missing Go | Arithmetic (bitmask-capable) enum; numeric values must match Accumulo's wire ordinals. |
| SB-SEC-016 | `NamespacePermissions` enum: `READ`, `WRITE`, `ALTER_NAMESPACE`, `GRANT`, `ALTER_TABLE`, `CREATE_TABLE`, `DROP_TABLE`, `BULK_IMPORT`, `DROP_NAMESPACE` (`pysharkbite.cpp:415-424`) | — | — | — | Missing Go | |
| SB-SEC-017 | `TablePermissions` enum: `READ`, `WRITE`, `GRANT`, `ALTER_TABLE`, `DROP_TABLE`, `BULK_IMPORT` (`pysharkbite.cpp:426-432`) | — | — | `test/python/TestSecurityOperations.py:38-40` | Missing Go | |
| SB-SEC-018 | Authorization enforcement on scan (cells hidden without the label) | Server-side; Shoal passes `ScannerOptions.Authorizations` through | `shoal_scanner_config.authorizations` | `capi/tests/lifecycle.c:84-89` (validation only) | Behavior mismatch | No Shoal test proves that a cell with visibility `blah2` is hidden when scanning with only `blah1` — the exact assertion made by `test/python/TestAuthorizations.py:68-78`. Requires live-cluster conformance ([§24](#sec-24)). `internal/visfilter` implements the evaluator for the embedded path only. |
| SB-SEC-019 | Server-side authorization failures on write | `AuthorizationFailure` (`accumulo/batch_writer.go:109`) | `shoal_authorization_failure_view` (`capi/include/shoal_types.h:244-252`) | `TestBatchWriterSurfacesAccumuloUpdateErrors` (`accumulo/batch_writer_test.go:942`); `capi/tests/result_bridge.c:56-101` | Not required (rationale required) | Rationale: Shoal superset; Sharkbite silently discards these. |

<a id="sec-14"></a>

## 14. Matrix: cluster status and monitoring (`SB-STAT`)

Reached through `AccumuloConnector.getStatistics()` (`pysharkbite.cpp:268`) and
demonstrated by `examples/pythonstats.py`. **Every row is `Missing Go`**: no
manager-status RPC is exposed by `accumulo/`, and `internal/managerclient` has
no status operation.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-STAT-001 | `AccumuloInfo.getTableMap()` / `.table_map` (`pysharkbite.cpp:210-211`) | — | — | `examples/pythonstats.py` | Missing Go | Maps table ID → `TableInfo`. |
| SB-STAT-002 | `AccumuloInfo.getTabletServerInfo()` / `.tablet_server_info` (`pysharkbite.cpp:212-213`) | — | — | `examples/pythonstats.py` | Missing Go | |
| SB-STAT-003 | `AccumuloInfo.getBadTabletServers()` / `.bad_servers` (`pysharkbite.cpp:214-215`) | — | — | — | Missing Go | |
| SB-STAT-004 | `AccumuloInfo.getState()` / `.state` (`pysharkbite.cpp:216-217`) | — | — | — | Missing Go | Returns `CoordinatorState`. |
| SB-STAT-005 | `AccumuloInfo.getGoalState()` / `.goal_state` (`pysharkbite.cpp:218-219`) | — | — | — | Missing Go | Returns `CoordinatorGoalState`. |
| SB-STAT-006 | `AccumuloInfo.getUnassignedTablets()` / `.unassigned_tablets` (`pysharkbite.cpp:220-221`) | — | — | — | Missing Go | |
| SB-STAT-007 | `AccumuloInfo.getServerShuttingDown()` / `.servs_shutting_down` (`pysharkbite.cpp:222-223`) | — | — | — | Missing Go | |
| SB-STAT-008 | `AccumuloInfo.getDeadServers()` / `.dead_servers` (`pysharkbite.cpp:224-225`) | — | — | — | Missing Go | |
| SB-STAT-009 | `TabletServerStatus.getTableMap()` / `.table_map` (`pysharkbite.cpp:159-160`) | — | — | `examples/pythonstats.py` | Missing Go | |
| SB-STAT-010 | `TabletServerStatus.getLastContact()` / `.last_contact` (`pysharkbite.cpp:161-162`) | — | — | — | Missing Go | |
| SB-STAT-011 | `TabletServerStatus.getName()` / `.name` (`pysharkbite.cpp:163-164`) | — | — | `examples/pythonstats.py` | Missing Go | |
| SB-STAT-012 | `TabletServerStatus.getOsLoad()` / `.os_load` (`pysharkbite.cpp:165-166`) | — | — | — | Missing Go | |
| SB-STAT-013 | `TabletServerStatus.getLookups()` / `.lookups` (`pysharkbite.cpp:167-168`) | — | — | — | Missing Go | |
| SB-STAT-014 | `TabletServerStatus.getIndexCacheHits()` / `.index_cache_hits` (`pysharkbite.cpp:169-170`) | — | — | — | Missing Go | |
| SB-STAT-015 | `TabletServerStatus.getDataCacheHits()` / `.data_cache_hits` (`pysharkbite.cpp:171-172`) | — | — | — | Missing Go | |
| SB-STAT-016 | `TabletServerStatus.getDataCacheRequests()` / `.data_cache_requests` (`pysharkbite.cpp:173-174`) | — | — | — | Missing Go | |
| SB-STAT-017 | `TabletServerStatus.getLogSorts()` / `.log_sorts` (`pysharkbite.cpp:175-176`) | — | — | — | Missing Go | |
| SB-STAT-018 | `TabletServerStatus.getFlushes()` / `.flushes` (`pysharkbite.cpp:177-178`) | — | — | — | Missing Go | |
| SB-STAT-019 | `TabletServerStatus.getSyncs()` / `.syncs` (`pysharkbite.cpp:179-180`) | — | — | — | Missing Go | |
| SB-STAT-020 | `TabletServerStatus.getHoldTime()` / `.hold_time` (`pysharkbite.cpp:181-182`) | — | — | — | Missing Go | |
| SB-STAT-021 | `TableInfo.getRecords()` / `.records` (`pysharkbite.cpp:145-146`) | — | — | — | Missing Go | |
| SB-STAT-022 | `TableInfo.getRecordsInMemory()` / `.records_in_memory` (`pysharkbite.cpp:147-148`) | — | — | — | Missing Go | |
| SB-STAT-023 | `TableInfo.getTablets()` / `.tablets` (`pysharkbite.cpp:149-150`) | — | — | — | Missing Go | |
| SB-STAT-024 | `TableInfo.getOnlineTablets()` / `.online_tables` (`pysharkbite.cpp:151-152`) | — | — | — | Missing Go | Note the property name is `online_tables` while the getter says tablets — preserve both spellings. |
| SB-STAT-025 | `TableInfo.getTableRates()` / `.table_rates` (`pysharkbite.cpp:153-154`) | — | — | — | Missing Go | |
| SB-STAT-026 | `TableInfo.getCompactioninfo()` / `.compaction_info` (`pysharkbite.cpp:155-156`) | — | — | `examples/pythonstats.py` | Missing Go | Note the lowercase `i` in `getCompactioninfo`. |
| SB-STAT-027 | `TableRates.getIngestRate()`, `getIngestRateByte()`, `getQueryRate()`, `getQueryRateByte()`, `getScanRate()` (`pysharkbite.cpp:109-119`) | — | — | — | Missing Go | |
| SB-STAT-028 | `TableRates.query_rate_byte` / `.scan_rate` read-only properties (`pysharkbite.cpp:110-120`) | — | — | — | Not required (rationale required) | Rationale: `query_rate_byte` is registered four times over the same member (`pysharkbite.cpp:110,112,114,116`) and no `ingest_rate*` property is registered at all. Reproduce the working subset only; see [§21](#sec-21). |
| SB-STAT-029 | `TableCompactions.getMinorCompactions()` / `.minors` (`pysharkbite.cpp:137-138`) | — | — | — | Missing Go | |
| SB-STAT-030 | `TableCompactions.getMajorCompactions()` / `.majors` (`pysharkbite.cpp:139-140`) | — | — | — | Missing Go | |
| SB-STAT-031 | `TableCompactions.getScans()` / `.scans` (`pysharkbite.cpp:141-142`) | — | — | `examples/pythonstats.py` | Missing Go | |
| SB-STAT-032 | `Compacting.getRunning()` / `.running` (`pysharkbite.cpp:122-123`) | — | — | `examples/pythonstats.py` | Missing Go | |
| SB-STAT-033 | `Compacting.getQueued()` / `.queued` (`pysharkbite.cpp:124-125`) | — | — | `examples/pythonstats.py` | Missing Go | |
| SB-STAT-034 | `RecoveryStatus.getName()` / `.name`, `getRuntime()` / `.runtime`, `getProgress()` / `.progress` (`pysharkbite.cpp:129-134`) | — | — | — | Missing Go | |
| SB-STAT-035 | `DeadServer.getServer()` / `.server`, `getLastContact()` / `.last_contact`, `getStatus()` / `.status` (`pysharkbite.cpp:187-192`) | — | — | — | Missing Go | |
| SB-STAT-036 | `CoordinatorGoalState` enum: `CLEAN_STOP`, `SAFE_MODE`, `NORMAL` (`pysharkbite.cpp:194-197`) | — | — | — | Missing Go | |
| SB-STAT-037 | `CoordinatorState` enum: `INITIAL`, `HAVE_LOCK`, `SAFE_MODE`, `NORMAL`, `UNLOAD_METADATA_TABLETS`, `UNLOAD_ROOT_TABLET`, `STOP` (`pysharkbite.cpp:200-207`) | — | — | — | Missing Go | |
| SB-STAT-038 | `pybind11::dynamic_attr()` on all status classes (`pysharkbite.cpp:108,121,128,136,144,158,186,209`) | — | — | — | Missing Go | Users may attach arbitrary attributes to status objects; a shim built on `__slots__` would break that. |

<a id="sec-15"></a>

## 15. Matrix: RFile, iterators, streams, and higher-level data helpers (`SB-RFILE`)

Rows 24 onward record the optional helper layers built on top of scanning.
`sharkbite.torch` is part of the pinned `sharkbite` package; `pandashark` is
present in the pinned repository but not shipped by `packages=['sharkbite']`,
so its rows are tracked for exhaustiveness and currently classified
`Not required` unless packaging scope changes.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-RFILE-001 | `RFileOperations.openForWrite(path) -> SequentialRFile` (`pysharkbite.cpp:589`, `include/data/constructs/rfile/RFileOperations.h:43`) | `internal/rfile.NewWriter(out io.Writer, opts WriterOptions)` (`internal/rfile/writer.go:143`) — **internal** | — | `test/python/TestRFileWrites.py:35-43` | Missing Go | Shoal has a complete RFile writer but does not export it. Exporting it (or a thin `rfile` public package) is the prerequisite. |
| SB-RFILE-002 | `RFileOperations.sequentialRead(path) -> SequentialRFile` (`pysharkbite.cpp:590` → `openSequential`, `RFileOperations.h:46`) | `internal/rfile` reader — **internal** | — | `test/python/TestRFileWrites.py:54` | Missing Go | Note the Python name (`sequentialRead`) differs from the C++ name (`openSequential`); the Python name is the contract. |
| SB-RFILE-003 | `RFileOperations.randomSeek(path) -> RFile` (`pysharkbite.cpp:588` → `open`, `RFileOperations.h:45`) | `internal/rfile` reader — **internal** | — | — | Missing Go | Returns a **raw owning pointer** in C++ (`RFile *`), unlike the other four overloads. See [§21](#sec-21). |
| SB-RFILE-004 | `RFileOperations.openManySequential(paths, versions=0, withDeletes=False, propogate=True, maxtimestamp=0)` (`pysharkbite.cpp:591`, `RFileOperations.h:55`) | `internal/rfile` + `internal/iterrt` merge iterators — **internal** | — | — | Missing Go | Defaults are C++-side and not declared to pybind11, so all five arguments are required from Python. |
| SB-RFILE-005 | `RFileOperations` methods are bound with `.def`, not `.def_static`, yet called as `RFileOperations.openForWrite(path)` (`pysharkbite.cpp:587-591`) | n/a | n/a | `test/python/TestRFileWrites.py:35` | Behavior mismatch | They work only as unbound class calls; calling them on an instance fails. The shim should expose real `@staticmethod`s, which is a superset of the working call form. |
| SB-RFILE-006 | `RFile.seek(StreamRelocation)` (`pysharkbite.cpp:499` → `relocate`) | — | — | `test/python/TestRFileWrites.py:59` | Missing Go | |
| SB-RFILE-007 | `RFile.hasNext() -> bool` (`pysharkbite.cpp:500`) | — | — | `test/python/TestRFileWrites.py:61` | Missing Go | |
| SB-RFILE-008 | `RFile.getTop() -> KeyValue` (`pysharkbite.cpp:501`) | — | — | — | Missing Go | |
| SB-RFILE-009 | `RFile.next()` (`pysharkbite.cpp:503`) | — | — | `test/python/TestRFileWrites.py:64` | Missing Go | Raw `next` with no `StopIteration` conversion (unlike `SequentialRFile.next`). |
| SB-RFILE-010 | `RFile.close()` (`pysharkbite.cpp:502`) | — | — | — | Missing Go | |
| SB-RFILE-011 | `SequentialRFile.append(KeyValue) -> bool` (`pysharkbite.cpp:530`) | `internal/rfile.Writer` — **internal** | — | `test/python/TestRFileWrites.py:41` | Missing Go | |
| SB-RFILE-012 | `SequentialRFile.addLocalityGroup(...)` (`pysharkbite.cpp:529`) | `internal/rfile` locality-group support — **internal** | — | — | Missing Go | |
| SB-RFILE-013 | `SequentialRFile.seek(Seekable)` (`pysharkbite.cpp:524`) | — | — | `test/python/TestRFileWrites.py:59` | Missing Go | |
| SB-RFILE-014 | `SequentialRFile.hasNext()` / `getTop()` / `getTopKey()` / `getTopValue()` / `close()` (`pysharkbite.cpp:525-528,531`) | — | — | `test/python/TestRFileWrites.py:61-64` | Missing Go | |
| SB-RFILE-015 | `SequentialRFile.next()` and `__next__` raising `StopIteration` at exhaustion (`pysharkbite.cpp:532-542`) | — | — | `test/python/TestRFileWrites.py:64` | Missing Go | Both spellings exist and behave identically. |
| SB-RFILE-016 | `KeyValueIterator()` constructor (`pysharkbite.cpp:507`) | `internal/iterrt.SortedKeyValueIterator` — **internal** | — | — | Missing Go | Sharkbite lets Python subclass this to implement iterators. |
| SB-RFILE-017 | `KeyValueIterator.seek` / `hasNext` / `getTopKey` / `getTopValue` / `next` / `__next__` (`pysharkbite.cpp:508-522`) | `internal/iterrt.SortedKeyValueIterator` methods — **internal** | — | — | Missing Go | Shoal's internal interface is a near-1:1 shape match (`Seek`, `Next`, `HasTop`, `GetTopKey`, `GetTopValue`, `DeepCopy`), so the Go-parity work is export plus adaptation, not new logic. |
| SB-RFILE-018 | `Seekable(Range)` (`pysharkbite.cpp:581`) | `internal/iterrt.Range` — **internal** | `shoal_range` (scan-only) | `test/python/TestRFileWrites.py:56` | Missing Go | |
| SB-RFILE-019 | `Seekable(Range, list[str], bool)` — column families + inclusive flag (`pysharkbite.cpp:582`) | `internal/iterrt` `Seek(r, columnFamilies, inclusive)` — **internal** | — | — | Missing Go | |
| SB-RFILE-020 | `Seekable.getRange()` / `getColumnFamilies()` / `isInclusive()` (`pysharkbite.cpp:583-585`) | — | — | — | Missing Go | |
| SB-RFILE-021 | `StreamRelocation` opaque base type (`pysharkbite.cpp:578`) | — | — | — | Missing Go | Registered with no methods; exists only as the base class of `Seekable`. |
| SB-RFILE-022 | Offline/standalone RFile compaction | `internal/offlinecompact.Run` + `cmd/shoal-offline-compact` — **internal/CLI** | — | `cmd/shoal-offline-compact/main_test.go` | Not required (rationale required) | Rationale: no Sharkbite equivalent (`examples/` has a `Compact` C++ target only). Recorded because it demonstrates Shoal already owns the RFile write path. |
| SB-RFILE-023 | RFile compression codecs (`compressor.h`, `zlibCompressor.h` included by `pysharkbite.cpp:33-34`) | `internal/rfile.WriterOptions.Codec` — **internal** | — | — | Not required (rationale required) | Rationale: the compressor classes are included but never bound to Python; not part of the Python contract. |
| SB-RFILE-024 | `sharkbite.torch.AccumuloCluster(instance, zookeepers, username, password, table=None, auths=None)` (`sharkbite/torch.py:5-7`) | Compose [SB-CONN-014](#sec-7) | Compose `shoal_connector_create` | `sharkbite/torch.py:5-7`; `examples/torchexample.py` | Missing C ABI | Thin `IterableDataset` wrapper over `AccumuloBase`; it adds no scan behavior on its own. |
| SB-RFILE-025 | `sharkbite.torch.AccumuloDataset(instance, zookeepers, username, password, table, auths, start_key_string, end_key_string, user_lambda=None)` (`sharkbite/torch.py:14-22`) | Compose `Range`, `tableOps.createScanner`, and range buffering | Compose scanner creation plus range buffering | `sharkbite/torch.py:14-22`; `examples/torchexample.py` | Missing C ABI | Constructor eagerly creates a scanner, buffers one closed-open `Range`, and stores an optional coercion lambda. |
| SB-RFILE-026 | `AccumuloDataset.coerce(key)` (`sharkbite/torch.py:24-36`) | n/a | n/a | `sharkbite/torch.py:24-36`; `examples/torchexample.py:73` | Missing C ABI | Public helper method. The `user_lambda` branch is the only working path shown by the pinned example; the default branch is defective (`key.getKey().getValue().get()` cannot succeed on a normal `KeyValue`) and is recorded in [§21](#sec-21). |
| SB-RFILE-027 | `AccumuloDataset.__iter__` / `__next__` (`sharkbite/torch.py:38-61`) | Compose `Results.__iter__` and scanner cleanup on exhaustion | Compose scan result iteration plus explicit close on exhaustion | `sharkbite/torch.py:38-61`; `examples/torchexample.py:76` | Missing C ABI | Iteration is lazy only after the constructor's eager scanner setup. On `StopIteration` it closes the scanner and nils both iterator handles. |
| SB-RFILE-028 | `AccumuloValueDataset(cluster, table, start_key_string, end_key_string)` (`sharkbite/torch.py:64-66`) | — | — | `sharkbite/torch.py:64-66` | Not required (rationale required) | Rationale: upstream defect, not a contract. The pinned constructor calls `super.__init__` instead of `super().__init__`, so it never initializes the base dataset state. See [§21](#sec-21). |
| SB-RFILE-029 | `AccumuloValueDataset.coerce(key)` (`sharkbite/torch.py:68-69`) | — | — | `sharkbite/torch.py:68-69` | Not required (rationale required) | Rationale: reachable only through the broken constructor above. If this helper is intentionally brought into scope later, define it afresh alongside a working constructor. |
| SB-RFILE-030 | `pandashark.read_accumulo(connector, ranges, columns=None, index_col=None, chunksize=1000)` overload (`pandashark/__init__.py:51-61`) | — | — | `pandashark/__init__.py:51-61` | Not required (rationale required) | Rationale: separate `pandashark` package boundary (see SB-PKG-013). This overload calls `connector.to_scanner()`, `fetch_ranges(ranges)`, then dispatches to the scanner overload. |
| SB-RFILE-031 | `pandashark.read_accumulo(scanner, columns=None, index_col=None, chunksize=1000)` overload (`pandashark/__init__.py:64-98`) | — | — | `pandashark/__init__.py:64-98`; `examples/dataframe.py:68-69` | Not required (rationale required) | Rationale: separate `pandashark` package boundary. The pinned helper is also defective: it calls `scanner.get(chunksize)`, so `chunksize` is passed as `begin_row` instead of controlling DataFrame batch size. See [§21](#sec-21). |
| SB-RFILE-032 | `pandashark.read_accumulo_nex(iterator, columns=None, index_col=None, chunksize=1000)` (`pandashark/__init__.py:100-110`) | — | — | `pandashark/__init__.py:100-110` | Not required (rationale required) | Rationale: separate `pandashark` package boundary. The helper is only a one-step alias over `iterator.__next__()`. |
| SB-RFILE-033 | `pandashark.pandassharkbite.DataFrameIterator(iterator)` / `__iter__` / `get_columns()` (`pandashark/pandassharkbite.py:36-52`) | — | — | `pandashark/pandassharkbite.py:36-52` | Not required (rationale required) | Rationale: separate `pandashark` package boundary. The constructor seeds a shared class-level `set` with `row`, `column`, `visibility`, and `value`, then iteration delegates straight to the wrapped iterator. |
| SB-RFILE-034 | `DataFrameIterator.__next__()` DataFrame conversion (`pandashark/pandassharkbite.py:54-73`) | — | — | `pandashark/pandassharkbite.py:54-73` | Not required (rationale required) | Rationale: separate `pandashark` package boundary. At exhaustion it calls `nextBatch()` on the wrapped iterator and returns the DataFrame accumulated so far (possibly empty) instead of raising `StopIteration`. |

<a id="sec-16"></a>

## 16. Matrix: HDFS (`SB-HDFS`)

Sharkbite ships a full HDFS client to Python via vendored `libhdfs3`. Shoal has
an HDFS **storage backend** (`internal/storage/hdfs.Backend`,
`internal/storage/hdfs/hdfs.go:65`) built on `colinmarc/hdfs`, exercised by
`make test-hdfs` and `.github/workflows/hdfs-integration.yml`, but it is
internal, is a `storage.Backend` (open/read/create/list/remove), and is not a
Java-`DataInput`-compatible stream API.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-HDFS-001 | `Hdfs(namenode, port)` (`pysharkbite.cpp:568`) | `internal/storage/hdfs.Backend` with `Option`s — **internal** | — | `examples/hdfslist.py`; `internal/storage/hdfs/hdfs_test.go` | Missing Go | Shoal configures via `HADOOP_CONF_DIR`, not host+port. |
| SB-HDFS-002 | `Hdfs.list(path) -> list[HdfsDirEnt]` (`pysharkbite.cpp:576`) | `storage.Lister.List` — **internal** | — | `examples/hdfslist.py` | Missing Go | |
| SB-HDFS-003 | `Hdfs.write(path) -> HdfsOutputStream` (`pysharkbite.cpp:569`) | `storage.WritableBackend.Create` — **internal** | — | `examples/hdfswrite.py` | Missing Go | |
| SB-HDFS-004 | `Hdfs.read(path) -> HdfsInputStream` (`pysharkbite.cpp:570`) | `storage.Backend.Open` → `storage.File` (`ReaderAt`) — **internal** | — | `examples/hdfswrite.py` | Missing Go | Shoal exposes `ReaderAt`, not a sequential typed stream. |
| SB-HDFS-005 | `Hdfs.remove(path, recursive)` (`pysharkbite.cpp:571`) | `storage.Remover.Remove` — **internal** | — | `examples/hdfswrite.py` | Missing Go | |
| SB-HDFS-006 | `Hdfs.rename(from, to)` (`pysharkbite.cpp:572`) | — | — | — | Missing Go | |
| SB-HDFS-007 | `Hdfs.move(from, to)` (`pysharkbite.cpp:573`) | — | — | — | Missing Go | |
| SB-HDFS-008 | `Hdfs.chown(path, owner, group)` (`pysharkbite.cpp:574`) | — | — | — | Missing Go | |
| SB-HDFS-009 | `Hdfs.mkdir(path)` (`pysharkbite.cpp:575`) | — | — | — | Missing Go | |
| SB-HDFS-010 | `HdfsDirEnt.getName()` / `getOwner()` / `getGroup()` / `getSize()` / `__str__` (`pysharkbite.cpp:544-551`) | — | — | `examples/hdfslist.py` | Missing Go | `__str__` format is `"owner group size name"`. |
| SB-HDFS-011 | `HdfsOutputStream.writeShort` / `writeInt` / `writeLong` / `writeString` / `write(buf, len)` (`pysharkbite.cpp:553-557`) | `internal/rfile/wire` Java `DataOutput` primitives — **internal** | — | `examples/hdfswrite.py` | Missing Go | Big-endian Java `DataOutput` framing; `writeString` is length-prefixed. |
| SB-HDFS-012 | `HdfsInputStream.readShort` / `readInt` / `readLong` / `readString` / `readBytes(buf, len)` (`pysharkbite.cpp:560-564`) | `internal/rfile/wire` Java `DataInput` primitives — **internal** | — | `examples/hdfswrite.py` | Missing Go | |
| SB-HDFS-013 | HDFS cancellation and deadlines | `internal/storage` backends take `context.Context`; issue [#10](https://github.com/phrocker/shoal-oss/issues/10) / PR [#85](https://github.com/phrocker/shoal-oss/pull/85) complete propagation | — | `make test-hdfs`; `.github/workflows/hdfs-integration.yml` | Missing Go | Sharkbite has no cancellation at all. Landing #10 first prevents the Python layer from inheriting an uninterruptible HDFS path. |
| SB-HDFS-014 | Kerberos / secure HDFS | `internal/storage/hdfs` live tests (issue [#9](https://github.com/phrocker/shoal-oss/issues/9), merged) | — | `internal/storage/hdfs/hdfs_live_test.go` | Missing Go | Sharkbite's vendored `libhdfs3` supports GSSAPI; parity claims need an explicit statement. |

<a id="sec-17"></a>

## 17. Matrix: logging (`SB-LOG`)

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-LOG-001 | `LoggingConfiguration.enableDebugLogger()` static (`pysharkbite.cpp:66`) | — (no exported logging control in `accumulo/`) | — | `test/python/TestRanges.py:12` (commented), `test/python/TestHedgedReads.py:12` | Missing Go | Global side effect: enables debug logging for all classes. |
| SB-LOG-002 | `LoggingConfiguration.enableTraceLogger()` static (`pysharkbite.cpp:67`) | — | — | `test/python/TestRFileWrites.py:50`; `test/python/TestHedgedReads.py:12` | Missing Go | |
| SB-LOG-003 | Structured/injectable logging | `internal/metadata.Walker.WithLogger`, `internal/obs` — **internal** | — | — | Behavior mismatch | Shoal's logging is injected per component rather than toggled globally. A compatible shim needs a process-global switch that maps onto whatever the public Go API eventually exposes. |

<a id="sec-18"></a>

## 18. Matrix: exceptions and error mapping (`SB-ERR`)

Python-visible exceptions are a strict subset of Sharkbite's C++ hierarchy.
`pysharkbite.cpp` registers only `TApplicationException` and `ClientException`;
all other C++ exception types below are C++-scope unless a row explicitly says
otherwise, and Python sees a generic `RuntimeError` when one escapes the
binding.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-ERR-001 | `pysharkbite.ClientException` registered as a Python exception with a translator (`pysharkbite.cpp:613-621`) | Error values, no exception type | `shoal_error` + `shoal_error_code` + `shoal_error_message` (`capi/include/shoal.h:224-237`) | `capi/tests/lifecycle.c:9-16`; `TestStatusForWriterErrors` (`cmd/shoal-capi/writer_export_test.go:67`) | Missing C ABI | The shim must define `sharkbite.ClientException` and raise it for every mapped status. Pinned tests catch it by name (`test/python/TestBadOperations.py:63`, `test/python/TestSecurityOperations.py:115`). |
| SB-ERR-002 | C++ `ClientException.getErrorCode()` with `CLIENT_ERROR_CODES` (0–13, including `TABLE_NOT_FOUND`, `RANGE_NOT_SPECIFIED`, `SCANNER_ALREADY_STARTED`) (`include/data/exceptions/ClientException.h:39,50-51`) | Sentinel errors (`accumulo/errors.go:5-52`) | `shoal_status` enum, 0–20 + 255 (`capi/include/shoal_types.h:98-119`) | `test/vandv/invalidscans.h`; `pysharkbite.cpp:613-619`; `TestStatusForTableAdministrationErrors` (`cmd/shoal-capi/table_admin_export_test.go:10`); `capi/tests/lifecycle.c:30-33,49-52,203-230` | Behavior mismatch | `getErrorCode()` is **not** Python-visible in the pinned binding; the numeric mapping matters for the C++/flat-C replacement claim and the port of `test/vandv/invalidscans.h`, not because Python ever saw the method. |
| SB-ERR-003 | `pysharkbite.TApplicationException` (Thrift) registered (`pysharkbite.cpp:612`) | Thrift types are internal by design (`accumulo/batch_writer.go:116` comment) | Mapped to `shoal_status` | `TestStatusForWriterErrors` (`cmd/shoal-capi/writer_export_test.go:67`) | Intentional divergence (approval required) | Shoal deliberately never leaks generated Thrift types. Any Sharkbite user catching `TApplicationException` will not see it. Requires approval plus a documented replacement (`ClientException` with a transport status). |
| SB-ERR-004 | Table not found | `ErrTableNotFound` (`accumulo/errors.go:15`) | `SHOAL_STATUS_NOT_FOUND` (9) via `shoal_connector_delete_table` (`capi/include/shoal_types.h:107`; `capi/include/shoal.h:114`) | `TestTableMutationsMapErrorsAndLifecycle` (`accumulo/table_admin_test.go:263`); `TestStatusForTableAdministrationErrors` (`cmd/shoal-capi/table_admin_export_test.go:10`); `capi/tests/lifecycle.c:212-215` | Covered | The status is now reachable for table-admin ABI calls. The remaining gap is the Python `ClientException` shim from [SB-ERR-001](#sec-18), not the ABI status itself. |
| SB-ERR-005 | Permission denied / `ThriftSecurityException` (`test/vandv/testSecurityOperations.h`) | `ErrPermissionDenied` (`accumulo/errors.go:32`) | `SHOAL_STATUS_PERMISSION_DENIED` (10) | — | Behavior mismatch | No named Shoal test asserts this mapping end to end; live-cluster conformance required. |
| SB-ERR-006 | `NotServingException` → transparent relocation (`include/data/exceptions/NotServingException.h:22`) | `isStaleScanError` + retry (`accumulo/scanner.go:350`) | Implicit — no separate ABI surface | `TestScannerRetriesNotServingAssignmentOnce` (`accumulo/scanner_test.go:294`) | Covered | Neither surfaces the exception to the user. |
| SB-ERR-007 | Validation failures exposed to Python as generic `RuntimeError` (underlying C++ type `IllegalArgumentException`) (`include/data/exceptions/IllegalArgumentException.h:23`; `pysharkbite.cpp:613-619`) | Plain `errors.New`/`fmt.Errorf` validation errors | `SHOAL_STATUS_INVALID_ARGUMENT` (1) | `test/python/TestBadOperations.py:67-79`; `capi/tests/lifecycle.c:77-96,116-142` | Missing C ABI | Sharkbite does **not** bind `IllegalArgumentException` as a Python class; broad `except RuntimeError` blocks are the observable contract. If the shim later raises `ClientException` or `ValueError` instead, record it as a divergence. |
| SB-ERR-008 | Lifecycle failures exposed to Python as generic `RuntimeError` (underlying C++ type `IllegalStateException`) (`include/data/exceptions/IllegalStateException.h:23`; `pysharkbite.cpp:613-619`) | `ErrBatchWriterClosed`, `ErrConnectorClosed` (`accumulo/batch_writer.go:27`, `accumulo/errors.go:51`) | `SHOAL_STATUS_CLOSED` (6) | `TestBatchWriterCloseLifecycle` (`accumulo/batch_writer_test.go:852`); `capi/tests/lifecycle.c:233-243` | Missing C ABI | As with SB-ERR-007, the Python-visible contract is a bare `RuntimeError`, not a bound `IllegalStateException` class. |
| SB-ERR-009 | `HDFSException` (`include/data/exceptions/HDFSException.h:24`) | `storage.ErrNotFound`, `storage.ErrReadOnly` — **internal** | — | — | Missing Go | Blocked with the rest of HDFS ([§16](#sec-16)). |
| SB-ERR-010 | Visibility parse failures exposed to Python as generic `RuntimeError` (underlying C++ type `VisibilityParseException`, with private `terms`/`offset`) (`include/data/exceptions/VisibilityParseException.h:23`; `pysharkbite.cpp:613-619`) | `internal/visfilter` parse errors — **internal** | — | — | Missing Go | `terms` and `offset` are C++-only in the pinned release; Python never receives them. If Shoal later exposes visibility-parse detail, treat it as additive, not required Sharkbite parity. |
| SB-ERR-011 | Iteration interruption exposed to Python as generic `RuntimeError` (underlying C++ type `IterationInterruptedException`) (`include/data/exceptions/InterationInterruptedException.h:23` — filename misspelled upstream; `pysharkbite.cpp:613-619`) | `context.Canceled` propagation | `SHOAL_STATUS_CANCELLED` (7) | `TestScannerCancellationStillClosesServerScan` (`accumulo/scanner_test.go:396`) | Missing C ABI | The pinned binding does not register a Python `IterationInterruptedException`; cancellation observed from Python would be a bare `RuntimeError`. |
| SB-ERR-012 | `APIException` (`include/interconnect/exceptions/APIException.h:23`) | — | — | — | Not required (rationale required) | Rationale: thrown only from the flat C API glue ([§19](#sec-19)), which is not part of the Python contract. |
| SB-ERR-013 | `JavaException` (`include/jni/JavaException.h:36`) | — | — | — | Not required (rationale required) | Rationale: only reachable through the JNI iterator build (`PYTHON_ITERATOR_SUPPORT`), which is tracked by SB-SCAN-016. |
| SB-ERR-014 | Python code catching bare `RuntimeError` alongside `ClientException` (`test/python/TestBadOperations.py:63,71,79,94`) | n/a | n/a | `test/python/TestBadOperations.py` | Missing C ABI | Because only `ClientException`/`TApplicationException` are registered, real Sharkbite code catches both named and bare `RuntimeError`. The shim must ensure `ClientException` derives from `RuntimeError` so existing broad catches keep working. |
| SB-ERR-015 | Errors carry no server identity | `MutationRejectionError.Server`, `BatchWriterCleanupError.Server`, `CleanupError.ScanID` (`accumulo/batch_writer.go:117,136`, `accumulo/scanner.go:117`) | `shoal_*_view.server` fields | `TestMalformedUpdateErrorsIncludeTabletServer` (`accumulo/batch_writer_test.go:993`) | Not required (rationale required) | Rationale: Shoal superset; additive detail on the exception object. |

<a id="sec-19"></a>

## 19. Matrix: C++ and flat-C surfaces used by tests and examples (`SB-CPP`)

Not Python-bound, but in scope for sharkbite#108 because pinned tests, examples,
and the shipped `capi` target consume them. These rows do not gate the Python
wheel, but they do gate any claim that Shoal replaces Sharkbite as a whole.

| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| SB-CPP-001 | Flat C API `create_connector` / `free_connector` (`include/extern/accumulo.h:111`) | `NewConnector` / `Close` | `shoal_connector_create` / `shoal_connector_free` (`capi/include/shoal.h:30,47`) | `TestNewConnectorLifecycle` (`accumulo/connector_test.go:9`); `capi/tests/lifecycle.c:54-57,262-264` | Covered | Shoal's ABI is a strict improvement (opaque handles, structured errors, `struct_size` versioning). |
| SB-CPP-002 | Flat C API `open_table` / `create_table` / `drop_table` / `free_table` (`include/extern/accumulo.h:117-119`) | `CreateTable` / `DeleteTable` / `TableExists` (`accumulo/table_admin.go:76,96,53`) | `shoal_connector_create_table` / `shoal_connector_delete_table` / `shoal_connector_table_exists` (`capi/include/shoal.h:104,109,114`) | `TestTableAdministrationLifecycleAndCancellation` (`accumulo/table_admin_test.go:62`); `capi/tests/lifecycle.c:185-217` | Behavior mismatch | Shoal exposes connector-plus-table-name calls and has no bound table handle analogous to `open_table` / `free_table`. Create/drop parity is present; the object model is not. |
| SB-CPP-003 | Flat C API `createMutation` / `put` / `freeMutation` (`include/extern/accumulo.h:125-127`) | `NewMutation` / `Put` | `shoal_mutation_create` / `shoal_mutation_put` / `shoal_mutation_free` (`capi/include/shoal.h:115,119,148`) | `TestMutationPublicModel` (`accumulo/mutation_test.go:8`); `capi/tests/lifecycle.c:144-176` | Covered | Sharkbite's `put(CMutation*, char*, char*, char*)` is NUL-terminated and therefore binary-unsafe; Shoal's `shoal_bytes` carries an explicit length. |
| SB-CPP-004 | Flat C API `createWriter` / `addMutation` / `closeWriter` (`include/extern/accumulo.h:133,149`) | `NewBatchWriter` / `Add` / `Close` | `shoal_connector_create_batch_writer` / `shoal_batch_writer_add` / `shoal_batch_writer_close` | `TestBatchWriterCloseLifecycle` (`accumulo/batch_writer_test.go:852`); `capi/tests/lifecycle.c:127-199` | Covered | |
| SB-CPP-005 | Flat C API `createScanner` / `addRange` / `addRanges` / `hasNext` / `onNext` / `nextMany` / `closeScanner` (`include/extern/accumulo.h:135-147`) | `NewScanner` / `Scan`, `NewBatchScanner` / `Scan` | `shoal_connector_create_scanner`, `shoal_scanner_scan`, `shoal_batch_scanner_scan`, `shoal_scanner_close` | `capi/tests/lifecycle.c:59-114`; `capi/tests/result_bridge.c:14-48` | Behavior mismatch | Sharkbite's flat C API is a **cursor** (`hasNext`/`onNext`/`nextMany`); Shoal's ABI is a single materialized result. Same structural gap as SB-SCAN-004. |
| SB-CPP-006 | Flat C structs `CKey` / `CValue` / `CKeyValue` / `KeyValueList` / `CRange` / `CAuthorizations` (`include/extern/accumulo_data.h`) | `Key`, `KeyValue`, `Range`, `Column` | `shoal_key`, `shoal_key_value_view`, `shoal_range`, `shoal_bytes` (`capi/include/shoal_types.h:63-188`) | `TestKeyRangePreservesFullBounds` (`accumulo/range_test.go:8`); `capi/tests/result_bridge.c:25-43` | Covered | Shoal's views are borrowed with documented lifetimes; Sharkbite's structs are caller-allocated with `populateKey` copying into them. |
| SB-CPP-007 | Flat C API `createKey` / `toKey` / `populateKey` (`include/extern/accumulo.h:159`) | — | `shoal_scan_result_get` fills a caller-owned view (`capi/include/shoal.h:103-106`) | `capi/tests/result_bridge.c:25-43` | Covered | ABI-only row: Shoal has no Go-level analogue because Go callers receive `KeyValue` values directly. |
| SB-CPP-008 | `TableOperations<K,V>::createScanner` / `createWriter` returning `unique_ptr`, `createSharedScanner` / `createSharedWriter` returning `shared_ptr` (`include/interconnect/tableOps/TableOperations.h:129,138,147,156`) | Go pointers with GC | Opaque handles with explicit free | `TestBatchWriterCloseLifecycle` (`accumulo/batch_writer_test.go:852`); `capi/tests/lifecycle.c:113-114,181-182` | Covered | Dual ownership models collapse into one safe model. |
| SB-CPP-009 | `ClientExample.h` demo class (`include/ClientExample.h`) | — | — | — | Not required (rationale required) | Rationale: documentation sample, not API. |
| SB-CPP-010 | C++ integration drivers (`test/vandv/IntegrationTest.cpp`, `bigwrite.h`, `deletetest.h`, `invalidscans.h`, `invalidtableops.h`, `testAuthsChange.h`, `testEmptyRange.h`, `testScanLocationMove.h`, `testSecurityOperations.h`, `testWriteRead.h`) | — | — | — | Missing Go | These encode the real behavioral contract (error codes, tablet moves, auth changes mid-session, empty ranges). They must be ported into Shoal's live-cluster conformance suite; see [§24](#sec-24). |
| SB-CPP-011 | Catch2 unit tests (`test/constructs/TestConstructs.cpp`, `TestColumn.cpp`, `TestSecurity.cpp`, `TestStreams.cpp`, `rfile_test.cpp`, `zlibTest.cpp`) | `accumulo/*_test.go`, `internal/rfile`, `internal/visfilter` tests | `capi/tests/*.c` | 90 Go test functions in `accumulo/`; C tests compiled by `TestSharedLibraryCABI` (`cmd/shoal-capi/cabi_test.go:13`) | Covered | Shoal's unit coverage of the shared constructs is at least as strong; the gap is integration coverage, not unit coverage. |
| SB-CPP-012 | Mock Accumulo service (`test/services/mockAccumulo.cpp`, `TestServer.h`) | Fake adapters in `accumulo/*_test.go` | Test seams (`cmd/shoal-capi/writer_test_seam.go`, `capi/tests/test_seam.h`) | `TestBatchWriterSurfacesAccumuloUpdateErrors` (`accumulo/batch_writer_test.go:942`); `capi/tests/lifecycle.c:178-243` | Covered | Sharkbite's mock server is never built (`test/services/CMakeLists.txt` is empty); Shoal's seams work. |
| SB-CPP-013 | Java standalone-mini-Accumulo harness (`test/19x/st/**`) | — | — | — | Missing Go | Shoal has no ephemeral-cluster harness. This is the delivery vehicle for [§24](#sec-24). |
| SB-CPP-014 | `native-iterators-jni/` (JNI iterator support jar) | — | — | — | Missing Go | Server-side prerequisite for SB-SCAN-016. |

<a id="sec-20"></a>

## 20. Matrix: cross-cutting semantics (`SB-XCUT`)

| ID | Concern | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SB-XCUT-001 | Object lifecycle / ownership | `shared_ptr` holders for most types; `unique_ptr` from `createScanner`/`createWriter`; raw owning `RFile*` from `RFileOperations::open`; `ZookeeperInstance` owns raw `ZooCache`/`ZooKeeper` pointers deleted in its destructor | Go GC + explicit `Close` on `Connector`, `BatchWriter`, `Instance` | Opaque handles; `*_close` then `*_free`; free implies best-effort close and NULLs the caller's variable (`capi/README.md`) | `TestConnectorRegistryLifecycle` (`cmd/shoal-capi/state_test.go:13`); `capi/tests/lifecycle.c:257-264` | Covered | Shoal's model is safer and covers every Sharkbite lifetime. The Python shim owns the mapping from refcount semantics to explicit close. |
| SB-XCUT-002 | Double free / use after free | Undefined behavior | n/a | `shoal_*_free` is NULL-safe and idempotent; handles are validated (`SHOAL_STATUS_INVALID_HANDLE`) | `capi/tests/lifecycle.c:109-114,262-264`; `header_cpp_test.cpp:9-14` | Covered | |
| SB-XCUT-003 | Copied vs borrowed memory | `Key` holds raw `char*`/`uint8_t*` with `disownRow`/`disownColumnFamily` flags (`include/data/constructs/Key.h:215-227`); callers can be handed borrowed buffers | All public byte accessors return defensive copies (`cloneRow`, `accumulo/discovery.go:222`; `Column.Family()`, `accumulo/scanner.go:56`; `Mutation.Row()`, `accumulo/mutation.go:33`) | Inputs borrowed for the call only and copied if retained; outputs borrowed from the owning result until it is freed (`capi/include/shoal_types.h:109-111,177-179`) | `TestDiscoveryInvalidationAndDefensiveCopies` (`accumulo/discovery_test.go:148`); `TestEffectiveTablePropertiesConcurrentCopyIsolation` (`accumulo/table_property_reads_test.go:211`); `capi/tests/result_bridge.c:25-43` | Covered | The shim must copy out of `shoal_key_value_view` before freeing the result, or hold the result alive behind the Python objects. |
| SB-XCUT-004 | Binary safety (non-UTF-8, embedded NUL) | Broken in several places: `Value.get()`, all `Key.get*()` accessors, and the `Key(const char*...)` constructor. Only `Value.get_bytes()` is safe | `[]byte` everywhere; no transcoding anywhere in `accumulo/` | `shoal_bytes{data,length}` everywhere; no NUL termination | `TestBatchWriterRoutesBatchesAndCopiesMutation` (`accumulo/batch_writer_test.go:194`); `capi/tests/result_bridge.c:17-23` (embedded NUL + empty fields); `capi/tests/lifecycle.c:144-171` (embedded-NUL row and value) | Covered | Shoal is byte-exact on both layers. The compatibility risk is entirely in the shim: see [§21](#sec-21) and [§22](#sec-22). |
| SB-XCUT-005 | Empty vs absent | Sharkbite conflates `""` with "unbounded" in `Range` | `Range` distinguishes them; nil boundaries are preserved through discovery and multi-scan | `SHOAL_RANGE_BOUND_UNBOUNDED` is a distinct kind | `TestDiscoveryPreservesEmptyRowBoundaries` (`accumulo/discovery_test.go:185`); `TestMultiScanExtentIdentityPreservesNilBoundaries` (`accumulo/scanner_test.go:691`) | Behavior mismatch | The shim must map Sharkbite's `""`/`None` bounds onto `UNBOUNDED` to keep `test/python/TestRanges.py:120,147` semantics. |
| SB-XCUT-006 | Concurrency | Internal mutexes protect pools/queues; no documented guarantee that one `Scanner`/`Writer` is safe for concurrent **user** calls | `BatchWriter` is mutex-guarded; `Connector` discovery is concurrency-safe | "Scanner and batch-scanner handles support concurrent scan calls" (`capi/README.md`) | `TestBatchWriterConcurrentAdds` (`accumulo/batch_writer_test.go:595`); `TestOwnedScannerCloseIsConcurrentAndIdempotent` (`cmd/shoal-capi/state_test.go:75`) | Covered | Shoal is a superset. The shim must release the GIL around blocking ABI calls or it will serialize what Shoal parallelizes. |
| SB-XCUT-007 | Deadlines | Only the ZooKeeper session timeout | `context.Context` on every call | `timeout_ms` on scan, writer add/flush/close (`capi/include/shoal.h:90,161,167,177`) | `TestOwnedScannerDeadline` (`cmd/shoal-capi/state_test.go:89`); `TestOwnedBatchWriterCloseDeadlineBoundsActiveWait` (`cmd/shoal-capi/state_test.go:168`); `capi/tests/lifecycle.c:225-231` | Covered | Deadline exceeded is sticky on the writer (`capi/tests/lifecycle.c:225-231`) — a behavior the shim must expose rather than retry around. |
| SB-XCUT-008 | Cancellation | None | `context.Context` cancellation, with cleanup that survives cancellation | **No cancel handle.** Only deadlines and close (searched `shoal.h` for `cancel`: no such symbol) | `TestScannerCancellationStillClosesServerScan` (`accumulo/scanner_test.go:396`); `TestBatchWriterParallelCancellationWaitsForWorkers` (`accumulo/batch_writer_test.go:743`) | Missing C ABI | Without a cancel entry point, a Python `KeyboardInterrupt` cannot interrupt an in-flight scan; the interpreter will block until the deadline. This is a prerequisite for a usable Python client, not a nicety. |
| SB-XCUT-009 | Retry and ambiguity | Transport-level protocol fallback and server reshuffling only; no notion of an ambiguous commit | Bounded retries for provably safe failures; ambiguous failures are terminal and sticky | `SHOAL_STATUS_AMBIGUOUS_WRITE`, `SHOAL_STATUS_RETRY_EXHAUSTED`, `SHOAL_WRITE_FAILURE_*` flags | `TestBatchWriterPreAcceptanceRetryExhaustionIsNotSticky` (`accumulo/batch_writer_test.go:1318`); `TestBatchWriterRelocatesExplicitlyUncommittedSuffix` (`accumulo/batch_writer_test.go:1063`); `TestBatchWriterParallelRetryDoesNotReplaySuccessfulServer` (`accumulo/batch_writer_test.go:1137`); `capi/tests/lifecycle.c:201-223` | Covered | Semantics differ from Sharkbite but are strictly safer; they must be surfaced, not hidden, by the shim. |
| SB-XCUT-010 | Result iteration model | Streaming pull with background producers (`include/scanner/constructs/Results.h:93,300`) | Materialized `[]KeyValue` | Materialized `shoal_scan_result` | `TestScannerContinuationAndCleanup` (`accumulo/scanner_test.go:220`) | Behavior mismatch | The single most consequential structural difference. Sharkbite users iterate 250k-row scans (`test/performance/QuarterMillionWrites.py`) without materializing them. |
| SB-XCUT-011 | ABI versioning | n/a (C++ ABI, no versioning) | n/a | `SHOAL_ABI_VERSION` macro + `shoal_abi_version()` (`capi/include/shoal_types.h:19`, `capi/include/shoal.h:10`), plus `struct_size` forward-compat on every config struct | `capi/tests/lifecycle.c:29,36-38,59-62,98-101,116-125`; `header_cpp_test.cpp:5-8` | Covered | Header/library version match is asserted at runtime and compile time. |
| SB-XCUT-012 | Capability discovery | n/a | n/a | `shoal_abi_capability_count`, `shoal_abi_capability_word_count`, `shoal_abi_capability_word`, `shoal_abi_has_capability` (`capi/include/shoal.h:34-39`); `SHOAL_ABI_CAPABILITY_TABLE_ADMIN` / `SHOAL_ABI_CAPABILITY_WORD0` (`capi/include/shoal_types.h:52-55,85-93`) | `TestABIDiscoveryValues` (`cmd/shoal-capi/abi_export_test.go:19`); `TestABIDiscoveryQueriesAreConcurrentAndStable` (`cmd/shoal-capi/abi_export_test.go:66`); `capi/tests/shared_library_query.c:63-111`; `capi/tests/lifecycle.c:112-126`; `capi/tests/header_cpp_test.cpp:16-47` | Covered | A Python wheel can now probe the loaded `libshoal` before binding optional surfaces. Callers still need dynamic symbol resolution when supporting pre-discovery libraries. |
| SB-XCUT-013 | Symbol visibility and calling convention | n/a | n/a | `SHOAL_API`/`SHOAL_CALL` with `__declspec` on Windows and `visibility("default")` elsewhere (`capi/include/shoal_types.h:7-17`) | `header_cpp_test.cpp:1-14` (compiles as C++11 under `extern "C"`) | Covered | |
| SB-XCUT-014 | C ABI build and CI | n/a | n/a | `make capi` (`Makefile:179-185`); C/C++ tests compiled and executed by `TestSharedLibraryCABI` (`cmd/shoal-capi/cabi_test.go:13,49-91`) | `.github/workflows/ci.yml` build/vet/race jobs include `cmd/shoal-capi` | Behavior mismatch | There is no dedicated CI job for `make capi`, and no Windows or macOS runner, so the packaged artifact path is unverified on the platforms a wheel matrix will need. |
| SB-XCUT-015 | Live-cluster conformance | Live tests exist but are excluded from CI (`.github/workflows/ccpp.yml` runs only three no-cluster CTest targets) | All 90 `accumulo/` tests are unit tests with fake adapters (no build tags, no env gating, no testcontainers) | C tests use compile-time seams (`capi/tests/test_seam.h`) | `make test-hdfs` + `.github/workflows/hdfs-integration.yml` are the only live-service tests in Shoal, and they cover HDFS only | Missing Go | Neither project verifies Accumulo client behavior against a live cluster in CI. See [§24](#sec-24). |
| SB-XCUT-016 | Wire compatibility of visibility expressions | `ColumnVisibility`/`VisibilityEvaluator` (`test/constructs/TestSecurity.cpp`) | `internal/visfilter` — **internal**, used by the embedded engine | Labels are passed through to the server | `internal/visfilter` unit tests | Behavior mismatch | For the client path both projects delegate evaluation to the tablet server, so parity is a live-cluster question. |
| SB-XCUT-017 | GIL behavior | pybind11 holds the GIL unless explicitly released; `Results.__anext__` resolves futures inline | n/a | n/a | `examples/asyncexample.py` | Missing C ABI | Shim obligation: release the GIL around every blocking ABI call. Without it, `asyncio` and threads regress relative to Sharkbite. |
| SB-XCUT-018 | Fork safety | Not documented; process-wide singleton pools make `fork()` unsafe | Per-connector pools | Handle table in `cmd/shoal-capi/state.go` | `TestConnectorRegistryLifecycle` (`cmd/shoal-capi/state_test.go:13`) | Behavior mismatch | The Go runtime is not fork-safe; `multiprocessing` with the default `fork` start method will break in ways Sharkbite users may not expect. Must be documented in the wheel's README and validated. |

<a id="sec-21"></a>

## 21. Unsafe or defective Sharkbite behavior (do not reproduce)

Each item is a defect in the pinned Sharkbite release, not a contract. The
compatibility shim must implement the listed replacement instead of copying the
behavior. None of these may be silently "fixed" without recording the change
here, because some code depends on the broken shape.

| ID | Defect | Location | Why it is unsafe | Compatibility shim |
| --- | --- | --- | --- | --- |
| SB-UNSAFE-001 | `Authorizations.__str__`/`__repr__` dereference `vec.end()-1` and `vec.back()` on an empty vector | `pysharkbite.cpp:464-484` | Undefined behavior — `str(Authorizations())` can crash the interpreter. `Authorizations()` with no entries is the most common construction in the pinned tests. | Return `[ ]` for empty, `[ a, b ]` otherwise. Match the non-empty format exactly. |
| SB-UNSAFE-002 | `Value.get()` transcodes bytes to `str` | `pysharkbite.cpp:274` | Raises `UnicodeDecodeError` on binary values; silently corrupts data on lossy platforms. | Keep `get()` but decode with `errors='surrogateescape'`; add `get_bytes()` (already in the contract) and document `get_bytes()` as the correct accessor. |
| SB-UNSAFE-003 | `Key.getRow()`/`getColumnFamily()`/`getColumnQualifier()`/`getColumnVisibility()` return `str` with no bytes alternative | `pysharkbite.cpp:321-325` | Binary row keys — normal in Accumulo — are unreachable from Python and raise on decode. | Same `surrogateescape` rule plus new `*_bytes()` accessors. Additive, so existing code is unaffected. |
| SB-UNSAFE-004 | `Key(const char*, ...)` constructor truncates at the first NUL | `pysharkbite.cpp:299-300` | Silent data corruption for binary components. | Accept `bytes` and pass length explicitly through `shoal_bytes`. |
| SB-UNSAFE-005 | `BatchWriter.flush()` cannot be called with zero arguments | `pysharkbite.cpp:487` vs `include/writer/Sink.h:62` | The documented API is unusable; pybind11 does not inherit the C++ default. | Expose `flush(override: bool = False)`. |
| SB-UNSAFE-006 | `BatchScanner.fetchColumn(cf)` cannot be called with one argument | `pysharkbite.cpp:436` vs `include/scanner/Source.h:85` | Same class of defect; column-family-only fetching is unreachable. | Expose `fetchColumn(cf, cq=None)`. |
| SB-UNSAFE-007 | `AccumuloScanner.select_column` calls `fetchColumn` with the wrong arity on both branches | `sharkbite/__init__.py:140-144` | The helper never works: the `cq` branch passes one argument, the no-`cq` branch passes `None`. | Fix the branches; keep the method name and signature. |
| SB-UNSAFE-008 | `AccumuloIterator.__next__` raises `StopIteration` when the chunk counter reaches `chunkSize` | `sharkbite/__init__.py:190-194` | Silent truncation: a scan with `chunksize=1000` returns only the first 1000 rows and reports success. `nextBatch()` is never called by anything. | Treat `chunksize` as a batching hint only; never terminate iteration early. Record as an intentional behavior change in [§26](#sec-26) because it changes result counts. |
| SB-UNSAFE-009 | `AccumuloIterator.__init__` is defined twice; the first definition is dead | `sharkbite/__init__.py:174-181` | The one-argument constructor documented by the first definition does not exist at runtime. | Provide `__init__(self, scanner, chunkSize=0)`. |
| SB-UNSAFE-010 | `AccumuloScanner` is defined twice in the same module; the first definition (with a `pass` constructor) is dead | `sharkbite/__init__.py:60-63` and `126-148` | `AccumuloBase.to_scanner()` silently depends on which definition wins. | Single definition. |
| SB-UNSAFE-011 | `AuthInfo::operator=` fails to copy the password | `include/data/constructs/security/AuthInfo.h:60-67` | Copy-assigned credentials silently authenticate with an empty password. | Shoal's `Credentials.clone()` copies everything (`accumulo/credentials.go:44`). Do not reproduce. |
| SB-UNSAFE-012 | `CompressorFactory::create()` constructs but never throws its error | `include/data/constructs/compressor/CompressorFactory.h:39-48` | An unsupported codec silently falls through instead of failing. | Fail loudly with `ClientException`. |
| SB-UNSAFE-013 | `RFileOperations::open` returns a raw owning pointer while its four siblings return `shared_ptr` | `include/data/constructs/rfile/RFileOperations.h:45` | Guaranteed leak from Python, which never deletes it. | Single owning model; close deterministically via context manager. |
| SB-UNSAFE-014 | `CompressorFactory::getZlibCompressor()` leaks a process-wide singleton | `include/data/constructs/compressor/CompressorFactory.h:35` | Intentional leak; harmless but not a contract. | Do not reproduce. |
| SB-UNSAFE-015 | `TableRates` registers `query_rate_byte` four times and never registers an ingest-rate property | `pysharkbite.cpp:110-120` | Three registrations are dead; the property set does not match the getters. | Register each property once; keep every getter name. |
| SB-UNSAFE-016 | `tableOps.import(...)` and `IterInfo.class()` use Python reserved words | `pysharkbite.cpp:241,93` | Unreachable through normal attribute syntax. | Keep the reserved-word names for `getattr` users and add usable aliases. |
| SB-UNSAFE-017 | `AccumuloWriter.__del__` performs network I/O during interpreter teardown | `sharkbite/__init__.py:128-129` | Exceptions are swallowed; module globals may already be torn down; writes can be silently lost. | Keep a best-effort `__del__`, emit a `ResourceWarning` when it has real work to do, and document `with`/`close()`. |
| SB-UNSAFE-018 | Server-side update errors are never surfaced to Python | `pysharkbite.cpp:486-497` (no failure accessor on the `Sink` binding) | Rejected mutations look like successful writes. | Surface `MutationRejectionError` through `ClientException`; see SB-WRITE-010 and [§26](#sec-26). |
| SB-UNSAFE-019 | `ZookeeperInstance` owns raw `ZooCache`/`ZooKeeper` pointers with manual `delete` | `include/data/constructs/client/zookeeperinstance.h` | Inconsistent with the rest of the API and easy to double-free. | Shoal's `Instance.Close()` is idempotent (`accumulo/instance.go:143`). |
| SB-UNSAFE-020 | Committed build artifacts in the test tree (`test/zookeeper/construct_test`, `test/zookeeper/testInstance`) and an empty `test/services/CMakeLists.txt` | `test/` | The mock-server test path is dead; the harness cannot be trusted as evidence. | Ignore as evidence; port the behavior into live-cluster conformance instead. |
| SB-UNSAFE-021 | `AccumuloScanner.__del__` performs network I/O during interpreter teardown | `sharkbite/__init__.py:162-164` | Same shutdown hazards as `AccumuloWriter.__del__`: exceptions are swallowed and module globals may already be torn down. | Keep only a best-effort finalizer and document `with`/`close()` as the contract. |
| SB-UNSAFE-022 | `AccumuloValueDataset.__init__` calls `super.__init__` instead of `super().__init__` | `sharkbite/torch.py:64-66` | The subclass constructor never initializes the base dataset state, so the public helper is unusable as written. | Do not reproduce the broken constructor; if this helper ever enters scope, publish a working constructor and record the divergence. |
| SB-UNSAFE-023 | `pandashark.read_accumulo(scanner, ..., chunksize)` passes `chunksize` as `begin_row` to `AccumuloScanner.get` | `pandashark/__init__.py:89-97` | The helper discards the scanner's configured ranges and misuses the `AccumuloScanner.get` signature. | Treat it as an upstream defect, not as a batching contract. |
| SB-UNSAFE-024 | `AccumuloDataset.coerce` default path calls `key.getKey().getValue().get()` | `sharkbite/torch.py:24-30` | Normal `KeyValue` inputs do not have `getKey().getValue()`, so the helper prints an exception and returns `0`. | Preserve only the working callable-override path unless a future divergence decision intentionally redesigns the default coercion behavior. |

<a id="sec-22"></a>

## 22. Behavior that must be preserved bit-for-bit

These are contracts, not defects. Changing any of them breaks working
Sharkbite programs, so each must be reproduced exactly and covered by a
compatibility test.

| ID | Contract | Source of truth |
| --- | --- | --- |
| SB-KEEP-001 | Both module names import: `import sharkbite` and `import pysharkbite`, with identical symbol sets. | `PYTHONREADME.md:10`; `sharkbite/__init__.py:5`; `test/python/testmodule/__init__.py:64` |
| SB-KEEP-002 | Every alias pair is present: `getInstanceName`/`instance_name`, `getInstanceId`/`instance_id`, `getUserName`/`username`, `getPassword`/`password`, `getPriority`/`priority`, `getName`/`name`, `getClass`/`class`. | `pysharkbite.cpp:71-74,78-83,89-94,99-104` |
| SB-KEEP-003 | `Key()` default timestamp is `9223372036854775807`. | `pysharkbite.cpp:299-300` |
| SB-KEEP-004 | `Mutation.put` default timestamp is `0` and is written literally. | `pysharkbite.cpp:363-364` |
| SB-KEEP-005 | `Range` accepts `""` and `None` as unbounded ends. | `test/python/TestRanges.py:120,147` |
| SB-KEEP-006 | `Range(row)` covers the whole row. | `pysharkbite.cpp:336`; `test/python/TestRanges.py:64` |
| SB-KEEP-007 | Scanner and writer both implement the context-manager protocol and close on exit. | `pysharkbite.cpp:438-443,489-495`; `test/python/TestWithFx.py:30,50` |
| SB-KEEP-008 | `scanner.withRange(...)` returns the scanner. | `pysharkbite.cpp:444-446`; `test/python/TestWithFx.py:50` |
| SB-KEEP-009 | `getResultSet()` may be called repeatedly and re-executes the accumulated ranges. | `test/python/TestRanges.py:64-86` |
| SB-KEEP-010 | Iterating a result set yields `KeyValue` objects and terminates with `StopIteration`. | `include/scanner/constructs/Results.h:280-288` |
| SB-KEEP-011 | `async for` over a result set works. | `pysharkbite.cpp:381-396`; `examples/asyncexample.py` |
| SB-KEEP-012 | `tableOperations.exists(False)` then `create(False)` is the canonical bootstrap sequence, and `create` on an existing table returns `False` rather than raising. | `test/python/TestWrites.py:15-19` |
| SB-KEEP-013 | `tableOperations.remove()` is the canonical teardown and must succeed on an existing table. | every `test/python/*.py` |
| SB-KEEP-014 | Writing an empty value is legal and scans back as an empty value. | `test/python/TestWrites.py:36,73-83` |
| SB-KEEP-015 | `Authorizations()` with no entries is legal for both scanning and writing. | `test/python/TestWrites.py:23,29` |
| SB-KEEP-016 | Invalid authorization characters raise. | `include/data/constructs/security/Authorizations.h:88` |
| SB-KEEP-017 | Cells whose visibility is not satisfied are omitted from scan results without error. | `test/python/TestAuthorizations.py:68-78` |
| SB-KEEP-018 | `ClientException` is catchable by name and is also caught by `except RuntimeError`. | `test/python/TestBadOperations.py:63`; pybind11 exception model |
| SB-KEEP-019 | Passing `None` where an `Authorizations` is required raises rather than crashing. | `test/python/TestBadOperations.py:75-79` |
| SB-KEEP-020 | Operating on a removed table raises. | `test/python/TestBadOperations.py:33-62` |
| SB-KEEP-021 | Re-authenticating as a dropped user raises `ClientException`. | `test/python/TestSecurityOperations.py:110-116` |
| SB-KEEP-022 | Scans survive tablet relocation transparently. | `test/vandv/testScanLocationMove.h` |
| SB-KEEP-023 | `writer.addMutation(m)` may be followed immediately by mutating or discarding `m`. | `test/python/TestWrites.py:46-49` |
| SB-KEEP-024 | Repeated create-writer/close-writer cycles on the same table handle work. | `test/python/TestWrites.py:29-51` |
| SB-KEEP-025 | A 1000-row write followed by a full scan returns exactly 1003 entries in the pinned fixture. | `test/python/TestWrites.py:93-97` |
| SB-KEEP-026 | `Value.get_bytes()` returns exact bytes with no transcoding. | `pysharkbite.cpp:275-279` |
| SB-KEEP-027 | RFile round trip: `openForWrite` → `append` → `close` → `sequentialRead` → `seek(Seekable(Range()))` → `hasNext`/`getTopKey`/`next` yields the written rows in order. | `test/python/TestRFileWrites.py:35-64` |
| SB-KEEP-028 | HDFS round trip: `write` → `writeString` → `read` → `readString` → `remove(path, False)`. | `examples/hdfswrite.py` |
| SB-KEEP-029 | `AccumuloWriter.put` substitutes the current epoch millisecond when `timestamp == 0`, and flushes the pending mutation when the row changes. | `sharkbite/__init__.py:76-95` |
| SB-KEEP-030 | Default thread count for the high-level helpers is 10. | `sharkbite/__init__.py:19` |
| SB-KEEP-031 | The ZooKeeper session timeout used by the convenience connector constructor is 1000 ms. | `pysharkbite.cpp:262`; `sharkbite/__init__.py:30` |
| SB-KEEP-032 | Status objects accept arbitrary user attributes (`dynamic_attr`). | `pysharkbite.cpp:108,121,128,136,144,158,186,209` |

<a id="sec-23"></a>

## 23. Gap ledger and ordered prerequisites

Work proceeds strictly in stage order. A stage may not start until the previous
stage is complete for the rows it depends on.

This ledger is the ordered-gap deliverable of Shoal issue
[#81](https://github.com/phrocker/shoal-oss/issues/81). Every `SB-GAP-*` entry
below without an existing issue link needs one filed and linked back to #81
before its stage starts; #81 stays open until every stage-1 through stage-4
entry is either closed or reclassified in [§26](#sec-26).

### 23.1 Stage 1 — Go parity (blocks everything)

| ID | Gap | Matrix rows | Existing issue/PR | Notes |
| --- | --- | --- | --- | --- |
| SB-GAP-GO-001 | Security operations: users, permissions, authorizations | SB-SEC-001…SB-SEC-017, SB-CONN-004 | none | Largest single Go gap. Four of ten pinned Python tests cannot run without `grantAuthorizations`. |
| SB-GAP-GO-002 | Namespace operations | SB-NS-001…SB-NS-007, SB-CONN-005 | none | `internal/managerclient` already models the namespace error kinds; needs FATE operations. |
| SB-GAP-GO-003 | Cluster status / `getStatistics` | SB-STAT-001…SB-STAT-038, SB-CONN-007 | none | 38 accessors over eight types plus two enums. |
| SB-GAP-GO-004 | Table splits (add), constraints, online compaction, row-bounded flush, and a Sharkbite-compatible bulk-import adapter | SB-TABLE-006, SB-TABLE-010, SB-TABLE-012, SB-TABLE-013, SB-TABLE-005 | [#80](https://github.com/phrocker/shoal-oss/issues/80)/PR [#83](https://github.com/phrocker/shoal-oss/pull/83) (listing only), [#88](https://github.com/phrocker/shoal-oss/issues/88)/PR [#93](https://github.com/phrocker/shoal-oss/pull/93) (split creation), [#65](https://github.com/phrocker/shoal-oss/issues/65) (online compaction), merged PR [#78](https://github.com/phrocker/shoal-oss/pull/78) (Bulk Import V2) | Split **listing** lands first and does not close SB-TABLE-010. Bulk Import V2 is public, but its staged-load-map contract does not close Sharkbite's `dir`/`fail_path` row. |
| SB-GAP-GO-005 | Public RFile API (read, sequential read, write, locality groups, iterators, seekables) | SB-RFILE-001…SB-RFILE-021 | none | Implementation exists in `internal/rfile` + `internal/iterrt`; the work is export and API design, not new logic. |
| SB-GAP-GO-006 | Public HDFS API (typed streams, dir entries, rename/move/chown/mkdir) | SB-HDFS-001…SB-HDFS-014 | [#10](https://github.com/phrocker/shoal-oss/issues/10)/PR [#85](https://github.com/phrocker/shoal-oss/pull/85) (cancellation), [#9](https://github.com/phrocker/shoal-oss/issues/9) (live coverage, merged) | Land cancellation before exposing HDFS to Python. |
| SB-GAP-GO-007 | Streaming scan results (cursor, not slice) | SB-SCAN-004, SB-XCUT-010, SB-CPP-005 | none | Prerequisite for 250k-row workloads; must be designed at the Go layer first because the ABI shape follows it. |
| SB-GAP-GO-008 | Server-side Python iterators | SB-SCAN-016, SB-DATA-056, SB-DATA-061…SB-DATA-064, SB-SCAN-007, SB-CPP-014 | none | Needs a decision: reimplement the JNI iterator shim, transpile to Go iterators (see `docs/iterator-forge-design.md`), or record as an intentional divergence. |
| SB-GAP-GO-009 | Hedged reads and RFile-only scanning options | SB-SCAN-014, SB-SCAN-015 | none | Published, documented Sharkbite options. |
| SB-GAP-GO-010 | Logging control | SB-LOG-001…SB-LOG-003 | none | Needs a public, process-global switch. |
| SB-GAP-GO-011 | Range predicates (`after_end_key`, `before_start_key`) and `Range.__str__` | SB-DATA-031…SB-DATA-033 | none | Logic exists unexported in `accumulo/range.go` and `internal/iterrt`. |
| SB-GAP-GO-012 | Authorizations as a first-class validated type | SB-DATA-047…SB-DATA-052 | none | Includes character validation (SB-KEEP-016) and `contains`. |
| SB-GAP-GO-013 | Configuration key/value bag | SB-CFG-002…SB-CFG-005 | none | Must at minimum accept and honor or explicitly reject `FILE_SYSTEM_ROOT`. |
| SB-GAP-GO-014 | Writer buffered-size accessor | SB-WRITE-006 | none | |
| SB-GAP-GO-015 | Live-cluster conformance harness | SB-XCUT-015, SB-CPP-010, SB-CPP-013 | [#74](https://github.com/phrocker/shoal-oss/issues/74) (conformance release gates) | Delivery vehicle for [§24](#sec-24). |

### 23.2 Stage 2 — C ABI parity (blocked by Stage 1 per row)

| ID | Gap | Matrix rows | Existing issue/PR | Notes |
| --- | --- | --- | --- | --- |
| SB-GAP-C-001 | Table-operation semantic adapters | SB-TABLE-001, SB-TABLE-002, SB-TABLE-005, SB-TABLE-022, SB-CPP-002 | [#82](https://github.com/phrocker/shoal-oss/issues/82) / PR [#84](https://github.com/phrocker/shoal-oss/pull/84) | Core table-admin entry points now ship on the ABI; remaining work is Sharkbite's recreate/createIfNot/bool adapters, row-bounded flush, and the lack of a bound table handle. |
| SB-GAP-C-002 | Capability-specific policy for future optional ABI surfaces | SB-SEC-*, SB-STAT-*, SB-RFILE-*, SB-HDFS-* | none | The discovery surface now ships in `shoal_abi_capability_*` / `shoal_abi_has_capability`; each future optional surface still needs a stable advertised capability bit before the wheel can probe it safely. |
| SB-GAP-C-003 | Cancellation handle | SB-XCUT-008, SB-CONN-013 | none | Required for `KeyboardInterrupt` and for `asyncio` integration. |
| SB-GAP-C-004 | Security and namespace ABI | SB-SEC-*, SB-NS-* | none | Follows SB-GAP-GO-001/002. |
| SB-GAP-C-005 | Streaming scan cursor on the ABI | SB-SCAN-004, SB-XCUT-010 | none | Follows SB-GAP-GO-007. |
| SB-GAP-C-006 | Cluster status ABI | SB-STAT-* | none | Follows SB-GAP-GO-003. |
| SB-GAP-C-007 | RFile and HDFS ABI | SB-RFILE-*, SB-HDFS-* | none | Follows SB-GAP-GO-005/006. |
| SB-GAP-C-008 | Range and key accessors on the ABI | SB-DATA-025…SB-DATA-030 | none | Needed so the shim can round-trip `Range` objects without shadow state. |
| SB-GAP-C-009 | Iterator-setting accessors | SB-DATA-057…SB-DATA-059 | none | |
| SB-GAP-C-010 | Windows and macOS artifact verification | SB-XCUT-014, SB-PKG-008 | none | `make capi` is not exercised by CI on any platform. |

### 23.3 Stage 3 — Compatibility tests (blocked by Stages 1 and 2)

| ID | Gap | Matrix rows | Notes |
| --- | --- | --- | --- |
| SB-GAP-T-001 | Port `test/vandv/*.h` behavioral assertions into a Shoal live-cluster suite | SB-CPP-010, SB-XCUT-015 | Error codes, tablet moves, auth changes mid-session, empty ranges, invalid table ops. |
| SB-GAP-T-002 | Port the ten pinned `test/python/*.py` files as the Python acceptance suite | all of [§5](#sec-5)–[§20](#sec-20) | They must pass **unmodified** except for the `-s/--solocation` argument. |
| SB-GAP-T-003 | Binary-safety differential tests (non-UTF-8 rows, values, visibilities; embedded NULs) | SB-XCUT-004, SB-UNSAFE-002…SB-UNSAFE-004 | Must run against a live cluster, not just the ABI. |
| SB-GAP-T-004 | Visibility-enforcement conformance | SB-SEC-018, SB-KEEP-017 | |
| SB-GAP-T-005 | Error-code mapping table test | SB-ERR-002 | Asserts the Sharkbite numeric code for every Shoal status. |
| SB-GAP-T-006 | Scale test equivalent to `QuarterMillionWrites.py` | SB-XCUT-010 | Proves the streaming decision. |
| SB-GAP-T-007 | Concurrency and GIL tests | SB-XCUT-006, SB-XCUT-017 | Threads plus `asyncio`. |
| SB-GAP-T-008 | Fork-safety test | SB-XCUT-018 | |

### 23.4 Stage 4 — Python and wheels (blocked by Stages 1–3)

| ID | Gap | Matrix rows | Notes |
| --- | --- | --- | --- |
| SB-GAP-P-001 | `sharkbite` package with `pysharkbite` compatibility module | SB-PKG-002…SB-PKG-004 | |
| SB-GAP-P-002 | Exception hierarchy rooted at `RuntimeError` | SB-ERR-001, SB-ERR-014, SB-KEEP-018 | |
| SB-GAP-P-003 | High-level helpers (`AccumuloBase`, `AccumuloWriter`, `AccumuloScanner`, `AccumuloIterator`) with the SB-UNSAFE fixes | SB-CONN-014, SB-WRITE-016…SB-WRITE-023, SB-SCAN-024, SB-SCAN-028…SB-SCAN-032 | |
| SB-GAP-P-004 | `sharkbite.torch` optional extra | SB-PKG-012, SB-RFILE-024…SB-RFILE-029 | `pandashark` remains separately tracked because the pinned `sharkbite` wheel does not ship it. |
| SB-GAP-P-005 | Wheel matrix and `manylinux` build | SB-PKG-007…SB-PKG-011 | Includes the Python floor decision. |
| SB-GAP-P-006 | Migration guide covering every approved divergence | [§26](#sec-26) | Ships with the first release. |

### 23.5 Related umbrella work

This matrix, its audit, and this ledger are tracked by Shoal issue
[#81](https://github.com/phrocker/shoal-oss/issues/81).
Issue [#59](https://github.com/phrocker/shoal-oss/issues/59) is #81's parent and
tracks the overall migration;
[#75](https://github.com/phrocker/shoal-oss/issues/75) tracks the
execution plane. Merged prerequisites already in `origin/main`:
[#48](https://github.com/phrocker/shoal-oss/issues/48)/PR [#52](https://github.com/phrocker/shoal-oss/pull/52) (connector lifecycle ABI),
[#61](https://github.com/phrocker/shoal-oss/issues/61)/PR [#62](https://github.com/phrocker/shoal-oss/pull/62) (scanner and owned result ABI),
[#63](https://github.com/phrocker/shoal-oss/issues/63)/PR [#64](https://github.com/phrocker/shoal-oss/pull/64) (mutation and BatchWriter ABI),
[#53](https://github.com/phrocker/shoal-oss/issues/53)/PR [#55](https://github.com/phrocker/shoal-oss/pull/55) (BatchWriter latency flushing),
[#54](https://github.com/phrocker/shoal-oss/issues/54)/PR [#56](https://github.com/phrocker/shoal-oss/pull/56) (effective table property reads),
PR [#78](https://github.com/phrocker/shoal-oss/pull/78) (Bulk Import V2),
and PR [#79](https://github.com/phrocker/shoal-oss/pull/79) (reachable embed service).
In-flight at the time of writing: PRs
[#83](https://github.com/phrocker/shoal-oss/pull/83),
[#84](https://github.com/phrocker/shoal-oss/pull/84),
[#85](https://github.com/phrocker/shoal-oss/pull/85),
[#87](https://github.com/phrocker/shoal-oss/pull/87),
[#91](https://github.com/phrocker/shoal-oss/pull/91),
and [#93](https://github.com/phrocker/shoal-oss/pull/93).

<a id="sec-24"></a>

## 24. Live-cluster conformance requirements

All 90 Go tests in `accumulo/` are unit tests driven by fake adapters: a search
for build tags, `testing.Short()`, environment gating, and testcontainers in
`accumulo/*_test.go` returns nothing. The only live-service coverage in Shoal is
HDFS (`make test-hdfs`, `.github/workflows/hdfs-integration.yml`). Sharkbite's
own live tests are equally excluded from its CI. Therefore **no row whose
semantics are defined by a real tablet server may be marked `Covered` on unit
evidence alone**.

Rows that require live-cluster evidence before they can close:

Some rows below are already `Covered` at the Go and C ABI layers. That status
records what Shoal's own layers prove; the live-cluster test additionally
validates the end-to-end Python shim against a real tablet server. A row that is
not yet `Covered` cannot be closed by a live test alone either — it needs both.

| Area | Rows | What must be proven against a real Accumulo |
| --- | --- | --- |
| Authentication | SB-CONN-012, SB-KEEP-021 | Bad credentials and dropped users raise the mapped exception. |
| Visibility | SB-SEC-018, SB-KEEP-017 | Cells are filtered exactly as `test/python/TestAuthorizations.py` asserts. |
| Server-side iterators | SB-SCAN-006, SB-SCAN-016 | A configured iterator actually transforms results. |
| Tablet movement | SB-SCAN-026, SB-KEEP-022 | Scans survive real splits and migrations. |
| Write failures | SB-WRITE-010, SB-WRITE-011, SB-WRITE-012 | Constraint violations, authorization failures, and ambiguous commits behave as modeled. |
| Binary safety | SB-XCUT-004, SB-GAP-T-003 | Non-UTF-8 and NUL-containing components survive a real round trip. |
| Range semantics | SB-DATA-020…SB-DATA-024, SB-KEEP-005, SB-KEEP-006 | Empty, `None`, and whole-row ranges return the pinned row counts. |
| Scale and streaming | SB-XCUT-010, SB-GAP-T-006 | 250k-row write and scan complete without materializing the result set. |
| Table lifecycle | SB-TABLE-001…SB-TABLE-003, SB-KEEP-012, SB-KEEP-013 | `exists`/`create`/`remove` sequencing matches, including the `create` returning `False` case. |

Minimum harness: an ephemeral single-node Accumulo 4 plus ZooKeeper (the role
Sharkbite's `test/19x/st` SMAC project played), driven from CI, with the ported
`test/python/*.py` suite as the acceptance gate.

<a id="sec-25"></a>

## 25. Counts by status and category

### 25.1 By status

| Status | Rows |
| --- | --- |
| Covered | 51 |
| Missing Go | 133 |
| Missing C ABI | 52 |
| Behavior mismatch | 65 |
| Intentional divergence (approval required) | 4 |
| Not required (rationale required) | 45 |
| **Total** | **350** |

### 25.2 By category

| Section | Prefix | Rows | Covered | Missing Go | Missing C ABI | Behavior mismatch | Intentional divergence | Not required |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [§5](#sec-5) Packaging and imports | `SB-PKG` | 14 | 0 | 0 | 9 | 1 | 1 | 3 |
| [§6](#sec-6) Configuration and credentials | `SB-CFG` | 18 | 0 | 4 | 5 | 5 | 1 | 3 |
| [§7](#sec-7) Connector and session | `SB-CONN` | 14 | 3 | 3 | 3 | 4 | 1 | 0 |
| [§8](#sec-8) Data model | `SB-DATA` | 64 | 16 | 10 | 10 | 23 | 0 | 5 |
| [§9](#sec-9) Scanners and results | `SB-SCAN` | 32 | 2 | 9 | 6 | 9 | 0 | 6 |
| [§10](#sec-10) Writers | `SB-WRITE` | 23 | 2 | 1 | 8 | 5 | 0 | 7 |
| [§11](#sec-11) Table operations | `SB-TABLE` | 22 | 8 | 3 | 0 | 5 | 0 | 6 |
| [§12](#sec-12) Namespaces | `SB-NS` | 8 | 0 | 7 | 0 | 1 | 0 | 0 |
| [§13](#sec-13) Security | `SB-SEC` | 19 | 0 | 17 | 0 | 1 | 0 | 1 |
| [§14](#sec-14) Cluster status | `SB-STAT` | 38 | 0 | 37 | 0 | 0 | 0 | 1 |
| [§15](#sec-15) RFile, streams, and helpers | `SB-RFILE` | 34 | 0 | 20 | 4 | 1 | 0 | 9 |
| [§16](#sec-16) HDFS | `SB-HDFS` | 14 | 0 | 14 | 0 | 0 | 0 | 0 |
| [§17](#sec-17) Logging | `SB-LOG` | 3 | 0 | 2 | 0 | 1 | 0 | 0 |
| [§18](#sec-18) Errors | `SB-ERR` | 15 | 2 | 2 | 5 | 2 | 1 | 3 |
| [§19](#sec-19) C++ and flat C | `SB-CPP` | 14 | 8 | 3 | 0 | 2 | 0 | 1 |
| [§20](#sec-20) Cross-cutting | `SB-XCUT` | 18 | 10 | 1 | 2 | 5 | 0 | 0 |
| **Total** | | **350** | **51** | **133** | **52** | **65** | **4** | **45** |

### 25.3 Reading the counts

Of 350 rows, **51 are `Covered`** — 14.6 percent. They are concentrated in
connector lifecycle, the key/value/mutation data model, table administration,
batch writing, ABI hygiene, and flat-C parity, which is exactly the ground the
merged ABI work
([#48](https://github.com/phrocker/shoal-oss/issues/48)/[#52](https://github.com/phrocker/shoal-oss/pull/52),
[#61](https://github.com/phrocker/shoal-oss/issues/61)/[#62](https://github.com/phrocker/shoal-oss/pull/62),
[#63](https://github.com/phrocker/shoal-oss/issues/63)/[#64](https://github.com/phrocker/shoal-oss/pull/64))
covers.

Another 45 rows are explicit `Not required` scope exclusions or upstream
defects. They are tracked so the audit stays exhaustive, but they do not need
to become `Covered` unless their scope decision changes.

The 133 `Missing Go` rows are dominated by cluster status (37), RFile and
streams (20), security (17), and HDFS (14) — surfaces where Shoal has never had
a client-side public implementation. The 52 `Missing C ABI` rows are led by the
data model (10), Python-layer packaging (9), writers (8), and scanners/results
(6), with configuration/credential mapping and errors close behind (5 each).

The 65 `Behavior mismatch` rows are the most dangerous category for a
compatibility project: the capability exists on both layers, so a naive
implementer will mark them done, but the observable semantics differ — types
(`str` versus `bytes`), defaults (`Key` timestamp, writer thread count, ZooKeeper
timeout), configuration timing (post-construction versus construction-time), and
result delivery (streaming versus materialized). Every one of them needs a
compatibility test, not a code review.

Counts are mechanically derived from this file: each matrix row is a table line
whose first cell begins with `SB-` and that contains exactly one of the six
status strings in its own cell. Re-run that extraction after any edit and update
this section.

<a id="sec-26"></a>

## 26. Divergences requiring explicit approval

No entry below is approved. Each blocks the gate until a named approver signs
it with a date. Approvals are recorded on Shoal issue
[#81](https://github.com/phrocker/shoal-oss/issues/81) and then mirrored into
this table in the same change; a comment on #81 alone does not lift the gate,
and neither does an edit here without the corresponding #81 decision. Adding a
divergence to this table is not approval.

Four rows in the matrix already carry the `Intentional divergence (approval
required)` status (SB-PKG-014, SB-CFG-014, SB-CONN-010, SB-ERR-003). The
remaining entries are **proposed** divergences attached to rows that currently
carry another status; approving one changes that row's status to
`Intentional divergence`, and rejecting it leaves the row as a gap that must be
implemented.

| ID | Divergence | Rows | Impact on existing Sharkbite programs | Approver | Date |
| --- | --- | --- | --- | --- | --- |
| SB-DIV-001 | Accumulo 4 only; 1.6.x–2.x unsupported | SB-PKG-014 | Every user on Accumulo 1.x/2.x cannot upgrade. Largest divergence in this document. | _unapproved_ | — |
| SB-DIV-002 | No password read-back (`AuthInfo.getPassword`/`password`) | SB-CFG-014 | Code that re-reads its own password breaks; credential leaks stop. | _unapproved_ | — |
| SB-DIV-003 | Per-connector transport and table-name caches instead of process-wide singletons | SB-CONN-010, SB-CONN-011 | Connection counts and cache-invalidation blast radius change. | _unapproved_ | — |
| SB-DIV-004 | Generated Thrift types never leak; `TApplicationException` is not raised | SB-ERR-003 | `except TApplicationException` blocks stop matching. | _unapproved_ | — |
| SB-DIV-005 | Server-side update errors are raised instead of silently discarded | SB-WRITE-010, SB-UNSAFE-018 | Programs that ignored rejected writes will now see exceptions. This is the correct behavior and should be approved, but it is a behavior change. | _unapproved_ | — |
| SB-DIV-006 | `AccumuloIterator` chunk size no longer truncates iteration | SB-SCAN-024, SB-UNSAFE-008 | Programs that relied on the truncation to bound work will now read the full range. | _unapproved_ | — |
| SB-DIV-007 | Server-side Python iterators are not reimplemented (if this route is chosen) | SB-SCAN-016 and its dependents | `PythonIterator` programs cannot migrate. Alternative: transpile to Go iterators (`docs/iterator-forge-design.md`). | _unapproved_ | — |
| SB-DIV-008 | Hedged reads and RFile-only scanning are not reimplemented (if this route is chosen) | SB-SCAN-014, SB-SCAN-015 | Documented beta options disappear. | _unapproved_ | — |
| SB-DIV-009 | Python floor raised above 3.6 | SB-PKG-007 | Users on Python 3.6–3.8 cannot upgrade. | _unapproved_ | — |
| SB-DIV-010 | Reversed ranges raise at construction instead of returning no rows | SB-DATA-034 | Programs that built reversed ranges and expected empty results now fail loudly. | _unapproved_ | — |

## Related documents

- `capi/README.md` — C ABI ownership, lifetime, and bootstrap contract.
- `accumulo/doc.go` — public Go client scope statement.
- `ARCHITECTURE.md` — component map.
- `docs/iterator-forge-design.md` — candidate route for SB-SCAN-016.
