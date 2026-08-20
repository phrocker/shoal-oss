# Python distribution and release artifacts

Shoal publishes the Python distribution as **`shoal-sharkbite`**. It provides
both compatibility import names:

```python
import sharkbite
import pysharkbite

assert pysharkbite.Client is sharkbite.Client
```

The distribution version is independent of the native ABI version. Release
`0.4.0` requires Shoal ABI `1.18.0` or newer within ABI major 1. The package is
still an incremental compatibility layer: publishing it does not make the
uncovered required rows in `sharkbite-compatibility.md` compatible. Scope is
defined by `sharkbite-client-scope.md`, not by the raw count of historical C++
declarations.

## Platform status

| Platform | Artifact status | Policy |
| --- | --- | --- |
| Linux x86-64, glibc 2.28+ | First supported wheel target | Build as `manylinux_2_28_x86_64` in a glibc 2.28-or-older manylinux environment and require `auditwheel show` to pass. |
| Linux aarch64, glibc 2.28+ | Build-capable, not yet release-validated | The scripts emit and verify `manylinux_2_28_aarch64`; publication waits for native aarch64 CI evidence. |
| macOS x86-64/arm64 | Go cross-build and unbundled package preview verified; native runtime unsupported | `scripts/cross_platform_artifacts.py` cross-builds the non-CGO Shoal command and reproducibly builds each platform-tagged preview. Preview wheels intentionally contain no `.dylib`, install into a target-platform layout, and prove imports plus the expected native-library discovery error. Publication still requires a native `c-shared` build, linked C/C++ execution, and clean native installs on each architecture. |
| Windows amd64 | Host-verified shared/static and Python artifact path | The deterministic evidence script builds shared and static C ABI artifacts, links and executes C11/C++11 lifecycle clients, verifies all 322 `shoal_*` exports plus ABI 1.18.0/capability discovery, and repeats clean wheel/sdist builds to prove identical hashes. |

The source distribution is platform-neutral Python source and intentionally
contains no native binary. A platform wheel contains exactly one native Shoal
library.

## Native-library trust and discovery

The loader uses this order:

1. an explicit path passed to `NativeAPI`;
2. an absolute path in `SHOAL_LIBRARY`;
3. the wheel's private `sharkbite/.libs` library after checking its SHA-256
   against `_shoal_bundle.json`;
4. the operating-system loader only when
   `SHOAL_ALLOW_SYSTEM_LIBRARY=1` is explicitly set.

The current working directory and repository build directories are never
searched implicitly. This prevents accidental or attacker-controlled library
preloading. The loader resolves ABI discovery symbols first, rejects a major
other than 1, rejects versions older than 1.18.0, checks the packed and tuple
versions agree, and dynamically resolves capability symbols before use.

## Reproducible build

Build dependencies are pinned in `python/pyproject.toml`. `SOURCE_DATE_EPOCH`
defaults to the source commit timestamp, Python hash randomization is fixed,
Go paths/build VCS metadata/build IDs are removed, and archive metadata is
normalized by the Python build backend.

From a clean checkout:

```text
python -m pip install build==1.2.2.post1 setuptools==75.8.0 wheel==0.45.1
python python/scripts/build_release.py --platform-tag manylinux_2_28_x86_64
python python/scripts/verify_release.py
```

The manylinux command must run in a controlled manylinux_2_28 build image. The
script rejects newer glibc hosts and requires an installed `auditwheel` unless
`--skip-auditwheel` is used for non-release development checks. It never
downloads a native dependency.

Outputs under `python/dist/`:

- a platform wheel with the native library and internal bundle digest;
- a source-only sdist;
- `SHA256SUMS`;
- deterministic `release-manifest.json` containing source commit, epoch,
  package/ABI versions, platform tag, sizes, digests, and the detached-signing
  expectation.

Sign `release-manifest.json` and `SHA256SUMS` in the publication system. Do not
place private keys or credentials in the repository or build environment.

`verify_release.py` validates both archives and manifests, then installs the
wheel into a fresh project-local virtual environment with `--no-index` and
with the Go toolchain absent from `PATH`. It also builds the source-only sdist
into an unbundled `py3-none-any` wheel and installs that in a second clean
environment. Both paths import both module names, load a trusted native
library, check ABI/capabilities, and execute a native mutation smoke test.

The build and verification scripts are host-portable. Cross-compilation is not
accepted as runtime evidence; macOS remains explicitly unverified until those
same commands run on a macOS host.

## Cross-platform evidence

Run the complete local evidence pass with:

```text
python scripts/cross_platform_artifacts.py
```

On Windows, `scripts\cross-platform-artifacts.ps1` is an equivalent wrapper.
The generated `build/cross-platform-artifacts/evidence.json` records host
tool versions, artifact hashes, export counts, ABI/capability checks, Go
cross-builds, and exact unsupported-runtime reasons. Generated binaries are
not committed.

The macOS previews are deliberately **unbundled**. They are packaging evidence,
not releasable wheels: `_shoal_preview.json` marks runtime unsupported,
`verify_preview.py` rejects any bundled native library, installs for the stated
target tag with `pip --target`, verifies both imports, and requires `NativeAPI`
to fail unless a trusted native library is explicitly supplied.
