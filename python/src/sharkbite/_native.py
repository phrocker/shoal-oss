from __future__ import annotations

import ctypes as C
import ctypes.util
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .errors import exception_for_status

ABI_MAJOR = 1
CAP_OWNED_SCAN_RESULT = 5
CAP_HIGH_LEVEL_CLIENT = 21
CAP_HIGH_LEVEL_SCANNER = 22
CAP_COMPATIBILITY_ERRORS = 23

CAPABILITY_SYMBOLS = {
    CAP_OWNED_SCAN_RESULT: {
        "shoal_scan_result_count",
        "shoal_scan_result_get",
        "shoal_scan_result_free",
    },
    CAP_HIGH_LEVEL_CLIENT: {
        "shoal_client_config_init",
        "shoal_client_create",
        "shoal_client_close",
        "shoal_client_free",
        "shoal_client_set_threads",
        "shoal_client_set_table",
    },
    CAP_HIGH_LEVEL_SCANNER: {
        "shoal_range_init",
        "shoal_client_select_column",
        "shoal_client_scan_range",
    },
}


class Bytes(C.Structure):
    _fields_ = [("data", C.POINTER(C.c_uint8)), ("length", C.c_size_t)]


class Key(C.Structure):
    _fields_ = [
        ("row", Bytes),
        ("column_family", Bytes),
        ("column_qualifier", Bytes),
        ("column_visibility", Bytes),
        ("timestamp", C.c_int64),
    ]


class RangeBound(C.Structure):
    _fields_ = [("kind", C.c_int32), ("row", Bytes), ("key", Key)]


class Range(C.Structure):
    _fields_ = [
        ("struct_size", C.c_uint32),
        ("start", RangeBound),
        ("end", RangeBound),
        ("start_inclusive", C.c_uint8),
        ("end_inclusive", C.c_uint8),
    ]


class KeyValueView(C.Structure):
    _fields_ = [
        ("row", Bytes),
        ("column_family", Bytes),
        ("column_qualifier", Bytes),
        ("column_visibility", Bytes),
        ("timestamp", C.c_int64),
        ("value", Bytes),
    ]


class ConnectorConfig(C.Structure):
    _fields_ = [
        ("struct_size", C.c_uint32),
        ("bootstrap", C.c_int32),
        ("instance_name", C.c_char_p),
        ("instance_id", C.c_char_p),
        ("zookeeper_servers", C.c_char_p),
        ("principal", C.c_char_p),
        ("password", C.POINTER(C.c_uint8)),
        ("password_length", C.c_size_t),
        ("accumulo_version", C.c_char_p),
        ("zookeeper_session_timeout_ms", C.c_int64),
        ("bootstrap_timeout_ms", C.c_int64),
        ("instance_secret", C.c_char_p),
        ("dial_timeout_ms", C.c_int64),
    ]


class ClientConfig(C.Structure):
    _fields_ = [
        ("struct_size", C.c_uint32),
        ("connector", C.POINTER(ConnectorConfig)),
        ("table_name", C.c_char_p),
        ("authorizations", C.POINTER(Bytes)),
        ("authorization_count", C.c_size_t),
        ("thread_count", C.c_int32),
    ]


@dataclass(frozen=True)
class RuntimeInfo:
    path: str
    version: tuple[int, int, int]
    capabilities: frozenset[int]


def _library_names() -> tuple[str, ...]:
    if sys.platform == "win32":
        return ("shoal.dll", "libshoal.dll")
    if sys.platform == "darwin":
        return ("libshoal.dylib", "shoal.dylib")
    return ("libshoal.so", "shoal.so")


def library_candidates(explicit: str | os.PathLike[str] | None = None) -> Iterable[str]:
    seen: set[str] = set()
    values: list[str] = []
    if explicit:
        values.append(os.fspath(explicit))
    if os.environ.get("SHOAL_LIBRARY"):
        values.append(os.environ["SHOAL_LIBRARY"])
    package_libs = Path(__file__).resolve().parent / ".libs"
    roots = (Path.cwd(), Path.cwd() / "bin" / "capi", package_libs)
    for root in roots:
        values.extend(str(root / name) for name in _library_names())
    found = ctypes.util.find_library("shoal")
    if found:
        values.append(found)
    values.extend(_library_names())
    for value in values:
        if value not in seen:
            seen.add(value)
            yield value


def _bytes(value: Bytes) -> bytes:
    if not value.data or value.length == 0:
        return b""
    return C.string_at(value.data, value.length)


class NativeAPI:
    def __init__(self, library: str | os.PathLike[str] | None = None) -> None:
        failures: list[str] = []
        self.lib: C.CDLL
        self.path = ""
        for candidate in library_candidates(library):
            try:
                self.lib = C.CDLL(candidate)
                self.path = candidate
                break
            except OSError as exc:
                failures.append(f"{candidate}: {exc}")
        else:
            raise ImportError(
                "unable to load the Shoal shared library; set SHOAL_LIBRARY. "
                + " | ".join(failures[-3:])
            )
        self._bind()
        major = int(self.lib.shoal_abi_version_major())
        if major != ABI_MAJOR:
            raise ImportError(f"unsupported Shoal ABI major {major}; expected {ABI_MAJOR}")
        self.version = (
            major,
            int(self.lib.shoal_abi_version_minor()),
            int(self.lib.shoal_abi_version_patch()),
        )
        count = int(self.lib.shoal_abi_capability_count())
        self.capabilities = frozenset(
            capability
            for capability in range(count)
            if self.lib.shoal_abi_has_capability(capability)
        )
        self.info = RuntimeInfo(self.path, self.version, self.capabilities)

    def _function(
        self, name: str, restype: object, *argtypes: object, required: bool = True
    ) -> bool:
        try:
            fn = getattr(self.lib, name)
        except AttributeError as exc:
            if required:
                raise ImportError(f"Shoal library lacks required symbol {name}") from exc
            return False
        fn.restype = restype
        fn.argtypes = list(argtypes)
        self._symbols.add(name)
        return True

    def _bind(self) -> None:
        P = C.c_void_p
        PP = C.POINTER(P)
        self._symbols: set[str] = set()
        self._function("shoal_abi_version_major", C.c_uint32)
        self._function("shoal_abi_version_minor", C.c_uint32)
        self._function("shoal_abi_version_patch", C.c_uint32)
        self._function("shoal_abi_capability_count", C.c_uint32)
        self._function("shoal_abi_has_capability", C.c_uint8, C.c_uint32)
        self._function("shoal_connector_config_init", None, C.POINTER(ConnectorConfig))
        self._function("shoal_connector_create", C.c_int32, C.POINTER(ConnectorConfig), PP, PP)
        self._function("shoal_connector_close", C.c_int32, P, PP)
        self._function("shoal_connector_free", None, PP)
        optional = {"required": False}
        self._function(
            "shoal_client_config_init", None, C.POINTER(ClientConfig), **optional
        )
        self._function("shoal_range_init", None, C.POINTER(Range), **optional)
        self._function(
            "shoal_client_create",
            C.c_int32,
            C.POINTER(ClientConfig),
            PP,
            PP,
            **optional,
        )
        self._function("shoal_client_close", C.c_int32, P, PP, **optional)
        self._function("shoal_client_free", None, PP, **optional)
        self._function(
            "shoal_client_set_threads", C.c_int32, P, C.c_int32, PP, **optional
        )
        self._function(
            "shoal_client_set_table", C.c_int32, P, C.c_char_p, PP, **optional
        )
        self._function(
            "shoal_client_select_column",
            C.c_int32,
            P,
            Bytes,
            C.POINTER(Bytes),
            PP,
            **optional,
        )
        self._function(
            "shoal_client_scan_range",
            C.c_int32,
            P,
            C.POINTER(Range),
            C.c_int64,
            PP,
            PP,
            **optional,
        )
        self._function("shoal_scan_result_count", C.c_size_t, P, **optional)
        self._function(
            "shoal_scan_result_get",
            C.c_int32,
            P,
            C.c_size_t,
            C.POINTER(KeyValueView),
            PP,
            **optional,
        )
        self._function("shoal_scan_result_free", None, PP, **optional)
        self._function("shoal_error_code", C.c_int32, P)
        self._function("shoal_error_message", C.c_char_p, P)
        self._function("shoal_error_free", None, PP)
        if hasattr(self.lib, "shoal_error_compatibility"):
            self._function("shoal_error_compatibility", C.c_int32, P)

    def require(self, *capabilities: int) -> None:
        missing_capabilities = [
            str(cap) for cap in capabilities if cap not in self.capabilities
        ]
        missing_symbols = sorted(
            symbol
            for capability in capabilities
            for symbol in CAPABILITY_SYMBOLS.get(capability, ())
            if symbol not in self._symbols
        )
        if missing_capabilities or missing_symbols:
            details = []
            if missing_capabilities:
                details.append("capabilities " + ", ".join(missing_capabilities))
            if missing_symbols:
                details.append("symbols " + ", ".join(missing_symbols))
            raise NotImplementedError(
                "loaded Shoal library lacks required ABI " + "; ".join(details)
            )

    def check(self, status: int, error: C.c_void_p) -> None:
        if status == 0:
            return
        message = f"Shoal operation failed with status {status}"
        compatibility = 0
        try:
            if error.value:
                raw = self.lib.shoal_error_message(error)
                if raw:
                    message = raw.decode("utf-8", "replace")
                if CAP_COMPATIBILITY_ERRORS in self.capabilities and hasattr(
                    self.lib, "shoal_error_compatibility"
                ):
                    compatibility = int(self.lib.shoal_error_compatibility(error))
        finally:
            if error.value:
                self.lib.shoal_error_free(C.byref(error))
        raise exception_for_status(status, message, compatibility_class=compatibility)

    @staticmethod
    def copy_view(view: KeyValueView) -> tuple[tuple[bytes, bytes, bytes, bytes, int], bytes]:
        return (
            (
                _bytes(view.row),
                _bytes(view.column_family),
                _bytes(view.column_qualifier),
                _bytes(view.column_visibility),
                int(view.timestamp),
            ),
            _bytes(view.value),
        )


def as_bytes(value: str | bytes | bytearray | memoryview) -> bytes:
    return value.encode() if isinstance(value, str) else bytes(value)


def c_bytes(value: bytes) -> tuple[Bytes, object]:
    if not value:
        return Bytes(None, 0), None
    buffer = (C.c_uint8 * len(value)).from_buffer_copy(value)
    return Bytes(C.cast(buffer, C.POINTER(C.c_uint8)), len(value)), buffer
