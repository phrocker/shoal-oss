from __future__ import annotations

import ctypes as C
from dataclasses import dataclass
from typing import Iterator, Sequence

from ._native import (
    CAP_HIGH_LEVEL_CLIENT,
    CAP_HIGH_LEVEL_SCANNER,
    CAP_OWNED_SCAN_RESULT,
    ClientConfig,
    ConnectorConfig,
    KeyValueView,
    NativeAPI,
    Range,
    as_bytes,
    c_bytes,
)


@dataclass(frozen=True)
class Key:
    row: bytes
    column_family: bytes
    column_qualifier: bytes
    column_visibility: bytes
    timestamp: int

    def getRow(self) -> bytes:
        return self.row

    def getColumnFamily(self) -> bytes:
        return self.column_family

    def getColumnQualifier(self) -> bytes:
        return self.column_qualifier

    def getColumnVisibility(self) -> bytes:
        return self.column_visibility

    def getTimestamp(self) -> int:
        return self.timestamp


class _Config:
    def __init__(
        self,
        api: NativeAPI,
        instance: str,
        zookeepers: str,
        username: str,
        password: str | bytes,
        accumulo_version: str = "4.0.0-SNAPSHOT",
    ) -> None:
        self.api = api
        self.config = ConnectorConfig()
        api.lib.shoal_connector_config_init(C.byref(self.config))
        self._keepalive: list[object] = []
        self.config.bootstrap = 2
        self.config.instance_name = self._string(instance)
        self.config.zookeeper_servers = self._string(zookeepers)
        self.config.principal = self._string(username)
        password_bytes = as_bytes(password)
        password_view, password_buffer = c_bytes(password_bytes)
        self.config.password = password_view.data
        self.config.password_length = password_view.length
        self._keepalive.append(password_buffer)
        self.config.accumulo_version = self._string(accumulo_version)

    def _string(self, value: str) -> bytes:
        encoded = value.encode()
        self._keepalive.append(encoded)
        return encoded


class Connector:
    def __init__(
        self,
        instance: str,
        zookeepers: str,
        username: str,
        password: str | bytes,
        *,
        accumulo_version: str = "4.0.0-SNAPSHOT",
        library: str | None = None,
        _api: NativeAPI | None = None,
    ) -> None:
        self._api = _api or NativeAPI(library)
        self._api.require(0, 1, 2)
        config = _Config(
            self._api, instance, zookeepers, username, password, accumulo_version
        )
        self._handle = C.c_void_p()
        error = C.c_void_p()
        status = self._api.lib.shoal_connector_create(
            C.byref(config.config), C.byref(self._handle), C.byref(error)
        )
        self._api.check(status, error)
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        error = C.c_void_p()
        status = self._api.lib.shoal_connector_close(self._handle, C.byref(error))
        try:
            self._api.check(status, error)
        finally:
            self._api.lib.shoal_connector_free(C.byref(self._handle))
            self._closed = True

    def __enter__(self) -> Connector:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class Client:
    def __init__(
        self,
        instance: str,
        zookeepers: str,
        username: str,
        password: str | bytes,
        table: str | None = None,
        auths: Sequence[str | bytes] | None = None,
        *,
        threads: int = 10,
        accumulo_version: str = "4.0.0-SNAPSHOT",
        library: str | None = None,
        _api: NativeAPI | None = None,
    ) -> None:
        self._api = _api or NativeAPI(library)
        self._api.require(
            CAP_HIGH_LEVEL_CLIENT, CAP_HIGH_LEVEL_SCANNER, CAP_OWNED_SCAN_RESULT
        )
        connector = _Config(
            self._api, instance, zookeepers, username, password, accumulo_version
        )
        config = ClientConfig()
        self._api.lib.shoal_client_config_init(C.byref(config))
        config.connector = C.pointer(connector.config)
        keepalive: list[object] = [connector]
        if table is not None:
            table_bytes = table.encode()
            keepalive.append(table_bytes)
            config.table_name = table_bytes
        auth_values = [c_bytes(as_bytes(value)) for value in (auths or ())]
        if auth_values:
            auth_array = (type(auth_values[0][0]) * len(auth_values))(
                *(item[0] for item in auth_values)
            )
            config.authorizations = auth_array
            config.authorization_count = len(auth_values)
            keepalive.extend([auth_array, *(item[1] for item in auth_values)])
        config.thread_count = threads
        self._handle = C.c_void_p()
        error = C.c_void_p()
        status = self._api.lib.shoal_client_create(
            C.byref(config), C.byref(self._handle), C.byref(error)
        )
        self._api.check(status, error)
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def set_threads(self, thread_count: int) -> None:
        error = C.c_void_p()
        status = self._api.lib.shoal_client_set_threads(
            self._handle, thread_count, C.byref(error)
        )
        self._api.check(status, error)

    def set_table(self, table: str) -> None:
        error = C.c_void_p()
        status = self._api.lib.shoal_client_set_table(
            self._handle, table.encode(), C.byref(error)
        )
        self._api.check(status, error)

    def select_column(self, family: str | bytes, qualifier: str | bytes | None = None) -> None:
        family_view, family_buffer = c_bytes(as_bytes(family))
        qualifier_pointer = None
        qualifier_view = None
        qualifier_buffer = None
        if qualifier is not None:
            qualifier_view, qualifier_buffer = c_bytes(as_bytes(qualifier))
            qualifier_pointer = C.byref(qualifier_view)
        error = C.c_void_p()
        status = self._api.lib.shoal_client_select_column(
            self._handle, family_view, qualifier_pointer, C.byref(error)
        )
        self._api.check(status, error)
        _ = (family_buffer, qualifier_buffer)

    def scanner(self) -> Scanner:
        return Scanner(self)

    def scan(
        self,
        begin_row: str | bytes,
        end_row: str | bytes | None = None,
        *,
        timeout_ms: int = 0,
    ) -> list[tuple[Key, bytes]]:
        return self.scanner().scan(begin_row, end_row, timeout_ms=timeout_ms)

    def getStatistics(self) -> None:
        raise NotImplementedError(
            "cluster statistics are an approved compatibility divergence (SB-DIV-016)"
        )

    def close(self) -> None:
        if self._closed:
            return
        error = C.c_void_p()
        status = self._api.lib.shoal_client_close(self._handle, C.byref(error))
        try:
            self._api.check(status, error)
        finally:
            self._api.lib.shoal_client_free(C.byref(self._handle))
            self._closed = True

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class Scanner:
    def __init__(self, client: Client) -> None:
        self._client = client
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def select_column(self, family: str | bytes, qualifier: str | bytes | None = None) -> None:
        self._ensure_open()
        self._client.select_column(family, qualifier)

    def scan(
        self,
        begin_row: str | bytes,
        end_row: str | bytes | None = None,
        *,
        timeout_ms: int = 0,
    ) -> list[tuple[Key, bytes]]:
        self._ensure_open()
        api = self._client._api
        scan_range = Range()
        api.lib.shoal_range_init(C.byref(scan_range))
        begin_view, begin_buffer = c_bytes(as_bytes(begin_row))
        scan_range.start.kind = 1
        scan_range.start.row = begin_view
        scan_range.start_inclusive = 1
        keepalive: list[object] = [begin_buffer]
        if end_row is not None:
            end_view, end_buffer = c_bytes(as_bytes(end_row))
            scan_range.end.kind = 1
            scan_range.end.row = end_view
            scan_range.end_inclusive = 0
            keepalive.append(end_buffer)
        result = C.c_void_p()
        error = C.c_void_p()
        status = api.lib.shoal_client_scan_range(
            self._client._handle,
            C.byref(scan_range),
            timeout_ms,
            C.byref(result),
            C.byref(error),
        )
        api.check(status, error)
        entries: list[tuple[Key, bytes]] = []
        try:
            for index in range(api.lib.shoal_scan_result_count(result)):
                view = KeyValueView()
                item_error = C.c_void_p()
                item_status = api.lib.shoal_scan_result_get(
                    result, index, C.byref(view), C.byref(item_error)
                )
                api.check(item_status, item_error)
                raw_key, value = api.copy_view(view)
                entries.append((Key(*raw_key), value))
        finally:
            api.lib.shoal_scan_result_free(C.byref(result))
        return entries

    def close(self) -> None:
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("scanner is closed")

    def __enter__(self) -> Scanner:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AccumuloBase(Client):
    def list_tables(self) -> None:
        raise NotImplementedError(
            "legacy list_tables is not part of the first Python delivery slice"
        )


class AccumuloScanner(AccumuloBase):
    def get(self, *_: object, **__: object) -> None:
        raise NotImplementedError(
            "legacy chunked AccumuloIterator is not part of the first Python delivery slice; "
            "use scan()"
        )

    def fetch_range(self, *_: object, **__: object) -> None:
        raise NotImplementedError(
            "legacy mutable scanner ranges are not part of the first Python delivery slice"
        )

    def fetch_ranges(self, *_: object, **__: object) -> None:
        raise NotImplementedError(
            "legacy mutable scanner ranges are not part of the first Python delivery slice"
        )


class AccumuloWriter:
    def __init__(self, *_: object, **__: object) -> None:
        raise NotImplementedError(
            "AccumuloWriter is not part of the first Python delivery slice"
        )


class AccumuloIterator(Iterator[tuple[Key, bytes]]):
    def __init__(self, *_: object, **__: object) -> None:
        raise NotImplementedError(
            "legacy AccumuloIterator is not part of the first Python delivery slice"
        )

    def __next__(self) -> tuple[Key, bytes]:
        raise StopIteration
