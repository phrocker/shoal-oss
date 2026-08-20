from __future__ import annotations

import ctypes as C
import time
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
from .compatibility import (
    PythonIterator,
    ScannerOptions,
    unsupported_python_iterator,
    unsupported_scanner_option,
)
from .configuration import AuthInfo, Instance, ZookeeperInstance
from .errors import ClosedError


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
        zookeeper_session_timeout_ms: int = 0,
    ) -> None:
        if not accumulo_version.startswith("4."):
            raise NotImplementedError(
                "Shoal compatibility targets Accumulo 4 only (SB-DIV-001)"
            )
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
        self.config.zookeeper_session_timeout_ms = zookeeper_session_timeout_ms

    def _string(self, value: str) -> bytes:
        encoded = value.encode()
        self._keepalive.append(encoded)
        return encoded


class Connector:
    def __init__(
        self,
        instance: str | AuthInfo,
        zookeepers: str | Instance,
        username: str | None = None,
        password: str | bytes | None = None,
        *,
        accumulo_version: str = "4.0.0-SNAPSHOT",
        library: str | None = None,
        _api: NativeAPI | None = None,
    ) -> None:
        self._api = _api or NativeAPI(library)
        self._api.require(0, 1, 2)
        if isinstance(instance, AuthInfo):
            if not isinstance(zookeepers, ZookeeperInstance):
                raise TypeError("AuthInfo construction requires a ZookeeperInstance")
            if username is not None or password is not None:
                raise TypeError(
                    "username and password are not accepted with AuthInfo"
                )
            auth = instance
            zk_instance = zookeepers
            if auth.getInstanceId() != zk_instance.getInstanceId():
                raise ValueError("AuthInfo instance ID does not match Instance")
            instance_name = zk_instance.getInstanceName()
            zookeeper_servers = zk_instance.getZookeepers()
            username = auth.getUserName()
            password = auth._password_bytes()
            session_timeout_ms = zk_instance.session_timeout_ms
        else:
            if not isinstance(zookeepers, str):
                raise TypeError("zookeepers must be a comma-separated string")
            if username is None or password is None:
                raise TypeError("username and password are required")
            instance_name = instance
            zookeeper_servers = zookeepers
            # Sharkbite's convenience constructor pins 1000 ms.
            session_timeout_ms = 1000
        config = _Config(
            self._api,
            instance_name,
            zookeeper_servers,
            username,
            password,
            accumulo_version,
            session_timeout_ms,
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
        self._ensure_open()
        return self

    def __copy__(self) -> Connector:
        raise TypeError("Shoal connector handles cannot be copied")

    def __deepcopy__(self, _: object) -> Connector:
        raise TypeError("Shoal connector handles cannot be copied")

    def mutation(self, row: str | bytes) -> object:
        self._ensure_open()
        from .writer import Mutation
        return Mutation(row, _api=self._api)

    def create_batch_writer(
        self, table: str, *, options: object | None = None
    ) -> object:
        self._ensure_open()
        from .writer import BatchWriter
        return BatchWriter(self, table, options=options)

    def tableOps(self, table: str) -> object:
        self._ensure_open()
        from .admin import TableOperations
        return TableOperations(self, table)

    def namespaceOps(self, nm: str = "") -> object:
        self._ensure_open()
        from .admin import NamespaceOperations
        return NamespaceOperations(self, nm)

    def securityOps(self) -> object:
        self._ensure_open()
        from .admin import SecurityOperations
        return SecurityOperations(self)

    def tableInfo(self) -> object:
        self._ensure_open()
        from .admin import TableInfo
        return TableInfo(self)

    def getStatistics(self) -> None:
        self._ensure_open()
        raise NotImplementedError(
            "Accumulo 4 removed the legacy manager-monitor statistics API; "
            "cluster statistics are unavailable (approved divergence SB-DIV-016)"
        )

    def _ensure_open(self) -> None:
        if self._closed:
            raise ClosedError("connector is closed")

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

    def list_tables(self, *, timeout_ms: int = 0) -> dict[str, str]:
        from ._native import TableView
        result = C.c_void_p()
        error = C.c_void_p()
        status = self._api.lib.shoal_client_list_tables(
            self._handle, timeout_ms, C.byref(result), C.byref(error)
        )
        self._api.check(status, error)
        tables: dict[str, str] = {}
        try:
            for index in range(self._api.lib.shoal_table_list_count(result)):
                view = TableView()
                item_error = C.c_void_p()
                item_status = self._api.lib.shoal_table_list_get(
                    result, index, C.byref(view), C.byref(item_error)
                )
                self._api.check(item_status, item_error)
                tables[view.name.decode()] = view.id.decode()
        finally:
            self._api.lib.shoal_table_list_free(C.byref(result))
        return tables

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
            "Accumulo 4 removed the legacy manager-monitor statistics API; "
            "cluster statistics are unavailable (approved divergence SB-DIV-016)"
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

    def setOption(self, option: ScannerOptions | int) -> None:
        raise unsupported_scanner_option(option)

    def removeOption(self, option: ScannerOptions | int) -> None:
        raise unsupported_scanner_option(option)

    def addIterator(self, iterator: object) -> None:
        if isinstance(iterator, PythonIterator):
            raise unsupported_python_iterator()
        raise NotImplementedError(
            "mutable legacy scanner iterator configuration is not supported; "
            "configure Shoal scanner iterators at construction"
        )

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
    def list_tables(self, *, timeout_ms: int = 0) -> list[str]:
        return list(super().list_tables(timeout_ms=timeout_ms))


class AccumuloScanner(AccumuloBase):
    def setOption(self, option: ScannerOptions | int) -> None:
        raise unsupported_scanner_option(option)

    def removeOption(self, option: ScannerOptions | int) -> None:
        raise unsupported_scanner_option(option)

    def addIterator(self, iterator: object) -> None:
        if isinstance(iterator, PythonIterator):
            raise unsupported_python_iterator()
        raise NotImplementedError(
            "mutable legacy scanner iterator configuration is not supported"
        )

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
        del auths
        if not table:
            raise ValueError("table is required")
        from .writer import BatchWriter, BatchWriterOptions
        self._connector = Connector(
            instance,
            zookeepers,
            username,
            password,
            accumulo_version=accumulo_version,
            library=library,
            _api=_api,
        )
        try:
            self._writer = BatchWriter(
                self._connector,
                table,
                options=BatchWriterOptions(max_write_threads=threads),
            )
        except BaseException:
            self._connector.close()
            raise
        self._closed = False

    def put(
        self,
        row: str | bytes,
        cf: str | bytes,
        cq: str | bytes,
        cv: str | bytes | None = None,
        timestamp: int = 0,
        value: str | bytes | None = None,
        *,
        timeout_ms: int = 0,
    ) -> None:
        if timestamp == 0:
            timestamp = int(time.time() * 1000)
        with self._writer.mutation(row) as mutation:
            mutation.put(cf, cq, cv or b"", timestamp, value or b"")
            self._writer.add_mutation(mutation, timeout_ms=timeout_ms)

    def putDelete(
        self,
        row: str | bytes,
        cf: str | bytes,
        cq: str | bytes,
        cv: str | bytes = b"",
        timestamp: int = 0,
        *,
        timeout_ms: int = 0,
    ) -> None:
        with self._writer.mutation(row) as mutation:
            mutation.delete(cf, cq, cv, timestamp)
            self._writer.add_mutation(mutation, timeout_ms=timeout_ms)

    def delete(self, key: Key, *, timeout_ms: int = 0) -> None:
        self.putDelete(
            key.row,
            key.column_family,
            key.column_qualifier,
            key.column_visibility,
            key.timestamp,
            timeout_ms=timeout_ms,
        )

    def flush(self, *, timeout_ms: int = 0) -> bool:
        return self._writer.flush(timeout_ms=timeout_ms)

    def close(self, *, timeout_ms: int = 0) -> None:
        if self._closed:
            return
        writer_error: BaseException | None = None
        try:
            self._writer.close(timeout_ms=timeout_ms)
        except BaseException as exc:
            writer_error = exc
        try:
            self._connector.close()
        except BaseException:
            if writer_error is None:
                raise
        finally:
            self._closed = True
        if writer_error is not None:
            raise writer_error

    def __enter__(self) -> AccumuloWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class AccumuloIterator(Iterator[tuple[Key, bytes]]):
    def __init__(self, *_: object, **__: object) -> None:
        raise NotImplementedError(
            "legacy AccumuloIterator is not part of the first Python delivery slice"
        )

    def __next__(self) -> tuple[Key, bytes]:
        raise StopIteration
