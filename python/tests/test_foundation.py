from __future__ import annotations

import ctypes as C
import copy
import unittest

import pysharkbite
import sharkbite
from sharkbite import Client, Connector, Key, PythonIterator, ScannerOptions
from sharkbite._native import (
    CAP_HIGH_LEVEL_CLIENT,
    CAPABILITY_SYMBOLS,
    Bytes,
    ClientConfig,
    KeyValueView,
    NativeAPI,
    Range,
)
from sharkbite.errors import (
    CLIENT_ERROR_CODES,
    ClientException,
    InvalidArgumentError,
    InvalidHandleError,
    exception_for_status,
)


class Function:
    def __init__(self, callback):
        self.callback = callback
        self.restype = None
        self.argtypes = None

    def __call__(self, *args):
        return self.callback(*args)


class FakeLibrary:
    def __init__(self):
        self.closed = []
        self.freed = []
        self.result_freed = 0
        self.authorizations = []
        self._buffers = []
        self.shoal_connector_config_init = Function(self._init)
        self.shoal_client_config_init = Function(self._init)
        self.shoal_range_init = Function(self._init)
        self.shoal_connector_create = Function(self._create)
        self.shoal_connector_close = Function(lambda handle, error: self._close("connector"))
        self.shoal_connector_free = Function(lambda handle: self._free("connector"))
        self.shoal_client_create = Function(self._create)
        self.shoal_client_close = Function(lambda handle, error: self._close("client"))
        self.shoal_client_free = Function(lambda handle: self._free("client"))
        self.shoal_client_set_threads = Function(lambda *args: 0)
        self.shoal_client_set_table = Function(lambda *args: 0)
        self.shoal_client_select_column = Function(lambda *args: 0)
        self.shoal_client_scan_range = Function(self._scan)
        self.shoal_scan_result_count = Function(lambda result: 1)
        self.shoal_scan_result_get = Function(self._get)
        self.shoal_scan_result_free = Function(self._result_free)

    @staticmethod
    def _init(pointer):
        C.cast(pointer, C.POINTER(C.c_uint32)).contents.value = C.sizeof(
            pointer._obj
        )

    def _create(self, config, out_handle, error):
        if isinstance(config._obj, ClientConfig):
            native = C.cast(config, C.POINTER(ClientConfig)).contents
            self.authorizations = [
                C.string_at(native.authorizations[index].data, native.authorizations[index].length)
                for index in range(native.authorization_count)
            ]
        C.cast(out_handle, C.POINTER(C.c_void_p)).contents.value = 123
        return 0

    def _close(self, kind):
        self.closed.append(kind)
        return 0

    def _free(self, kind):
        self.freed.append(kind)

    @staticmethod
    def _scan(handle, scan_range, timeout, out_result, error):
        C.cast(out_result, C.POINTER(C.c_void_p)).contents.value = 456
        return 0

    def _value(self, value: bytes) -> Bytes:
        buffer = (C.c_uint8 * len(value)).from_buffer_copy(value)
        self._buffers.append(buffer)
        return Bytes(C.cast(buffer, C.POINTER(C.c_uint8)), len(value))

    def _get(self, result, index, out_view, error):
        view = C.cast(out_view, C.POINTER(KeyValueView)).contents
        view.row = self._value(b"row")
        view.column_family = self._value(b"cf")
        view.column_qualifier = self._value(b"cq")
        view.column_visibility = self._value(b"A")
        view.timestamp = 7
        view.value = self._value(b"value")
        return 0

    def _result_free(self, result):
        self.result_freed += 1


class FakeAPI:
    def __init__(self):
        self.lib = FakeLibrary()
        self.capabilities = frozenset(range(31))

    def require(self, *capabilities):
        missing = set(capabilities) - self.capabilities
        if missing:
            raise NotImplementedError

    @staticmethod
    def check(status, error):
        if status:
            raise AssertionError(status)

    @staticmethod
    def copy_view(view):
        def read(value):
            return C.string_at(value.data, value.length) if value.length else b""

        return (
            (
                read(view.row),
                read(view.column_family),
                read(view.column_qualifier),
                read(view.column_visibility),
                view.timestamp,
            ),
            read(view.value),
        )


class FoundationTests(unittest.TestCase):
    def test_import_aliases(self):
        self.assertIs(pysharkbite.Client, sharkbite.Client)
        self.assertIs(pysharkbite.Key, sharkbite.Key)

    def test_connector_context_close_is_idempotent(self):
        api = FakeAPI()
        with Connector("i", "zk", "u", "p", _api=api) as connector:
            self.assertFalse(connector.closed)
        connector.close()
        self.assertEqual(api.lib.closed, ["connector"])
        self.assertEqual(api.lib.freed, ["connector"])

    def test_connector_copy_is_explicitly_rejected(self):
        api = FakeAPI()
        with Connector("i", "zk", "u", "p", _api=api) as connector:
            with self.assertRaisesRegex(TypeError, "cannot be copied"):
                copy.copy(connector)
            with self.assertRaisesRegex(TypeError, "cannot be copied"):
                copy.deepcopy(connector)

    def test_client_scan_copies_and_frees_owned_result(self):
        api = FakeAPI()
        with Client("i", "zk", "u", "p", table="t", _api=api) as client:
            with client.scanner() as scanner:
                rows = scanner.scan(b"a", b"z")
        self.assertEqual(
            rows, [(Key(b"row", b"cf", b"cq", b"A", 7), b"value")]
        )
        self.assertEqual(api.lib.result_freed, 1)
        self.assertEqual(api.lib.closed, ["client"])
        self.assertEqual(api.lib.freed, ["client"])

    def test_scan_authorizations_are_copied_to_native_enforcement(self):
        api = FakeAPI()
        auths = [bytearray(b"blah1")]
        with Client("i", "zk", "u", "p", table="t", auths=auths, _api=api):
            auths[0][:] = b"other"
            self.assertEqual(api.lib.authorizations, [b"blah1"])

    def test_approved_divergence_is_explicit(self):
        client = object.__new__(Client)
        with self.assertRaisesRegex(NotImplementedError, "SB-DIV-016"):
            client.getStatistics()

    def test_accumulo_version_scope_is_explicit(self):
        with self.assertRaisesRegex(NotImplementedError, "SB-DIV-001"):
            Connector("i", "zk", "u", "p", accumulo_version="2.1.0", _api=FakeAPI())

    def test_scanner_options_and_python_iterator_are_importable_but_unsupported(self):
        self.assertEqual(int(ScannerOptions.HedgedReads), 1)
        self.assertEqual(int(ScannerOptions.RFileScanOnly), 2)
        iterator = PythonIterator("iter", 7).onNext("lambda key, value: True")
        self.assertEqual(iterator.getName(), "iter")
        self.assertEqual(iterator.priority(), 7)
        self.assertEqual(iterator.getClass(), "org.poma.accumulo.JythonIterator")
        with self.assertRaisesRegex(RuntimeError, "python script"):
            PythonIterator("iter", "class iter: pass", 7).onNext("lambda value: value")
        scanner = object.__new__(sharkbite.AccumuloScanner)
        with self.assertRaisesRegex(NotImplementedError, "SB-DIV-008"):
            scanner.setOption(ScannerOptions.HedgedReads)
        with self.assertRaisesRegex(NotImplementedError, "SB-DIV-008"):
            scanner.setOption(ScannerOptions.RFileScanOnly)
        with self.assertRaisesRegex(NotImplementedError, "SB-DIV-007"):
            scanner.addIterator(iterator)

    def test_unsupported_scanner_iterator_is_explicit(self):
        scanner = object.__new__(sharkbite.AccumuloScanner)
        with self.assertRaisesRegex(NotImplementedError, "use scan"):
            scanner.get(b"a")

    def test_status_and_compatibility_exception_mapping(self):
        self.assertIsInstance(exception_for_status(1, "bad"), InvalidArgumentError)
        self.assertIsInstance(exception_for_status(2, "handle"), InvalidHandleError)
        self.assertIsInstance(
            exception_for_status(15, "client", compatibility_class=1),
            ClientException,
        )
        table = exception_for_status(
            9,
            "table missing",
            compatibility_class=1,
            compatibility_code=9,
            source_class=1,
            source_name="ClientException",
        )
        self.assertIsInstance(table, ClientException)
        self.assertEqual(table.getErrorCode(), 9)
        self.assertEqual(CLIENT_ERROR_CODES[table.getErrorCode()], "Table not found in instance")
        illegal = exception_for_status(
            1,
            "bad argument",
            compatibility_class=0,
            source_class=6,
            source_name="IllegalArgumentException",
        )
        self.assertIs(type(illegal), RuntimeError)
        self.assertEqual(illegal.source_name, "IllegalArgumentException")
        hdfs = exception_for_status(
            9,
            "missing path",
            compatibility_class=0,
            source_class=5,
            source_name="HDFSException",
        )
        self.assertIs(type(hdfs), RuntimeError)
        self.assertEqual(hdfs.source_name, "HDFSException")

    def test_capability_negotiation_also_requires_dynamic_symbols(self):
        api = object.__new__(NativeAPI)
        api.capabilities = frozenset({CAP_HIGH_LEVEL_CLIENT})
        api._symbols = set(CAPABILITY_SYMBOLS[CAP_HIGH_LEVEL_CLIENT]) - {
            "shoal_client_create"
        }
        with self.assertRaisesRegex(NotImplementedError, "shoal_client_create"):
            api.require(CAP_HIGH_LEVEL_CLIENT)


if __name__ == "__main__":
    unittest.main()
