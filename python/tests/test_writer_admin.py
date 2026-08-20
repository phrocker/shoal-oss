from __future__ import annotations

import ctypes as C
import unittest

from sharkbite import (
    Authorizations,
    BatchWriter,
    Connector,
    LoggingConfiguration,
    Mutation,
    NamespacePermissions,
    SystemPermissions,
    TablePermissions,
)
from sharkbite._native import Bytes, NamespaceView, PropertyView, TableView


class Function:
    def __init__(self, callback):
        self.callback = callback

    def __call__(self, *args):
        return self.callback(*args)


def raw(value: Bytes) -> bytes:
    return C.string_at(value.data, value.length) if value.length else b""


class FakeLibrary:
    def __init__(self):
        self.calls = []
        self.frees = []
        self._buffers = []
        self.shoal_connector_config_init = Function(self._init)
        self.shoal_connector_create = Function(self._create)
        self.shoal_connector_close = Function(lambda *_: 0)
        self.shoal_connector_free = Function(lambda *_: self.frees.append("connector"))
        self.shoal_batch_writer_config_init = Function(self._init)
        self.shoal_connector_create_batch_writer = Function(self._writer_create)
        self.shoal_batch_writer_add = Function(self._writer_add)
        self.shoal_batch_writer_flush = Function(lambda *args: self._record("flush", *args))
        self.shoal_batch_writer_size = Function(self._writer_size)
        self.shoal_batch_writer_close = Function(lambda *args: self._record("writer_close", *args))
        self.shoal_batch_writer_free = Function(lambda *_: self.frees.append("writer"))
        self.shoal_mutation_create = Function(self._mutation_create)
        self.shoal_mutation_put = Function(self._mutation_put)
        self.shoal_mutation_put_latest = Function(lambda *args: self._record("put_latest", *args))
        self.shoal_mutation_delete = Function(lambda *args: self._record("delete", *args))
        self.shoal_mutation_delete_latest = Function(lambda *args: self._record("delete_latest", *args))
        self.shoal_mutation_size = Function(self._mutation_size)
        self.shoal_mutation_free = Function(lambda *_: self.frees.append("mutation"))
        self.shoal_write_failure_get_flags = Function(lambda *_: 0)
        self.shoal_write_failure_free = Function(lambda *_: self.frees.append("failure"))
        self.shoal_logging_set_level = Function(self._logging_set)
        self.shoal_logging_get_level = Function(lambda: self.log_level)
        self.log_level = 0
        self._bind_admin()

    @staticmethod
    def _init(pointer):
        C.cast(pointer, C.POINTER(C.c_uint32)).contents.value = C.sizeof(pointer._obj)

    @staticmethod
    def _create(config, out_handle, error):
        C.cast(out_handle, C.POINTER(C.c_void_p)).contents.value = 1
        return 0

    @staticmethod
    def _writer_create(connector, config, out_handle, error):
        C.cast(out_handle, C.POINTER(C.c_void_p)).contents.value = 2
        return 0

    def _record(self, name, *args):
        self.calls.append((name, args))
        return 0

    def _mutation_create(self, row, out_handle, error):
        self.calls.append(("mutation_create", raw(row)))
        C.cast(out_handle, C.POINTER(C.c_void_p)).contents.value = 3
        return 0

    def _mutation_put(self, handle, cf, cq, cv, timestamp, value, error):
        self.calls.append(("put", raw(cf), raw(cq), raw(cv), timestamp, raw(value)))
        return 0

    def _mutation_size(self, handle, out_size, error):
        C.cast(out_size, C.POINTER(C.c_size_t)).contents.value = 7
        return 0

    def _writer_add(self, *args):
        self.calls.append(("add", args[2]))
        return 0

    @staticmethod
    def _writer_size(handle, timeout, out_size, error):
        C.cast(out_size, C.POINTER(C.c_size_t)).contents.value = 3
        return 0

    def _logging_set(self, level, error):
        self.log_level = level
        return 0

    def _bind_admin(self):
        for name in (
            "create_table", "delete_table", "rename_table", "flush_table",
            "flush_table_range", "set_table_property", "remove_table_property",
            "create_namespace", "delete_namespace", "rename_namespace",
            "set_namespace_property", "remove_namespace_property", "create_user",
            "drop_user", "change_password", "change_user_authorizations",
            "grant_system_permission", "revoke_system_permission",
            "grant_table_permission", "revoke_table_permission",
            "grant_namespace_permission", "revoke_namespace_permission",
            "add_table_splits", "remove_table_constraint",
        ):
            setattr(
                self,
                "shoal_connector_" + name,
                Function(lambda *args, _name=name: self._record(_name, *args)),
            )
        self.shoal_connector_table_exists = Function(self._exists)
        self.shoal_connector_namespace_exists = Function(self._exists)
        self.shoal_connector_has_system_permission = Function(self._has)
        self.shoal_connector_has_table_permission = Function(self._has)
        self.shoal_connector_has_namespace_permission = Function(self._has)
        self.shoal_connector_list_tables = Function(self._result)
        self.shoal_table_list_count = Function(lambda *_: 1)
        self.shoal_table_list_get = Function(self._table_get)
        self.shoal_table_list_free = Function(lambda *_: self.frees.append("tables"))
        self.shoal_connector_list_namespaces = Function(self._result)
        self.shoal_namespace_list_count = Function(lambda *_: 1)
        self.shoal_namespace_list_get = Function(self._namespace_get)
        self.shoal_namespace_list_free = Function(lambda *_: self.frees.append("namespaces"))
        for prefix in ("table", "namespace"):
            setattr(self, f"shoal_connector_effective_{prefix}_properties", Function(self._result))
            setattr(self, f"shoal_{prefix}_properties_count", Function(lambda *_: 1))
            setattr(self, f"shoal_{prefix}_properties_get", Function(self._property_get))
            setattr(self, f"shoal_{prefix}_properties_free", Function(lambda *_args, _prefix=prefix: self.frees.append(_prefix + "_properties")))
        self.shoal_connector_namespace_properties = Function(self._result)
        self.shoal_connector_versioned_namespace_properties = Function(self._result)
        self.shoal_versioned_properties_version = Function(lambda *_: 8)
        self.shoal_versioned_properties_count = Function(lambda *_: 1)
        self.shoal_versioned_properties_get = Function(self._property_get)
        self.shoal_versioned_properties_free = Function(lambda *_: self.frees.append("versioned_properties"))
        self.shoal_connector_list_table_splits = Function(self._result)
        self.shoal_connector_get_user_authorizations = Function(self._result)
        self.shoal_bytes_list_count = Function(lambda *_: 1)
        self.shoal_bytes_list_get = Function(self._bytes_get)
        self.shoal_bytes_list_free = Function(lambda *_: self.frees.append("bytes"))
        self.shoal_connector_add_table_constraint = Function(self._add_constraint)
        self.shoal_connector_list_table_constraints = Function(self._result)
        self.shoal_table_constraint_list_count = Function(lambda *_: 0)
        self.shoal_table_constraint_view_init = Function(self._init)
        self.shoal_table_constraint_list_get = Function(lambda *_: 0)
        self.shoal_table_constraint_list_free = Function(lambda *_: None)

    @staticmethod
    def _exists(*args):
        C.cast(args[-2], C.POINTER(C.c_uint8)).contents.value = 1
        return 0

    @staticmethod
    def _has(*args):
        C.cast(args[-2], C.POINTER(C.c_uint8)).contents.value = 1
        return 0

    @staticmethod
    def _result(*args):
        C.cast(args[-2], C.POINTER(C.c_void_p)).contents.value = 99
        return 0

    @staticmethod
    def _table_get(result, index, out_view, error):
        view = C.cast(out_view, C.POINTER(TableView)).contents
        view.name, view.id = b"t", b"1"
        return 0

    @staticmethod
    def _namespace_get(result, index, out_view, error):
        view = C.cast(out_view, C.POINTER(NamespaceView)).contents
        view.name, view.id = b"n", b"2"
        return 0

    @staticmethod
    def _property_get(result, index, out_view, error):
        view = C.cast(out_view, C.POINTER(PropertyView)).contents
        view.key, view.value = b"k", b"v"
        return 0

    def _bytes_get(self, result, index, out_view, error):
        buffer = (C.c_uint8 * 3).from_buffer_copy(b"abc")
        self._buffers.append(buffer)
        view = C.cast(out_view, C.POINTER(Bytes)).contents
        view.data = C.cast(buffer, C.POINTER(C.c_uint8))
        view.length = 3
        return 0

    @staticmethod
    def _add_constraint(connector, table, class_name, timeout, out_number, error):
        C.cast(out_number, C.POINTER(C.c_int32)).contents.value = 4
        return 0


class FakeAPI:
    def __init__(self):
        self.lib = FakeLibrary()
        self.capabilities = frozenset(range(30))

    def require(self, *capabilities):
        if set(capabilities) - self.capabilities:
            raise NotImplementedError

    @staticmethod
    def check(status, error):
        if status:
            raise AssertionError(status)

    def check_write(self, status, failure, error):
        self.check(status, error)


class WriterAdminTests(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.connector = Connector("i", "zk", "u", "p", _api=self.api)

    def tearDown(self):
        self.connector.close()

    def test_mutation_and_writer_copy_inputs_and_close_once(self):
        row = bytearray(b"row")
        with Mutation(row, _api=self.api) as mutation:
            row[:] = b"bad"
            mutation.put(b"cf", b"cq", b"A", 7, b"value")
            self.assertEqual(mutation.size(), 7)
            with BatchWriter(self.connector, "t") as writer:
                self.assertTrue(writer.addMutation(mutation))
                self.assertEqual(writer.size(), 3)
                self.assertTrue(writer.flush())
                writer.close()
        self.assertIn(("mutation_create", b"row"), self.api.lib.calls)
        self.assertIn(("put", b"cf", b"cq", b"A", 7, b"value"), self.api.lib.calls)
        self.assertEqual(self.api.lib.frees.count("writer"), 1)
        self.assertEqual(self.api.lib.frees.count("mutation"), 1)

    def test_process_wide_logging_control_uses_stable_abi(self):
        LoggingConfiguration._set(1, self.api)
        self.assertEqual(self.api.lib.log_level, 1)
        LoggingConfiguration._set(2, self.api)
        self.assertEqual(self.api.lib.log_level, 2)

    def test_table_namespace_security_legacy_shapes(self):
        table = self.connector.tableOps("t")
        self.assertTrue(table.exists())
        self.assertEqual(table.getProperties(), {"k": "v"})
        self.assertEqual(table.listSplits(), [b"abc"])
        self.assertEqual(table.addConstraint("Constraint"), 4)
        with self.assertRaisesRegex(NotImplementedError, "online compaction"):
            table.compact(b"", b"", True)

        namespaces = self.connector.namespaceOps()
        self.assertEqual(namespaces.list(), ["n"])
        self.assertEqual(namespaces.list_with_ids(), {"n": "2"})
        self.assertEqual(namespaces.getLocalProperties("n"), {"k": "v"})
        self.assertEqual(namespaces.getVersionedProperties("n"), (8, {"k": "v"}))
        namespaces.rename("new", "old")
        self.assertIn(
            b"old",
            [arg for name, args in self.api.lib.calls if name == "rename_namespace" for arg in args],
        )

        security = self.connector.securityOps()
        self.assertEqual(security.get_auths("u"), Authorizations([b"abc"]))
        self.assertTrue(
            security.has_system_permission("u", SystemPermissions.CREATE_TABLE)
        )
        self.assertTrue(
            security.has_table_permission("u", "t", TablePermissions.WRITE)
        )
        self.assertTrue(
            security.has_namespace_permission("u", "n", NamespacePermissions.READ)
        )
        security.grant_table_permission("u", "t", TablePermissions.WRITE)

    def test_owned_admin_results_are_freed(self):
        table_info = self.connector.tableInfo()
        self.assertEqual(table_info.list(), {"t": "1"})
        self.assertEqual(table_info.list_tables(), ["t"])
        self.assertEqual(table_info.table_id("t"), "1")
        self.assertEqual(table_info.table_name("1"), "t")
        self.assertTrue(table_info.exists("t"))
        self.assertEqual(self.connector.tableOps("t").getProperties(), {"k": "v"})
        self.assertEqual(self.connector.tableOps("t").listSplits(), [b"abc"])
        self.assertIn("tables", self.api.lib.frees)
        self.assertIn("table_properties", self.api.lib.frees)
        self.assertIn("bytes", self.api.lib.frees)


if __name__ == "__main__":
    unittest.main()
