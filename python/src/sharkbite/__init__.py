"""Incremental Shoal Python binding; intentionally not full compatibility."""

from ._native import NativeAPI, RuntimeInfo
from .admin import (
    Authorizations,
    NamespaceOperations,
    NamespacePermissions,
    SecurityOperations,
    ShoalSystemPermissions,
    ShoalTablePermissions,
    SystemPermissions,
    TableInfo,
    TableOperations,
    TablePermissions,
)
from .client import (
    AccumuloBase,
    AccumuloIterator,
    AccumuloScanner,
    AccumuloWriter,
    Client,
    Connector,
    Key,
    Scanner,
)
from .errors import (
    AmbiguousWriteError,
    AlreadyExistsError,
    CancelledError,
    ClientException,
    ClosedError,
    DeadlineExceededError,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
    ShoalError,
    UnsupportedError,
)
from .writer import BatchWriter, BatchWriterOptions, Mutation

AccumuloConnector = Connector

__all__ = [
    "AccumuloBase",
    "AccumuloConnector",
    "AccumuloIterator",
    "AccumuloScanner",
    "AccumuloWriter",
    "AmbiguousWriteError",
    "AlreadyExistsError",
    "Authorizations",
    "BatchWriter",
    "BatchWriterOptions",
    "CancelledError",
    "Client",
    "ClientException",
    "ClosedError",
    "Connector",
    "DeadlineExceededError",
    "InvalidArgumentError",
    "Key",
    "NativeAPI",
    "NamespaceOperations",
    "NamespacePermissions",
    "NotFoundError",
    "PermissionDeniedError",
    "RuntimeInfo",
    "Scanner",
    "SecurityOperations",
    "ShoalSystemPermissions",
    "ShoalTablePermissions",
    "ShoalError",
    "UnsupportedError",
    "SystemPermissions",
    "TableInfo",
    "TableOperations",
    "TablePermissions",
    "Mutation",
]
