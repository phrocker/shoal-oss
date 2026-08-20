"""Early Shoal Python binding; intentionally not full Sharkbite compatibility."""

from ._native import NativeAPI, RuntimeInfo
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

__all__ = [
    "AccumuloBase",
    "AccumuloIterator",
    "AccumuloScanner",
    "AccumuloWriter",
    "AlreadyExistsError",
    "CancelledError",
    "Client",
    "ClientException",
    "ClosedError",
    "Connector",
    "DeadlineExceededError",
    "InvalidArgumentError",
    "Key",
    "NativeAPI",
    "NotFoundError",
    "PermissionDeniedError",
    "RuntimeInfo",
    "Scanner",
    "ShoalError",
    "UnsupportedError",
]
