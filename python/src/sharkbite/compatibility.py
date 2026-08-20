from __future__ import annotations

import ctypes as C
from enum import IntFlag

from ._native import CAP_CLIENT_PARITY_CONTROLS, NativeAPI


class ScannerOptions(IntFlag):
    HedgedReads = 0x1
    RFileScanOnly = 0x2
    ENABLE_HEDGED_READS = HedgedReads
    ENABLE_RFILE_SCANNER = RFileScanOnly


class PythonIterator:
    def __init__(self, name: str, script_or_priority: str | int, priority: int | None = None):
        self._name = name
        if priority is None:
            self._script = ""
            self._priority = int(script_or_priority)
        else:
            self._script = str(script_or_priority)
            self._priority = int(priority)

    def onNext(self, source: str) -> PythonIterator:
        if self._script:
            raise RuntimeError(
                "Cannot provide -onNext when a python script is provided"
            )
        self._script = source
        return self

    def getPriority(self) -> int:
        return self._priority

    def priority(self) -> int:
        return self.getPriority()

    def getName(self) -> str:
        return self._name

    def name(self) -> str:
        return self.getName()

    def getClass(self) -> str:
        return "org.poma.accumulo.JythonIterator"


def unsupported_scanner_option(option: ScannerOptions | int) -> NotImplementedError:
    try:
        normalized = ScannerOptions(option)
    except (TypeError, ValueError):
        return NotImplementedError(f"unsupported Sharkbite scanner option: {option!r}")
    if normalized & ScannerOptions.HedgedReads:
        return NotImplementedError(
            "ScannerOptions.HedgedReads is an approved stable divergence "
            "(SB-DIV-008): Shoal does not race RPC and RFile scans"
        )
    if normalized & ScannerOptions.RFileScanOnly:
        return NotImplementedError(
            "ScannerOptions.RFileScanOnly is an approved stable divergence "
            "(SB-DIV-008): use RFileOperations for explicit RFile access"
        )
    return NotImplementedError(f"unsupported Sharkbite scanner option: {option!r}")


def unsupported_python_iterator() -> NotImplementedError:
    return NotImplementedError(
        "server-side Python iterator execution is an approved stable divergence "
        "(SB-DIV-007); the pinned Sharkbite execution path was non-functional"
    )


class LoggingConfiguration:
    @staticmethod
    def _set(level: int, api: NativeAPI | None = None) -> None:
        native = api or NativeAPI()
        native.require(CAP_CLIENT_PARITY_CONTROLS)
        error = C.c_void_p()
        status = native.lib.shoal_logging_set_level(level, C.byref(error))
        native.check(status, error)

    @staticmethod
    def enableDebugLogger() -> None:
        LoggingConfiguration._set(1)

    @staticmethod
    def enableTraceLogger() -> None:
        LoggingConfiguration._set(2)

    @staticmethod
    def disableLogger() -> None:
        LoggingConfiguration._set(0)
