from __future__ import annotations


class ShoalError(RuntimeError):
    """Base exception raised by the Shoal binding."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class ClientException(ShoalError):
    """Compatibility exception for Sharkbite client failures."""


class InvalidArgumentError(ShoalError):
    pass


class InvalidHandleError(ShoalError):
    pass


class ClosedError(ShoalError):
    pass


class CancelledError(ShoalError):
    pass


class DeadlineExceededError(ShoalError):
    pass


class NotFoundError(ShoalError):
    pass


class PermissionDeniedError(ShoalError):
    pass


class AlreadyExistsError(ShoalError):
    pass


class UnsupportedError(NotImplementedError, ShoalError):
    pass


class OutOfMemoryError(ShoalError):
    pass


class BootstrapError(ShoalError):
    pass


class UnavailableError(ShoalError):
    pass


class DiscoveryUnavailableError(UnavailableError):
    pass


class TabletUnavailableError(UnavailableError):
    pass


class SecurityUnavailableError(UnavailableError):
    pass


class RangeSpansTabletsError(ShoalError):
    pass


class CleanupError(ShoalError):
    pass


class OperationError(ShoalError):
    pass


class RetryExhaustedError(ShoalError):
    pass


class MutationRejectedError(ShoalError):
    pass


class AmbiguousWriteError(ShoalError):
    pass


class NamespaceNotEmptyError(ShoalError):
    pass


class TableOfflineError(ShoalError):
    pass


class UserNotFoundError(ShoalError):
    pass


class BadCredentialsError(PermissionDeniedError):
    pass


class IncompleteError(ShoalError):
    pass


class InternalError(ShoalError):
    pass


STATUS_EXCEPTIONS: dict[int, type[BaseException]] = {
    1: InvalidArgumentError,
    2: InvalidHandleError,
    3: OutOfMemoryError,
    4: UnsupportedError,
    5: BootstrapError,
    6: ClosedError,
    7: CancelledError,
    8: DeadlineExceededError,
    9: NotFoundError,
    10: PermissionDeniedError,
    11: DiscoveryUnavailableError,
    12: TabletUnavailableError,
    13: RangeSpansTabletsError,
    14: CleanupError,
    15: OperationError,
    16: RetryExhaustedError,
    17: MutationRejectedError,
    18: AmbiguousWriteError,
    19: AlreadyExistsError,
    20: UnavailableError,
    21: NamespaceNotEmptyError,
    22: TableOfflineError,
    23: UserNotFoundError,
    24: BadCredentialsError,
    25: SecurityUnavailableError,
    26: IncompleteError,
    255: InternalError,
}


def exception_for_status(
    status: int, message: str, *, compatibility_class: int = 0
) -> BaseException:
    cls = ClientException if compatibility_class == 1 else STATUS_EXCEPTIONS.get(status)
    if cls is None:
        cls = ShoalError
    return cls(message, status=status)  # type: ignore[call-arg]
