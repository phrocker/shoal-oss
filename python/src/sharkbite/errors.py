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


STATUS_EXCEPTIONS: dict[int, type[BaseException]] = {
    1: InvalidArgumentError,
    2: InvalidArgumentError,
    4: UnsupportedError,
    6: ClosedError,
    7: CancelledError,
    8: DeadlineExceededError,
    9: NotFoundError,
    10: PermissionDeniedError,
    19: AlreadyExistsError,
}


def exception_for_status(
    status: int, message: str, *, compatibility_class: int = 0
) -> BaseException:
    cls = STATUS_EXCEPTIONS.get(status)
    if cls is None:
        cls = ClientException if compatibility_class == 1 else ShoalError
    return cls(message, status=status)  # type: ignore[call-arg]
