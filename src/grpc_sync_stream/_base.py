"""What ``BidiStream`` and ``ResponseStream`` share: reading, status, cancel, ``with``.

Only the public ``grpc.Call`` API is used on the call object, so anything
``grpc.intercept_channel`` hands back (including interceptor wrappers) works
the same as the raw ``_MultiThreadedRendezvous``.
"""

from __future__ import annotations

import warnings
import weakref
from collections.abc import Callable, Iterator
from types import TracebackType
from typing import Any, Final, Generic, TypeVar

import grpc

from grpc_sync_stream._reader import ResponseReader
from grpc_sync_stream._status import status_code
from grpc_sync_stream._types import EOF, EOFType, Metadata

T = TypeVar("T")
_S = TypeVar("_S", bound="ResponseStreamBase[Any]")

#: How long ``__exit__`` waits for the reader thread after the call is over.
EXIT_JOIN_TIMEOUT: Final = 1.0


def cancel_call(call: object) -> None:
    """Finalizer of a stream dropped while its call runs: cancel it, as grpc does."""
    if isinstance(call, grpc.RpcContext):
        call.cancel()


def warn_not_cancellable(call: object) -> None:
    warnings.warn(
        f"cannot cancel: the interceptor returned {type(call).__name__}, not a grpc.Call",
        RuntimeWarning,
        stacklevel=3,
    )


class ResponseStreamBase(Generic[T]):
    """Common part of streams whose responses are read through a reader thread."""

    _call: object
    _reader: ResponseReader[T]
    _read_timeout: float | None

    def _start(
        self,
        call: Iterator[T],
        read_timeout: float | None,
        on_finish: Callable[[], None] = lambda: None,
    ) -> None:
        """Start reading ``call``; ``on_finish`` runs on the reader thread once it is over.

        Neither the reader nor ``on_finish`` may reference ``self``: then a
        stream dropped without ``with``/``cancel()`` gets collected and its
        finalizer cancels the call, like grpc does for a dropped call.
        """
        self._call = call
        self._read_timeout = read_timeout
        self._reader = ResponseReader(call, on_finish=on_finish)
        weakref.finalize(self, cancel_call, call).atexit = False

    def _close_writes(self) -> None:
        """Half-close the request side, if there is one."""

    def read(self, timeout: float | None = None) -> T | EOFType:
        """Return the next message, or :data:`EOF` once the stream ended with OK.

        Raises ``grpc.RpcError`` if the call failed, and ``TimeoutError`` if
        nothing arrived within ``timeout`` (the stream stays usable). Without
        ``timeout`` the ``read_timeout`` given to the constructor applies.
        """
        return self._reader.read(self._read_timeout if timeout is None else timeout)

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        message = self.read()
        if message is EOF:
            raise StopIteration
        return message

    def _status_call(self) -> grpc.Call | None:
        """Wait for the call to end and return the object holding its real status."""
        self._reader.wait()
        outcome = self._reader.outcome
        if isinstance(outcome, grpc.Call):
            return outcome
        if isinstance(self._call, grpc.Call):
            return self._call
        return None

    def code(self) -> grpc.StatusCode:
        """Wait for the call to end and return its status code."""
        return status_code(self._status_call(), self._reader.outcome)

    def details(self) -> str | None:
        """Wait for the call to end and return its status details."""
        call = self._status_call()
        if call is not None:
            return call.details()
        outcome = self._reader.outcome
        return None if outcome is EOF else str(outcome)

    def initial_metadata(self) -> Metadata | None:
        """Wait for the server's initial metadata, as sync grpc does."""
        if isinstance(self._call, grpc.Call):
            return self._call.initial_metadata()
        call = self._status_call()
        return call.initial_metadata() if call is not None else None

    def trailing_metadata(self) -> Metadata | None:
        """Wait for the call to end and return the trailing metadata."""
        call = self._status_call()
        return call.trailing_metadata() if call is not None else None

    def done(self) -> bool:
        """Whether the call is over; ``read()`` then no longer blocks."""
        return self._reader.finished

    def cancel(self) -> bool:
        """Cancel the call; afterwards ``read()`` raises ``RpcError`` with CANCELLED.

        Returns False if the call was already over. A call object from an
        interceptor that is not a ``grpc.Call`` cannot be cancelled: then a
        ``RuntimeWarning`` is issued and False returned.
        """
        if self._reader.finished:
            return False
        if not isinstance(self._call, grpc.RpcContext):
            warn_not_cancellable(self._call)
            return False
        cancelled = self._call.cancel()
        if cancelled:
            self._reader.mark_cancelled()
        return cancelled

    def __enter__(self: _S) -> _S:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._close_writes()
        if not self.done():
            self.cancel()
        self._reader.join(EXIT_JOIN_TIMEOUT)
