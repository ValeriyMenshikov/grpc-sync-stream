"""``RequestStream``: stream-unary calls, built on ``multicallable.future()``."""

from __future__ import annotations

import threading
import weakref
from collections.abc import Callable
from types import TracebackType
from typing import Any, Generic, TypeVar

import grpc

from grpc_sync_stream._base import cancel_call
from grpc_sync_stream._queue import RequestQueue
from grpc_sync_stream._status import CancelledRpcError, raise_finished
from grpc_sync_stream._types import Metadata, MetadataArg, as_metadata

Req = TypeVar("Req")
Resp = TypeVar("Resp")
_RS = TypeVar("_RS", bound="RequestStream[Any, Any]")


def _finisher(requests: RequestQueue[Any], finished: threading.Event) -> Callable[[], None]:
    """Mark the call as over. Must not reference the stream: grpc keeps its done
    callbacks while the call runs, and a dropped stream has to be collectable."""

    def finish() -> None:
        # Also runs on grpc's thread: must not block or query the future.
        requests.abort()
        finished.set()

    return finish


class RequestStream(Generic[Req, Resp]):
    """A stream-unary call: ``write()`` requests, ``done_writing()``, then ``result()``.

    Runs on ``multicallable.future()``, so it needs no thread of its own.
    """

    def __init__(
        self,
        method: grpc.StreamUnaryMultiCallable[Req, Resp],
        *,
        metadata: MetadataArg = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
        wait_for_ready: bool | None = None,
        compression: grpc.Compression | None = None,
        max_pending_writes: int | None = None,
    ) -> None:
        self._requests: RequestQueue[Req] = RequestQueue(max_pending_writes)
        self._finished = threading.Event()
        self._future = method.future(
            self._requests,
            timeout=timeout,
            metadata=as_metadata(metadata),
            credentials=credentials,
            wait_for_ready=wait_for_ready,
            compression=compression,
        )
        self._finish = _finisher(self._requests, self._finished)
        finish = self._finish
        self._future.add_done_callback(lambda _future: finish())
        weakref.finalize(self, cancel_call, self._future).atexit = False

    def _cancelled_error(self) -> grpc.RpcError:
        # A plain grpcio future is itself the RpcError carrying CANCELLED.
        if isinstance(self._future, grpc.RpcError):
            return self._future
        return CancelledRpcError()

    def _failure(self) -> Exception | None:
        """The call's error, or None if it succeeded. Only valid once finished."""
        if self._future.cancelled():
            return self._cancelled_error()
        return self._future.exception()

    def _status_call(self) -> grpc.Call:
        """Wait for the call to end and return the object holding its real status.

        An interceptor failure gives a future whose ``code()`` is always
        INTERNAL; the real status sits on ``future.exception()``.
        """
        self._finished.wait()
        failure = self._failure()
        return failure if isinstance(failure, grpc.Call) else self._future

    def write(self, message: Req) -> None:
        """Queue ``message`` for sending without blocking.

        Blocks only when ``max_pending_writes`` is set and the queue is full.
        Raises ``RuntimeError`` after ``done_writing()``, and the call's
        ``grpc.RpcError`` if it has already failed.
        """
        # grpc wakes result() before it runs done callbacks: check the future
        # itself, so a write() after result() never queues a lost message.
        if self._future.done():
            self._finish()
        if not self._requests.put(message):
            self._finished.wait()
            raise_finished(self._failure())

    def done_writing(self) -> None:
        """Half-close the request stream. Idempotent."""
        self._requests.close()

    def result(self, timeout: float | None = None) -> Resp:
        """Wait for the response; raise ``grpc.RpcError`` if the call failed.

        Raises ``TimeoutError`` if the call is still running after ``timeout``.
        """
        try:
            # Positional: grpc's _FailureOutcome names this parameter differently.
            return self._future.result(timeout)
        except grpc.FutureTimeoutError:
            raise TimeoutError(f"no response within {timeout}s") from None
        except grpc.FutureCancelledError:
            raise self._cancelled_error() from None
        finally:
            if self._future.done():
                self._finish()

    def code(self) -> grpc.StatusCode:
        """Wait for the call to end and return its status code."""
        return self._status_call().code()

    def details(self) -> str | None:
        """Wait for the call to end and return its status details."""
        return self._status_call().details()

    def initial_metadata(self) -> Metadata | None:
        """Wait for the server's initial metadata, as sync grpc does."""
        if self.done():
            return self._status_call().initial_metadata()
        return self._future.initial_metadata()

    def trailing_metadata(self) -> Metadata | None:
        """Wait for the call to end and return the trailing metadata."""
        return self._status_call().trailing_metadata()

    def done(self) -> bool:
        """Whether the call is over."""
        return self._future.done()

    def cancel(self) -> bool:
        """Cancel the call; afterwards ``result()`` raises ``RpcError`` with CANCELLED.

        Returns False if the call was already over.
        """
        if self.done():
            return False
        cancelled = self._future.cancel()
        if cancelled:
            self._finish()
        return cancelled

    def __enter__(self: _RS) -> _RS:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.done_writing()
        if not self.done():
            self.cancel()
