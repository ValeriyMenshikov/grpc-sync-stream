"""``max_pending_writes``: ``write()`` blocks while the request queue is full.

grpc drains the queue as fast as the network allows, so a fake multicallable
that never reads on its own makes the blocking observable.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from typing import Any

import grpc
import pytest

from grpc_sync_stream import EOF, BidiStream, RequestStream
from tests.conftest import Stubs, msg


class FakeCancelled(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.CANCELLED


class FakeCall(grpc.Call):
    """Yields no responses; ends with CANCELLED once cancelled."""

    def __init__(self) -> None:
        self._cancelled = threading.Event()

    def __iter__(self) -> FakeCall:
        return self

    def __next__(self) -> str:
        self._cancelled.wait()
        raise FakeCancelled

    def cancel(self) -> bool:
        self._cancelled.set()
        return True

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.CANCELLED

    def details(self) -> str:
        return ""

    def initial_metadata(self) -> Any:
        return ()

    def trailing_metadata(self) -> Any:
        return ()

    def is_active(self) -> bool:
        return not self._cancelled.is_set()

    def time_remaining(self) -> float:
        return float("inf")

    def add_callback(self, callback: Callable[[], None]) -> bool:
        return False


class FakeStreamStream(grpc.StreamStreamMultiCallable):  # type: ignore[type-arg]
    def __init__(self) -> None:
        self.requests: Iterator[str] = iter(())
        self.call = FakeCall()

    def __call__(self, request_iterator: Iterator[str], *args: Any, **kwargs: Any) -> Any:
        self.requests = request_iterator
        return self.call


class FakeFuture(grpc.Future):  # type: ignore[type-arg]
    """Never completes on its own; ``cancel()`` completes it."""

    def __init__(self) -> None:
        self._cancelled = False
        self._callbacks: list[Callable[[Any], None]] = []

    def cancel(self) -> bool:
        if self._cancelled:
            return False
        self._cancelled = True
        for callback in self._callbacks:
            callback(self)
        return True

    def cancelled(self) -> bool:
        return self._cancelled

    def running(self) -> bool:
        return not self._cancelled

    def done(self) -> bool:
        return self._cancelled

    def result(self, timeout: float | None = None) -> Any:
        raise grpc.FutureCancelledError

    def exception(self, timeout: float | None = None) -> Any:
        return None

    def traceback(self, timeout: float | None = None) -> Any:
        return None

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        self._callbacks.append(fn)


class FakeStreamUnary(grpc.StreamUnaryMultiCallable):  # type: ignore[type-arg]
    def __init__(self) -> None:
        self.requests: Iterator[str] = iter(())
        self.call = FakeFuture()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def with_call(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def future(self, request_iterator: Iterator[str], *args: Any, **kwargs: Any) -> Any:
        self.requests = request_iterator
        return self.call


def _blocked_write(
    s: BidiStream[str, str] | RequestStream[str, str], value: str
) -> tuple[threading.Thread, list[BaseException]]:
    errors: list[BaseException] = []

    def write() -> None:
        try:
            s.write(value)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=write, daemon=True)
    thread.start()
    thread.join(0.1)
    return thread, errors


def test_write_blocks_while_queue_is_full() -> None:
    fake = FakeStreamStream()
    s = BidiStream[str, str](fake, max_pending_writes=2)
    s.write("a")
    s.write("b")
    thread, errors = _blocked_write(s, "c")
    assert thread.is_alive()

    assert next(fake.requests) == "a"
    thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    assert [next(fake.requests), next(fake.requests)] == ["b", "c"]
    s.cancel()


def test_blocked_write_raises_when_call_ends() -> None:
    fake = FakeStreamStream()
    s = BidiStream[str, str](fake, max_pending_writes=1)
    s.write("a")
    thread, errors = _blocked_write(s, "b")
    assert thread.is_alive()
    s.cancel()
    thread.join(5)
    assert [type(e) for e in errors] == [FakeCancelled]


def test_blocked_write_raises_after_done_writing() -> None:
    fake = FakeStreamStream()
    s = BidiStream[str, str](fake, max_pending_writes=1)
    s.write("a")
    thread, errors = _blocked_write(s, "b")
    s.done_writing()
    thread.join(5)
    assert [type(e) for e in errors] == [RuntimeError]
    assert list(fake.requests) == ["a"]
    s.cancel()


def test_invalid_max_pending_writes() -> None:
    with pytest.raises(ValueError, match="max_pending_writes"):
        BidiStream[str, str](FakeStreamStream(), max_pending_writes=0)


def test_bounded_queue_on_real_call(stubs: Stubs) -> None:
    with BidiStream(stubs.chat, max_pending_writes=1) as s:
        for i in range(100):
            s.write(msg(str(i)))
        s.done_writing()
        assert [m.value for m in s] == [str(i) for i in range(100)]
        assert s.read() is EOF


def test_request_stream_write_blocks_while_queue_is_full() -> None:
    fake = FakeStreamUnary()
    s = RequestStream[str, str](fake, max_pending_writes=1)
    s.write("a")
    thread, errors = _blocked_write(s, "b")
    assert thread.is_alive()

    assert next(fake.requests) == "a"
    thread.join(5)
    assert not thread.is_alive()
    assert errors == []

    thread, errors = _blocked_write(s, "c")
    assert thread.is_alive()
    assert s.cancel()
    thread.join(5)
    assert [e.code() for e in errors if isinstance(e, grpc.RpcError)] == [grpc.StatusCode.CANCELLED]
