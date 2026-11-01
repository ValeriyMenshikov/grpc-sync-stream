"""Once ``read()`` / ``result()`` has reported the end, the stream is over for every method.

The window between grpc finishing a call and the library noticing it is tiny,
so these tests widen it: busy threads and a minimal GIL switch interval.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Iterator

import grpc
import pytest

from grpc_sync_stream import EOF, BidiStream, RequestStream, ResponseStream
from tests.conftest import Stubs, msg

ROUNDS = 200
BUSY_THREADS = 4


@pytest.fixture(autouse=True)
def contention() -> Iterator[None]:
    stop = threading.Event()

    def spin() -> None:
        while not stop.is_set():
            sum(range(100))

    threads = [threading.Thread(target=spin, daemon=True) for _ in range(BUSY_THREADS)]
    interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    for t in threads:
        t.start()
    try:
        yield
    finally:
        stop.set()
        sys.setswitchinterval(interval)
        for t in threads:
            t.join()


def test_bidi_is_over_once_read_returns_eof(stubs: Stubs) -> None:
    for _ in range(ROUNDS):
        with BidiStream(stubs.chat) as s:
            s.write(msg("end"))
            assert s.read(timeout=5) is EOF
            assert s.done()
            with pytest.raises(RuntimeError, match="finished"):
                s.write(msg("late"))


def test_bidi_is_over_once_read_raises(stubs: Stubs) -> None:
    for _ in range(ROUNDS):
        with BidiStream(stubs.chat) as s:
            s.write(msg("fail"))
            with pytest.raises(grpc.RpcError):
                s.read(timeout=5)
            assert s.done()
            with pytest.raises(grpc.RpcError) as err:
                s.write(msg("late"))
            assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_response_stream_is_over_once_read_returns_eof(stubs: Stubs) -> None:
    for _ in range(ROUNDS):
        with ResponseStream(stubs.expand, msg("x")) as s:
            assert len(list(s)) == 3
            assert s.done()


def test_request_stream_is_over_once_result_returns(stubs: Stubs) -> None:
    for _ in range(ROUNDS):
        with RequestStream(stubs.join) as s:
            s.write(msg("end"))
            assert s.result(timeout=5) == msg("")
            assert s.done()
            with pytest.raises(RuntimeError, match="finished"):
                s.write(msg("late"))


def test_request_stream_is_over_once_result_raises(stubs: Stubs) -> None:
    for _ in range(ROUNDS):
        with RequestStream(stubs.join) as s:
            s.write(msg("fail"))
            with pytest.raises(grpc.RpcError):
                s.result(timeout=5)
            assert s.done()
            with pytest.raises(grpc.RpcError) as err:
                s.write(msg("late"))
            assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
