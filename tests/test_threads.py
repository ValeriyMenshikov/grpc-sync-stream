"""No thread outlives its stream, however the call ends."""

from __future__ import annotations

import gc
import time

import grpc
import pytest

from grpc_sync_stream import BidiStream, RequestStream, ResponseStream
from tests.conftest import Stubs, client_threads, msg, wait_until

N = 100


@pytest.fixture
def baseline(stubs: Stubs) -> int:
    # Warm the channel up so its lazily created threads are not counted as leaks.
    with BidiStream(stubs.chat) as s:
        s.write(msg("warm"))
        s.done_writing()
        list(s)
    return _stable_count()


def _stable_count() -> int:
    """Thread count once threads of earlier calls have finished exiting."""
    count = client_threads()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        time.sleep(0.2)
        now = client_threads()
        if now == count:
            break
        count = now
    return count


def _settles(baseline: int) -> bool:
    return wait_until(lambda: client_threads() <= baseline, timeout=10)


def test_cancelled_bidi_streams_do_not_leak_threads(stubs: Stubs, baseline: int) -> None:
    streams = [BidiStream(stubs.chat) for _ in range(N)]
    for s in streams:
        s.write(msg("x"))
    # After the echo, grpc's request thread is surely blocked waiting for the next write.
    for s in streams:
        assert s.read(timeout=5) == msg("X")
    assert client_threads() > baseline
    for s in streams:
        s.cancel()
    for s in streams:
        with pytest.raises(grpc.RpcError):
            s.read(timeout=5)
    assert _settles(baseline), (client_threads(), baseline)


def test_bidi_streams_left_by_with_do_not_leak_threads(stubs: Stubs, baseline: int) -> None:
    for _ in range(N):
        with BidiStream(stubs.chat) as s:
            s.write(msg("x"))
            assert s.read(timeout=5) == msg("X")
    assert _settles(baseline), (client_threads(), baseline)


def test_failed_and_server_ended_bidi_streams_do_not_leak_threads(stubs: Stubs, baseline: int) -> None:
    # The client never calls done_writing(): only the end of the call frees grpc's request thread.
    streams = [BidiStream(stubs.chat) for _ in range(N)]
    for i, s in enumerate(streams):
        s.write(msg("fail" if i % 2 else "end"))
    assert all(wait_until(s.done) for s in streams)
    assert _settles(baseline), (client_threads(), baseline)


def test_deadlined_bidi_streams_do_not_leak_threads(stubs: Stubs, baseline: int) -> None:
    streams = [BidiStream(stubs.chat, timeout=0.05) for _ in range(N)]
    assert all(wait_until(s.done) for s in streams)
    assert {s.code() for s in streams} == {grpc.StatusCode.DEADLINE_EXCEEDED}
    assert _settles(baseline), (client_threads(), baseline)


def test_cancelled_response_streams_do_not_leak_threads(stubs: Stubs, baseline: int) -> None:
    for _ in range(N):
        ResponseStream(stubs.expand, msg("slow")).cancel()
    assert _settles(baseline), (client_threads(), baseline)


def test_cancelled_request_streams_do_not_leak_threads(stubs: Stubs, baseline: int) -> None:
    # Nothing is written, so grpc's request thread blocks waiting for the first write.
    streams = [RequestStream(stubs.join) for _ in range(N)]
    for s in streams:
        s.cancel()
    assert _settles(baseline), (client_threads(), baseline)


def test_dropped_streams_are_cancelled(stubs: Stubs, baseline: int) -> None:
    # Like a plain grpc call: a stream nobody references any more is cancelled
    # on collection. Without it both servers would wait for requests forever.
    bidi = [BidiStream(stubs.chat) for _ in range(N)]
    unary = [RequestStream(stubs.join) for _ in range(N)]
    assert client_threads() > baseline
    del bidi, unary
    gc.collect()
    assert _settles(baseline), (client_threads(), baseline)
