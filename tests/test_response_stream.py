from __future__ import annotations

import time

import grpc
import pytest

from grpc_sync_stream import EOF, ResponseStream
from tests.conftest import MANY, Stubs, msg


def test_read_until_eof(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("a"), metadata=(("x-user", "bob"),)) as s:
        assert s.read(timeout=5) == msg("a-0")
        assert s.read(timeout=5) == msg("a-1")
        assert s.read(timeout=5) == msg("a-2")
        assert s.read(timeout=5) is EOF
        assert s.read() is EOF
        assert s.code() == grpc.StatusCode.OK
        assert dict(s.initial_metadata() or ()) == {"x-echo-user": "bob"}
        assert dict(s.trailing_metadata() or ())["x-trailer"] == "bye"


def test_iteration(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("many")) as s:
        assert [m.value for m in s] == [f"many-{i}" for i in range(MANY)]


def test_error_after_partial_response(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("fail")) as s:
        assert s.read(timeout=5) == msg("fail-0")
        started = time.monotonic()
        with pytest.raises(grpc.RpcError) as first:
            s.read(timeout=10)
        assert time.monotonic() - started < 2
        assert first.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        with pytest.raises(grpc.RpcError) as second:
            s.read()
        assert second.value is first.value
        assert s.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert s.details() == "boom"


def test_iteration_raises_error(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("fail")) as s:
        seen: list[str] = []
        with pytest.raises(grpc.RpcError) as err:
            seen.extend(m.value for m in s)
        assert seen == ["fail-0"]
        assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_read_timeout_keeps_stream_alive(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("slow")) as s:
        with pytest.raises(TimeoutError):
            s.read(timeout=0.05)
        assert s.read(timeout=5) == msg("slow-0")


def test_read_timeout_from_constructor(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("slow"), read_timeout=0.05) as s:
        with pytest.raises(TimeoutError):
            s.read()
        assert s.read(timeout=5) == msg("slow-0")


def test_deadline_exceeded(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("slow"), timeout=0.1) as s:
        with pytest.raises(grpc.RpcError) as err:
            s.read(timeout=5)
        assert err.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED


def test_cancel_running_call(stubs: Stubs) -> None:
    s = ResponseStream(stubs.expand, msg("slow"))
    assert s.cancel() is True
    with pytest.raises(grpc.RpcError) as err:
        s.read(timeout=5)
    assert err.value.code() == grpc.StatusCode.CANCELLED
    assert s.code() == grpc.StatusCode.CANCELLED
    assert s.done()
    assert s.cancel() is False


def test_exit_cancels_unfinished_call(stubs: Stubs) -> None:
    with ResponseStream(stubs.expand, msg("slow")) as s:
        pass
    assert s.done()
    assert s.code() == grpc.StatusCode.CANCELLED


def test_exit_does_not_replace_exception(stubs: Stubs) -> None:
    with pytest.raises(KeyError), ResponseStream(stubs.expand, msg("slow")):
        raise KeyError("x")
