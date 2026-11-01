from __future__ import annotations

import time

import grpc
import pytest

from grpc_sync_stream import RequestStream
from tests.conftest import SLOW, Stubs, msg, wait_until


def test_write_then_result(stubs: Stubs) -> None:
    with RequestStream(stubs.join, metadata=(("x-user", "carol"),)) as s:
        for word in ("a", "b", "c"):
            s.write(msg(word))
        s.done_writing()
        assert s.result(timeout=5) == msg("A,B,C")
        assert s.code() == grpc.StatusCode.OK
        assert s.done()
        assert dict(s.initial_metadata() or ()) == {"x-echo-user": "carol"}
        assert dict(s.trailing_metadata() or ())["x-trailer"] == "bye"


def test_result_raises_real_status(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("fail"))
        with pytest.raises(grpc.RpcError) as err:
            s.result(timeout=5)
        assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert s.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert s.details() == "boom"


def test_result_timeout_keeps_call_alive(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("slow"))
        s.done_writing()
        with pytest.raises(TimeoutError):
            s.result(timeout=0.05)
        assert not s.done()
        assert s.result(timeout=5) == msg("SLOW")


def test_result_without_timeout_waits(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("slow"))
        s.done_writing()
        started = time.monotonic()
        assert s.result() == msg("SLOW")
        assert time.monotonic() - started >= SLOW * 0.8


def test_deadline_exceeded(stubs: Stubs) -> None:
    with RequestStream(stubs.join, timeout=0.1) as s:
        s.write(msg("slow"))
        s.done_writing()
        with pytest.raises(grpc.RpcError) as err:
            s.result(timeout=5)
        assert err.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED


def test_write_after_done_writing_raises(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.done_writing()
        with pytest.raises(RuntimeError, match="done_writing"):
            s.write(msg("late"))
        assert s.result(timeout=5) == msg("")


def test_write_after_failure_raises_the_error(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("fail"))
        assert wait_until(s.done)
        with pytest.raises(grpc.RpcError) as err:
            s.write(msg("late"))
        assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_write_after_early_response_raises(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("a"))
        s.write(msg("end"))
        assert s.result(timeout=5) == msg("A")
        with pytest.raises(RuntimeError, match="finished"):
            s.write(msg("late"))


def test_done_writing_is_idempotent(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("x"))
        s.done_writing()
        s.done_writing()
        assert s.result(timeout=5) == msg("X")
        s.done_writing()


def test_cancel(stubs: Stubs) -> None:
    s = RequestStream(stubs.join)
    s.write(msg("a"))
    assert s.cancel() is True
    with pytest.raises(grpc.RpcError) as err:
        s.result(timeout=5)
    assert err.value.code() == grpc.StatusCode.CANCELLED
    assert s.code() == grpc.StatusCode.CANCELLED
    assert s.done()
    with pytest.raises(grpc.RpcError) as write_err:
        s.write(msg("late"))
    assert write_err.value.code() == grpc.StatusCode.CANCELLED
    assert s.cancel() is False


def test_exit_cancels_unfinished_call(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("slow"))
    assert wait_until(s.done)
    assert s.code() == grpc.StatusCode.CANCELLED
    with pytest.raises(RuntimeError, match="done_writing"):
        s.write(msg("late"))


def test_exit_keeps_finished_call_status(stubs: Stubs) -> None:
    with RequestStream(stubs.join) as s:
        s.write(msg("x"))
        s.done_writing()
        assert s.result(timeout=5) == msg("X")
    assert s.code() == grpc.StatusCode.OK


def test_exit_does_not_replace_exception(stubs: Stubs) -> None:
    with pytest.raises(ZeroDivisionError), RequestStream(stubs.join) as s:
        s.write(msg("a"))
        _ = 1 / 0
