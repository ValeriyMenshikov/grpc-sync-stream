from __future__ import annotations

import logging
import threading
import time

import grpc
import pytest

from grpc_sync_stream import EOF, BidiStream
from tests.conftest import MANY, SLOW, Stubs, msg, wait_until


def test_ping_pong(stubs: Stubs) -> None:
    with BidiStream(stubs.chat, metadata=(("x-user", "a"),), timeout=30) as s:
        s.write(msg("hi"))
        reply = s.read(timeout=5)
        assert reply is not EOF
        assert reply.value == "HI"
        s.write(msg("again"))
        assert s.read(timeout=5) == msg("AGAIN")
        s.done_writing()
        assert s.read() is EOF
        assert s.code() == grpc.StatusCode.OK
        assert s.done()


def test_status_and_metadata(stubs: Stubs) -> None:
    with BidiStream(stubs.chat, metadata=(("x-user", "alice"),)) as s:
        assert dict(s.initial_metadata() or ()) == {"x-echo-user": "alice"}
        s.done_writing()
        assert s.read() is EOF
        assert s.code() == grpc.StatusCode.OK
        assert s.details() in (None, "")
        assert dict(s.trailing_metadata() or ())["x-trailer"] == "bye"


# read()


def test_read_eof_is_immediate_when_server_ends_stream(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("end"))
        started = time.monotonic()
        assert s.read(timeout=10) is EOF
        assert time.monotonic() - started < 2


def test_read_after_eof_returns_eof_again(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.done_writing()
        assert s.read() is EOF
        assert s.read() is EOF
        assert s.read(timeout=0) is EOF


def test_read_raises_real_status_immediately(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("fail"))
        started = time.monotonic()
        with pytest.raises(grpc.RpcError) as first:
            s.read(timeout=10)
        assert time.monotonic() - started < 2
        assert first.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert first.value.details() == "boom"
        with pytest.raises(grpc.RpcError) as second:
            s.read()
        assert second.value is first.value
        assert s.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert s.details() == "boom"


def test_read_timeout_keeps_stream_alive(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("slow"))
        with pytest.raises(TimeoutError):
            s.read(timeout=0.05)
        assert not s.done()
        assert s.read(timeout=5) == msg("SLOW")
        s.write(msg("ok"))
        assert s.read(timeout=5) == msg("OK")
        s.done_writing()
        assert s.read() is EOF


def test_read_timeout_from_constructor(stubs: Stubs) -> None:
    with BidiStream(stubs.chat, read_timeout=0.05) as s:
        s.write(msg("slow"))
        with pytest.raises(TimeoutError):
            s.read()
        assert s.read(timeout=5) == msg("SLOW")


def test_read_without_timeout_waits(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("slow"))
        started = time.monotonic()
        assert s.read() == msg("SLOW")
        assert time.monotonic() - started >= SLOW * 0.8


def test_deadline_exceeded(stubs: Stubs) -> None:
    with BidiStream(stubs.chat, timeout=0.1) as s:
        s.write(msg("slow"))
        with pytest.raises(grpc.RpcError) as err:
            s.read(timeout=5)
        assert err.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        assert s.code() == grpc.StatusCode.DEADLINE_EXCEEDED


def test_many_responses_to_one_request(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("many"))
        s.done_writing()
        assert [m.value for m in s] == [f"many-{i}" for i in range(MANY)]


# iteration


def test_iteration_reads_until_eof(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        for word in ("a", "b", "c"):
            s.write(msg(word))
        s.done_writing()
        assert [m.value for m in s] == ["A", "B", "C"]
        assert s.read() is EOF


def test_iteration_raises_error(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("a"))
        s.write(msg("fail"))
        seen: list[str] = []
        with pytest.raises(grpc.RpcError) as err:
            seen.extend(m.value for m in s)
        assert seen == ["A"]
        assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


# write()


def test_write_does_not_block(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        started = time.monotonic()
        for i in range(1000):
            s.write(msg(str(i)))
        assert time.monotonic() - started < 1
        s.done_writing()
        assert len(list(s)) == 1000


def test_write_after_done_writing_raises(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.done_writing()
        with pytest.raises(RuntimeError, match="done_writing"):
            s.write(msg("late"))


def test_write_after_failure_raises_the_error(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("fail"))
        with pytest.raises(grpc.RpcError) as read_err:
            s.read(timeout=5)
        with pytest.raises(grpc.RpcError) as write_err:
            s.write(msg("late"))
        assert write_err.value is read_err.value


def test_write_after_server_finished_ok_raises(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("end"))
        assert s.read(timeout=5) is EOF
        with pytest.raises(RuntimeError, match="finished"):
            s.write(msg("late"))
        assert s.code() == grpc.StatusCode.OK


def test_done_writing_is_idempotent(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("x"))
        s.done_writing()
        s.done_writing()
        assert s.read(timeout=5) == msg("X")
        assert s.read(timeout=5) is EOF
        s.done_writing()


# cancel()


def test_cancel_makes_read_raise_cancelled(stubs: Stubs) -> None:
    s = BidiStream(stubs.chat)
    s.write(msg("many"))
    assert s.read(timeout=5) is not EOF
    assert s.cancel() is True
    with pytest.raises(grpc.RpcError) as err:
        s.read(timeout=5)
    assert err.value.code() == grpc.StatusCode.CANCELLED
    with pytest.raises(grpc.RpcError) as again:
        s.read(timeout=5)
    assert again.value is err.value
    assert s.code() == grpc.StatusCode.CANCELLED
    assert s.done()
    with pytest.raises(grpc.RpcError):
        s.write(msg("late"))
    assert s.cancel() is False


def test_cancel_while_read_is_blocked(stubs: Stubs) -> None:
    s = BidiStream(stubs.chat)
    errors: list[BaseException] = []

    def reader() -> None:
        try:
            s.read()
        except grpc.RpcError as exc:
            errors.append(exc)

    thread = threading.Thread(target=reader)
    thread.start()
    time.sleep(0.05)
    s.cancel()
    thread.join(5)
    assert not thread.is_alive()
    assert [e.code() for e in errors if isinstance(e, grpc.RpcError)] == [grpc.StatusCode.CANCELLED]


def test_cancel_after_completion_is_a_noop(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("x"))
        s.done_writing()
        assert wait_until(s.done)
        assert s.cancel() is False
        assert s.read() == msg("X")
        assert s.read() is EOF
        assert s.code() == grpc.StatusCode.OK


def test_cancel_logs_nothing(
    stubs: Stubs, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    hooked: list[object] = []
    monkeypatch.setattr(threading, "excepthook", hooked.append)
    caplog.set_level(logging.DEBUG)
    s = BidiStream(stubs.chat)
    s.write(msg("slow"))
    s.cancel()
    assert wait_until(s.done)
    s.__exit__(None, None, None)
    assert hooked == []
    assert [r for r in caplog.records if r.name != "tests.interceptor"] == []


# __exit__


def test_exit_half_closes_and_cancels_unfinished_call(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.write(msg("slow"))
    assert s.done()
    assert s.code() == grpc.StatusCode.CANCELLED
    with pytest.raises(RuntimeError, match="done_writing"):
        s.write(msg("late"))


def test_exit_keeps_finished_call_status(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        s.done_writing()
        assert s.read(timeout=5) is EOF
    assert s.code() == grpc.StatusCode.OK


def test_exit_does_not_replace_exception(stubs: Stubs) -> None:
    class Boom(Exception):
        pass

    with pytest.raises(Boom), BidiStream(stubs.chat) as s:
        s.write(msg("slow"))
        raise Boom
    assert s.code() == grpc.StatusCode.CANCELLED


def test_exit_does_not_replace_rpc_error(stubs: Stubs) -> None:
    with pytest.raises(grpc.RpcError) as err, BidiStream(stubs.chat) as s:
        s.write(msg("fail"))
        s.read(timeout=5)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_concurrent_reader_and_writer(stubs: Stubs) -> None:
    received: list[str] = []
    with BidiStream(stubs.chat) as s:

        def reader() -> None:
            received.extend(m.value for m in s)

        thread = threading.Thread(target=reader)
        thread.start()
        for i in range(200):
            s.write(msg(f"m{i}"))
        s.done_writing()
        thread.join(5)
    assert received == [f"M{i}" for i in range(200)]


def test_negative_read_timeout_is_rejected(stubs: Stubs) -> None:
    with BidiStream(stubs.chat) as s:
        with pytest.raises(ValueError, match="timeout"):
            s.read(timeout=-1)
        s.cancel()
        with pytest.raises(ValueError, match="timeout"):
            s.read(timeout=-1)


def test_metadata_after_cancel_before_headers(stubs: Stubs) -> None:
    s = BidiStream(stubs.chat)
    assert s.cancel()
    # No headers ever arrive: these must return, not wait for them.
    assert s.initial_metadata() is not None
    assert s.code() == grpc.StatusCode.CANCELLED
