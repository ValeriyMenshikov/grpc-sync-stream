"""Interceptor-specific behaviour. The rest of the suite also runs through an interceptor."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import grpc
import pytest

from grpc_sync_stream import EOF, BidiStream, RequestStream, ResponseStream
from tests.conftest import SERVICE, LoggingInterceptor, Stubs, msg


def test_logging_interceptor_sees_all_four_call_kinds(target: str) -> None:
    interceptor = LoggingInterceptor()
    with grpc.insecure_channel(target) as raw:
        stubs = Stubs(grpc.intercept_channel(raw, interceptor), interceptor)
        assert stubs.upper(msg("u")) == msg("U")
        with ResponseStream(stubs.expand, msg("e")) as rs:
            assert len(list(rs)) == 3
        with RequestStream(stubs.join) as qs:
            qs.write(msg("j"))
            qs.done_writing()
            assert qs.result(timeout=5) == msg("J")
        with BidiStream(stubs.chat) as bs:
            bs.write(msg("c"))
            assert bs.read(timeout=5) == msg("C")
            bs.done_writing()
            assert bs.read(timeout=5) is EOF

    starts = [e[1:] for e in interceptor.events if e[0] == "start"]
    assert starts == [
        ("unary_unary", f"/{SERVICE}/Upper"),
        ("unary_stream", f"/{SERVICE}/Expand"),
        ("stream_unary", f"/{SERVICE}/Join"),
        ("stream_stream", f"/{SERVICE}/Chat"),
    ]
    # Responses really went through the interceptor's own call wrapper.
    assert ("response", "C") in interceptor.events
    assert ("response", "e-0") in interceptor.events


class Denied(grpc.RpcError, grpc.Call):
    """What an auth interceptor raises to reject a call locally."""

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.PERMISSION_DENIED

    def details(self) -> str:
        return "no token"

    def initial_metadata(self) -> Any:
        return ()

    def trailing_metadata(self) -> Any:
        return (("x-reason", "auth"),)

    def is_active(self) -> bool:
        return False

    def time_remaining(self) -> float:
        return float("inf")

    def cancel(self) -> bool:
        return False

    def add_callback(self, callback: Callable[[], None]) -> bool:
        return False


class Rejecting(
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """grpc turns the exception into ``_FailureOutcome``, whose own ``code()`` is always INTERNAL."""

    def intercept_unary_stream(self, *args: Any) -> Any:
        raise Denied

    def intercept_stream_unary(self, *args: Any) -> Any:
        raise Denied

    def intercept_stream_stream(self, *args: Any) -> Any:
        raise Denied


@pytest.fixture
def rejected(target: str) -> Iterator[Stubs]:
    with grpc.insecure_channel(target) as raw:
        yield Stubs(grpc.intercept_channel(raw, Rejecting()), None)


def test_request_stream_reports_real_status_not_internal(rejected: Stubs) -> None:
    with RequestStream(rejected.join) as s:
        with pytest.raises(Denied):
            s.result(timeout=5)
        assert s.done()
        assert s.code() == grpc.StatusCode.PERMISSION_DENIED
        assert s.details() == "no token"
        assert s.trailing_metadata() == (("x-reason", "auth"),)
        with pytest.raises(Denied):
            s.write(msg("late"))


def test_bidi_stream_reports_real_status_not_internal(rejected: Stubs) -> None:
    with BidiStream(rejected.chat) as s:
        with pytest.raises(Denied):
            s.read(timeout=5)
        assert s.code() == grpc.StatusCode.PERMISSION_DENIED
        assert s.details() == "no token"
        with pytest.raises(Denied):
            s.write(msg("late"))


def test_response_stream_reports_real_status_not_internal(rejected: Stubs) -> None:
    with ResponseStream(rejected.expand, msg("x")) as s:
        with pytest.raises(Denied):
            list(s)
        assert s.code() == grpc.StatusCode.PERMISSION_DENIED


class Generating(grpc.UnaryStreamClientInterceptor, grpc.StreamStreamClientInterceptor):
    """A common shortcut: re-yield the responses, hiding grpc's call object.

    It breaks the interceptor contract (hence the ignores), but it is seen in the wild.
    """

    def intercept_unary_stream(  # type: ignore[override]
        self, continuation: Any, details: Any, request: Any
    ) -> Iterator[Any]:
        yield from continuation(details, request)

    def intercept_stream_stream(  # type: ignore[override]
        self, continuation: Any, details: Any, requests: Any
    ) -> Iterator[Any]:
        yield from continuation(details, requests)


@pytest.fixture
def generating(target: str) -> Iterator[Stubs]:
    with grpc.insecure_channel(target) as raw:
        yield Stubs(grpc.intercept_channel(raw, Generating()), None)


def test_generator_interceptor_streams_and_reports_status(generating: Stubs) -> None:
    with ResponseStream(generating.expand, msg("x")) as s:
        assert [m.value for m in s] == ["x-0", "x-1", "x-2"]
        assert s.code() == grpc.StatusCode.OK
        assert s.details() is None


def test_generator_interceptor_cancel_warns(generating: Stubs) -> None:
    with BidiStream(generating.chat) as s:
        with pytest.warns(RuntimeWarning, match="not a grpc.Call"):
            assert s.cancel() is False
        assert not s.done()
        s.write(msg("x"))
        s.done_writing()
        assert [m.value for m in s] == ["X"]
