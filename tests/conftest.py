"""A local in-process gRPC server and channels to it, plain and intercepted.

Every test that takes ``stubs`` runs twice: on a plain channel and through
``grpc.intercept_channel`` with an interceptor that logs all four call kinds
and wraps stream responses in its own ``grpc.Call`` object.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from concurrent import futures
from dataclasses import dataclass, field
from typing import Any

import grpc
import pytest
from google.protobuf.wrappers_pb2 import StringValue

SERVICE = "test.Echo"
SLOW = 0.5
MANY = 50
SERVER_THREAD_PREFIX = "test-server"

log = logging.getLogger("tests.interceptor")


def _meta(context: grpc.ServicerContext) -> None:
    user = dict(context.invocation_metadata()).get("x-user", "")
    context.send_initial_metadata((("x-echo-user", str(user)),))
    context.set_trailing_metadata((("x-trailer", "bye"),))


def _upper(request: StringValue, context: grpc.ServicerContext) -> StringValue:
    return StringValue(value=request.value.upper())


def _chat(requests: Iterator[StringValue], context: grpc.ServicerContext) -> Iterator[StringValue]:
    """Bidi echo: upper-cases each message; a few words trigger special behaviour."""
    _meta(context)
    for request in requests:
        text = request.value
        if text == "fail":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "boom")
        if text == "end":
            return  # the server finishes before the client
        if text == "many":
            for i in range(MANY):
                yield StringValue(value=f"many-{i}")
            continue
        if text == "slow":
            time.sleep(SLOW)
        yield StringValue(value=text.upper())


def _expand(request: StringValue, context: grpc.ServicerContext) -> Iterator[StringValue]:
    """Server stream: three numbered copies of the request."""
    _meta(context)
    text = request.value
    count = MANY if text == "many" else 3
    for i in range(count):
        if text == "slow":
            time.sleep(SLOW)
        yield StringValue(value=f"{text}-{i}")
        if text == "fail":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "boom")


def _join(requests: Iterator[StringValue], context: grpc.ServicerContext) -> StringValue:
    """Client stream: joins upper-cased requests with commas."""
    _meta(context)
    parts = []
    for request in requests:
        text = request.value
        if text == "fail":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "boom")
        if text == "end":
            break  # answer before the client is done writing
        if text == "slow":
            time.sleep(SLOW)
        parts.append(text.upper())
    return StringValue(value=",".join(parts))


def _handler(kind: str, fn: Callable[..., Any]) -> grpc.RpcMethodHandler[Any, Any]:
    factory = getattr(grpc, f"{kind}_rpc_method_handler")
    handler: grpc.RpcMethodHandler[Any, Any] = factory(
        fn, request_deserializer=StringValue.FromString, response_serializer=StringValue.SerializeToString
    )
    return handler


@pytest.fixture(scope="session")
def target() -> Iterator[str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=256, thread_name_prefix=SERVER_THREAD_PREFIX))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                SERVICE,
                {
                    "Upper": _handler("unary_unary", _upper),
                    "Expand": _handler("unary_stream", _expand),
                    "Join": _handler("stream_unary", _join),
                    "Chat": _handler("stream_stream", _chat),
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"127.0.0.1:{port}"
    server.stop(None)


class LoggedCall(grpc.Call):
    """An interceptor's own call object: delegates to the real call and logs responses."""

    def __init__(self, call: Any, events: list[tuple[str, ...]]) -> None:
        self._call = call
        self._events = events

    def __iter__(self) -> LoggedCall:
        return self

    def __next__(self) -> Any:
        message = next(self._call)
        self._events.append(("response", message.value))
        return message

    def initial_metadata(self) -> Any:
        return self._call.initial_metadata()

    def trailing_metadata(self) -> Any:
        return self._call.trailing_metadata()

    def code(self) -> grpc.StatusCode:
        code: grpc.StatusCode = self._call.code()
        return code

    def details(self) -> str:
        details: str = self._call.details()
        return details

    def is_active(self) -> bool:
        active: bool = self._call.is_active()
        return active

    def time_remaining(self) -> float:
        remaining: float = self._call.time_remaining()
        return remaining

    def cancel(self) -> bool:
        cancelled: bool = self._call.cancel()
        return cancelled

    def add_callback(self, callback: Callable[[], None]) -> bool:
        added: bool = self._call.add_callback(callback)
        return added


class LoggingInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Logs every call kind; wraps stream responses in :class:`LoggedCall`."""

    def __init__(self) -> None:
        self.events: list[tuple[str, ...]] = []

    def _start(self, kind: str, details: grpc.ClientCallDetails) -> None:
        log.info("%s %s", kind, details.method)
        self.events.append(("start", kind, details.method))

    def intercept_unary_unary(self, continuation: Any, details: grpc.ClientCallDetails, request: Any) -> Any:
        self._start("unary_unary", details)
        return continuation(details, request)

    def intercept_unary_stream(self, continuation: Any, details: grpc.ClientCallDetails, request: Any) -> Any:
        self._start("unary_stream", details)
        return LoggedCall(continuation(details, request), self.events)

    def intercept_stream_unary(
        self, continuation: Any, details: grpc.ClientCallDetails, requests: Any
    ) -> Any:
        self._start("stream_unary", details)
        return continuation(details, requests)

    def intercept_stream_stream(
        self, continuation: Any, details: grpc.ClientCallDetails, requests: Any
    ) -> Any:
        self._start("stream_stream", details)
        return LoggedCall(continuation(details, requests), self.events)


@dataclass
class Stubs:
    channel: grpc.Channel
    interceptor: LoggingInterceptor | None
    upper: grpc.UnaryUnaryMultiCallable[StringValue, StringValue] = field(init=False)
    expand: grpc.UnaryStreamMultiCallable[StringValue, StringValue] = field(init=False)
    join: grpc.StreamUnaryMultiCallable[StringValue, StringValue] = field(init=False)
    chat: grpc.StreamStreamMultiCallable[StringValue, StringValue] = field(init=False)

    def __post_init__(self) -> None:
        ser, de = StringValue.SerializeToString, StringValue.FromString
        self.upper = self.channel.unary_unary(f"/{SERVICE}/Upper", ser, de)
        self.expand = self.channel.unary_stream(f"/{SERVICE}/Expand", ser, de)
        self.join = self.channel.stream_unary(f"/{SERVICE}/Join", ser, de)
        self.chat = self.channel.stream_stream(f"/{SERVICE}/Chat", ser, de)


@pytest.fixture(params=["plain", "intercepted"])
def stubs(request: pytest.FixtureRequest, target: str) -> Iterator[Stubs]:
    with grpc.insecure_channel(target) as raw:
        if request.param == "plain":
            yield Stubs(raw, None)
        else:
            interceptor = LoggingInterceptor()
            yield Stubs(grpc.intercept_channel(raw, interceptor), interceptor)


def msg(value: str) -> StringValue:
    return StringValue(value=value)


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def client_threads() -> int:
    """``threading.active_count()`` minus the server pool, which grows lazily."""
    return sum(1 for t in threading.enumerate() if not t.name.startswith(SERVER_THREAD_PREFIX))
