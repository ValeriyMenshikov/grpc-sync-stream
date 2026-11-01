"""A tiny echo service for the examples: no codegen, messages are ``StringValue``.

``EchoStub`` stands in for a generated ``*_pb2_grpc`` stub.
"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent import futures

import grpc
from google.protobuf.wrappers_pb2 import StringValue

SERVICE = "example.Echo"


def _chat(requests: Iterator[StringValue], context: grpc.ServicerContext) -> Iterator[StringValue]:
    for request in requests:
        if request.value == "fail":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "boom")
        yield StringValue(value=request.value.upper())


def _expand(request: StringValue, context: grpc.ServicerContext) -> Iterator[StringValue]:
    for i in range(3):
        yield StringValue(value=f"{request.value}-{i}")


def _join(requests: Iterator[StringValue], context: grpc.ServicerContext) -> StringValue:
    return StringValue(value=",".join(r.value for r in requests))


def serve() -> tuple[grpc.Server, str]:
    """Start the server on a free local port; return it and its address."""
    de, ser = StringValue.FromString, StringValue.SerializeToString
    handlers = {
        "Chat": grpc.stream_stream_rpc_method_handler(_chat, de, ser),
        "Expand": grpc.unary_stream_rpc_method_handler(_expand, de, ser),
        "Join": grpc.stream_unary_rpc_method_handler(_join, de, ser),
    }
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SERVICE, handlers),))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}"


class EchoStub:
    def __init__(self, channel: grpc.Channel) -> None:
        ser, de = StringValue.SerializeToString, StringValue.FromString
        self.Chat = channel.stream_stream(f"/{SERVICE}/Chat", ser, de)
        self.Expand = channel.unary_stream(f"/{SERVICE}/Expand", ser, de)
        self.Join = channel.stream_unary(f"/{SERVICE}/Join", ser, de)
