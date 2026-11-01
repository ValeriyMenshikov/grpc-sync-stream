"""Checked by mypy only (not collected by pytest): Req/Resp come from typed stubs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc
from google.protobuf.wrappers_pb2 import Int64Value, StringValue

from grpc_sync_stream import EOF, BidiStream, EOFType, RequestStream, ResponseStream

if TYPE_CHECKING:
    from typing_extensions import assert_type


def inferred_from_typed_stubs(
    chat: grpc.StreamStreamMultiCallable[StringValue, Int64Value],
    expand: grpc.UnaryStreamMultiCallable[StringValue, Int64Value],
    join: grpc.StreamUnaryMultiCallable[StringValue, Int64Value],
) -> None:
    with BidiStream(chat) as bidi:
        assert_type(bidi, BidiStream[StringValue, Int64Value])
        bidi.write(StringValue(value="x"))
        bidi.write(Int64Value(value=1))  # type: ignore[arg-type]
        message = bidi.read()
        assert_type(message, Int64Value | EOFType)
        if message is not EOF:
            assert_type(message, Int64Value)
        for item in bidi:
            assert_type(item, Int64Value)

    with ResponseStream(expand, StringValue(value="x")) as responses:
        assert_type(responses, ResponseStream[Int64Value])
        assert_type(responses.read(timeout=1), Int64Value | EOFType)

    with RequestStream(join) as requests:
        assert_type(requests, RequestStream[StringValue, Int64Value])
        requests.write(StringValue(value="x"))
        assert_type(requests.result(), Int64Value)
        assert_type(requests.code(), grpc.StatusCode)


def explicit_for_untyped_multicallables(chat: Any) -> None:
    bidi = BidiStream[StringValue, Int64Value](chat)
    assert_type(bidi.read(), Int64Value | EOFType)
