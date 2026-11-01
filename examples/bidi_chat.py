"""Ping-pong over a bidi stream, without asyncio."""

from __future__ import annotations

import grpc
from echo_server import EchoStub, serve
from google.protobuf.wrappers_pb2 import StringValue as Msg

from grpc_sync_stream import EOF, BidiStream


def main() -> None:
    server, target = serve()
    with grpc.insecure_channel(target) as channel:
        stub = EchoStub(channel)

        with BidiStream(stub.Chat, metadata=(("x-user", "a"),), timeout=30) as s:
            for word in ("hi", "there"):
                s.write(Msg(value=word))
                reply = s.read(timeout=5)
                assert reply is not EOF
                print(f"{word} -> {reply.value}")
            s.done_writing()
            assert s.read() is EOF
            print("status:", s.code())

        with BidiStream(stub.Chat) as s:
            s.write(Msg(value="fail"))
            try:
                s.read(timeout=5)
            except grpc.RpcError as err:
                print("error:", err.code(), err.details())
    server.stop(None)


if __name__ == "__main__":
    main()
