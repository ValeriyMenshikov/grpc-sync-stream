"""Read a unary-stream call message by message, or iterate it."""

from __future__ import annotations

import grpc
from echo_server import EchoStub, serve
from google.protobuf.wrappers_pb2 import StringValue as Msg

from grpc_sync_stream import EOF, ResponseStream


def main() -> None:
    server, target = serve()
    with grpc.insecure_channel(target) as channel:
        stub = EchoStub(channel)

        with ResponseStream(stub.Expand, Msg(value="a"), read_timeout=5) as s:
            first = s.read()
            assert first is not EOF
            print("first:", first.value)
            print("rest:", [m.value for m in s])
            print("status:", s.code())
    server.stop(None)


if __name__ == "__main__":
    main()
