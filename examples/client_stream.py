"""Write requests one by one, then wait for the single response."""

from __future__ import annotations

import grpc
from echo_server import EchoStub, serve
from google.protobuf.wrappers_pb2 import StringValue as Msg

from grpc_sync_stream import RequestStream


def main() -> None:
    server, target = serve()
    with grpc.insecure_channel(target) as channel:
        stub = EchoStub(channel)

        with RequestStream(stub.Join) as s:
            for word in ("a", "b", "c"):
                s.write(Msg(value=word))
            s.done_writing()
            print("result:", s.result(timeout=5).value)
            print("status:", s.code())
    server.stop(None)


if __name__ == "__main__":
    main()
