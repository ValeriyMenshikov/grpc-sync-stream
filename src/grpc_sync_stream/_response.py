"""``ResponseStream``: unary-stream calls."""

from __future__ import annotations

from typing import Generic, TypeVar

import grpc

from grpc_sync_stream._base import ResponseStreamBase
from grpc_sync_stream._types import MetadataArg, as_metadata

Req = TypeVar("Req")
Resp = TypeVar("Resp")


class ResponseStream(ResponseStreamBase[Resp], Generic[Resp]):
    """A unary-stream call: the request goes to the constructor, responses come from ``read()``."""

    def __init__(
        self,
        method: grpc.UnaryStreamMultiCallable[Req, Resp],
        request: Req,
        *,
        metadata: MetadataArg = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
        wait_for_ready: bool | None = None,
        compression: grpc.Compression | None = None,
        read_timeout: float | None = None,
    ) -> None:
        call = method(
            request,
            timeout=timeout,
            metadata=as_metadata(metadata),
            credentials=credentials,
            wait_for_ready=wait_for_ready,
            compression=compression,
        )
        self._start(call, read_timeout)
