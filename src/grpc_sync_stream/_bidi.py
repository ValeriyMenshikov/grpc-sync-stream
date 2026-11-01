"""``BidiStream``: stream-stream calls."""

from __future__ import annotations

from typing import Generic, TypeVar

import grpc

from grpc_sync_stream._base import ResponseStreamBase
from grpc_sync_stream._queue import RequestQueue
from grpc_sync_stream._status import raise_finished
from grpc_sync_stream._types import MetadataArg, as_metadata

Req = TypeVar("Req")
Resp = TypeVar("Resp")


class BidiStream(ResponseStreamBase[Resp], Generic[Req, Resp]):
    """A stream-stream call driven by ``write()`` / ``done_writing()`` / ``read()``.

    The call starts in the constructor. One daemon thread per stream reads
    responses; grpc itself pulls written requests from an in-memory queue.
    """

    def __init__(
        self,
        method: grpc.StreamStreamMultiCallable[Req, Resp],
        *,
        metadata: MetadataArg = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
        wait_for_ready: bool | None = None,
        compression: grpc.Compression | None = None,
        read_timeout: float | None = None,
        max_pending_writes: int | None = None,
    ) -> None:
        self._requests: RequestQueue[Req] = RequestQueue(max_pending_writes)
        call = method(
            self._requests,
            timeout=timeout,
            metadata=as_metadata(metadata),
            credentials=credentials,
            wait_for_ready=wait_for_ready,
            compression=compression,
        )
        self._start(call, read_timeout, on_finish=self._requests.abort)

    def _close_writes(self) -> None:
        self._requests.close()

    def write(self, message: Req) -> None:
        """Queue ``message`` for sending without blocking.

        Blocks only when ``max_pending_writes`` is set and the queue is full.
        Raises ``RuntimeError`` after ``done_writing()``, and the call's
        ``grpc.RpcError`` if it has already failed.
        """
        if not self._requests.put(message):
            self._reader.wait()
            outcome = self._reader.outcome
            raise_finished(outcome if isinstance(outcome, Exception) else None)

    def done_writing(self) -> None:
        """Half-close the request stream. Idempotent."""
        self._requests.close()

    def cancel(self) -> bool:
        cancelled = super().cancel()
        if cancelled:
            self._requests.abort()
        return cancelled
