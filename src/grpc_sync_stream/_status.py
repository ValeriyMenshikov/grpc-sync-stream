"""Call status: where the real code comes from, and errors for finished calls."""

from __future__ import annotations

from typing import Any

import grpc

from grpc_sync_stream._types import EOF, Outcome


class CancelledRpcError(grpc.RpcError):
    """CANCELLED error for calls whose own object is not an ``RpcError``.

    Only used for interceptor-supplied call objects; a plain grpcio call
    raises itself with CANCELLED, exactly like sync grpc does.
    """

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.CANCELLED

    def details(self) -> str:
        return "Locally cancelled by application!"

    def initial_metadata(self) -> None:
        return None

    def trailing_metadata(self) -> tuple[Any, ...]:
        return ()


def status_code(call: grpc.Call | None, outcome: Outcome | None) -> grpc.StatusCode:
    code = call.code() if call is not None else None
    if code is not None:
        return code
    return grpc.StatusCode.OK if outcome is EOF else grpc.StatusCode.UNKNOWN


def raise_finished(failure: Exception | None) -> None:
    """``write()`` hit a call that is over: re-raise its error, or report the end."""
    if failure is not None:
        raise failure
    raise RuntimeError("write() after the call has finished")
