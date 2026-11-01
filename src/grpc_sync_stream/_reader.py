"""The reader thread: drains the response iterator so ``read()`` can time out."""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable, Iterator
from typing import Generic, TypeVar

from grpc_sync_stream._types import EOF, EOFType, Outcome

T = TypeVar("T")


class _Terminal:
    __slots__ = ("outcome",)

    def __init__(self, outcome: Outcome) -> None:
        self.outcome = outcome


class ResponseReader(Generic[T]):
    """A daemon thread that drains the response iterator into a queue.

    The last item is always a terminal marker carrying ``EOF`` or the
    exception, so ``read()`` learns about the end immediately.
    """

    def __init__(self, responses: Iterator[T], on_finish: Callable[[], None]) -> None:
        self._responses = responses
        self._on_finish = on_finish
        self._queue: queue.SimpleQueue[T | _Terminal] = queue.SimpleQueue()
        self._outcome: Outcome | None = None
        self._finished = threading.Event()
        self._cancelled = False
        self._thread = threading.Thread(target=self._run, name="grpc-sync-stream-reader", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        outcome: Outcome
        try:
            for message in self._responses:
                self._queue.put(message)
        except Exception as exc:
            outcome = exc
        else:
            outcome = EOF
        self._outcome = outcome
        # Everything that marks the call as over happens before read() can see
        # the end: once read() returned EOF or raised, done() is True and
        # write() fails instead of queueing a message nobody will send.
        self._on_finish()
        self._finished.set()
        self._queue.put(_Terminal(outcome))

    @property
    def finished(self) -> bool:
        return self._finished.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._finished.wait(timeout)

    def join(self, timeout: float) -> None:
        self._thread.join(timeout)

    @property
    def outcome(self) -> Outcome:
        """The final outcome. Only valid after ``wait()`` returned True."""
        assert self._outcome is not None
        return self._outcome

    def mark_cancelled(self) -> None:
        """Make ``read()`` report the cancellation instead of buffered messages."""
        self._cancelled = True

    def read(self, timeout: float | None) -> T | EOFType:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be >= 0 or None")
        if self._cancelled:
            if not self._finished.wait(timeout):
                raise TimeoutError(f"no message within {timeout}s")
            if isinstance(self._outcome, Exception):
                raise self._outcome
        try:
            item = self._queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(f"no message within {timeout}s") from None
        if isinstance(item, _Terminal):
            # Put it back: every later read() must see the same outcome.
            self._queue.put(item)
            if isinstance(item.outcome, Exception):
                raise item.outcome
            return item.outcome
        return item
