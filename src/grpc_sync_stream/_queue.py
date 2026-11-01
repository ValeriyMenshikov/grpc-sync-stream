"""The request queue: what ``write()`` fills and grpc drains."""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Iterator
from typing import Generic, TypeVar

T = TypeVar("T")


class RequestQueue(Generic[T]):
    """The request iterator handed to grpc, which drains it in its own thread.

    ``close()`` is a client half-close: grpc gets what is queued, then the end
    of the stream. ``abort()`` is called once the call is over by any means:
    it wakes a grpc thread blocked in ``__next__``, otherwise that thread would
    wait for a request forever and leak.
    """

    def __init__(self, maxsize: int | None) -> None:
        if maxsize is not None and maxsize < 1:
            raise ValueError("max_pending_writes must be >= 1 or None")
        self._items: deque[T] = deque()
        self._maxsize = maxsize
        self._cond = threading.Condition()
        self._closed = False
        self._aborted = False

    def put(self, item: T) -> bool:
        """Enqueue ``item``; return False if the call is already over.

        Blocks while the queue is full. Raises ``RuntimeError`` after ``close()``.
        """
        with self._cond:
            while True:
                if self._closed:
                    raise RuntimeError("write() after done_writing()")
                if self._aborted:
                    return False
                if self._maxsize is None or len(self._items) < self._maxsize:
                    break
                self._cond.wait()
            self._items.append(item)
            self._cond.notify_all()
            return True

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def abort(self) -> None:
        with self._cond:
            self._aborted = True
            self._items.clear()
            self._cond.notify_all()

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        with self._cond:
            while not (self._items or self._closed or self._aborted):
                self._cond.wait()
            if self._aborted or not self._items:
                raise StopIteration
            item = self._items.popleft()
            self._cond.notify_all()
            return item
