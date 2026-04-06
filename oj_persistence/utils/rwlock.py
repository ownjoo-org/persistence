from __future__ import annotations

import threading
from contextlib import contextmanager


class ReadWriteLock:
    """
    Writer-preferring readers-writer lock.

    - Multiple readers may hold the lock simultaneously (shared mode).
    - A writer gets exclusive access; new readers wait while any writer
      is waiting or active (writer preference prevents writer starvation).

    Usage::

        lock = ReadWriteLock()

        with lock.read():
            ...  # shared — other readers may enter concurrently

        with lock.write():
            ...  # exclusive — all other readers and writers are blocked
    """

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.Lock())
        self._readers: int = 0
        self._writing: bool = False
        self._writers_waiting: int = 0

    @contextmanager
    def read(self):
        with self._condition:
            # Writer preference: wait while a write is active or pending.
            while self._writing or self._writers_waiting > 0:
                self._condition.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextmanager
    def write(self):
        with self._condition:
            self._writers_waiting += 1
            while self._writing or self._readers > 0:
                self._condition.wait()
            self._writers_waiting -= 1
            self._writing = True
        try:
            yield
        finally:
            with self._condition:
                self._writing = False
                self._condition.notify_all()
