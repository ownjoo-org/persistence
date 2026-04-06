from __future__ import annotations

from typing import Any, Callable, Optional

from oj_persistence.utils.rwlock import ReadWriteLock


class InMemoryStore:
    """
    AbstractStore backed by an in-process dict.

    Thread safety: ReadWriteLock — multiple concurrent reads allowed;
    writes are exclusive.
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._lock = ReadWriteLock()

    def create(self, key: str, value: Any) -> None:
        with self._lock.write():
            if key in self._data:
                raise KeyError(key)
            self._data[key] = value

    def read(self, key: str) -> Any:
        with self._lock.read():
            return self._data.get(key)

    def update(self, key: str, value: Any) -> None:
        with self._lock.write():
            if key not in self._data:
                raise KeyError(key)
            self._data[key] = value

    def upsert(self, key: str, value: Any) -> None:
        with self._lock.write():
            self._data[key] = value

    def delete(self, key: str) -> None:
        with self._lock.write():
            self._data.pop(key, None)

    def list(self, predicate: Optional[Callable[[Any], bool]] = None) -> list[Any]:
        with self._lock.read():
            values = list(self._data.values())
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]
