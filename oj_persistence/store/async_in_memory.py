from __future__ import annotations

from collections.abc import Callable
from typing import Any

from oj_persistence.store.async_base import AsyncAbstractStore


class AsyncInMemoryStore(AsyncAbstractStore):
    """
    AsyncAbstractStore backed by an in-process dict.

    All operations complete without I/O. Useful for testing async pipelines
    and lightweight use cases that do not require disk persistence.

    Thread safety is not a concern — asyncio is single-threaded. Concurrent
    async tasks sharing one instance are safe as long as they yield between
    mutations (which all callers of ``await`` do).
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    async def create(self, key: str, value: Any) -> None:
        if key in self._data:
            raise KeyError(key)
        self._data[key] = value

    async def read(self, key: str) -> Any:
        return self._data.get(key)

    async def update(self, key: str, value: Any) -> None:
        if key not in self._data:
            raise KeyError(key)
        self._data[key] = value

    async def upsert(self, key: str, value: Any) -> None:
        self._data[key] = value

    async def delete(self, key: str) -> None:
        self._data.pop(key, None)

    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        values = list(self._data.values())
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]
