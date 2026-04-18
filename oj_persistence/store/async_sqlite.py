from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

from oj_persistence.store.async_base import AsyncAbstractStore
from oj_persistence.store.sqlite import SqliteStore


class AsyncSqliteStore(AsyncAbstractStore):
    """
    Async wrapper around SqliteStore.

    All operations are dispatched to a thread via asyncio.to_thread so the
    event loop is never blocked by SQLite I/O or lock contention.

    The underlying SqliteStore is accessible via sync_store for use cases
    that mix sync and async code (e.g. registering in PersistenceManager
    while the async pipeline is running).

    Use as an async context manager to guarantee the connection is closed:

        async with AsyncSqliteStore('data/store.db') as store:
            await store.upsert('k', value)
    """

    def __init__(self, path: str | Path = ':memory:') -> None:
        self._store = SqliteStore(path)

    @property
    def sync_store(self) -> SqliteStore:
        """The underlying sync store — usable directly or in PersistenceManager."""
        return self._store

    # ------------------------------------------------------------------ context manager

    async def __aenter__(self) -> AsyncSqliteStore:
        return self

    async def __aexit__(self, *args) -> None:
        self._store.close()

    # ------------------------------------------------------------------ CRUDL

    async def create(self, key: str, value: Any) -> None:
        await asyncio.to_thread(self._store.create, key, value)

    async def read(self, key: str) -> Any:
        return await asyncio.to_thread(self._store.read, key)

    async def update(self, key: str, value: Any) -> None:
        await asyncio.to_thread(self._store.update, key, value)

    async def upsert(self, key: str, value: Any) -> None:
        await asyncio.to_thread(self._store.upsert, key, value)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._store.delete, key)

    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        return await asyncio.to_thread(self._store.list, predicate)

    async def list_page(self, offset: int, limit: int) -> list[Any]:
        """Return up to limit values starting at offset without loading the full table."""
        return await asyncio.to_thread(self._store.list_page, offset, limit)

    async def list_by_field(self, json_path: str, value: Any) -> list[Any]:
        """Async equivalent of SqliteStore.list_by_field()."""
        return await asyncio.to_thread(self._store.list_by_field, json_path, value)
