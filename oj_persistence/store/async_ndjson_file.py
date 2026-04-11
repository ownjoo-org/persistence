from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

from oj_persistence.store.async_base import AsyncAbstractStore
from oj_persistence.store.ndjson_file import NdjsonFileStore


class AsyncNdjsonFileStore(AsyncAbstractStore):
    """
    Async, buffered wrapper around NdjsonFileStore.

    Best suited for append-heavy streaming pipelines where records are written
    once and rarely updated. For async pipelines that need indexed key lookups
    or external database durability, prefer AsyncSqliteStore or
    AsyncSqlAlchemyStore.

    Writes are accumulated in memory and flushed to disk in two situations:

    1. **Size trigger**: the buffer reaches ``batch_size`` items — flush fires
       immediately inline before the next upsert returns.
    2. **Time trigger**: a background task wakes every ``flush_interval`` seconds
       and flushes whatever is in the buffer, even if it is only a single item.
       This exploits natural pauses in upstream data flow so disk I/O happens
       during idle time rather than on the hot path.

    Use as an async context manager to guarantee the final (partial) buffer is
    always written before control returns to the caller:

        async with AsyncNdjsonFileStore('data/chars.ndjson') as store:
            await store.upsert('1', char_dict)
        # all buffered items flushed here, timer task cleanly cancelled

    The underlying :class:`NdjsonFileStore` is accessible via ``store.sync_store``
    so it can be registered in a :class:`PersistenceManager` for sync joins after
    the async pipeline has finished.
    """

    def __init__(
        self,
        path: str | Path,
        batch_size: int = 50,
        flush_interval: float = 5.0,
    ) -> None:
        self._store = NdjsonFileStore(path)
        self._buffer: list[tuple[str, Any]] = []
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._lock = asyncio.Lock()
        self._timer_task: asyncio.Task | None = None

    @property
    def sync_store(self) -> NdjsonFileStore:
        """The underlying sync store — register this in PersistenceManager."""
        return self._store

    # ------------------------------------------------------------------ context manager

    async def __aenter__(self) -> AsyncNdjsonFileStore:
        self._timer_task = asyncio.create_task(self._timer_loop())
        return self

    async def __aexit__(self, *args) -> None:
        if self._timer_task is not None:
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass
            self._timer_task = None
        await self._flush()  # write any remaining items unconditionally

    # ------------------------------------------------------------------ timer

    async def _timer_loop(self) -> None:
        """Wake every flush_interval seconds; flush if the buffer is non-empty."""
        while True:
            await asyncio.sleep(self._flush_interval)
            async with self._lock:
                if self._buffer:
                    await self._flush_locked()

    # ------------------------------------------------------------------ flush

    async def _flush(self) -> None:
        """Acquire lock and flush. Safe to call from outside the timer."""
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Flush without acquiring the lock — caller must hold self._lock."""
        if not self._buffer:
            return
        batch, self._buffer = self._buffer, []
        await asyncio.to_thread(self._store.upsert_many, batch)

    # ------------------------------------------------------------------ upsert (buffered)

    async def upsert(self, key: str, value: Any) -> None:
        """Buffer the item; flush immediately if the buffer is full."""
        async with self._lock:
            self._buffer.append((key, value))
            if len(self._buffer) >= self._batch_size:
                await self._flush_locked()

    # ----------------------------------- remaining CRUDL (unbuffered, delegated to thread)

    async def create(self, key: str, value: Any) -> None:
        await asyncio.to_thread(self._store.create, key, value)

    async def read(self, key: str) -> Any:
        return await asyncio.to_thread(self._store.read, key)

    async def update(self, key: str, value: Any) -> None:
        await asyncio.to_thread(self._store.update, key, value)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._store.delete, key)

    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        return await asyncio.to_thread(self._store.list, predicate)
