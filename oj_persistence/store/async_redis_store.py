from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import redis.asyncio as aioredis

from oj_persistence.store.async_base import AsyncAbstractStore


class AsyncRedisStore(AsyncAbstractStore):
    """
    Async AbstractStore backed by Redis.

    Mirrors RedisStore semantics exactly, using redis-py's built-in asyncio
    client (``redis.asyncio``). All values are serialised to JSON strings.

    Pass any configured ``redis.asyncio.Redis`` client:

        client = redis.asyncio.Redis(host='redis.internal', port=6379, ssl=True)
        store = AsyncRedisStore(client, prefix='myapp:feed:')

    The ``prefix`` namespaces this store within a shared Redis instance.

    Call close() or use as an async context manager to release connections:

        async with AsyncRedisStore(client, prefix='ns:') as store:
            await store.upsert('k', value)
    """

    def __init__(self, client: aioredis.Redis, *, prefix: str = 'oj:') -> None:
        self._client = client
        self._prefix = prefix

    def _k(self, key: str) -> str:
        return f'{self._prefix}{key}'

    # ------------------------------------------------------------------ CRUDL

    async def create(self, key: str, value: Any) -> None:
        result = await self._client.set(self._k(key), json.dumps(value), nx=True)
        if not result:
            raise KeyError(key)

    async def read(self, key: str) -> Any:
        raw = await self._client.get(self._k(key))
        return json.loads(raw) if raw is not None else None

    async def update(self, key: str, value: Any) -> None:
        result = await self._client.set(self._k(key), json.dumps(value), xx=True)
        if not result:
            raise KeyError(key)

    async def upsert(self, key: str, value: Any) -> None:
        await self._client.set(self._k(key), json.dumps(value))

    async def delete(self, key: str) -> None:
        await self._client.delete(self._k(key))

    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        values = []
        async for raw_key in self._client.scan_iter(f'{self._prefix}*'):
            raw = await self._client.get(raw_key)
            if raw is not None:
                v = json.loads(raw)
                if predicate is None or predicate(v):
                    values.append(v)
        return values

    # ------------------------------------------------------------------ lifecycle

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AsyncRedisStore:
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
