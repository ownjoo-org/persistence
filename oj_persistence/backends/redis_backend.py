"""Redis backend — string keys with optional per-key TTL.

Key scheme: ``{prefix}{table}:{key}`` → JSON-serialised value.

Table "creation" is a no-op (keys appear on first write). Dropping or
truncating a table scans and deletes all keys in the namespace.

TTL: if the value is a dict containing a ``_ttl`` key (Unix epoch int/float),
the backend sets a Redis EXPIREAT on that key after writing.

Capabilities: PAGINATION (SCAN-based offset), NATIVE_UPSERT.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Callable
from typing import Any

from ..base import Backend, Capability

_SAFE_NAME = re.compile(r'^[\w-]+$')


def _check_name(name: str) -> None:
    if not _SAFE_NAME.match(name):
        raise ValueError(
            f'invalid table name {name!r} — only letters, digits, underscores, hyphens allowed'
        )


class RedisBackend(Backend):
    capabilities = frozenset({Capability.PAGINATION, Capability.NATIVE_UPSERT})

    def __init__(self, url: str, db: int = 0, prefix: str = '') -> None:
        self._url = url
        self._db = db
        self._prefix = prefix
        self._client = None

    def _rkey(self, table: str, key: str) -> str:
        return f'{self._prefix}{table}:{key}'

    def _pattern(self, table: str) -> str:
        return f'{self._prefix}{table}:*'

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        import redis
        self._client = redis.Redis.from_url(self._url, db=self._db, decode_responses=True)

    async def aclose(self) -> None:
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)  # no-op; keys are created on first write

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_by_pattern_sync, table)

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_by_pattern_sync, table)

    def _delete_by_pattern_sync(self, table: str) -> None:
        client = self._client
        assert client is not None
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor, match=self._pattern(table), count=100)
            if keys:
                client.delete(*keys)
            if cursor == 0:
                break

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        return await asyncio.to_thread(self._table_exists_sync, table)

    def _table_exists_sync(self, table: str) -> bool:
        client = self._client
        assert client is not None
        _, keys = client.scan(0, match=self._pattern(table), count=1)
        return bool(keys)

    # ------------------------------------------------------------------ helpers

    def _set_with_ttl(self, rkey: str, value: Any) -> None:
        client = self._client
        assert client is not None
        client.set(rkey, json.dumps(value))
        if isinstance(value, dict):
            ttl = value.get('_ttl')
            if ttl is not None:
                client.expireat(rkey, int(ttl))

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_sync, table, key, value)

    def _create_sync(self, table: str, key: str, value: Any) -> None:
        client = self._client
        assert client is not None
        rkey = self._rkey(table, key)
        if not client.setnx(rkey, json.dumps(value)):
            raise KeyError(key)
        if isinstance(value, dict) and '_ttl' in value:
            client.expireat(rkey, int(value['_ttl']))

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        return await asyncio.to_thread(self._read_sync, table, key)

    def _read_sync(self, table: str, key: str) -> Any | None:
        client = self._client
        assert client is not None
        raw = client.get(self._rkey(table, key))
        return json.loads(raw) if raw is not None else None

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._update_sync, table, key, value)

    def _update_sync(self, table: str, key: str, value: Any) -> None:
        client = self._client
        assert client is not None
        rkey = self._rkey(table, key)
        if not client.exists(rkey):
            raise KeyError(key)
        self._set_with_ttl(rkey, value)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._upsert_sync, table, key, value)

    def _upsert_sync(self, table: str, key: str, value: Any) -> None:
        self._set_with_ttl(self._rkey(table, key), value)

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_sync, table, key)

    def _delete_sync(self, table: str, key: str) -> None:
        client = self._client
        assert client is not None
        client.delete(self._rkey(table, key))

    async def alist(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        _check_name(table)
        return await asyncio.to_thread(self._list_sync, table, predicate)

    def _list_sync(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None,
    ) -> list[Any]:
        client = self._client
        assert client is not None
        keys: list[str] = []
        cursor = 0
        while True:
            cursor, batch = client.scan(cursor, match=self._pattern(table), count=100)
            keys.extend(batch)
            if cursor == 0:
                break
        if not keys:
            return []
        raws = client.mget(*keys)
        values = [json.loads(r) for r in raws if r is not None]
        if predicate is not None:
            values = [v for v in values if predicate(v)]
        return values

    async def aiter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> AsyncIterator[Any]:
        values = await self.alist(table, predicate)
        for v in values:
            yield v

    # ------------------------------------------------------------------ optional

    async def alist_page(self, table: str, offset: int, limit: int) -> list[Any]:
        _check_name(table)
        return await asyncio.to_thread(self._list_page_sync, table, offset, limit)

    def _list_page_sync(self, table: str, offset: int, limit: int) -> list[Any]:
        all_values = self._list_sync(table, None)
        return all_values[offset:offset + limit]
