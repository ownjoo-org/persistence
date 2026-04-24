"""TinyDB backend — pure-Python JSON file database.

Each backend instance owns one TinyDB file at ``path``. Logical tables map to
TinyDB tables within that file. Records are stored as
``{pk: key, value: <json-string>}`` documents.

TinyDB is not thread-safe, so all operations serialize through a single lock.

Capabilities: PAGINATION (slice-based), NATIVE_UPSERT.
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

from ..base import Backend, Capability

_SAFE_NAME = re.compile(r'^[\w-]+$')


def _check_name(name: str) -> None:
    if not _SAFE_NAME.match(name):
        raise ValueError(
            f'invalid table name {name!r} — only letters, digits, underscores, hyphens allowed'
        )


class TinyDbBackend(Backend):
    capabilities = frozenset({Capability.PAGINATION, Capability.NATIVE_UPSERT})

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._db = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        from tinydb import TinyDB
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = TinyDB(str(self._path))

    async def aclose(self) -> None:
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)  # TinyDB creates tables lazily; no-op

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._drop_table_sync, table)

    def _drop_table_sync(self, table: str) -> None:
        with self._lock:
            assert self._db is not None
            self._db.drop_table(table)

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._truncate_sync, table)

    def _truncate_sync(self, table: str) -> None:
        with self._lock:
            assert self._db is not None
            self._db.table(table).truncate()

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        return await asyncio.to_thread(self._table_exists_sync, table)

    def _table_exists_sync(self, table: str) -> bool:
        with self._lock:
            assert self._db is not None
            return table in self._db.tables()

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_sync, table, key, value)

    def _create_sync(self, table: str, key: str, value: Any) -> None:
        from tinydb import Query
        with self._lock:
            assert self._db is not None
            t = self._db.table(table)
            if t.contains(Query().pk == key):
                raise KeyError(key)
            t.insert({'pk': key, 'value': json.dumps(value)})

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        return await asyncio.to_thread(self._read_sync, table, key)

    def _read_sync(self, table: str, key: str) -> Any | None:
        from tinydb import Query
        with self._lock:
            assert self._db is not None
            doc = self._db.table(table).get(Query().pk == key)
        return json.loads(doc['value']) if doc else None

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._update_sync, table, key, value)

    def _update_sync(self, table: str, key: str, value: Any) -> None:
        from tinydb import Query
        with self._lock:
            assert self._db is not None
            updated = self._db.table(table).update({'value': json.dumps(value)}, Query().pk == key)
        if not updated:
            raise KeyError(key)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._upsert_sync, table, key, value)

    def _upsert_sync(self, table: str, key: str, value: Any) -> None:
        from tinydb import Query
        with self._lock:
            assert self._db is not None
            self._db.table(table).upsert(
                {'pk': key, 'value': json.dumps(value)},
                Query().pk == key,
            )

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_sync, table, key)

    def _delete_sync(self, table: str, key: str) -> None:
        from tinydb import Query
        with self._lock:
            assert self._db is not None
            self._db.table(table).remove(Query().pk == key)

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
        with self._lock:
            assert self._db is not None
            docs = self._db.table(table).all()
        values = [json.loads(d['value']) for d in docs]
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
