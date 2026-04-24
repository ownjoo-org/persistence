"""JSON backend — one JSON object file per table.

Each logical table maps to ``{base_dir}/{table}.json``. The file is a plain
JSON object: ``{"key": <value>, ...}``. The entire file is loaded into memory
for reads and rewritten atomically on mutations.

For files small enough to hold in memory this is the simplest option. For
large files (>~1 MB) or high write throughput, prefer Sqlite.

Capabilities: PAGINATION (slice-based).
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


class JsonBackend(Backend):
    capabilities = frozenset({Capability.PAGINATION})

    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)
        self._locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()

    def _lock(self, table: str) -> threading.Lock:
        with self._locks_lock:
            if table not in self._locks:
                self._locks[table] = threading.Lock()
            return self._locks[table]

    def _path(self, table: str) -> Path:
        return self._base / f'{table}.json'

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._base.mkdir, parents=True, exist_ok=True)

    async def aclose(self) -> None:
        pass

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_table_sync, table)

    def _create_table_sync(self, table: str) -> None:
        p = self._path(table)
        if not p.exists():
            p.write_text('{}', encoding='utf-8')

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        p = self._path(table)
        await asyncio.to_thread(lambda: p.unlink(missing_ok=True))

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._truncate_sync, table)

    def _truncate_sync(self, table: str) -> None:
        with self._lock(table):
            self._path(table).write_text('{}', encoding='utf-8')

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        return await asyncio.to_thread(self._path(table).exists)

    # ------------------------------------------------------------------ internals

    def _load(self, table: str) -> dict[str, Any]:
        p = self._path(table)
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding='utf-8'))

    def _save(self, table: str, data: dict[str, Any]) -> None:
        p = self._path(table)
        tmp = p.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, indent=2), encoding='utf-8')
        tmp.replace(p)

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_sync, table, key, value)

    def _create_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            data = self._load(table)
            if key in data:
                raise KeyError(key)
            data[key] = value
            self._save(table, data)

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        return await asyncio.to_thread(self._read_sync, table, key)

    def _read_sync(self, table: str, key: str) -> Any | None:
        return self._load(table).get(key)

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._update_sync, table, key, value)

    def _update_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            data = self._load(table)
            if key not in data:
                raise KeyError(key)
            data[key] = value
            self._save(table, data)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._upsert_sync, table, key, value)

    def _upsert_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            data = self._load(table)
            data[key] = value
            self._save(table, data)

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_sync, table, key)

    def _delete_sync(self, table: str, key: str) -> None:
        with self._lock(table):
            data = self._load(table)
            data.pop(key, None)
            self._save(table, data)

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
        values = list(self._load(table).values())
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
        all_values = list(self._load(table).values())
        return all_values[offset:offset + limit]
