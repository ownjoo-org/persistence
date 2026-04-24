"""NDJSON backend — one file per table, full-rewrite on mutations.

Each logical table maps to ``{base_dir}/{table}.ndjson``. Records are stored
as newline-delimited JSON objects: ``{"pk": key, "value": <any json>}``.

Mutations rewrite the whole file under a per-table write lock. Suited for
small tables where portability matters more than write throughput. For large
or high-concurrency tables, use Sqlite.
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


class NdjsonBackend(Backend):
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
        return self._base / f'{table}.ndjson'

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._base.mkdir, parents=True, exist_ok=True)

    async def aclose(self) -> None:
        pass

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)
        p = self._path(table)
        await asyncio.to_thread(lambda: p.touch(exist_ok=True))

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        p = self._path(table)
        await asyncio.to_thread(lambda: p.unlink(missing_ok=True))

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._truncate_sync, table)

    def _truncate_sync(self, table: str) -> None:
        with self._lock(table):
            self._path(table).write_text('', encoding='utf-8')

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        return await asyncio.to_thread(self._path(table).exists)

    # ------------------------------------------------------------------ internals

    def _read_all(self, table: str) -> list[tuple[str, Any]]:
        p = self._path(table)
        if not p.exists():
            return []
        records: list[tuple[str, Any]] = []
        with p.open(encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                records.append((obj['pk'], obj['value']))
        return records

    def _write_all(self, table: str, records: list[tuple[str, Any]]) -> None:
        with self._path(table).open('w', encoding='utf-8') as f:
            for pk, value in records:
                f.write(json.dumps({'pk': pk, 'value': value}) + '\n')

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_sync, table, key, value)

    def _create_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            records = self._read_all(table)
            if any(pk == key for pk, _ in records):
                raise KeyError(key)
            records.append((key, value))
            self._write_all(table, records)

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        return await asyncio.to_thread(self._read_sync, table, key)

    def _read_sync(self, table: str, key: str) -> Any | None:
        for pk, value in self._read_all(table):
            if pk == key:
                return value
        return None

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._update_sync, table, key, value)

    def _update_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            records = self._read_all(table)
            for i, (pk, _) in enumerate(records):
                if pk == key:
                    records[i] = (key, value)
                    self._write_all(table, records)
                    return
            raise KeyError(key)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._upsert_sync, table, key, value)

    def _upsert_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            records = self._read_all(table)
            for i, (pk, _) in enumerate(records):
                if pk == key:
                    records[i] = (key, value)
                    self._write_all(table, records)
                    return
            records.append((key, value))
            self._write_all(table, records)

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_sync, table, key)

    def _delete_sync(self, table: str, key: str) -> None:
        with self._lock(table):
            records = self._read_all(table)
            new = [(pk, v) for pk, v in records if pk != key]
            if len(new) != len(records):
                self._write_all(table, new)

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
        values = [v for _, v in self._read_all(table)]
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
        all_values = [v for _, v in self._read_all(table)]
        return all_values[offset:offset + limit]
