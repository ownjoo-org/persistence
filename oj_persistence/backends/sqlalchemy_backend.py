"""SQLAlchemy backend — any SQLAlchemy-supported SQL database.

Each logical table maps to a SQL table with ``pk TEXT PRIMARY KEY, value TEXT``.
Uses synchronous SQLAlchemy Core + ``asyncio.to_thread`` (consistent with the
SQLite backend pattern). Works with PostgreSQL, MySQL, SQLite (via SQLAlchemy),
Oracle, and any other dialect supported by SQLAlchemy.

Pass a full SQLAlchemy URL: ``postgresql://user:pass@host/db``,
``mysql+pymysql://...``, ``sqlite:///path/to/file.db``, etc.

Capabilities: PAGINATION (LIMIT/OFFSET), NATIVE_UPSERT.
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

from ..base import Backend, Capability

_SAFE_NAME = re.compile(r'^[\w-]+$')


def _check_name(name: str) -> None:
    if not _SAFE_NAME.match(name):
        raise ValueError(
            f'invalid table name {name!r} — only letters, digits, underscores, hyphens allowed'
        )


class SqlAlchemyBackend(Backend):
    capabilities = frozenset({Capability.PAGINATION, Capability.NATIVE_UPSERT})

    def __init__(self, url: str) -> None:
        self._url = url
        self._engine = None
        self._metadata = None
        self._sa_tables: dict[str, Any] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        from sqlalchemy import MetaData, create_engine
        self._engine = create_engine(self._url)
        self._metadata = MetaData()

    async def aclose(self) -> None:
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception:
                pass
            self._engine = None
            self._metadata = None
            self._sa_tables.clear()

    # ------------------------------------------------------------------ helpers

    def _sa_table(self, table: str) -> Any:
        with self._lock:
            if table not in self._sa_tables:
                from sqlalchemy import Column, Table, Text
                t = Table(
                    table,
                    self._metadata,
                    Column('pk', Text, primary_key=True),
                    Column('value', Text, nullable=False),
                    extend_existing=True,
                )
                self._sa_tables[table] = t
            return self._sa_tables[table]

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_table_sync, table)

    def _create_table_sync(self, table: str) -> None:
        self._sa_table(table).create(self._engine, checkfirst=True)

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._drop_table_sync, table)

    def _drop_table_sync(self, table: str) -> None:
        self._sa_table(table).drop(self._engine, checkfirst=True)
        with self._lock:
            self._sa_tables.pop(table, None)

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._truncate_sync, table)

    def _truncate_sync(self, table: str) -> None:
        t = self._sa_table(table)
        with self._engine.begin() as conn:
            conn.execute(t.delete())

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        return await asyncio.to_thread(self._table_exists_sync, table)

    def _table_exists_sync(self, table: str) -> bool:
        from sqlalchemy import inspect
        return inspect(self._engine).has_table(table)

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_sync, table, key, value)

    def _create_sync(self, table: str, key: str, value: Any) -> None:
        from sqlalchemy.exc import IntegrityError
        t = self._sa_table(table)
        try:
            with self._engine.begin() as conn:
                conn.execute(t.insert().values(pk=key, value=json.dumps(value)))
        except IntegrityError:
            raise KeyError(key)

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        return await asyncio.to_thread(self._read_sync, table, key)

    def _read_sync(self, table: str, key: str) -> Any | None:
        t = self._sa_table(table)
        with self._engine.connect() as conn:
            row = conn.execute(t.select().where(t.c.pk == key)).fetchone()
        return json.loads(row[1]) if row else None

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._update_sync, table, key, value)

    def _update_sync(self, table: str, key: str, value: Any) -> None:
        t = self._sa_table(table)
        with self._engine.begin() as conn:
            result = conn.execute(
                t.update().where(t.c.pk == key).values(value=json.dumps(value))
            )
        if result.rowcount == 0:
            raise KeyError(key)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._upsert_sync, table, key, value)

    def _upsert_sync(self, table: str, key: str, value: Any) -> None:
        from sqlalchemy.exc import IntegrityError
        t = self._sa_table(table)
        payload = json.dumps(value)
        with self._engine.begin() as conn:
            try:
                conn.execute(t.insert().values(pk=key, value=payload))
            except IntegrityError:
                conn.execute(t.update().where(t.c.pk == key).values(value=payload))

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_sync, table, key)

    def _delete_sync(self, table: str, key: str) -> None:
        t = self._sa_table(table)
        with self._engine.begin() as conn:
            conn.execute(t.delete().where(t.c.pk == key))

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
        t = self._sa_table(table)
        with self._engine.connect() as conn:
            rows = conn.execute(t.select()).fetchall()
        values = [json.loads(r[1]) for r in rows]
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
        t = self._sa_table(table)
        with self._engine.connect() as conn:
            rows = conn.execute(t.select().limit(limit).offset(offset)).fetchall()
        return [json.loads(r[1]) for r in rows]
