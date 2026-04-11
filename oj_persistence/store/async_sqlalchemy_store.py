from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from oj_persistence.store.async_base import AsyncAbstractStore

_SAFE_TABLE_NAME = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
_SAFE_JSON_PATH = re.compile(r'^[\$\.\w\[\]]+$')
_CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS {table} (key VARCHAR(255) PRIMARY KEY, value TEXT NOT NULL)'


class AsyncSqlAlchemyStore(AsyncAbstractStore):
    """
    Async AbstractStore backed by any SQLAlchemy async-compatible database.

    Requires an async-capable driver in the connection URL:

        AsyncSqlAlchemyStore('postgresql+asyncpg://user:pass@host/db')
        AsyncSqlAlchemyStore('mysql+aiomysql://user:pass@host/db')
        AsyncSqlAlchemyStore('sqlite+aiosqlite:///path/to/file.db')

    Call initialize() (or use as a context manager) before performing any
    operations — this creates the backing table if it does not yet exist:

        async with AsyncSqlAlchemyStore(url) as store:
            await store.upsert('k', value)

        # or explicitly:
        store = AsyncSqlAlchemyStore(url)
        await store.initialize()
        ...
        await store.dispose()

    Additional keyword arguments are forwarded to create_async_engine().
    """

    def __init__(self, url: str, *, table: str = 'oj_store', **engine_kwargs) -> None:
        if not _SAFE_TABLE_NAME.match(table):
            raise ValueError(f"Invalid table name {table!r} — only letters, digits, and underscores allowed")
        self._table = table
        self._engine = create_async_engine(url, **engine_kwargs)

    # ------------------------------------------------------------------ lifecycle

    async def initialize(self) -> None:
        """Create the backing table if it does not exist."""
        async with self._engine.begin() as conn:
            await conn.execute(text(_CREATE_TABLE.format(table=self._table)))

    async def dispose(self) -> None:
        """Release all pooled connections."""
        await self._engine.dispose()

    async def __aenter__(self) -> AsyncSqlAlchemyStore:
        await self.initialize()
        return self

    async def __aexit__(self, *args) -> None:
        await self.dispose()

    # ------------------------------------------------------------------ CRUDL

    async def create(self, key: str, value: Any) -> None:
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    text(f'INSERT INTO {self._table} (key, value) VALUES (:k, :v)'),
                    {'k': key, 'v': json.dumps(value)},
                )
        except IntegrityError:
            raise KeyError(key)

    async def read(self, key: str) -> Any:
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    text(f'SELECT value FROM {self._table} WHERE key = :k'),
                    {'k': key},
                )
            ).fetchone()
        return json.loads(row[0]) if row else None

    async def update(self, key: str, value: Any) -> None:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(f'UPDATE {self._table} SET value = :v WHERE key = :k'),
                {'k': key, 'v': json.dumps(value)},
            )
        if result.rowcount == 0:
            raise KeyError(key)

    async def upsert(self, key: str, value: Any) -> None:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(f'UPDATE {self._table} SET value = :v WHERE key = :k'),
                {'k': key, 'v': json.dumps(value)},
            )
            if result.rowcount == 0:
                await conn.execute(
                    text(f'INSERT INTO {self._table} (key, value) VALUES (:k, :v)'),
                    {'k': key, 'v': json.dumps(value)},
                )

    async def delete(self, key: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text(f'DELETE FROM {self._table} WHERE key = :k'),
                {'k': key},
            )

    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        async with self._engine.connect() as conn:
            rows = (await conn.execute(text(f'SELECT value FROM {self._table}'))).fetchall()
        values = [json.loads(row[0]) for row in rows]
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]

    # ------------------------------------------------------------------ indexing

    def _json_extract_expr(self, json_path: str) -> str:
        dialect = self._engine.dialect.name
        parts = json_path.lstrip('$.').split('.')
        if dialect in ('sqlite', 'mysql'):
            return f"json_extract(value, '{json_path}')"
        elif dialect == 'postgresql':
            if len(parts) == 1:
                return f"value::jsonb->>'{parts[0]}'"
            expr = 'value::jsonb'
            for part in parts[:-1]:
                expr += f"->'{part}'"
            expr += f"->>'{parts[-1]}'"
            return expr
        else:
            raise NotImplementedError(f"JSON indexing not supported for dialect '{dialect}'")

    async def add_json_index(self, json_path: str, index_name: str | None = None) -> None:
        """
        Create a database index on a JSON field so list_by_field() is fast.

        Supported dialects: SQLite, MySQL, PostgreSQL.
        """
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f"Unsafe json_path: {json_path!r}")
        name = index_name or 'idx_' + json_path.lstrip('$.').replace('.', '_').replace('[', '_').replace(']', '')
        expr = self._json_extract_expr(json_path)
        sql = f'CREATE INDEX IF NOT EXISTS "{name}" ON {self._table} ({expr})'
        async with self._engine.begin() as conn:
            await conn.execute(text(sql))

    async def list_by_field(self, json_path: str, value: Any) -> list[Any]:
        """
        Return all values where the JSON field at json_path equals value.

        Pushes the filter into SQL so an index created by add_json_index()
        is exercised.
        """
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f"Unsafe json_path: {json_path!r}")
        expr = self._json_extract_expr(json_path)
        sql = f'SELECT value FROM {self._table} WHERE {expr} = :v'
        async with self._engine.connect() as conn:
            rows = (await conn.execute(text(sql), {'v': value})).fetchall()
        return [json.loads(row[0]) for row in rows]
