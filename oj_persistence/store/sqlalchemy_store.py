from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from oj_persistence.store.base import AbstractStore

_SAFE_TABLE_NAME = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
_SAFE_JSON_PATH = re.compile(r'^[\$\.\w\[\]]+$')
_CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS {table} (key VARCHAR(255) PRIMARY KEY, value TEXT NOT NULL)'


class SqlAlchemyStore(AbstractStore):
    """
    AbstractStore backed by any SQLAlchemy-supported relational database.

    Pass a standard SQLAlchemy connection URL and the correct driver package
    for your backend:

        SqlAlchemyStore('postgresql+psycopg2://user:pass@host/db')
        SqlAlchemyStore('mysql+pymysql://user:pass@host/db')
        SqlAlchemyStore('mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server')
        SqlAlchemyStore('sqlite:///path/to/file.db')

    Additional keyword arguments are forwarded to ``create_engine()``, so
    connection-pool tuning, timeouts, and echo mode are all configurable:

        SqlAlchemyStore(url, pool_size=10, echo=True)

    Thread-safe: SQLAlchemy's connection pool manages per-thread connections;
    no external locking is needed.

    Call dispose() or use as a context manager to release pool connections.
    """

    def __init__(self, url: str, *, table: str = 'oj_store', **engine_kwargs) -> None:
        if not _SAFE_TABLE_NAME.match(table):
            raise ValueError(f"Invalid table name {table!r} — only letters, digits, and underscores allowed")
        self._table = table
        self._engine = create_engine(url, **engine_kwargs)
        with self._engine.begin() as conn:
            conn.execute(text(_CREATE_TABLE.format(table=self._table)))

    # ------------------------------------------------------------------ CRUDL

    def create(self, key: str, value: Any) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(f'INSERT INTO {self._table} (key, value) VALUES (:k, :v)'),
                    {'k': key, 'v': json.dumps(value)},
                )
        except IntegrityError:
            raise KeyError(key)

    def read(self, key: str) -> Any:
        with self._engine.connect() as conn:
            row = conn.execute(
                text(f'SELECT value FROM {self._table} WHERE key = :k'),
                {'k': key},
            ).fetchone()
        return json.loads(row[0]) if row else None

    def update(self, key: str, value: Any) -> None:
        with self._engine.begin() as conn:
            result = conn.execute(
                text(f'UPDATE {self._table} SET value = :v WHERE key = :k'),
                {'k': key, 'v': json.dumps(value)},
            )
        if result.rowcount == 0:
            raise KeyError(key)

    def upsert(self, key: str, value: Any) -> None:
        with self._engine.begin() as conn:
            result = conn.execute(
                text(f'UPDATE {self._table} SET value = :v WHERE key = :k'),
                {'k': key, 'v': json.dumps(value)},
            )
            if result.rowcount == 0:
                conn.execute(
                    text(f'INSERT INTO {self._table} (key, value) VALUES (:k, :v)'),
                    {'k': key, 'v': json.dumps(value)},
                )

    def delete(self, key: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(f'DELETE FROM {self._table} WHERE key = :k'),
                {'k': key},
            )

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(f'SELECT value FROM {self._table}')).fetchall()
        values = [json.loads(row[0]) for row in rows]
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]

    # ------------------------------------------------------------------ indexing

    def _json_extract_expr(self, json_path: str) -> str:
        """Return a dialect-appropriate SQL expression for extracting a JSON field."""
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

    def add_json_index(self, json_path: str, index_name: str | None = None) -> None:
        """
        Create a database index on a JSON field so list_by_field() is fast.

        Supported dialects: SQLite, MySQL, PostgreSQL.
        json_path uses SQLite json_extract() syntax, e.g. '$.name' or '$.address.city'.
        PostgreSQL paths are translated to jsonb operators automatically.
        """
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f"Unsafe json_path: {json_path!r}")
        name = index_name or 'idx_' + json_path.lstrip('$.').replace('.', '_').replace('[', '_').replace(']', '')
        expr = self._json_extract_expr(json_path)
        sql = f'CREATE INDEX IF NOT EXISTS "{name}" ON {self._table} ({expr})'
        with self._engine.begin() as conn:
            conn.execute(text(sql))

    def list_by_field(self, json_path: str, value: Any) -> list[Any]:
        """
        Return all values where the JSON field at json_path equals value.

        Pushes the filter into SQL so an index created by add_json_index()
        is exercised — unlike list(predicate) which always does a full Python scan.
        """
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f"Unsafe json_path: {json_path!r}")
        expr = self._json_extract_expr(json_path)
        sql = f'SELECT value FROM {self._table} WHERE {expr} = :v'
        with self._engine.connect() as conn:
            rows = conn.execute(text(sql), {'v': value}).fetchall()
        return [json.loads(row[0]) for row in rows]

    # ------------------------------------------------------------------ lifecycle

    def dispose(self) -> None:
        """Release all pooled connections."""
        self._engine.dispose()

    def __enter__(self) -> SqlAlchemyStore:
        return self

    def __exit__(self, *args) -> None:
        self.dispose()
