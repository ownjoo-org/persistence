from __future__ import annotations

import json
import operator
import re
import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, TYPE_CHECKING

from oj_persistence.store.base import AbstractStore

if TYPE_CHECKING:
    from oj_persistence.relation import JoinCondition, Op

_SAFE_JSON_PATH = re.compile(r'^[\$\.\w\[\]]+$')
_SAFE_TABLE_NAME = re.compile(r'^\w+$')

_OP_SQL: dict[str, str] = {
    '==': '=',
    '!=': '!=',
    '<':  '<',
    '<=': '<=',
    '>':  '>',
    '>=': '>=',
}


class SqliteStore(AbstractStore):
    """
    AbstractStore backed by a SQLite database.

    Key lookups and mutations are O(log n) via the primary-key B-tree index,
    compared to O(n) for sequential file stores. Suitable for read-heavy
    workloads or large datasets where NDJSON scan cost is prohibitive.

    Field-level indexes on JSON values can be added via add_json_index():

        store.add_json_index('$.name')
        store.list(lambda v: v['name'] == 'Alice')   # SQLite uses the index

    Thread-safe via a single threading.Lock — all operations are serialized.
    Use ':memory:' for an ephemeral in-process store (testing, caching).

    Multiple tables in the same database file are supported via the table
    parameter — pass the same path with different table names.

    Call close() or use as a context manager to release the connection.
    """

    @classmethod
    def _from_connection(cls, conn: sqlite3.Connection, *, table: str = 'store') -> 'SqliteStore':
        """
        Create a SqliteStore that reuses an existing shared connection.

        Used by PersistenceManager when multiple tables belong to the same
        SQLite group — they share one connection so SQL JOINs can span tables.
        """
        if not _SAFE_TABLE_NAME.match(table):
            raise ValueError(
                f"Invalid table name {table!r} — only letters, digits, and underscores allowed"
            )
        instance = cls.__new__(cls)
        instance._path = None       # no path; connection is externally owned
        instance._table = table
        instance._conn = conn
        instance._lock = threading.Lock()
        with instance._lock:
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS {table} '
                f'(key TEXT PRIMARY KEY, value TEXT NOT NULL)'
            )
            conn.commit()
        return instance

    def __init__(self, path: str | Path = ':memory:', *, table: str = 'store') -> None:
        if not _SAFE_TABLE_NAME.match(table):
            raise ValueError(
                f"Invalid table name {table!r} — only letters, digits, and underscores allowed"
            )
        self._path = str(path)
        self._table = table
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute(
                f'CREATE TABLE IF NOT EXISTS {self._table} '
                f'(key TEXT PRIMARY KEY, value TEXT NOT NULL)'
            )
            self._conn.commit()

    # ------------------------------------------------------------------ CRUDL

    def create(self, key: str, value: Any) -> None:
        with self._lock:
            try:
                self._conn.execute(
                    f'INSERT INTO {self._table} (key, value) VALUES (?, ?)',
                    (key, json.dumps(value)),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                raise KeyError(key)

    def read(self, key: str) -> Any:
        with self._lock:
            row = self._conn.execute(
                f'SELECT value FROM {self._table} WHERE key = ?', (key,)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def update(self, key: str, value: Any) -> None:
        with self._lock:
            cursor = self._conn.execute(
                f'UPDATE {self._table} SET value = ? WHERE key = ?',
                (json.dumps(value), key),
            )
            self._conn.commit()
        if cursor.rowcount == 0:
            raise KeyError(key)

    def upsert(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                f'INSERT INTO {self._table} (key, value) VALUES (?, ?)'
                f' ON CONFLICT(key) DO UPDATE SET value = excluded.value',
                (key, json.dumps(value)),
            )
            self._conn.commit()

    def delete(self, key: str) -> None:
        with self._lock:
            self._conn.execute(f'DELETE FROM {self._table} WHERE key = ?', (key,))
            self._conn.commit()

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._lock:
            rows = self._conn.execute(f'SELECT value FROM {self._table}').fetchall()
        values = [json.loads(row[0]) for row in rows]
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]

    # ------------------------------------------------------------------ indexing

    def add_json_index(self, json_path: str, index_name: str | None = None) -> None:
        """
        Create a SQLite expression index on a JSON field.

        Speeds up list(predicate) when SQLite can push the predicate into the
        query — most useful for equality and range checks on known fields.

        Parameters
        ----------
        json_path:   JSONPath expression understood by json_extract(), e.g. '$.name'
        index_name:  Optional explicit index name. Auto-generated from json_path if omitted.
        """
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f"Unsafe json_path — only $, letters, digits, '.', '_', '[', ']' allowed: {json_path!r}")
        name = index_name or 'idx_' + json_path.lstrip('$').lstrip('.').replace('.', '_').replace('[', '_').replace(']', '')
        # Parameters are prohibited in CREATE INDEX expressions; json_path is validated above.
        sql = (
            f"CREATE INDEX IF NOT EXISTS \"{name}\" "
            f"ON {self._table}(json_extract(value, '{json_path}'))"
        )
        with self._lock:
            self._conn.execute(sql)
            self._conn.commit()

    def list_by_field(self, json_path: str, value: Any) -> list[Any]:
        """
        Return all values where the JSON field at json_path equals value.

        Pushes the filter into SQL so an index created by add_json_index()
        is actually exercised — unlike list(predicate) which always does a
        full Python-side scan.

        json_path follows SQLite's json_extract() syntax, e.g. '$.name' or
        '$.address.city'.
        """
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f"Unsafe json_path — only $, letters, digits, '.', '_', '[', ']' allowed: {json_path!r}")
        sql = f"SELECT value FROM {self._table} WHERE json_extract(value, '{json_path}') = ?"
        with self._lock:
            rows = self._conn.execute(sql, (value,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    # ------------------------------------------------------------------ native join

    def native_join(
        self,
        right_store: 'SqliteStore',
        on: 'JoinCondition | list[JoinCondition]',
        how: str = 'inner',
        left_fields: list[str] | None = None,
        right_fields: list[str] | None = None,
    ) -> list[tuple[Any, Any]]:
        """
        Execute a SQL JOIN between this store's table and right_store's table.

        Both stores must share the same sqlite3 connection (same group).
        Only INNER and LEFT joins are issued as SQL; RIGHT and OUTER fall back
        to the Python path in apply_relation().

        Returns list of (left_val, right_val) tuples; projection applied if
        left_fields / right_fields are given.  The manager's relate() handles
        the where filter after this method returns.
        """
        from oj_persistence.relation import JoinCondition as JC
        from oj_persistence.utils.relation import _project

        conditions: list[JC] = on if isinstance(on, list) else [on]

        lt, rt = self._table, right_store._table
        on_clauses = ' AND '.join(
            f"json_extract(l.value, '$.{c.left_field}') "
            f"{_OP_SQL[c.op.value]} "
            f"json_extract(r.value, '$.{c.right_field}')"
            for c in conditions
        )

        sql_how = {'inner': 'INNER JOIN', 'left': 'LEFT JOIN'}.get(how)
        if sql_how is None:
            # RIGHT / OUTER not delegated here — caller must use Python path
            raise NotImplementedError(f"native_join does not support how={how!r}")

        sql = (
            f"SELECT l.value, r.value "
            f"FROM {lt} l {sql_how} {rt} r ON {on_clauses}"
        )
        with self._lock:
            rows = self._conn.execute(sql).fetchall()

        results = []
        for lrow, rrow in rows:
            lv = json.loads(lrow)
            rv = json.loads(rrow) if rrow is not None else None
            results.append((_project(lv, left_fields), _project(rv, right_fields)))
        return results

    # ------------------------------------------------------------------ lifecycle

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SqliteStore:
        return self

    def __exit__(self, *args) -> None:
        self.close()
