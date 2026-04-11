from __future__ import annotations

import json
import re
import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from oj_persistence.store.base import AbstractStore

_CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS store (key TEXT PRIMARY KEY, value TEXT NOT NULL)'
_SAFE_JSON_PATH = re.compile(r'^[\$\.\w\[\]]+$')


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

    Call close() or use as a context manager to release the connection.
    """

    def __init__(self, path: str | Path = ':memory:') -> None:
        self._path = str(path)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute(_CREATE_TABLE)
            self._conn.commit()

    # ------------------------------------------------------------------ CRUDL

    def create(self, key: str, value: Any) -> None:
        with self._lock:
            try:
                self._conn.execute(
                    'INSERT INTO store (key, value) VALUES (?, ?)',
                    (key, json.dumps(value)),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                raise KeyError(key)

    def read(self, key: str) -> Any:
        with self._lock:
            row = self._conn.execute(
                'SELECT value FROM store WHERE key = ?', (key,)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def update(self, key: str, value: Any) -> None:
        with self._lock:
            cursor = self._conn.execute(
                'UPDATE store SET value = ? WHERE key = ?',
                (json.dumps(value), key),
            )
            self._conn.commit()
        if cursor.rowcount == 0:
            raise KeyError(key)

    def upsert(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                'INSERT INTO store (key, value) VALUES (?, ?)'
                ' ON CONFLICT(key) DO UPDATE SET value = excluded.value',
                (key, json.dumps(value)),
            )
            self._conn.commit()

    def delete(self, key: str) -> None:
        with self._lock:
            self._conn.execute('DELETE FROM store WHERE key = ?', (key,))
            self._conn.commit()

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._lock:
            rows = self._conn.execute('SELECT value FROM store').fetchall()
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
        sql = f"CREATE INDEX IF NOT EXISTS \"{name}\" ON store(json_extract(value, '{json_path}'))"
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
        sql = f"SELECT value FROM store WHERE json_extract(value, '{json_path}') = ?"
        with self._lock:
            rows = self._conn.execute(sql, (value,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    # ------------------------------------------------------------------ lifecycle

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SqliteStore:
        return self

    def __exit__(self, *args) -> None:
        self.close()
