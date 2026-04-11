from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from tinydb import TinyDB
from tinydb.storages import MemoryStorage

from oj_persistence.store.base import AbstractStore


class TinyDbStore(AbstractStore):
    """
    AbstractStore backed by TinyDB.

    TinyDB is a lightweight, pure-Python document store. Unlike the NDJSON and
    SQLite stores, it natively handles heterogeneous document structures — each
    stored value can have a completely different shape without any schema
    declaration.

    Standard AbstractStore interface (CRUDL) is supported. For richer queries
    beyond what list(predicate) covers, use query() with TinyDB's query DSL:

        from tinydb import where
        results = store.query(where('_value')['city'] == 'NYC')

    Values are stored internally as {'_key': key, '_value': value} so that
    the key participates in TinyDB's document model without colliding with
    arbitrary value fields.

    Thread-safe via threading.Lock — all operations are serialized.

    Parameters
    ----------
    path:   Path to the JSON file. Omit (or pass None) for in-memory storage.
    """

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            self._db = TinyDB(storage=MemoryStorage)
        else:
            self._db = TinyDB(str(path))
        self._table = self._db.table('store')
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ CRUDL

    def create(self, key: str, value: Any) -> None:
        with self._lock:
            if self._table.contains(self._key_query(key)):
                raise KeyError(key)
            self._table.insert({'_key': key, '_value': value})

    def read(self, key: str) -> Any:
        with self._lock:
            doc = self._table.get(self._key_query(key))
        return doc['_value'] if doc else None

    def update(self, key: str, value: Any) -> None:
        with self._lock:
            if not self._table.contains(self._key_query(key)):
                raise KeyError(key)
            self._table.update({'_value': value}, self._key_query(key))

    def upsert(self, key: str, value: Any) -> None:
        with self._lock:
            self._table.upsert({'_key': key, '_value': value}, self._key_query(key))

    def delete(self, key: str) -> None:
        with self._lock:
            self._table.remove(self._key_query(key))

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._lock:
            docs = self._table.all()
        values = [doc['_value'] for doc in docs]
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]

    # ------------------------------------------------------------------ TinyDB-native query

    def query(self, condition) -> list[Any]:
        """
        Execute a TinyDB-native query and return matching values.

        Values are stored under the '_value' field, so reference them via:

            from tinydb import where
            store.query(where('_value')['name'] == 'Alice')
            store.query(where('_value')['age'] > 30)

        Returns a list of the stored values (without the internal '_key' field).
        """
        with self._lock:
            docs = self._table.search(condition)
        return [doc['_value'] for doc in docs]

    # ------------------------------------------------------------------ lifecycle

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> TinyDbStore:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ------------------------------------------------------------------ internal

    @staticmethod
    def _key_query(key: str):
        from tinydb import where
        return where('_key') == key
