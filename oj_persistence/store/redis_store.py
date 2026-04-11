from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import redis as redis_lib

from oj_persistence.store.base import AbstractStore


class RedisStore(AbstractStore):
    """
    AbstractStore backed by Redis.

    Redis is a natural fit for AbstractStore — its native data model is
    key → value with O(1) reads and writes. All values are serialised to
    JSON strings for storage.

    Pass any configured ``redis.Redis`` client so the caller controls
    connection details (host, port, SSL, authentication, Sentinel, Cluster):

        client = redis.Redis(host='redis.internal', port=6379, ssl=True)
        store = RedisStore(client, prefix='myapp:users:')

    The ``prefix`` namespaces this store within a shared Redis instance.
    Two stores with different prefixes are completely isolated from each
    other. Choose a prefix that is unique to the store's purpose.

    Thread-safe: redis-py uses a connection pool by default.

    Call close() or use as a context manager to release pool connections.
    """

    def __init__(self, client: redis_lib.Redis, *, prefix: str = 'oj:') -> None:
        self._client = client
        self._prefix = prefix

    def _k(self, key: str) -> str:
        return f'{self._prefix}{key}'

    # ------------------------------------------------------------------ CRUDL

    def create(self, key: str, value: Any) -> None:
        # SET NX — only write if key does not exist
        result = self._client.set(self._k(key), json.dumps(value), nx=True)
        if not result:
            raise KeyError(key)

    def read(self, key: str) -> Any:
        raw = self._client.get(self._k(key))
        return json.loads(raw) if raw is not None else None

    def update(self, key: str, value: Any) -> None:
        # SET XX — only write if key already exists
        result = self._client.set(self._k(key), json.dumps(value), xx=True)
        if not result:
            raise KeyError(key)

    def upsert(self, key: str, value: Any) -> None:
        self._client.set(self._k(key), json.dumps(value))

    def delete(self, key: str) -> None:
        self._client.delete(self._k(key))

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        # SCAN is cursor-based and non-blocking, unlike KEYS which blocks the server
        values = []
        for raw_key in self._client.scan_iter(f'{self._prefix}*'):
            raw = self._client.get(raw_key)
            if raw is not None:
                v = json.loads(raw)
                if predicate is None or predicate(v):
                    values.append(v)
        return values

    # ------------------------------------------------------------------ lifecycle

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> RedisStore:
        return self

    def __exit__(self, *args) -> None:
        self.close()
