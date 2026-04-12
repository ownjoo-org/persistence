from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from oj_persistence.store.async_base import AsyncAbstractStore
from oj_persistence.utils.join import VALID_JOIN_TYPES, apply_join


class AsyncPersistenceManager:
    """
    Singleton registry of named AsyncAbstractStore instances.

    Registry mutations (register/unregister/get_or_create) are synchronous and
    thread-safe. All data operations (create/read/update/upsert/delete/list/join)
    are async and must be awaited.

    Correct usage:

        pm = AsyncPersistenceManager()
        pm.get_or_create('users', lambda: AsyncSqliteStore('users.db'))
        await pm.create('users', 'u1', {'name': 'Alice'})
        await pm.read('users', 'u1')
        await pm.upsert('users', 'u1', {'name': 'Charlie'})
        await pm.delete('users', 'u1')
        await pm.list('users')
        await pm.join('users', 'orders', on=lambda u, o: u['id'] == o['user_id'])
    """

    _instance: AsyncPersistenceManager | None = None
    _init_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> AsyncPersistenceManager:
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._stores: dict[str, AsyncAbstractStore] = {}
                    instance._registry_lock = threading.Lock()
                    cls._instance = instance
        return cls._instance

    # ------------------------------------------------------------------ configuration

    async def configure(self, name: str, **store_config) -> None:
        """
        Create, initialize, and register a store from keyword config.

        Supported types
        ---------------
        sqlite    : path (default ':memory:'), table (default 'store')
        in_memory : no additional kwargs

        Example
        -------
            await pm.configure('users', type='sqlite', path='data/app.db', table='users')
            await pm.configure('cache', type='in_memory')
        """
        from pathlib import Path

        cfg = dict(store_config)
        store_type = cfg.pop('type')

        if store_type == 'sqlite':
            from oj_persistence.store.async_sqlalchemy_store import AsyncSqlAlchemyStore
            path = cfg.pop('path', ':memory:')
            table = cfg.pop('table', 'store')
            if path != ':memory:':
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                url = f'sqlite+aiosqlite:///{path}'
            else:
                url = 'sqlite+aiosqlite:///:memory:'
            store = AsyncSqlAlchemyStore(url, table=table, **cfg)
            await store.initialize()
        elif store_type == 'in_memory':
            from oj_persistence.store.async_in_memory import AsyncInMemoryStore
            store = AsyncInMemoryStore(**cfg)
        else:
            raise ValueError(
                f"Unknown store type {store_type!r}. Supported: sqlite, in_memory"
            )

        self.register(name, store)

    # ------------------------------------------------------------------ registry

    def get_or_create(self, name: str, factory: Callable[[], AsyncAbstractStore]) -> AsyncAbstractStore:
        """Return the store under name, creating it via factory if absent. Factory called at most once."""
        store = self._stores.get(name)
        if store is None:
            with self._registry_lock:
                store = self._stores.get(name)
                if store is None:
                    store = factory()
                    self._stores[name] = store
        return store

    def register(self, name: str, store: AsyncAbstractStore) -> None:
        """Register a store under name, replacing any existing entry."""
        with self._registry_lock:
            self._stores[name] = store

    def get_store(self, name: str) -> AsyncAbstractStore | None:
        """Return the store registered under name, or None."""
        return self._stores.get(name)

    def unregister(self, name: str) -> None:
        """Remove the store registered under name. No-op if not found."""
        with self._registry_lock:
            self._stores.pop(name, None)

    # ------------------------------------------------------------------ internals

    def _get_required(self, name: str) -> AsyncAbstractStore:
        store = self._stores.get(name)
        if store is None:
            raise KeyError(name)
        return store

    # ------------------------------------------------------------------ CRUDL

    async def create(self, store_name: str, key: str, value: Any) -> None:
        await self._get_required(store_name).create(key, value)

    async def read(self, store_name: str, key: str) -> Any:
        return await self._get_required(store_name).read(key)

    async def update(self, store_name: str, key: str, value: Any) -> None:
        await self._get_required(store_name).update(key, value)

    async def upsert(self, store_name: str, key: str, value: Any) -> None:
        await self._get_required(store_name).upsert(key, value)

    async def delete(self, store_name: str, key: str) -> None:
        await self._get_required(store_name).delete(key)

    async def list(self, store_name: str, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        return await self._get_required(store_name).list(predicate)

    # ------------------------------------------------------------------ join

    async def join(
        self,
        left: str,
        right: str,
        on: Callable[[Any, Any], bool],
        how: str = 'inner',
        where: Callable[[Any, Any], bool] | None = None,
    ) -> list[tuple[Any, Any]]:
        """
        Relational join across two registered async stores.

        Parameters
        ----------
        left, right : store names (must be registered)
        on          : join predicate — called as on(left_val, right_val)
        how         : 'inner' | 'left' | 'right' | 'outer'
        where       : optional filter applied only to matched pairs (both non-None)

        Returns
        -------
        list of (left_val, right_val) tuples. Unmatched sides are None.
        """
        if how not in VALID_JOIN_TYPES:
            raise ValueError(f"Invalid how='{how}'. Expected one of: {sorted(VALID_JOIN_TYPES)}")

        left_vals = await self._get_required(left).list()
        right_vals = await self._get_required(right).list()
        return apply_join(left_vals, right_vals, on, how, where)

    # ------------------------------------------------------------------ list_by_field

    async def list_by_field(self, store_name: str, json_path: str, value: Any) -> list[Any]:
        """
        Return all values where the JSON field at json_path equals value.

        Delegates to the store's list_by_field() if it supports the method.
        Raises NotImplementedError for stores that do not support SQL pushdown.
        """
        store = self._get_required(store_name)
        if not hasattr(store, 'list_by_field'):
            raise NotImplementedError(
                f"Store '{store_name}' ({type(store).__name__}) does not support list_by_field(). "
                "Use a SQL-backed store (AsyncSqliteStore, AsyncSqlAlchemyStore) instead."
            )
        return await store.list_by_field(json_path, value)
