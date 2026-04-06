from __future__ import annotations

import threading
from typing import Any, Callable, Optional

from oj_persistence.store.base import AbstractStore

_VALID_JOIN_TYPES = {'inner', 'left', 'right', 'outer'}


class PersistenceManager:
    """
    Singleton registry of named AbstractStore instances.

    One instance per process — all threads share the same registry.
    Thread safety is guaranteed by a class-level lock during construction
    and an instance-level lock for registry mutations.

    Correct usage — all data operations go through the manager:

        pm = PersistenceManager()
        pm.get_or_create('users', lambda: InMemoryStore())   # register once
        pm.create('users', 'u1', {'name': 'Alice'})
        pm.read('users', 'u1')
        pm.update('users', 'u1', {'name': 'Bob'})
        pm.upsert('users', 'u1', {'name': 'Charlie'})
        pm.delete('users', 'u1')
        pm.list('users')
        pm.join('users', 'orders', on=lambda u, o: u['id'] == o['user_id'])

    Stores are thread-safe independently (RWLock), but the correct usage model
    is that threads share only the manager singleton — they never hold a
    reference to a store and call its methods directly.
    """

    _instance: Optional['PersistenceManager'] = None
    _init_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'PersistenceManager':
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._stores: dict[str, AbstractStore] = {}
                    instance._registry_lock = threading.Lock()
                    cls._instance = instance
        return cls._instance

    def get_or_create(self, name: str, factory: Callable[[], AbstractStore]) -> AbstractStore:
        """
        Return the store registered under name, creating it via factory if absent.

        The factory is called at most once per name. Subsequent calls with the
        same name return the existing store regardless of the factory provided.
        Thread-safe: factory is never called concurrently for the same name.
        """
        store = self._stores.get(name)
        if store is None:
            with self._registry_lock:
                store = self._stores.get(name)
                if store is None:
                    store = factory()
                    self._stores[name] = store
        return store

    def register(self, name: str, store: AbstractStore) -> None:
        """Register a store under name, replacing any existing entry."""
        with self._registry_lock:
            self._stores[name] = store

    def get_store(self, name: str) -> Optional[AbstractStore]:
        """Return the store registered under name, or None."""
        return self._stores.get(name)

    def unregister(self, name: str) -> None:
        """Remove the store registered under name. No-op if not found."""
        with self._registry_lock:
            self._stores.pop(name, None)

    # ------------------------------------------------------------------
    # CRUDL — the primary data interface for callers
    # ------------------------------------------------------------------

    def _get_required(self, name: str) -> AbstractStore:
        store = self._stores.get(name)
        if store is None:
            raise KeyError(name)
        return store

    def create(self, store_name: str, key: str, value: Any) -> None:
        """Create a new entry in the named store. Raises KeyError if store or key already exists."""
        self._get_required(store_name).create(key, value)

    def read(self, store_name: str, key: str) -> Any:
        """Return the value for key from the named store, or None if not found."""
        return self._get_required(store_name).read(key)

    def update(self, store_name: str, key: str, value: Any) -> None:
        """Update an existing entry. Raises KeyError if store or key not found."""
        self._get_required(store_name).update(key, value)

    def upsert(self, store_name: str, key: str, value: Any) -> None:
        """Create or overwrite an entry in the named store."""
        self._get_required(store_name).upsert(key, value)

    def delete(self, store_name: str, key: str) -> None:
        """Remove an entry from the named store. No-op if key not found."""
        self._get_required(store_name).delete(key)

    def list(self, store_name: str, predicate: Optional[Callable[[Any], bool]] = None) -> list[Any]:
        """Return all values from the named store, optionally filtered by predicate."""
        return self._get_required(store_name).list(predicate)

    def join(
        self,
        left: str,
        right: str,
        on: Callable[[Any, Any], bool],
        how: str = 'inner',
        where: Optional[Callable[[Any, Any], bool]] = None,
    ) -> list[tuple[Any, Any]]:
        """
        Relational join across two registered stores.

        Parameters
        ----------
        left, right : store names (must be registered)
        on          : join predicate — called as on(left_val, right_val)
        how         : 'inner' | 'left' | 'right' | 'outer'
        where       : optional filter applied only to matched pairs (both non-None);
                      unmatched rows produced by left/right/outer are always included

        Returns
        -------
        list of (left_val, right_val) tuples. Unmatched sides are None.

        Complexity
        ----------
        O(m × n) — naive nested loop over store.list() on both sides.
        DB-backed stores (SQLite, Postgres, SQLAlchemy, …) should override
        with a delegated query rather than relying on this implementation.
        """
        if how not in _VALID_JOIN_TYPES:
            raise ValueError(f"Invalid how='{how}'. Expected one of: {sorted(_VALID_JOIN_TYPES)}")

        left_vals = self._get_required(left).list()
        right_vals = self._get_required(right).list()
        results: list[tuple[Any, Any]] = []

        if how == 'inner':
            for lv in left_vals:
                for rv in right_vals:
                    if on(lv, rv) and (where is None or where(lv, rv)):
                        results.append((lv, rv))

        elif how == 'left':
            for lv in left_vals:
                has_on_match = False
                for rv in right_vals:
                    if on(lv, rv):
                        has_on_match = True
                        if where is None or where(lv, rv):
                            results.append((lv, rv))
                if not has_on_match:
                    results.append((lv, None))

        elif how == 'right':
            for rv in right_vals:
                has_on_match = False
                for lv in left_vals:
                    if on(lv, rv):
                        has_on_match = True
                        if where is None or where(lv, rv):
                            results.append((lv, rv))
                if not has_on_match:
                    results.append((None, rv))

        elif how == 'outer':
            matched_right: set[int] = set()
            for lv in left_vals:
                has_on_match = False
                for i, rv in enumerate(right_vals):
                    if on(lv, rv):
                        has_on_match = True
                        matched_right.add(i)
                        if where is None or where(lv, rv):
                            results.append((lv, rv))
                if not has_on_match:
                    results.append((lv, None))
            for i, rv in enumerate(right_vals):
                if i not in matched_right:
                    results.append((None, rv))

        return results
