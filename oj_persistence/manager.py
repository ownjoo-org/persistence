from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from oj_persistence.exceptions import UpsertNotSupportedError
from oj_persistence.refs import GroupRef, StoreRef
from oj_persistence.relation import Relation
from oj_persistence.store.base import AbstractStore
from oj_persistence.utils.join import VALID_JOIN_TYPES, apply_join
from oj_persistence.utils.relation import apply_relation


class PersistenceManager:
    """
    Singleton registry of named AbstractStore instances.

    One instance per process — all threads share the same registry.
    Thread safety is guaranteed by a class-level lock during construction
    and an instance-level lock for registry mutations.

    Correct usage — all data operations go through the manager:

        pm = PersistenceManager()
        pm.get_or_create('users', lambda: SqliteStore('users.db'))  # register once
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

    _instance: PersistenceManager | None = None
    _init_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> PersistenceManager:
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._stores: dict[str, AbstractStore] = {}
                    instance._registry_lock = threading.Lock()
                    # group_id → (GroupRef, conn_kwargs)
                    instance._groups: dict[str, tuple[GroupRef, dict]] = {}
                    # store_id → StoreRef (only for stores created via configure/add_table)
                    instance._store_refs: dict[str, StoreRef] = {}
                    # group_id → shared resources (e.g. sqlite3.Connection)
                    instance._group_resources: dict[str, dict] = {}
                    cls._instance = instance
        return cls._instance

    # ------------------------------------------------------------------ group API

    def create_group(self, store_type: str, group_id: str | None = None, **conn_kwargs) -> GroupRef:
        """
        Declare a backing store group (database file, in-memory namespace, etc.).

        Parameters
        ----------
        store_type  : 'in_memory' | 'sqlite'
        group_id    : optional name; UUID generated if omitted
        **conn_kwargs : connection details (path= for sqlite); hidden from callers after this call

        Returns
        -------
        GroupRef — hold this to call add_table() or pass group_id to add_table().
        """
        if group_id is None:
            group_id = str(uuid4())
        ref = GroupRef(group_id=group_id, store_type=store_type)
        resources: dict = {}
        if store_type == 'sqlite':
            import sqlite3
            path = conn_kwargs.get('path', ':memory:')
            resources['conn'] = sqlite3.connect(str(path), check_same_thread=False)
        with self._registry_lock:
            self._groups[group_id] = (ref, dict(conn_kwargs))
            self._group_resources[group_id] = resources
        return ref

    def add_table(
        self,
        group_id: str,
        store_id: str | None = None,
        table_id: str | None = None,
    ) -> StoreRef:
        """
        Create a table within an existing group and register it.

        Parameters
        ----------
        group_id  : must match a previously created group
        store_id  : name used for CRUDL calls; UUID generated if omitted
        table_id  : logical table name inside the backing store; UUID if omitted

        Returns
        -------
        StoreRef — use store_ref.store_id in all subsequent CRUDL calls.
        """
        if group_id not in self._groups:
            raise KeyError(f"Group '{group_id}' not found. Call create_group() first.")
        group_ref, conn_kwargs = self._groups[group_id]
        if store_id is None:
            store_id = str(uuid4())
        if table_id is None:
            table_id = f't_{uuid4().hex}'  # SQL-safe: no hyphens, letter prefix
        resources = self._group_resources.get(group_id, {})
        store = self._instantiate_store(group_ref.store_type, table_id, conn_kwargs, resources)
        store_ref = StoreRef(
            store_id=store_id,
            group_id=group_id,
            table_id=table_id,
            store_type=group_ref.store_type,
        )
        with self._registry_lock:
            self._stores[store_id] = store
            self._store_refs[store_id] = store_ref
        return store_ref

    def configure(self, store_type: str, store_id: str | None = None, **kwargs) -> StoreRef:
        """
        Convenience: create an implicit group and one table in a single call.

        Use create_group() + add_table() when you need multiple tables sharing
        the same backing store (e.g. two tables in one SQLite file).

        Returns
        -------
        StoreRef — use store_ref.store_id in all subsequent CRUDL calls.
        """
        group_id = str(uuid4())
        self.create_group(store_type=store_type, group_id=group_id, **kwargs)
        return self.add_table(group_id=group_id, store_id=store_id)

    def catalog(self) -> dict[str, StoreRef]:
        """Return a snapshot of all stores registered via configure() or add_table()."""
        return dict(self._store_refs)

    def group_catalog(self) -> dict[str, GroupRef]:
        """Return a snapshot of all groups registered via create_group()."""
        return {gid: ref for gid, (ref, _) in self._groups.items()}

    @staticmethod
    def _instantiate_store(
        store_type: str, table_id: str, conn_kwargs: dict, resources: dict
    ) -> AbstractStore:
        if store_type == 'in_memory':
            from oj_persistence.store.in_memory import InMemoryStore
            return InMemoryStore()
        if store_type == 'sqlite':
            from oj_persistence.store.sqlite import SqliteStore
            shared_conn = resources.get('conn')
            if shared_conn is not None:
                return SqliteStore._from_connection(shared_conn, table=table_id)
            path = conn_kwargs.get('path', ':memory:')
            return SqliteStore(path, table=table_id)
        raise ValueError(
            f"Unknown store type {store_type!r}. Supported: in_memory, sqlite"
        )

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

    def get_store(self, name: str) -> AbstractStore | None:
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

    def upsert(self, store_name: str, key: str, value: Any, *, allow_inefficient: bool = False) -> None:
        """
        Create or overwrite an entry in the named store.

        Raises UpsertNotSupportedError for stores that require a full file
        rewrite to satisfy the operation (NDJSON, CSV, flat-file stores).
        Pass allow_inefficient=True to override and accept the performance cost.
        Prefer create() for new records and update() for existing ones.
        """
        store = self._get_required(store_name)
        if not store.supports_native_upsert and not allow_inefficient:
            raise UpsertNotSupportedError(
                f"Store '{store_name}' ({type(store).__name__}) requires a full file rewrite "
                f"for upsert. Use create() or update() instead, or pass allow_inefficient=True."
            )
        store.upsert(key, value)

    def delete(self, store_name: str, key: str) -> None:
        """Remove an entry from the named store. No-op if key not found."""
        self._get_required(store_name).delete(key)

    def list(self, store_name: str, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        """Return all values from the named store, optionally filtered by predicate."""
        return self._get_required(store_name).list(predicate)

    def relate(self, relation: Relation) -> list[tuple[Any, Any]]:
        """
        Execute a declarative Relation query across two registered stores.

        When both stores belong to the same group and the backing supports
        native joins (SQLite INNER/LEFT), the query is pushed down to SQL.
        Otherwise falls back to the Python nested-loop path.

        The relation's where filter is always applied in Python after joining.
        """
        left_store  = self._get_required(relation.left_store)
        right_store = self._get_required(relation.right_store)

        # Same-group native join: delegate to the left store if supported
        left_ref  = self._store_refs.get(relation.left_store)
        right_ref = self._store_refs.get(relation.right_store)
        if (
            left_ref is not None
            and right_ref is not None
            and left_ref.group_id == right_ref.group_id
            and hasattr(left_store, 'native_join')
            and relation.how in ('inner', 'left')
        ):
            try:
                pairs = left_store.native_join(
                    right_store,
                    on=relation.on,
                    how=relation.how,
                    left_fields=relation.left_fields,
                    right_fields=relation.right_fields,
                )
                if relation.where is not None:
                    pairs = [(l, r) for l, r in pairs if r is None or relation.where(l, r)]
                return pairs
            except NotImplementedError:
                pass  # fall through to Python path

        # Python path: fetch both sides and join in memory
        left_vals  = left_store.list()
        right_vals = right_store.list()
        return apply_relation(
            left_vals, right_vals,
            on=relation.on,
            how=relation.how,
            left_fields=relation.left_fields,
            right_fields=relation.right_fields,
            where=relation.where,
        )

    def join(
        self,
        left: str,
        right: str,
        on: Callable[[Any, Any], bool],
        how: str = 'inner',
        where: Callable[[Any, Any], bool] | None = None,
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
        O(m x n) — naive nested loop over store.list() on both sides.
        DB-backed stores (SQLite, Postgres, SQLAlchemy, …) should override
        with a delegated query rather than relying on this implementation.
        """
        if how not in VALID_JOIN_TYPES:
            raise ValueError(f"Invalid how='{how}'. Expected one of: {sorted(VALID_JOIN_TYPES)}")

        left_vals = self._get_required(left).list()
        right_vals = self._get_required(right).list()
        return apply_join(left_vals, right_vals, on, how, where)
