"""The Manager — only public entry point into the library.

PROPOSAL — not wired into the package yet. See ./README.md for context.

Consumers hand tables to the Manager via ``register(name, spec)`` and then talk
to the Manager about those tables by name. They never see a store, connection,
handle, or any backend internal. Sync and async methods are equally first-class
— async uses the ``a`` prefix (Python idiom).

File-safety invariant
---------------------
Per-file coordination (sqlite connection pool, writer lock, WAL mode) is
enforced **below** the Manager layer, in a process-scoped ``_BackendRegistry``
singleton. Any number of ``Manager`` instances can coexist — they each have
their own table-name namespace — but if two of them register ``Sqlite(path=X)``
they get the **same** ``SqliteBackend`` object, and therefore the same
connection pool, writer lock, and WAL configuration. There is no way for two
Managers to race on a file.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

from .base import (
    Backend,
    BackendSpec,
    Capability,
    Csv,
    DynamoDB,
    InMemory,
    Json,
    Ndjson,
    Redis,
    S3,
    Sqlite,
    SqlAlchemy,
    TableAlreadyRegistered,
    TableNotRegistered,
    TinyDb,
    UnsupportedOperation,
)

# ------------------------------------------------------------------ backend registry
#
# Process-scoped. Guarantees one Backend per connection target, regardless of
# how many Manager instances exist. This is where file-level coordination lives.

class _BackendRegistry:
    """Thread-safe, reference-counted registry of Backend instances.

    Not part of the public API. Each ``Manager`` delegates backend acquisition
    to the single module-level ``_BACKENDS`` instance.
    """

    def __init__(self) -> None:
        self._backends: dict[tuple, Backend] = {}
        self._refcounts: dict[tuple, int] = {}
        self._lock = threading.RLock()

    def acquire(self, spec: BackendSpec) -> tuple[Backend, bool]:
        """Return ``(backend, created)`` for ``spec``. Constructs a new backend
        if no other Manager has one registered for this connection target.
        Increments the refcount either way. Caller is responsible for calling
        ``release(spec)`` exactly once per ``acquire()``.
        """
        key = _dedup_key(spec)
        with self._lock:
            backend = self._backends.get(key)
            created = False
            if backend is None:
                backend = _construct_backend(spec)
                self._backends[key] = backend
                self._refcounts[key] = 0
                created = True
            self._refcounts[key] += 1
            return backend, created

    def release(self, spec: BackendSpec) -> Backend | None:
        """Decrement the refcount. Returns the backend if this was the last
        reference (caller must then ``await backend.aclose()``). Returns
        ``None`` otherwise.
        """
        key = _dedup_key(spec)
        with self._lock:
            if self._refcounts.get(key, 0) <= 0:
                return None
            self._refcounts[key] -= 1
            if self._refcounts[key] == 0:
                backend = self._backends.pop(key)
                self._refcounts.pop(key, None)
                return backend
            return None

    def snapshot(self) -> dict[tuple, int]:
        """Testing/introspection: current refcounts."""
        with self._lock:
            return dict(self._refcounts)


_BACKENDS = _BackendRegistry()


def _dedup_key(spec: BackendSpec) -> tuple:
    """Spec → hashable key used for backend dedup.

    The rule: two specs share a backend iff their connection target is
    identical. For path-based backends (Sqlite, Ndjson, TinyDb) that's the
    type + path. For Redis it's the url + db. For InMemory it's a per-spec
    identity (each ``InMemory()`` gets its own backend — they don't share).
    """
    if isinstance(spec, InMemory):
        return ('in_memory', id(spec))
    if isinstance(spec, Sqlite):
        return ('sqlite', str(spec.path))
    if isinstance(spec, Ndjson):
        return ('ndjson', str(spec.path))
    if isinstance(spec, Json):
        return ('json', str(spec.path))
    if isinstance(spec, Csv):
        return ('csv', str(spec.path))
    if isinstance(spec, TinyDb):
        return ('tinydb', str(spec.path))
    if isinstance(spec, Redis):
        return ('redis', spec.url, spec.db)
    if isinstance(spec, SqlAlchemy):
        return ('sqlalchemy', spec.url)
    if isinstance(spec, DynamoDB):
        return ('dynamodb', spec.region, spec.prefix, spec.endpoint_url)
    if isinstance(spec, S3):
        return ('s3', spec.bucket, spec.prefix, spec.region)
    raise TypeError(f'unknown BackendSpec: {type(spec).__name__}')


def _construct_backend(spec: BackendSpec) -> Backend:
    """Spec → concrete Backend instance. Backends import their own modules
    lazily so users can install subset extras without importing everything."""
    if isinstance(spec, Sqlite):
        from .backends.sqlite_backend import SqliteBackend
        return SqliteBackend(path=str(spec.path), pool_size=spec.pool_size)
    if isinstance(spec, InMemory):
        from .backends.in_memory_backend import InMemoryBackend
        return InMemoryBackend()
    if isinstance(spec, DynamoDB):
        from .backends.dynamodb_backend import DynamoDbBackend
        return DynamoDbBackend(
            region=spec.region,
            prefix=spec.prefix,
            endpoint_url=spec.endpoint_url,
        )
    if isinstance(spec, Ndjson):
        from .backends.ndjson_backend import NdjsonBackend
        return NdjsonBackend(base_dir=spec.path)
    if isinstance(spec, Json):
        from .backends.json_backend import JsonBackend
        return JsonBackend(base_dir=spec.path)
    if isinstance(spec, Csv):
        from .backends.csv_backend import CsvBackend
        return CsvBackend(base_dir=spec.path)
    if isinstance(spec, Redis):
        from .backends.redis_backend import RedisBackend
        return RedisBackend(url=spec.url, db=spec.db, prefix=spec.prefix)
    if isinstance(spec, SqlAlchemy):
        from .backends.sqlalchemy_backend import SqlAlchemyBackend
        return SqlAlchemyBackend(url=spec.url)
    if isinstance(spec, TinyDb):
        from .backends.tinydb_backend import TinyDbBackend
        return TinyDbBackend(path=spec.path)
    if isinstance(spec, S3):
        from .backends.s3_backend import S3Backend
        return S3Backend(bucket=spec.bucket, prefix=spec.prefix, region=spec.region)
    raise TypeError(f'unknown BackendSpec: {type(spec).__name__}')

__all__ = [
    'Manager',
    'Csv', 'DynamoDB', 'InMemory', 'Json', 'Ndjson', 'Redis', 'S3', 'Sqlite', 'SqlAlchemy', 'TinyDb',
    'Capability',
    'TableAlreadyRegistered', 'TableNotRegistered', 'UnsupportedOperation',
]


class Manager:
    """Process-scoped singleton that owns every backing resource.

    ``Manager()`` always returns the same instance — all application code
    shares one table registry and one set of backend connections. This prevents
    two independent call sites from opening conflicting connections to the same
    file or remote store.

    Typical use::

        from oj_persistence import Manager, Sqlite, DynamoDB

        # configure once at startup (idempotent — safe to call from multiple modules)
        Manager().register('users',    Sqlite(path='data/app.db'))
        Manager().register('sessions', DynamoDB(region='us-east-1'))

        # use anywhere — always the same instance
        Manager().create('users', 'u1', {'name': 'Alice'})
        await Manager().aread('sessions', 'sid:abc')

    Test isolation
    --------------
    Unit tests that need a clean slate should call ``Manager._reset()`` in
    sync setUp / tearDown, or ``await Manager._areset()`` in async fixtures.
    Both close all open backends and return the singleton to an empty state
    so the next ``Manager()`` starts fresh.
    """

    _instance: 'Manager | None' = None
    _singleton_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'Manager':
        with cls._singleton_lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._singleton_initialized = False
                cls._instance = inst
            return cls._instance

    @classmethod
    def _reset(cls) -> None:
        """Close all backends and discard the singleton. For test use only.

        Must be called from a non-async context (sync setUp / tearDown).
        Use ``await Manager._areset()`` from async test fixtures instead.
        """
        with cls._singleton_lock:
            if cls._instance is not None:
                try:
                    cls._instance._run_sync(cls._instance.aclose())
                except Exception:
                    pass
            cls._instance = None

    @classmethod
    async def _areset(cls) -> None:
        """Async version of _reset() for use inside async setUp / tearDown."""
        with cls._singleton_lock:
            instance = cls._instance
            cls._instance = None
        if instance is not None:
            try:
                await instance.aclose()
            except Exception:
                pass

    # ------------------------------------------------------------------ init

    def __init__(self) -> None:
        if getattr(self, '_singleton_initialized', False):
            return
        # table name → backend instance that owns it (looked up from _BACKENDS)
        self._tables: dict[str, Backend] = {}
        # table name → spec used to register it (needed for release() on drop)
        self._specs: dict[str, BackendSpec] = {}
        # table name → set of capabilities declared required by the caller
        self._required_capabilities: dict[str, frozenset[Capability]] = {}
        self._state_lock = threading.RLock()
        # Background event loop for sync → async bridging. Started lazily.
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._singleton_initialized = True

    # ------------------------------------------------------------------ registration

    def register(
        self,
        table: str,
        spec: BackendSpec,
        *,
        requires: frozenset[Capability] | None = None,
        replace: bool = False,
    ) -> None:
        """Register a table to live in the backend described by ``spec``.

        ``requires`` lets the caller declare up-front which capabilities they
        plan to use (e.g. ``frozenset({Capability.PAGINATION})``). If the
        chosen backend doesn't support them, ``register`` raises
        ``UnsupportedOperation`` immediately — not later at the call site.

        Raises ``TableAlreadyRegistered`` if ``table`` is already known, unless
        ``replace=True``, in which case the existing table is dropped first
        (data lost; connection closed if it was the last reference).
        """
        self._run_sync(self.aregister(table, spec, requires=requires, replace=replace))

    async def aregister(
        self,
        table: str,
        spec: BackendSpec,
        *,
        requires: frozenset[Capability] | None = None,
        replace: bool = False,
    ) -> None:
        with self._state_lock:
            if table in self._tables:
                if not replace:
                    raise TableAlreadyRegistered(table)
                await self._drop_locked(table)

            backend, created = _BACKENDS.acquire(spec)
            caps_required = requires or frozenset()
            missing = caps_required - backend.capabilities
            if missing:
                released = _BACKENDS.release(spec)
                if released is not None:
                    await released.aclose()
                raise UnsupportedOperation(
                    f"table {table!r}: backend {type(backend).__name__} is missing "
                    f"required capabilities {sorted(c.value for c in missing)}"
                )

            self._tables[table] = backend
            self._specs[table] = spec
            self._required_capabilities[table] = caps_required

        # Backend-side work (open on first use, create the table) happens
        # outside the state lock so I/O doesn't block other Manager methods.
        if created:
            await backend.aopen()
        await backend.acreate_table(table)

    # ------------------------------------------------------------------ inspection

    def tables(self) -> list[str]:
        """All registered table names, in registration order."""
        with self._state_lock:
            return list(self._tables)

    def exists(self, table: str) -> bool:
        with self._state_lock:
            return table in self._tables

    def capabilities(self, table: str) -> frozenset[Capability]:
        """Capabilities of the backend holding ``table``."""
        return self._backend(table).capabilities

    # ------------------------------------------------------------------ management

    def drop(self, table: str) -> None:
        """Delete ``table``'s data and unregister it. Closes the underlying
        connection if ``table`` was the last user of it."""
        self._run_sync(self.adrop(table))

    async def adrop(self, table: str) -> None:
        async with self._async_state_lock():
            await self._drop_locked(table)

    def truncate(self, table: str) -> None:
        """Remove all rows from ``table`` but keep it registered."""
        self._run_sync(self.atruncate(table))

    async def atruncate(self, table: str) -> None:
        backend = self._backend(table)
        await backend.atruncate_table(table)

    def close(self) -> None:
        """Close every backend the Manager has opened. No-op if already closed."""
        self._run_sync(self.aclose())

    async def aclose(self) -> None:
        with self._state_lock:
            specs = list(self._specs.values())
            self._tables.clear()
            self._specs.clear()
            self._required_capabilities.clear()
        # Release one refcount per registered table. If we were the last user
        # of a backend, close it. Other Managers still referencing the same
        # backend keep it alive.
        to_close: list[Backend] = []
        for spec in specs:
            released = _BACKENDS.release(spec)
            if released is not None:
                to_close.append(released)
        for backend in to_close:
            await backend.aclose()

    # ------------------------------------------------------------------ CRUDL (async)

    async def acreate(self, table: str, key: str, value: Any) -> None:
        await self._backend(table).acreate(table, key, value)

    async def aread(self, table: str, key: str) -> Any | None:
        return await self._backend(table).aread(table, key)

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        await self._backend(table).aupdate(table, key, value)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        await self._backend(table).aupsert(table, key, value)

    async def adelete(self, table: str, key: str) -> None:
        await self._backend(table).adelete(table, key)

    async def alist(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        return await self._backend(table).alist(table, predicate)

    async def aiter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> AsyncIterator[Any]:
        async for value in self._backend(table).aiter(table, predicate):
            yield value

    # ------------------------------------------------------------------ CRUDL (sync)

    def create(self, table: str, key: str, value: Any) -> None:
        self._run_sync(self.acreate(table, key, value))

    def read(self, table: str, key: str) -> Any | None:
        return self._run_sync(self.aread(table, key))

    def update(self, table: str, key: str, value: Any) -> None:
        self._run_sync(self.aupdate(table, key, value))

    def upsert(self, table: str, key: str, value: Any) -> None:
        self._run_sync(self.aupsert(table, key, value))

    def delete(self, table: str, key: str) -> None:
        self._run_sync(self.adelete(table, key))

    def list(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        return self._run_sync(self.alist(table, predicate))

    def iter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> Iterator[Any]:
        """Sync generator — yields one value at a time by driving the async
        iterator through the background loop."""
        aiter_obj = self._backend(table).aiter(table, predicate)
        while True:
            try:
                yield self._run_sync(aiter_obj.__anext__())
            except StopAsyncIteration:
                return

    # ------------------------------------------------------------------ indexed/paginated

    async def alist_page(self, table: str, offset: int, limit: int) -> list[Any]:
        backend = self._require_capability(table, Capability.PAGINATION)
        return await backend.alist_page(table, offset, limit)

    def list_page(self, table: str, offset: int, limit: int) -> list[Any]:
        return self._run_sync(self.alist_page(table, offset, limit))

    async def alist_by_field(self, table: str, json_path: str, value: Any) -> list[Any]:
        backend = self._require_capability(table, Capability.FIELD_INDEX)
        return await backend.alist_by_field(table, json_path, value)

    def list_by_field(self, table: str, json_path: str, value: Any) -> list[Any]:
        return self._run_sync(self.alist_by_field(table, json_path, value))

    async def aadd_index(self, table: str, json_path: str, *, name: str | None = None) -> None:
        backend = self._require_capability(table, Capability.FIELD_INDEX)
        await backend.aadd_index(table, json_path, name=name)

    def add_index(self, table: str, json_path: str, *, name: str | None = None) -> None:
        self._run_sync(self.aadd_index(table, json_path, name=name))

    # ------------------------------------------------------------------ joins / relations

    async def ajoin(
        self,
        left: str,
        right: str,
        on: Callable[[Any, Any], bool],
        how: str = 'inner',
        where: Callable[[Any, Any], bool] | None = None,
    ) -> list[tuple[Any, Any]]:
        """Join two tables. When both share the same backend instance AND that
        backend declares ``Capability.NATIVE_JOIN``, the Manager pushes the join
        into the backend (SQL JOIN for SQLite). Otherwise falls back to an
        in-Python nested loop."""
        left_backend = self._backend(left)
        right_backend = self._backend(right)
        if left_backend is right_backend and Capability.NATIVE_JOIN in left_backend.capabilities:
            try:
                return await left_backend.anative_join(left, right, on=on, how=how, where=where)
            except NotImplementedError:
                pass  # fall through to Python path
        # Python path: materialize both sides and pair up. Kept here (not in
        # Backend) because it's backend-agnostic.
        from oj_persistence.utils.join import apply_join  # existing util
        left_vals = await left_backend.alist(left)
        right_vals = await right_backend.alist(right)
        return apply_join(left_vals, right_vals, on, how, where)

    def join(
        self,
        left: str,
        right: str,
        on: Callable[[Any, Any], bool],
        how: str = 'inner',
        where: Callable[[Any, Any], bool] | None = None,
    ) -> list[tuple[Any, Any]]:
        return self._run_sync(self.ajoin(left, right, on, how=how, where=where))

    # ------------------------------------------------------------------ internals

    def _backend(self, table: str) -> Backend:
        with self._state_lock:
            backend = self._tables.get(table)
        if backend is None:
            raise TableNotRegistered(table)
        return backend

    def _require_capability(self, table: str, cap: Capability) -> Backend:
        backend = self._backend(table)
        if cap not in backend.capabilities:
            raise UnsupportedOperation(
                f"table {table!r}: backend {type(backend).__name__} does not support {cap.value}"
            )
        return backend

    async def _drop_locked(self, table: str) -> None:
        """Drop a table and release its backend refcount. Caller holds _state_lock."""
        backend = self._tables.pop(table, None)
        spec = self._specs.pop(table, None)
        self._required_capabilities.pop(table, None)
        if backend is None or spec is None:
            raise TableNotRegistered(table)
        await backend.adrop_table(table)
        # If this was the last reference in the entire process, close it.
        released = _BACKENDS.release(spec)
        if released is not None:
            await released.aclose()

    def _async_state_lock(self):
        """Async-flavored RLock acquire. For the simple cases here, the sync
        RLock is enough — we enter it synchronously, do a tiny bit of work,
        then release. The real I/O happens outside."""
        class _Ctx:
            def __init__(self, lock: threading.RLock) -> None:
                self._lock = lock
            async def __aenter__(self) -> None:
                self._lock.acquire()
            async def __aexit__(self, *exc) -> None:
                self._lock.release()
        return _Ctx(self._state_lock)

    def _run_sync(self, coro):
        """Execute ``coro`` on the Manager's background event loop and wait
        for the result. This is how sync methods reuse the async implementation.

        Implementation detail: we lazy-start a single daemon thread running an
        event loop for the lifetime of the Manager. All sync → async bridging
        funnels through it, so sqlite connections attached to the loop remain
        on one thread (a requirement for sqlite3 without check_same_thread).

        Not safe to call from within a running event loop — use the ``a*``
        method directly in that case. We detect and raise.
        """
        try:
            asyncio.get_running_loop()
            raise RuntimeError(
                "Manager sync methods can't be called from inside an async context; "
                "use the a-prefixed async method instead."
            )
        except RuntimeError as exc:
            if "can't be called" in str(exc):
                raise
            # get_running_loop raises RuntimeError when no loop is running — good, that's expected.

        if self._loop is None or not self._loop.is_running():
            self._start_background_loop()
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def _start_background_loop(self) -> None:
        """Start the dedicated event loop thread used by _run_sync."""
        ready = threading.Event()

        def _run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            ready.set()
            self._loop.run_forever()

        self._loop_thread = threading.Thread(target=_run, daemon=True, name='pm-loop')
        self._loop_thread.start()
        ready.wait()
