# oj-persistence v2 — proposal

Design proposal for the next major version of the library. Files in this directory
are **not** imported by the current package — they're reviewable artifacts showing
the intended shape before any migration is attempted.

## Contract

The **Manager is the only thing that touches a backing resource**. Consumers hand
the Manager a table name and a spec (`Sqlite(path=...)`, `InMemory()`, ...) via
`register()`, then interact only through the Manager's public API. They never see
a store object, a connection, a handle, a ref, or a file descriptor.

Corollary: if a consumer needs an operation the Manager doesn't expose, the fix
is to add it to the Manager — not to reach past it. A million holes exist in
Python's private-attribute hygiene; we won't use them.

### File-safety, in detail

"Only the Manager touches a file" is enforced at the **file level**, not the
Manager-object level. A process-scoped `_BackendRegistry` guarantees that
each unique connection target (sqlite path, redis url, etc.) is backed by
**exactly one** `Backend` instance. Any number of `Manager()` objects can
coexist — for test isolation, per-tenant table namespacing, whatever — and
if two of them register `Sqlite(path='app.db')`, they both receive the same
underlying `SqliteBackend` with its single connection pool and writer lock.
Two Managers cannot race on a file, ever.

The `Backend` is reference-counted across all Managers that hold it. When
the last `drop()` / `close()` releases the final reference, the Backend is
closed and its resources released. A fresh `register()` after that point
constructs a new Backend.

## Public API (full shape)

See [`manager.py`](./manager.py) for signatures and docstrings. In brief:

```python
from oj_persistence import Manager, Sqlite, InMemory, Ndjson

pm = Manager()

# Registration — tell the Manager where a table lives.
pm.register('users', Sqlite(path='data/app.db'))
pm.register('orders', Sqlite(path='data/app.db'))  # same path → Manager reuses connection
pm.register('cache', InMemory())

# CRUDL — sync or async, same semantics.
pm.create('users', 'u1', {'name': 'Alice'})
await pm.acreate('users', 'u1', {'name': 'Alice'})

value = pm.read('users', 'u1')
value = await pm.aread('users', 'u1')

# Iteration — sync or async generator.
for v in pm.iter('users'): ...
async for v in pm.aiter('users'): ...

# Indexed / paginated access (capability-gated at register time).
pm.add_index('users', '$.email')
pm.list_by_field('users', '$.email', 'alice@x.com')
pm.list_page('users', offset=0, limit=100)

# Joins — pushed down to SQL when left and right share a backing file.
pm.join('users', 'orders', on=...)

# Management.
pm.truncate('users')         # clear data, keep table + registration
pm.drop('users')             # clear + unregister + release backend resources if last table
pm.close()                   # close everything (shutdown)
pm.tables()                  # ['users', 'orders', 'cache']
pm.exists('users')           # bool
```

Every public data method has an `a`-prefixed async counterpart (`aread`, `alist`,
`ajoin`, etc.). Iteration is `iter` / `aiter`. Management methods (`register`,
`drop`, `truncate`, `close`) likewise mirror sync/async.

## Capabilities are declared, not discovered

Each `Backend` declares a `frozenset[Capability]` at the class level. The Manager
checks capabilities at `register()` time and raises immediately if a call site
will never work on the chosen backend. No more "works with SQLite, throws
`NotImplementedError` with in-memory" surprises at runtime.

```python
class Capability(StrEnum):
    PAGINATION    = 'pagination'     # list_page(offset, limit)
    FIELD_INDEX   = 'field_index'    # add_index / list_by_field
    NATIVE_JOIN   = 'native_join'    # SQL JOIN pushdown across same-file tables
    NATIVE_UPSERT = 'native_upsert'  # in-place upsert without file rewrite
```

`Sqlite`: `{PAGINATION, FIELD_INDEX, NATIVE_JOIN, NATIVE_UPSERT}`
`InMemory`: `{PAGINATION, FIELD_INDEX, NATIVE_UPSERT}` (sorted dict keeps order)
`Ndjson`: `{PAGINATION}` (scan-based)
`Redis`: `{NATIVE_UPSERT}` (KV lookup)

The consumer's expectation when calling `pm.list_page('users', ...)` is that it
works. If `users` is registered against a backend that doesn't support pagination,
`register()` fails — not the eventual call site deep in production code.

## Concurrency guarantees

The Manager is the only entity touching a backing resource, so it's free to
impose a concurrency model that consumers can rely on.

### For SQLite files

- **WAL mode is on by default.** (`PRAGMA journal_mode=WAL` at first open.)
- **One writer connection per file, serialized by an internal lock.**
- **A pool of reader connections per file** (default 4; configurable via `Sqlite(pool_size=N)`).
- Reads and writes run on a thread-pool executor so the async event loop is never blocked.

Resulting guarantees a consumer can rely on:

1. Concurrent `aread`/`alist`/`aiter` calls against the same or different tables
   in the same file **do not block each other**.
2. An in-flight write does **not block any read**. Readers see the last committed
   snapshot (WAL semantics).
3. Concurrent writes serialize (one at a time). Writers never wait for readers.
4. `truncate` / `drop` acquire the writer lock, so they cannot interleave with
   an in-flight write.

### For other backends

- **InMemory**: one threading lock; reads and writes serialize. Cheap enough
  that contention isn't worth splitting.
- **Ndjson**: append-serial for writes; reads scan the file on demand. A
  write-in-flight will briefly block a read that hits the in-progress line.
- **Redis**: concurrency is the server's problem; client just dispatches.
- **SQLAlchemy**: backend-specific (Postgres / MySQL handle MVCC themselves).

In all cases the consumer contract is the same: the Manager serializes enough
to keep the data consistent, and the async methods never block the event loop
for longer than a thread hop.

### What the Manager does *not* guarantee

- **Cross-file transactions.** A write to `users` and a write to `cache`
  (different backends) are not atomic together. The Manager exposes per-table
  transactional primitives (see `pm.transaction(table)` — TBD) for atomicity
  within a single backend, but cross-backend transactions are out of scope.
- **Snapshot isolation across multiple async reads.** Two `await pm.aread(...)`
  calls on the same table can see different snapshots if a writer lands
  between them. This matches SQLite's WAL semantics; callers needing a stable
  view should explicitly snapshot (`pm.alist` returns a materialized list).

## Migration from v1

Consumers that used `pm.configure()`: swap to `pm.register(name, Sqlite(path=...))`.
Dropped: `create_group`, `add_table`, `group_catalog`, `GroupRef`, `StoreRef`,
`get_store`, `sync_store` property, `store_context`, every `pm._private` attribute.

fast_etl's `DiskCollector`:
- `configure_store()` → `pm.register(self.store_id, Sqlite(path=...))` + `pm.truncate(self.store_id)`
- `_close_store()` → `pm.drop(self.store_id)`
- `cleanup()` → `pm.drop(self.store_id)` (file removal is now the Manager's job)

io-chains `PersistenceLink`:
- `pm.store_context(store_id)` wrapper → gone; writes just work on the registered table
- The link no longer needs a lifecycle context — the Manager's writer connection is always live

## Backends in this rev

Shipped: `Sqlite`, `InMemory`, `Ndjson`. Others (Redis, SqlAlchemy, TinyDB,
Versioned) are carried forward with adapters to the new `Backend` interface
but not re-architected. That work can be staged.

## Not in scope for v2

- Multi-process concurrency. The Manager is per-process. If two processes open
  the same sqlite file, they share the file-level WAL guarantees but no
  application-level coordination.
- Horizontal scaling. If you need multi-writer, you want a real database
  behind `SqlAlchemy(url='postgresql://...')`, not this.
