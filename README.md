# oj-persistence

Thread-safe persistence management with pluggable storage backends.

## Installation

```bash
pip install oj-persistence
```

Install only the driver packages you need (see [Storage backends](#storage-backends)):

```bash
pip install psycopg2-binary   # PostgreSQL via SqlAlchemyStore
pip install asyncpg           # PostgreSQL via AsyncSqlAlchemyStore
pip install pymysql           # MySQL/MariaDB via SqlAlchemyStore
pip install aiosqlite         # SQLite via AsyncSqliteStore / AsyncSqlAlchemyStore
```

## Quick start

```python
from oj_persistence import PersistenceManager

pm = PersistenceManager()

# Register stores by name — all data ops go through the manager
ref = pm.configure('sqlite', store_id='users', path='users.db')

pm.create('users', 'u1', {'name': 'Alice', 'role': 'admin'})
pm.read('users', 'u1')                                    # {'name': 'Alice', 'role': 'admin'}
pm.update('users', 'u1', {'name': 'Alice', 'role': 'owner'})
pm.delete('users', 'u2')

admins = pm.list('users', predicate=lambda v: v['role'] == 'owner')
```

---

## Manager-as-gatekeeper

Both `PersistenceManager` and `AsyncPersistenceManager` are **singletons**. All data
operations go through the manager — callers never hold a direct reference to a store.
Stores are identified by a `store_id` string; the underlying connection details (file
path, table name, shared connection) are hidden inside the manager.

```
caller → PersistenceManager.create/read/update/delete/list
                        ↓
              named store registry
                        ↓
              AbstractStore (sqlite, redis, …)
```

### Registering stores

Three ways to register a store, in order of preference:

```python
# 1. configure() — one call creates the group, table, and store reference
ref = pm.configure('sqlite', store_id='users', path='users.db')

# 2. register() — attach an already-constructed store
from oj_persistence import SqliteStore
pm.register('cache', SqliteStore(':memory:'))

# 3. get_or_create() — lazy registration with a factory (factory called once)
pm.get_or_create('users', lambda: SqliteStore('users.db'))
```

`configure()` returns a `StoreRef` — an opaque frozen dataclass holding
`store_id`, `group_id`, `table_id`, and `store_type`.  Hold onto `ref.store_id`
to use in subsequent CRUDL calls; the rest is for introspection only.

---

## CRUDL semantics

| Method   | Behaviour |
|----------|-----------|
| `create` | Raises `KeyError` if key already exists |
| `read`   | Returns `None` if key is missing |
| `update` | Raises `KeyError` if key does not exist |
| `upsert` | Creates or overwrites; raises `UpsertNotSupportedError` for file-backed stores unless `allow_inefficient=True` |
| `delete` | No-op if key is missing |
| `list`   | Returns all values, optionally filtered |

### Upsert guard

File-backed stores (`NdjsonFileStore`, `CsvFileStore`, `FlatFileStore`, `IjsonFileStore`,
`AsyncNdjsonFileStore`) require a full file rewrite for every `upsert`. The manager
blocks this by default:

```python
from oj_persistence import UpsertNotSupportedError

# Raises UpsertNotSupportedError — avoids silently expensive rewrites
pm.upsert('events', 'e1', {'type': 'login'})

# Override when you accept the cost
pm.upsert('events', 'e1', {'type': 'login'}, allow_inefficient=True)
```

For these stores, prefer `create()` for new records and `update()` for existing ones.
All SQL-backed and in-memory stores (`SqliteStore`, `SqlAlchemyStore`, `RedisStore`,
`InMemoryStore`, and async variants) support native upsert and are never blocked.

---

## Store groups — multiple tables, one backing store

Use `create_group()` + `add_table()` when multiple logical stores must share the same
SQLite database file (so SQL JOINs can run inside the database rather than in Python):

```python
# Sync
group = pm.create_group('sqlite', path='app.db')
users_ref  = pm.add_table(group.group_id, store_id='users')
orders_ref = pm.add_table(group.group_id, store_id='orders')

# Async — add_table is async
group = pm.create_group('sqlite', path='app.db')
users_ref  = await pm.add_table(group.group_id, store_id='users')
orders_ref = await pm.add_table(group.group_id, store_id='orders')
```

All tables in the same group share one `sqlite3.Connection`, enabling native SQL
`INNER JOIN` / `LEFT JOIN` push-down via `relate()` (see [Relations](#relations)).

`store_id` and `table_id` are auto-UUID'd when omitted.

### configure() — single-table convenience

When you only need one table per backing store, `configure()` wraps `create_group` +
`add_table` in one call:

```python
# Sync
ref = pm.configure('sqlite', store_id='users', path='users.db')
ref = pm.configure('in_memory', store_id='cache')

# Async
ref = await pm.configure('sqlite', store_id='users', path='users.db')
ref = await pm.configure('in_memory', store_id='cache')
```

Supported store types and their keyword arguments:

| `store_type` | kwargs | notes |
|---|---|---|
| `sqlite`    | `path` (default `':memory:'`) | Each `configure()` call opens its own connection |
| `in_memory` | — | No persistence; suitable for tests |

### Catalog

```python
# All stores registered via configure() or add_table()
pm.catalog()        # dict[store_id, StoreRef]
pm.group_catalog()  # dict[group_id, GroupRef]
```

---

## Relations

For structured cross-store queries, use the declarative `Relation` API instead of
free-function `join()`. Relations support field projection, post-join filters, and
automatic SQL push-down for same-group SQLite stores.

```python
from oj_persistence import Relation, JoinCondition, Op

rel = Relation(
    left_store='users',
    right_store='orders',
    on=JoinCondition('id', Op.EQ, 'user_id'),
    how='inner',                            # 'inner' | 'left' | 'right' | 'outer'
    left_fields=['id', 'name'],             # project left side (None = all fields)
    right_fields=['order_id', 'total'],     # project right side
    where=lambda u, o: o['total'] > 100,   # post-join filter on matched pairs
)

pairs = pm.relate(rel)
# or
pairs = await async_pm.relate(rel)
```

`Op` supports: `EQ`, `NE`, `LT`, `LE`, `GT`, `GE` (maps to `==`, `!=`, `<`, `<=`, `>`, `>=`).

Multiple conditions are ANDed:

```python
on=[
    JoinCondition('dept_id', Op.EQ, 'dept_id'),
    JoinCondition('joined_year', Op.GE, 'fiscal_year'),
]
```

### Same-group SQL push-down

When both stores belong to the same SQLite group (via `create_group` + `add_table`),
`relate()` pushes `INNER JOIN` and `LEFT JOIN` down into the shared SQLite connection —
no Python-side data fetching required:

```python
group = pm.create_group('sqlite', path='app.db')
users_ref  = pm.add_table(group.group_id, store_id='users')
orders_ref = pm.add_table(group.group_id, store_id='orders')

pm.relate(Relation('users', 'orders', on=JoinCondition('id', Op.EQ, 'user_id')))
# → executes: SELECT l.value, r.value FROM users l INNER JOIN orders r ON ...
```

`RIGHT` and `OUTER` joins always use the Python path (not supported as SQL in SQLite).
The `where` filter is always applied in Python after the join.

### Ad-hoc joins

For quick lambda-predicate joins without field projection, use `join()`:

```python
ON = lambda user, order: user['id'] == order['user_id']

results = pm.join('users', 'orders', on=ON, how='left',
                  where=lambda u, o: o['total'] > 100)
# returns list of (left_val, right_val) tuples; unmatched sides are None
```

Supported `how` values: `inner` (default), `left`, `right`, `outer`.

> **Note:** `join()` and the Python-path fallback in `relate()` call `list()` on both
> stores and perform an O(m × n) nested loop. For large datasets, prefer same-group
> `relate()` with SQL push-down or a query pushed down via `SqlAlchemyStore`.

---

## Storage backends

### Choosing a backend

| Use case | Recommended backend |
|---|---|
| Local file persistence, indexed key lookups | `SqliteStore` |
| External database (PostgreSQL, MySQL, SQL Server) | `SqlAlchemyStore` |
| Shared cache, high-throughput writes, TTL | `RedisStore` |
| Heterogeneous / schema-free documents | `TinyDbStore` |
| Append-heavy streaming, rarely rewritten records | `NdjsonFileStore` |
| Versioned records with full history | `VersionedStore` |
| Tests or ephemeral in-process caching | `InMemoryStore` |

### Sync backends

| Class | Backed by | Notes |
|---|---|---|
| `SqliteStore` | SQLite (`sqlite3`) | O(log n) key ops; optional JSON field indexes; zero extra deps |
| `SqlAlchemyStore` | Any SQLAlchemy database | PostgreSQL, MySQL, SQL Server, Oracle via connection URL |
| `RedisStore` | Redis | Native key-value; `SET NX`/`XX` for strict create/update; SCAN-based list |
| `TinyDbStore` | TinyDB | Schema-free documents; native query DSL via `.query()` |
| `InMemoryStore` | In-process dict | Fastest; not persistent; no extra deps |
| `VersionedStore` | Any `AbstractStore` | Envelope wrapper; preserves full history; `read_latest()` and `list_versions()` |
| `NdjsonFileStore` | NDJSON file | O(1) append-creates; O(n) reads/updates; best for streaming writes |
| `FlatFileStore` | JSON file | Full load/save per op; simple but slow for large datasets |
| `IjsonFileStore` | JSON file | Streaming reads via `ijson`; lower memory than `FlatFileStore` |
| `CsvFileStore` | CSV file | Field values round-trip as strings |

### Async backends

Async backends implement `AsyncAbstractStore` and are designed for use with
[`io_chains`](https://github.com/ownjoo-org/io_chains) `PersistenceLink` and
other async pipelines.

| Class | Backed by | Notes |
|---|---|---|
| `AsyncSqliteStore` | SQLite | Wraps `SqliteStore` via `asyncio.to_thread` |
| `AsyncSqlAlchemyStore` | Any SQLAlchemy async database | Native async via `create_async_engine`; requires `initialize()` or context manager |
| `AsyncRedisStore` | Redis | Native async via `redis.asyncio`; SCAN-based list |
| `AsyncInMemoryStore` | In-process dict | Zero overhead; no locking needed (asyncio is single-threaded) |
| `AsyncNdjsonFileStore` | NDJSON file | Buffered writes flushed on context exit or batch-size trigger |
| `AsyncVersionedStore` | Any `AsyncAbstractStore` | Async envelope wrapper; full history with `read_latest()` and `list_versions()` |

---

## Usage examples

### AsyncPersistenceManager — async registry

`AsyncPersistenceManager` is the async counterpart to `PersistenceManager`.
Registry methods (`register`, `get_store`, `get_or_create`, `unregister`) are
synchronous; all data operations are `async`.

```python
from oj_persistence import AsyncPersistenceManager

pm = AsyncPersistenceManager()

# Set up stores (configure is async for the async manager)
ref = await pm.configure('sqlite', store_id='users', path='users.db')

await pm.create('users', 'u1', {'name': 'Alice'})
await pm.upsert('users', 'u2', {'name': 'Bob'})
await pm.read('users', 'u1')

# SQL pushdown via list_by_field
await pm.list_by_field('users', '$.role', 'admin')

# Relational join across two async stores
pairs = await pm.join('users', 'orders', on=lambda u, o: u['id'] == o['user_id'], how='left')

# Declarative relation with SQL push-down (same-group SQLite)
from oj_persistence import Relation, JoinCondition, Op
pairs = await pm.relate(Relation('users', 'orders', on=JoinCondition('id', Op.EQ, 'user_id')))
```

### store_context() — flush guarantee for pipelines

`store_context(store_id)` is an async context manager that enters the named store's
lifecycle. Use it to guarantee buffered writes (e.g. `AsyncNdjsonFileStore`) are
flushed on exit. The store reference is never exposed to the caller.

```python
async with pm.store_context('events'):
    await pm.create('events', 'e1', {'type': 'login'})
# buffer is flushed here — 'e1' is durable on disk
```

This is primarily used by `PersistenceLink` internally. Call it directly when you need
the same flush guarantee outside a pipeline.

### PersistenceLink (io_chains)

`PersistenceLink` is a mid-chain tap in an `io_chains` pipeline. It writes each item
to a named store via the manager, then passes the item downstream unchanged. The store's
lifecycle context is managed automatically.

```python
from oj_persistence import AsyncPersistenceManager, AsyncSqliteStore
from io_chains.links.persistence_link import PersistenceLink
from io_chains.links.chain import Chain

pm = AsyncPersistenceManager()
pm.register('users', AsyncSqliteStore('users.db'))

chain = Chain(
    source=fetch_users(),
    links=[
        PersistenceLink(
            manager=pm,
            store_id='users',
            key_fn=lambda item: item['id'],
            operation='upsert',         # 'upsert' (default) | 'create' | 'update'
        ),
    ],
)
await chain()
```

### SqlAlchemyStore — external databases

```python
from oj_persistence import SqlAlchemyStore

# PostgreSQL
store = SqlAlchemyStore('postgresql+psycopg2://user:pass@host/db')

# MySQL / MariaDB
store = SqlAlchemyStore('mysql+pymysql://user:pass@host/db')

# SQL Server via PyODBC
store = SqlAlchemyStore('mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server')

# Custom table name and engine options
store = SqlAlchemyStore('postgresql+psycopg2://...', table='my_cache', pool_size=10)
```

### RedisStore

```python
import redis
from oj_persistence import RedisStore

client = redis.Redis(host='redis.internal', port=6379, ssl=True)
store = RedisStore(client, prefix='myapp:users:')

store.upsert('u1', {'name': 'Alice'})
store.read('u1')   # {'name': 'Alice'}
```

### TinyDbStore — document queries

```python
from tinydb import where
from oj_persistence import TinyDbStore

store = TinyDbStore('data.json')
store.upsert('1', {'name': 'Alice', 'city': 'NYC'})
store.upsert('2', {'name': 'Bob',   'city': 'LA'})

# Standard AbstractStore interface
store.list(lambda v: v['city'] == 'NYC')

# TinyDB-native query DSL
store.query(where('_value')['city'] == 'NYC')
```

### NdjsonFileStore — streaming writes

NDJSON is well-suited to pipelines that append many records and rarely need
to rewrite existing ones. Reads and updates require a full file scan (O(n)),
so it is not the right choice for key-lookup-heavy workloads.

```python
from oj_persistence import NdjsonFileStore

store = NdjsonFileStore('events.ndjson')
store.create('evt:1', {'type': 'login', 'user': 'u1'})   # O(1) append
store.read('evt:1')                                        # O(n) scan
```

### SqliteStore — JSON field indexes and list_by_field

SQL-backed stores support pushing equality filters into the database so an
index is actually exercised — skipping the Python-side predicate scan entirely.

```python
from oj_persistence import SqliteStore

store = SqliteStore('users.db')
store.upsert('u1', {'name': 'Alice', 'role': 'admin'})
store.upsert('u2', {'name': 'Bob',   'role': 'viewer'})

# Create a database index on a JSON field
store.add_json_index('$.role')

# Filter pushed into SQL — index is used
admins = store.list_by_field('$.role', 'admin')   # [{'name': 'Alice', 'role': 'admin'}]
```

`list_by_field` and `add_json_index` are available on `SqliteStore`,
`AsyncSqliteStore`, `SqlAlchemyStore`, and `AsyncSqlAlchemyStore`.

Via the manager:

```python
pm.list_by_field('users', '$.role', 'admin')
await async_pm.list_by_field('users', '$.role', 'admin')
```

### VersionedStore — full history with latest-read

Wrap any store to store every write as an immutable version, keyed by
`{key}#{seq}`. Standard CRUDL operates on the latest version; the full
history is always available.

```python
from oj_persistence import VersionedStore, SqliteStore

store = VersionedStore(SqliteStore('history.db'))

store.upsert('u1', {'name': 'Alice', 'role': 'admin'})
store.upsert('u1', {'name': 'Alice', 'role': 'owner'})  # second version

store.read_latest('u1')    # {'name': 'Alice', 'role': 'owner'}
store.list_versions('u1')  # [{_key, _seq, _inserted_at, _value}, ...]
store.list()               # [latest value per key]
```

The async variant `AsyncVersionedStore` mirrors this interface with `await`.

### File store compression

All file-backed stores (`NdjsonFileStore`, `CsvFileStore`, `FlatFileStore`,
`IjsonFileStore`) accept a `compression` parameter. Compression is applied
transparently at open time — no changes to CRUDL calls.

```python
from oj_persistence import NdjsonFileStore, CsvFileStore

# Explicit codec
store = NdjsonFileStore('data.ndjson.gz', compression='gzip')

# Auto-detect from extension (.gz, .bz2, .xz, .lzma)
store = CsvFileStore('data.csv.gz', compression='auto')

store.upsert('u1', {'name': 'Alice', 'role': 'admin'})
store.read('u1')   # decompressed transparently
```

Supported codecs: `'gzip'`, `'bz2'`, `'lzma'` (also matches `.xz`), `'auto'`.

### Streaming files to disk — from_stream / stream_to_file

Download a byte stream directly to disk, then open it as a store. Two
patterns are supported: a one-step classmethod or a standalone utility.

**One step (recommended):**

```python
import httpx
from oj_persistence import CsvFileStore, NdjsonFileStore

async with httpx.AsyncClient() as client:
    response = await client.get('https://example.com/users.csv.gz', stream=True)

    # Stream bytes to disk, decompress on open
    store = await CsvFileStore.from_stream(
        response.aiter_bytes(),
        '/tmp/users.csv.gz',
        compression='auto',
    )

users = store.list()
```

**Two steps (store the file, open later):**

```python
from oj_persistence.utils.streaming import stream_to_file, stream_to_file_sync

# Async
path = await stream_to_file(response.aiter_bytes(), '/tmp/data.ndjson')

# Sync (also accepts file-like objects with .read())
import requests
r = requests.get('https://example.com/data.ndjson', stream=True)
path = stream_to_file_sync(r.iter_content(chunk_size=65536), '/tmp/data.ndjson')

store = NdjsonFileStore(path)
```

`stream_to_file` accepts `AsyncIterable[bytes]`, `Iterable[bytes]`, or any
file-like object with a `.read(n)` method. Writes go to a UUID-named temp
file in the same directory and are renamed atomically on success. On failure
the temp file is deleted unless `debug=True` (or `OJ_PERSISTENCE_DEBUG=1`
is set in the environment).

```python
# Leave temp files on disk for inspection after failures
path = await stream_to_file(source, '/tmp/out.ndjson', debug=True)

# Or set the environment variable once for the whole process
# OJ_PERSISTENCE_DEBUG=1 python my_script.py
```

---

## Custom stores

```python
from oj_persistence.store.base import AbstractStore             # sync
from oj_persistence.store.async_base import AsyncAbstractStore  # async
from oj_persistence.store.abstract_file import AbstractFileStore  # file-backed (adds compression + from_stream)

class MyStore(AbstractStore):
    def create(self, key, value): ...
    def read(self, key): ...
    def update(self, key, value): ...
    def upsert(self, key, value): ...
    def delete(self, key): ...
    def list(self, predicate=None): ...

pm.register('custom', MyStore())
```

If your store requires a full file rewrite for `upsert`, set the class attribute:

```python
class MyFileStore(AbstractStore):
    supports_native_upsert: bool = False
    ...
```

The manager will block `upsert()` calls unless `allow_inefficient=True` is passed.

Inherit from `AbstractFileStore` to get `compression` and `from_stream` /
`from_stream_sync` for free in any file-backed custom store.

---

## Requirements

- Python >= 3.11
- `ijson` — required for `IjsonFileStore`
- `tinydb` — required for `TinyDbStore`
- `sqlalchemy>=2.0` — required for `SqlAlchemyStore` / `AsyncSqlAlchemyStore`
- `redis>=5.0` — required for `RedisStore` / `AsyncRedisStore`

Database drivers (install only what you need):

| Backend | Sync driver | Async driver |
|---|---|---|
| PostgreSQL | `psycopg2-binary` | `asyncpg` |
| MySQL / MariaDB | `pymysql` | `aiomysql` |
| SQL Server | `pyodbc` | — |
| SQLite | stdlib `sqlite3` | `aiosqlite` |

For testing without a live Redis server: `pip install "fakeredis[lua]"`.
