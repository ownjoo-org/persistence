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
from oj_persistence import PersistenceManager, SqliteStore

pm = PersistenceManager()

# Register stores by name — all data ops go through the manager
pm.get_or_create('users', lambda: SqliteStore('users.db'))

pm.create('users', 'u1', {'name': 'Alice', 'role': 'admin'})
pm.upsert('users', 'u2', {'name': 'Bob',   'role': 'viewer'})

pm.read('users', 'u1')                                    # {'name': 'Alice', 'role': 'admin'}
pm.update('users', 'u1', {'name': 'Alice', 'role': 'owner'})
pm.delete('users', 'u2')

admins = pm.list('users', predicate=lambda v: v['role'] == 'owner')
```

## CRUDL semantics

| Method   | Behaviour                               |
|----------|-----------------------------------------|
| `create` | Raises `KeyError` if key already exists |
| `read`   | Returns `None` if key is missing        |
| `update` | Raises `KeyError` if key does not exist |
| `upsert` | Creates or overwrites — never raises    |
| `delete` | No-op if key is missing                 |
| `list`   | Returns all values, optionally filtered |

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

## Usage examples

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

### AsyncPersistenceManager — async registry

`AsyncPersistenceManager` is the async counterpart to `PersistenceManager`.
Registry methods (`register`, `get_store`, `get_or_create`, `unregister`) are
synchronous; all data operations are `async`.

```python
from oj_persistence import AsyncPersistenceManager, AsyncSqliteStore

pm = AsyncPersistenceManager()
pm.get_or_create('users', lambda: AsyncSqliteStore('users.db'))

await pm.upsert('users', 'u1', {'name': 'Alice'})
await pm.read('users', 'u1')

# SQL pushdown via list_by_field
await pm.list_by_field('users', '$.role', 'admin')

# Relational join across two async stores
ON = lambda u, o: u['id'] == o['user_id']
pairs = await pm.join('users', 'orders', on=ON, how='left')
```

#### configure() — set up stores from config

`configure()` creates, initializes, and registers a store in one async call.
Pass the store type and any type-specific keyword arguments:

```python
pm = AsyncPersistenceManager()

# SQLite — one database file, multiple named tables
await pm.configure('users',    type='sqlite', path='data/app.db', table='users')
await pm.configure('orders',   type='sqlite', path='data/app.db', table='orders')

# In-memory (tests, ephemeral caches)
await pm.configure('cache', type='in_memory')

# All data ops work immediately after configure()
await pm.upsert('users', 'u1', {'name': 'Alice'})
```

Supported types and their kwargs:

| type | kwargs | notes |
|---|---|---|
| `sqlite` | `path` (default `':memory:'`), `table` (default `'store'`) | requires `aiosqlite` |
| `in_memory` | — | no persistence; suitable for tests |

`configure()` is idempotent per name — if the store already exists it is
replaced. The directory for `path` is created automatically if it does not
exist.

### Async pipeline (io_chains PersistenceLink)

```python
import redis.asyncio as aioredis
from oj_persistence import AsyncRedisStore, AsyncSqlAlchemyStore

# Redis — zero-latency writes in a hot pipeline
client = aioredis.Redis(host='redis.internal')
store = AsyncRedisStore(client, prefix='feed:')

async with store:
    await store.upsert('item:1', {'title': 'Breaking news'})

# PostgreSQL — durable writes via asyncpg
async with AsyncSqlAlchemyStore('postgresql+asyncpg://user:pass@host/db') as store:
    await store.upsert('item:1', {'title': 'Breaking news'})
```

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

## Relational joins

```python
ON = lambda user, order: user['id'] == order['user_id']

results = pm.join('users', 'orders', on=ON, how='left',
                  where=lambda u, o: o['total'] > 100)
# returns list of (left_val, right_val) tuples; unmatched sides are None
```

Supported `how` values: `inner` (default), `left`, `right`, `outer`.

> **Note:** joins call `list()` on both stores and perform an O(m × n) nested
> loop in Python. For large datasets, prefer a query pushed down to the
> database via `SqlAlchemyStore` or `SqliteStore`.

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

Inherit from `AbstractFileStore` to get `compression` and `from_stream` /
`from_stream_sync` for free in any file-backed custom store.

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
