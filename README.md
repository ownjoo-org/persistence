# oj-persistence

Thread-safe, async-first persistence with a **single gatekeeper** (`Manager`) in
front of pluggable backends. Consumers register tables by name + spec; they
never see a store, connection, handle, or file descriptor.

## Installation

```bash
pip install oj-persistence
```

SQLite and InMemory need no extra dependencies. Install drivers only for the
backends you actually use:

```bash
pip install "oj-persistence[dynamodb]"   # boto3 — for the DynamoDB backend
pip install "oj-persistence[dev]"        # pytest + aiosqlite + fakeredis + moto
```

## Quick start

```python
from oj_persistence import Manager, Sqlite, InMemory

pm = Manager()

pm.register('users', Sqlite(path='data/app.db'))
pm.register('orders', Sqlite(path='data/app.db'))   # same path → shared connection
pm.register('cache', InMemory())

pm.create('users', 'u1', {'name': 'Alice', 'role': 'admin'})
pm.read('users', 'u1')                               # {'name': 'Alice', 'role': 'admin'}
pm.update('users', 'u1', {'name': 'Alice', 'role': 'owner'})
pm.upsert('users', 'u2', {'name': 'Bob'})
pm.delete('users', 'u2')

owners = pm.list('users', predicate=lambda v: v['role'] == 'owner')

pm.close()
```

Every sync method has an `a`-prefixed async counterpart with identical
semantics:

```python
await pm.aregister('users', Sqlite(path='data/app.db'))
await pm.acreate('users', 'u1', {'name': 'Alice'})
await pm.aread('users', 'u1')
async for v in pm.aiter('users'):
    ...
await pm.aclose()
```

---

## Manager-as-gatekeeper

The **Manager is the only thing that touches a backing resource**. Consumers
hand it a table name and a spec (`Sqlite(path=...)`, `InMemory()`, ...), then
interact only through the Manager's public API. No store object, no connection,
no handle, no ref.

```
caller → Manager.create/read/update/upsert/delete/list/iter/join
                          ↓
                 table-name registry
                          ↓
         Backend  (SqliteBackend, InMemoryBackend, ...)
```

If you need an operation the Manager doesn't expose, the fix is to add it to
the Manager — not to reach past it.

### File-safety across Manager instances

File-safety is enforced at the **file level**, not the Manager-object level. A
process-scoped `_BackendRegistry` guarantees each unique connection target
(sqlite path, redis url, etc.) is backed by **exactly one** Backend. You can
freely construct multiple `Manager()` instances — for test isolation, per-tenant
namespacing, whatever. If two of them register `Sqlite(path='app.db')`, they
share one underlying `SqliteBackend` with its single writer connection, reader
pool, and WAL configuration. Two Managers cannot race on a file, ever.

Backends are reference-counted across Managers. When the last `drop()` /
`close()` releases the final reference, the backend is closed and its
resources released.

### Register → register, drop, truncate, close

```python
pm.register(table, spec, *, requires=..., replace=False)
pm.drop(table)          # delete data + unregister + release backend if last user
pm.truncate(table)      # clear data but keep registration
pm.close()              # release everything this Manager owns
pm.tables()             # ['users', 'orders', 'cache']
pm.exists('users')      # bool
pm.capabilities('users')  # frozenset[Capability]
```

`register()` raises `TableAlreadyRegistered` if the name is in use; pass
`replace=True` to drop the previous registration first.

---

## CRUDL semantics

| Method   | Behaviour |
|----------|-----------|
| `create` | Raises `KeyError` if the key already exists |
| `read`   | Returns `None` if the key is missing |
| `update` | Raises `KeyError` if the key does not exist |
| `upsert` | Create or overwrite |
| `delete` | No-op if the key is missing |
| `list`   | Returns all values, optionally filtered by `predicate` |
| `iter`   | Generator form of `list`; prefer for large tables |

---

## Capabilities

Every backend declares a `frozenset[Capability]` at the class level. The
Manager checks them at `register()` time — no more "works on SQLite, throws
`NotImplementedError` in-memory" surprises at the eventual call site.

```python
class Capability(StrEnum):
    PAGINATION    = 'pagination'     # list_page(offset, limit)
    FIELD_INDEX   = 'field_index'    # add_index / list_by_field
    NATIVE_JOIN   = 'native_join'    # SQL JOIN pushdown for same-file tables
    NATIVE_UPSERT = 'native_upsert'  # in-place upsert without file rewrite
```

| Backend     | Capabilities |
|-------------|--------------|
| `Sqlite`    | `PAGINATION`, `FIELD_INDEX`, `NATIVE_JOIN`, `NATIVE_UPSERT` |
| `InMemory`  | `PAGINATION`, `FIELD_INDEX`, `NATIVE_UPSERT` |
| `Json`      | `PAGINATION` |
| `Ndjson`    | `PAGINATION` |
| `Csv`       | `PAGINATION` |
| `TinyDb`    | `PAGINATION`, `NATIVE_UPSERT` |
| `Redis`     | `PAGINATION`, `NATIVE_UPSERT` |
| `SqlAlchemy`| `PAGINATION`, `NATIVE_UPSERT` |
| `DynamoDB`  | `PAGINATION`, `NATIVE_UPSERT` |

Declare what you need up-front and the Manager validates before you run:

```python
from oj_persistence import Capability, UnsupportedOperation

# Fails at register() — InMemory has no NATIVE_JOIN
pm.register(
    'users',
    InMemory(),
    requires=frozenset({Capability.NATIVE_JOIN}),
)  # raises UnsupportedOperation
```

Calling a capability-gated method on a backend that doesn't declare it raises
`UnsupportedOperation` at call time as a secondary safety net.

---

## Backends

### `Sqlite(path, pool_size=4)`

Local SQLite file with WAL mode, one writer connection, and a reader pool.
`path=':memory:'` (the default) gives an in-memory SQLite database.

```python
pm.register('users', Sqlite(path='data/app.db'))
pm.register('users', Sqlite(path='data/app.db', pool_size=8))
```

Multiple tables registered against the same `path` share one backend — reads
across tables run concurrently, and joins push down into SQL (see
[Joins](#joins)).

### `InMemory()`

Dict-backed, single-lock. Fastest; not persistent. Each `InMemory()` spec
gets its own backend — two `InMemory()` calls never share state. Useful for
tests and ephemeral caches.

### `Json(path)`

A directory of JSON files — each table maps to ``{path}/{table}.json``. The
file is a plain JSON object ``{"key": value, ...}``, loaded fully into memory
on each operation and rewritten atomically on mutations. Best for small tables
where human-readability matters.

```python
pm.register('config', Json(path='data/'))
pm.upsert('config', 'app', {'debug': True, 'port': 8080})
```

### `Ndjson(path)`

A directory of NDJSON files — each table maps to ``{path}/{table}.ndjson``.
Each record is a JSON line. Mutations rewrite the file under a per-table lock.
Good for append-friendly workloads and log-style data.

```python
pm.register('events', Ndjson(path='data/'))
pm.create('events', 'ev1', {'type': 'click', 'user': 'u1'})
```

### `Csv(path)`

A directory of CSV files — each table maps to ``{path}/{table}.csv``. The
first column is always ``key``; remaining columns are the fields of the value
dict. Column names are inferred from the first write. Values are always
``dict[str, str]`` on read (CSV stores strings).

```python
pm.register('users', Csv(path='data/'))
pm.upsert('users', 'u1', {'name': 'Alice', 'role': 'admin'})
row = pm.read('users', 'u1')  # {'name': 'Alice', 'role': 'admin'}
```

### `DynamoDB(region, prefix='', endpoint_url=None)`

AWS DynamoDB with PAY_PER_REQUEST billing — no capacity planning required.
Each logical table maps to one DynamoDB table named `{prefix}{table}`.

```python
from oj_persistence import Manager, DynamoDB

pm = Manager()
pm.register('sessions', DynamoDB(region='us-east-1', prefix='myapp_'))
pm.upsert('sessions', 'sid:abc', {'user_id': 'u1', '_ttl': 1735689600})
pm.read('sessions', 'sid:abc')
pm.close()
```

**TTL**: if the value is a dict containing a `_ttl` key (Unix epoch as
`float`/`int`), the backend stores it as DynamoDB's native TTL attribute.
DynamoDB will automatically expire and delete the item after that timestamp.

**Local testing**: pass `endpoint_url='http://localhost:8000'` to target
DynamoDB Local or a `moto` mock server instead of AWS.

**Install**: `pip install "oj-persistence[dynamodb]"` (adds `boto3`).

### `Redis(url, db=0, prefix='')`

String keys with optional per-key TTL. Key scheme: ``{prefix}{table}:{key}``.
Requires ``pip install "oj-persistence[redis]"``.

```python
pm.register('sessions', Redis(url='redis://localhost:6379', prefix='myapp_'))
pm.upsert('sessions', 'sid:abc', {'user': 'u1', '_ttl': 1735689600})
```

### `SqlAlchemy(url)`

Any SQLAlchemy-supported SQL database (PostgreSQL, MySQL, Oracle, etc.).
Each table maps to a SQL table with ``pk TEXT PRIMARY KEY, value TEXT``.
Requires ``pip install "oj-persistence[sqlalchemy]"`` plus the appropriate
database driver.

```python
pm.register('users', SqlAlchemy(url='postgresql://user:pass@host/db'))
```

### `TinyDb(path)`

Pure-Python JSON database. Each backend instance owns one TinyDB file; logical
tables are TinyDB tables within it. Good for small embedded use cases with no
external dependencies. Requires ``pip install "oj-persistence[tinydb]"``.

```python
pm.register('cache', TinyDb(path='data/cache.db'))
```

---

## Pagination, field indexes, `list_by_field`

```python
pm.register('users', Sqlite(path='app.db'))

for i in range(100):
    pm.create('users', f'u{i:03d}', {'i': i, 'role': 'admin' if i % 10 == 0 else 'viewer'})

# Pagination (offset/limit) — ordered by key
page = pm.list_page('users', offset=20, limit=10)

# Create a JSON-field index, then filter via pushed-down SQL
pm.add_index('users', '$.role')
admins = pm.list_by_field('users', '$.role', 'admin')
```

`InMemory` supports `list_page` and `list_by_field` too — the index call is a
no-op there (dict scans are O(n) regardless).

---

## Joins

`pm.join(left, right, on, how='inner', where=None)` pairs values from two
tables. When both tables share the same backend instance **and** that backend
declares `Capability.NATIVE_JOIN`, the join pushes down into the backend (SQL
`INNER JOIN` / `LEFT JOIN` for SQLite). Otherwise the Manager materializes
both sides and performs a nested-loop join in Python.

```python
pm.register('users',  Sqlite(path='app.db'))
pm.register('orders', Sqlite(path='app.db'))   # same file → shared backend

pm.create('users',  'u1', {'id': 'u1', 'name': 'Alice'})
pm.create('orders', 'o1', {'user_id': 'u1', 'total': 150})

pairs = pm.join(
    'users', 'orders',
    on=lambda u, o: u['id'] == o['user_id'],
    how='left',
    where=lambda u, o: o['total'] > 100,
)
# -> [({'id': 'u1', 'name': 'Alice'}, {'user_id': 'u1', 'total': 150})]
```

Supported `how` values: `inner`, `left`, `right`, `outer`. `RIGHT` and `OUTER`
joins always run the Python path (SQLite has no native syntax for them). The
`where` filter is always applied in Python after the join.

> For large tables, keep the tables on the same SQLite file so `NATIVE_JOIN`
> kicks in. Python-path joins are O(m × n).

---

## Concurrency guarantees

### SQLite files

- **WAL mode** (`PRAGMA journal_mode=WAL`) set at first open.
- **One writer** connection per file, serialized by an internal lock.
- **Pool of reader** connections (`pool_size`, default 4) — concurrent readers
  run truly in parallel.
- All I/O runs on a thread-pool executor, so async methods never block the
  event loop.

Resulting guarantees:

1. Concurrent `aread` / `alist` / `aiter` calls on the same or different
   tables in the same file **do not block each other**.
2. An in-flight write does **not block any read**. Readers see the last
   committed snapshot (WAL semantics).
3. Concurrent writes serialize (one at a time). Writers never wait for readers.
4. `truncate` / `drop` acquire the writer lock, so they cannot interleave
   with an in-flight write.

### InMemory

One threading lock; reads and writes serialize. Cheap enough that contention
isn't worth splitting.

### What the Manager does *not* guarantee

- **Cross-file transactions.** A write to `users` (Sqlite) and a write to
  `cache` (InMemory) are not atomic together.
- **Snapshot isolation across multiple async reads.** Two `await pm.aread(...)`
  calls can see different snapshots if a writer lands between them. Callers
  needing a stable view should explicitly snapshot via `await pm.alist(...)`.

---

## Sync / async duality

The Manager exposes both flavors of every data method. Pick whichever matches
your calling context:

| Sync        | Async        |
|-------------|--------------|
| `register`  | `aregister`  |
| `create`    | `acreate`    |
| `read`      | `aread`      |
| `update`    | `aupdate`    |
| `upsert`    | `aupsert`    |
| `delete`    | `adelete`    |
| `list`      | `alist`      |
| `iter`      | `aiter`      |
| `list_page` | `alist_page` |
| `list_by_field` | `alist_by_field` |
| `add_index` | `aadd_index` |
| `join`      | `ajoin`      |
| `truncate`  | `atruncate`  |
| `drop`      | `adrop`      |
| `close`     | `aclose`     |

The sync entry points run the async implementation on a Manager-owned
background event loop — so they must **not** be called from inside a running
event loop. Use the `a`-prefixed method there instead (the sync method raises
`RuntimeError` to keep you honest).

---

## Migration from v1

The v1 surface (`PersistenceManager`, `AsyncPersistenceManager`, `*Store`
classes, `configure()` / `create_group()` / `add_table()`, `StoreRef` /
`GroupRef`, `store_context()`, upsert-gating, `Relation` / `JoinCondition`) is
gone as of v0.1.0. Rough mapping:

| v1 | v2 |
|----|----|
| `PersistenceManager()` / `AsyncPersistenceManager()` | `Manager()` (unified sync + async) |
| `pm.configure('sqlite', store_id='users', path='app.db')` | `pm.register('users', Sqlite(path='app.db'))` |
| `pm.register('users', SqliteStore('app.db'))` | `pm.register('users', Sqlite(path='app.db'))` |
| `pm.create_group('sqlite', ...)` + `pm.add_table(...)` | Two `pm.register(..., Sqlite(path=same))` calls share the backend |
| `pm.read/write/list/...` | Same names; `a`-prefixed variants for async |
| `pm.unregister(store_id)` | `pm.drop(table)` |
| `Relation(..., JoinCondition(...))` | `pm.join(left, right, on=callable, how=...)` |
| `store_context()` context manager | Removed — writes just work on the registered table |
| `allow_inefficient=True` upsert override | Removed (only native-upsert backends ship in v2) |

Legacy v1 store classes (`NdjsonFileStore`, `CsvFileStore`, `VersionedStore`,
etc.) still live under `oj_persistence.store` for direct programmatic use, but
they are **not** wired into the v2 Manager. Prefer `Sqlite` / `InMemory`
through the Manager; use the legacy stores only if you have an existing script
that constructs them directly.

---

## Utilities

### `oj_persistence.utils.streaming` — stream bytes to a file atomically

```python
import httpx
from oj_persistence.utils.streaming import stream_to_file, stream_to_file_sync

async with httpx.AsyncClient() as client:
    response = await client.get('https://example.com/data.ndjson.gz', stream=True)
    path = await stream_to_file(response.aiter_bytes(), '/tmp/data.ndjson.gz')

# Or sync, from an iterable or a .read()-capable object
import requests
r = requests.get('https://example.com/data.ndjson', stream=True)
path = stream_to_file_sync(r.iter_content(chunk_size=65536), '/tmp/data.ndjson')
```

Writes go to a UUID-named temp file in the same directory and are renamed
atomically on success. On failure the temp file is deleted unless `debug=True`
(or the `OJ_PERSISTENCE_DEBUG=1` env var is set).

### `oj_persistence.utils.compression` — codec resolution for file-backed stores

Legacy file stores under `oj_persistence.store` accept a `compression`
parameter (`'gzip'`, `'bz2'`, `'lzma'`, `'auto'`). Auto-detection keys off the
file extension (`.gz`, `.bz2`, `.xz`, `.lzma`).

---

## Requirements

- Python >= 3.11
- `sqlalchemy>=2.0`, `redis>=5.0`, `tinydb`, `ijson` — installed by default;
  needed only by the legacy store classes or by future backends.
- `boto3>=1.26` — optional; required only for the `DynamoDB` backend
  (`pip install "oj-persistence[dynamodb]"`).

For testing without a live Redis server: `pip install "fakeredis[lua]"`.
For testing the DynamoDB backend without AWS: `pip install moto[dynamodb]`.

## Version / stability

Current: **v0.3.0**. All backends fully implemented: `Sqlite`, `InMemory`,
`Json`, `Ndjson`, `Csv`, `Redis`, `SqlAlchemy`, `TinyDb`, `DynamoDB`. The
legacy `store/` layer has been removed. The Manager surface is stable.
