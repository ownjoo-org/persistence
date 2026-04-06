# oj-persistence

Thread-safe persistence management with pluggable storage backends.

## Installation

```bash
pip install oj-persistence
```

## Overview

`oj-persistence` provides a singleton `PersistenceManager` that acts as the single I/O interface for all data operations. Callers register named stores with the manager and interact exclusively through it — stores are never touched directly for data operations.

Designed for use with multi-threaded applications and async pipelines (e.g. [`io_chains`](https://github.com/ownjoo-org/io_chains)).

## Quick start

```python
from oj_persistence import PersistenceManager, InMemoryStore, NdjsonFileStore

pm = PersistenceManager()

# Register stores by name
pm.get_or_create('cache', lambda: InMemoryStore())
pm.get_or_create('events', lambda: NdjsonFileStore('events.ndjson'))

# All data ops go through the manager
pm.create('cache', 'user:1', {'name': 'Alice'})
pm.upsert('events', 'evt:1', {'type': 'login', 'user': 'user:1'})

value = pm.read('cache', 'user:1')   # {'name': 'Alice'}
pm.update('cache', 'user:1', {'name': 'Alice', 'role': 'admin'})
pm.delete('cache', 'user:1')

results = pm.list('cache', predicate=lambda v: v.get('role') == 'admin')
```

## CRUDL semantics

| Method     | Behaviour                                      |
|------------|------------------------------------------------|
| `create`   | Raises `KeyError` if key already exists        |
| `read`     | Returns `None` if key is missing               |
| `update`   | Raises `KeyError` if key does not exist        |
| `upsert`   | Creates or overwrites — never raises           |
| `delete`   | No-op if key is missing                        |
| `list`     | Returns all values, optionally filtered        |

## Relational joins

```python
ON = lambda user, order: user['id'] == order['user_id']

results = pm.join('users', 'orders', on=ON, how='left',
                  where=lambda u, o: o['total'] > 100)
# returns list of (left, right) tuples; unmatched rows have None on the missing side
```

Supported `how` values: `inner` (default), `left`, `right`, `outer`.

## Storage backends

| Class           | Format                    | Notes                              |
|-----------------|---------------------------|------------------------------------|
| `InMemoryStore` | In-process dict           | Fastest; not persistent            |
| `FlatFileStore` | JSON (full load/save)     | Simple; loads entire file per op   |
| `NdjsonFileStore` | NDJSON (one obj/line)   | Append-only creates; O(1) writes   |
| `IjsonFileStore` | Standard JSON (streaming) | Streaming reads via `ijson`        |
| `CsvFileStore`  | CSV                       | Fieldnames inferred or pre-specified |

All file-backed stores are thread-safe via a writer-preferring `ReadWriteLock`.

## Custom stores

Implement `AbstractStore`:

```python
from oj_persistence.store.base import AbstractStore

class MyStore(AbstractStore):
    def create(self, key, value): ...
    def read(self, key): ...
    def update(self, key, value): ...
    def upsert(self, key, value): ...
    def delete(self, key): ...
    def list(self, predicate=None): ...

pm.register('custom', MyStore())
```

## Requirements

- Python >= 3.11
- `ijson` (for `IjsonFileStore`)
