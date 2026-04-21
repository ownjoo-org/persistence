"""Manager API tests — register, CRUDL, capabilities, refcount sharing."""

from __future__ import annotations

import pytest

from oj_persistence import (
    Capability,
    InMemory,
    Manager,
    Sqlite,
    TableAlreadyRegistered,
    TableNotRegistered,
    UnsupportedOperation,
)
from oj_persistence import manager as manager_module


# ------------------------------------------------------------------ sync API

def test_register_crud_in_memory() -> None:
    pm = Manager()
    pm.register('users', InMemory())

    pm.create('users', 'u1', {'name': 'Alice'})
    assert pm.read('users', 'u1') == {'name': 'Alice'}
    pm.upsert('users', 'u1', {'name': 'Charlie'})
    assert pm.read('users', 'u1') == {'name': 'Charlie'}
    pm.delete('users', 'u1')
    assert pm.read('users', 'u1') is None

    pm.close()


def test_register_crud_sqlite(tmp_path) -> None:
    pm = Manager()
    pm.register('users', Sqlite(path=str(tmp_path / 'app.db')))

    pm.create('users', 'u1', {'name': 'Alice'})
    assert pm.read('users', 'u1') == {'name': 'Alice'}
    pm.upsert('users', 'u1', {'name': 'Charlie'})
    assert pm.read('users', 'u1') == {'name': 'Charlie'}
    pm.delete('users', 'u1')
    assert pm.read('users', 'u1') is None

    pm.close()


def test_tables_and_exists() -> None:
    pm = Manager()
    assert pm.tables() == []
    assert not pm.exists('users')
    pm.register('users', InMemory())
    assert pm.tables() == ['users']
    assert pm.exists('users')
    pm.close()


def test_list_all_and_predicate() -> None:
    pm = Manager()
    pm.register('users', InMemory())
    pm.create('users', 'u1', {'role': 'admin'})
    pm.create('users', 'u2', {'role': 'user'})
    pm.create('users', 'u3', {'role': 'admin'})
    all_users = pm.list('users')
    assert len(all_users) == 3
    admins = pm.list('users', predicate=lambda v: v['role'] == 'admin')
    assert len(admins) == 2
    pm.close()


def test_iter_generator() -> None:
    pm = Manager()
    pm.register('users', InMemory())
    for i in range(5):
        pm.create('users', f'u{i}', {'i': i})
    values = list(pm.iter('users'))
    assert len(values) == 5
    pm.close()


# ------------------------------------------------------------------ async API

@pytest.mark.asyncio
async def test_register_crud_async_in_memory() -> None:
    pm = Manager()
    await pm.aregister('users', InMemory())
    await pm.acreate('users', 'u1', {'name': 'Alice'})
    assert await pm.aread('users', 'u1') == {'name': 'Alice'}
    await pm.aclose()


@pytest.mark.asyncio
async def test_register_crud_async_sqlite(tmp_path) -> None:
    pm = Manager()
    await pm.aregister('users', Sqlite(path=str(tmp_path / 'app.db')))
    await pm.acreate('users', 'u1', {'name': 'Alice'})
    assert await pm.aread('users', 'u1') == {'name': 'Alice'}
    await pm.aclose()


@pytest.mark.asyncio
async def test_aiter() -> None:
    pm = Manager()
    await pm.aregister('users', InMemory())
    for i in range(5):
        await pm.acreate('users', f'u{i}', {'i': i})
    collected = []
    async for v in pm.aiter('users'):
        collected.append(v)
    assert len(collected) == 5
    await pm.aclose()


# ------------------------------------------------------------------ registration semantics

def test_register_duplicate_raises() -> None:
    pm = Manager()
    pm.register('users', InMemory())
    with pytest.raises(TableAlreadyRegistered):
        pm.register('users', InMemory())
    pm.close()


def test_register_replace_drops_old(tmp_path) -> None:
    pm = Manager()
    pm.register('users', Sqlite(path=str(tmp_path / 'a.db')))
    pm.create('users', 'u1', {'name': 'Alice'})
    pm.register('users', Sqlite(path=str(tmp_path / 'b.db')), replace=True)
    # New file, so the key shouldn't exist
    assert pm.read('users', 'u1') is None
    pm.close()


def test_data_op_on_unregistered_table_raises() -> None:
    pm = Manager()
    with pytest.raises(TableNotRegistered):
        pm.read('ghost', 'k')
    pm.close()


# ------------------------------------------------------------------ capabilities

def test_pagination_works_on_both_backends(tmp_path) -> None:
    pm = Manager()
    pm.register('mem', InMemory())
    pm.register('db', Sqlite(path=str(tmp_path / 'app.db')))
    for i in range(20):
        pm.create('mem', f'k{i:02d}', {'i': i})
        pm.create('db', f'k{i:02d}', {'i': i})
    assert len(pm.list_page('mem', offset=5, limit=5)) == 5
    assert len(pm.list_page('db', offset=5, limit=5)) == 5
    pm.close()


def test_required_capability_rejected_at_register() -> None:
    pm = Manager()
    # InMemory doesn't support NATIVE_JOIN; declaring it required should fail
    with pytest.raises(UnsupportedOperation):
        pm.register(
            'users',
            InMemory(),
            requires=frozenset({Capability.NATIVE_JOIN}),
        )
    pm.close()


def test_capability_runtime_dispatch(tmp_path) -> None:
    """list_by_field works on both backends. Redis/Ndjson would be where
    we'd see the opposite (feature missing) — those aren't ported yet."""
    pm = Manager()
    pm.register('users', InMemory())
    pm.register('db', Sqlite(path=str(tmp_path / 'app.db')))
    pm.create('users', 'u1', {'email': 'a@x.com', 'name': 'Alice'})
    pm.create('users', 'u2', {'email': 'b@x.com', 'name': 'Bob'})
    pm.create('db', 'u1', {'email': 'a@x.com'})
    pm.create('db', 'u2', {'email': 'b@x.com'})
    assert len(pm.list_by_field('users', '$.email', 'a@x.com')) == 1
    assert len(pm.list_by_field('db', '$.email', 'a@x.com')) == 1
    pm.close()


# ------------------------------------------------------------------ refcount sharing

def test_two_managers_share_sqlite_backend(tmp_path) -> None:
    """The core guarantee: two Managers pointing at the same file share
    the exact same Backend instance, so writes and reads coordinate."""
    db_path = str(tmp_path / 'shared.db')
    pm1 = Manager()
    pm2 = Manager()
    pm1.register('users', Sqlite(path=db_path))
    pm2.register('orders', Sqlite(path=db_path))

    # Same underlying backend instance
    b1 = pm1._tables['users']
    b2 = pm2._tables['orders']
    assert b1 is b2

    # Writes through one Manager are visible to reads via the other's (shared) conn
    pm1.create('users', 'u1', {'name': 'Alice'})
    pm2.create('orders', 'o1', {'user': 'u1'})

    # Close pm1 — backend stays open because pm2 still references it
    pm1.close()
    registry = manager_module._BACKENDS.snapshot()
    assert any(refcount > 0 for refcount in registry.values())

    # pm2 still works
    assert pm2.read('orders', 'o1') == {'user': 'u1'}

    pm2.close()


def test_drop_releases_refcount(tmp_path) -> None:
    pm = Manager()
    db_path = str(tmp_path / 'app.db')
    pm.register('a', Sqlite(path=db_path))
    pm.register('b', Sqlite(path=db_path))
    before = manager_module._BACKENDS.snapshot()
    key = ('sqlite', db_path)
    assert before[key] == 2

    pm.drop('a')
    mid = manager_module._BACKENDS.snapshot()
    assert mid[key] == 1

    pm.drop('b')
    after = manager_module._BACKENDS.snapshot()
    assert key not in after

    pm.close()


def test_in_memory_does_not_dedupe() -> None:
    """Two InMemory() specs get independent backends by design."""
    pm = Manager()
    pm.register('a', InMemory())
    pm.register('b', InMemory())
    assert pm._tables['a'] is not pm._tables['b']
    pm.close()


# ------------------------------------------------------------------ capabilities introspection

def test_capabilities_query(tmp_path) -> None:
    pm = Manager()
    pm.register('mem', InMemory())
    pm.register('db', Sqlite(path=str(tmp_path / 'app.db')))

    mem_caps = pm.capabilities('mem')
    db_caps = pm.capabilities('db')

    assert Capability.PAGINATION in mem_caps
    assert Capability.NATIVE_JOIN not in mem_caps

    assert Capability.PAGINATION in db_caps
    assert Capability.NATIVE_JOIN in db_caps

    pm.close()
