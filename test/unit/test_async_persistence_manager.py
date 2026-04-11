import threading
import unittest
from unittest.mock import AsyncMock

from oj_persistence.async_manager import AsyncPersistenceManager
from oj_persistence.store.async_base import AsyncAbstractStore
from oj_persistence.store.async_in_memory import AsyncInMemoryStore


def make_store() -> AsyncAbstractStore:
    store = AsyncMock(spec=AsyncAbstractStore)
    store.list.return_value = []
    return store


class TestAsyncPersistenceManagerSingleton(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    def test_same_instance_returned(self):
        a = AsyncPersistenceManager()
        b = AsyncPersistenceManager()
        self.assertIs(a, b)

    def test_concurrent_construction_yields_single_instance(self):
        instances = []
        lock = threading.Lock()

        def create():
            pm = AsyncPersistenceManager()
            with lock:
                instances.append(pm)

        threads = [threading.Thread(target=create) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(set(id(i) for i in instances)), 1)


class TestAsyncManagerCRUDL(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()
        self.store = make_store()
        self.pm.register('s', self.store)

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_create_delegates_to_store(self):
        await self.pm.create('s', 'k', 'v')
        self.store.create.assert_awaited_once_with('k', 'v')

    async def test_read_delegates_to_store(self):
        await self.pm.read('s', 'k')
        self.store.read.assert_awaited_once_with('k')

    async def test_update_delegates_to_store(self):
        await self.pm.update('s', 'k', 'v')
        self.store.update.assert_awaited_once_with('k', 'v')

    async def test_upsert_delegates_to_store(self):
        await self.pm.upsert('s', 'k', 'v')
        self.store.upsert.assert_awaited_once_with('k', 'v')

    async def test_delete_delegates_to_store(self):
        await self.pm.delete('s', 'k')
        self.store.delete.assert_awaited_once_with('k')

    async def test_list_delegates_to_store(self):
        await self.pm.list('s')
        self.store.list.assert_awaited_once_with(None)

    async def test_list_with_predicate_delegates_to_store(self):
        pred = lambda v: True
        await self.pm.list('s', pred)
        self.store.list.assert_awaited_once_with(pred)

    async def test_read_returns_store_value(self):
        self.store.read.return_value = 42
        self.assertEqual(await self.pm.read('s', 'k'), 42)

    async def test_list_returns_store_values(self):
        self.store.list.return_value = [1, 2, 3]
        self.assertEqual(await self.pm.list('s'), [1, 2, 3])

    async def test_crudl_on_unregistered_store_raises(self):
        for coro in [
            self.pm.create('ghost', 'k', 'v'),
            self.pm.read('ghost', 'k'),
            self.pm.update('ghost', 'k', 'v'),
            self.pm.upsert('ghost', 'k', 'v'),
            self.pm.delete('ghost', 'k'),
            self.pm.list('ghost'),
        ]:
            with self.assertRaises(KeyError):
                await coro


class TestAsyncPersistenceManagerRegistry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    def test_register_and_get_store(self):
        store = make_store()
        self.pm.register('users', store)
        self.assertIs(self.pm.get_store('users'), store)

    def test_get_store_missing_returns_none(self):
        self.assertIsNone(self.pm.get_store('nonexistent'))

    def test_register_replaces_existing(self):
        first, second = make_store(), make_store()
        self.pm.register('s', first)
        self.pm.register('s', second)
        self.assertIs(self.pm.get_store('s'), second)

    def test_unregister_removes_store(self):
        self.pm.register('s', make_store())
        self.pm.unregister('s')
        self.assertIsNone(self.pm.get_store('s'))

    def test_unregister_missing_is_noop(self):
        self.pm.unregister('nonexistent')

    def test_get_or_create_calls_factory_once(self):
        store = make_store()
        factory = lambda: store
        result1 = self.pm.get_or_create('s', factory)
        result2 = self.pm.get_or_create('s', factory)
        self.assertIs(result1, store)
        self.assertIs(result1, result2)

    def test_multiple_stores_independent(self):
        a, b = make_store(), make_store()
        self.pm.register('a', a)
        self.pm.register('b', b)
        self.assertIs(self.pm.get_store('a'), a)
        self.assertIs(self.pm.get_store('b'), b)


class TestAsyncPersistenceManagerJoin(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def _seed(self):
        users = AsyncInMemoryStore()
        orders = AsyncInMemoryStore()
        self.pm.register('users', users)
        self.pm.register('orders', orders)
        await users.upsert('u1', {'id': 'u1', 'name': 'Alice'})
        await users.upsert('u2', {'id': 'u2', 'name': 'Bob'})
        await orders.upsert('o1', {'id': 'o1', 'user_id': 'u1', 'total': 100})
        await orders.upsert('o2', {'id': 'o2', 'user_id': 'u1', 'total': 200})

    async def test_inner_join(self):
        await self._seed()
        ON = lambda u, o: u['id'] == o['user_id']
        result = await self.pm.join('users', 'orders', on=ON)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(pair[0]['name'] == 'Alice' for pair in result))

    async def test_left_join_includes_unmatched(self):
        await self._seed()
        ON = lambda u, o: u['id'] == o['user_id']
        result = await self.pm.join('users', 'orders', on=ON, how='left')
        # Bob has no orders — appears as (Bob, None)
        unmatched = [pair for pair in result if pair[1] is None]
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0][0]['name'], 'Bob')

    async def test_invalid_how_raises(self):
        self.pm.register('a', AsyncInMemoryStore())
        self.pm.register('b', AsyncInMemoryStore())
        with self.assertRaises(ValueError):
            await self.pm.join('a', 'b', on=lambda x, y: True, how='diagonal')

    async def test_join_unregistered_store_raises(self):
        self.pm.register('a', AsyncInMemoryStore())
        with self.assertRaises(KeyError):
            await self.pm.join('a', 'ghost', on=lambda x, y: True)


class TestAsyncPersistenceManagerListByField(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_list_by_field_delegates_when_supported(self):
        from oj_persistence.store.async_sqlite import AsyncSqliteStore
        store = AsyncSqliteStore(':memory:')
        await store.upsert('1', {'role': 'admin',  'name': 'Alice'})
        await store.upsert('2', {'role': 'viewer', 'name': 'Bob'})
        self.pm.register('users', store)

        result = await self.pm.list_by_field('users', '$.role', 'admin')
        self.assertEqual(result, [{'role': 'admin', 'name': 'Alice'}])
        store.sync_store.close()

    async def test_list_by_field_raises_when_unsupported(self):
        self.pm.register('mem', AsyncInMemoryStore())
        with self.assertRaises(NotImplementedError):
            await self.pm.list_by_field('mem', '$.role', 'admin')


if __name__ == '__main__':
    unittest.main()
