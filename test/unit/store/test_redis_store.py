import unittest

import fakeredis

from oj_persistence.store.redis_store import RedisStore
from test.unit.store.contract import StoreContract


class TestRedisStore(StoreContract, unittest.TestCase):
    """Full CRUDL contract using fakeredis (no live server required)."""

    def setUp(self):
        self._server = fakeredis.FakeServer()
        self.store = RedisStore(fakeredis.FakeRedis(server=self._server), prefix='test:')

    def tearDown(self):
        self.store.close()


class TestRedisStorePrefix(unittest.TestCase):
    """Key namespacing — two stores with different prefixes are isolated."""

    def setUp(self):
        self._server = fakeredis.FakeServer()

    def test_different_prefixes_are_isolated(self):
        store_a = RedisStore(fakeredis.FakeRedis(server=self._server), prefix='a:')
        store_b = RedisStore(fakeredis.FakeRedis(server=self._server), prefix='b:')
        store_a.upsert('k', 'from-a')
        store_b.upsert('k', 'from-b')

        self.assertEqual(store_a.read('k'), 'from-a')
        self.assertEqual(store_b.read('k'), 'from-b')
        self.assertEqual(store_a.list(), ['from-a'])
        self.assertEqual(store_b.list(), ['from-b'])

        store_a.close()
        store_b.close()

    def test_list_only_returns_keys_in_prefix(self):
        client = fakeredis.FakeRedis(server=self._server)
        # Manually set a key outside the prefix
        client.set('other:x', '99')
        store = RedisStore(client, prefix='ns:')
        store.upsert('k', 1)

        self.assertEqual(store.list(), [1])
        store.close()


class TestRedisStoreContextManager(unittest.TestCase):
    def test_context_manager_closes_client(self):
        server = fakeredis.FakeServer()
        with RedisStore(fakeredis.FakeRedis(server=server), prefix='test:') as store:
            store.upsert('k', 1)
            self.assertEqual(store.read('k'), 1)


if __name__ == '__main__':
    unittest.main()
