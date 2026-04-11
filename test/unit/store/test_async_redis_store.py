import unittest

import fakeredis

from oj_persistence.store.async_redis_store import AsyncRedisStore
from test.unit.store.async_contract import AsyncStoreContract


class TestAsyncRedisStore(AsyncStoreContract, unittest.IsolatedAsyncioTestCase):
    """Full async CRUDL contract using fakeredis (no live server required)."""

    async def asyncSetUp(self):
        self._server = fakeredis.FakeServer()
        self.store = AsyncRedisStore(fakeredis.FakeAsyncRedis(server=self._server), prefix='test:')

    async def asyncTearDown(self):
        await self.store.close()


class TestAsyncRedisStorePrefix(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._server = fakeredis.FakeServer()

    async def test_different_prefixes_are_isolated(self):
        store_a = AsyncRedisStore(fakeredis.FakeAsyncRedis(server=self._server), prefix='a:')
        store_b = AsyncRedisStore(fakeredis.FakeAsyncRedis(server=self._server), prefix='b:')
        await store_a.upsert('k', 'from-a')
        await store_b.upsert('k', 'from-b')

        self.assertEqual(await store_a.read('k'), 'from-a')
        self.assertEqual(await store_b.read('k'), 'from-b')
        self.assertEqual(await store_a.list(), ['from-a'])
        self.assertEqual(await store_b.list(), ['from-b'])

        await store_a.close()
        await store_b.close()

    async def test_list_only_returns_keys_in_prefix(self):
        client = fakeredis.FakeAsyncRedis(server=self._server)
        await client.set('other:x', '99')
        store = AsyncRedisStore(client, prefix='ns:')
        await store.upsert('k', 1)

        self.assertEqual(await store.list(), [1])
        await store.close()


class TestAsyncRedisStoreContextManager(unittest.IsolatedAsyncioTestCase):
    async def test_context_manager_closes_client(self):
        server = fakeredis.FakeServer()
        async with AsyncRedisStore(fakeredis.FakeAsyncRedis(server=server), prefix='test:') as store:
            await store.upsert('k', 1)
            self.assertEqual(await store.read('k'), 1)


if __name__ == '__main__':
    unittest.main()
