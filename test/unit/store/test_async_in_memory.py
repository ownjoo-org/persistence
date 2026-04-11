import unittest

from oj_persistence.store.async_in_memory import AsyncInMemoryStore
from test.unit.store.async_contract import AsyncStoreContract


class TestAsyncInMemoryStore(AsyncStoreContract, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = AsyncInMemoryStore()

    async def test_context_manager_returns_self(self):
        async with AsyncInMemoryStore() as store:
            self.assertIsInstance(store, AsyncInMemoryStore)

    async def test_context_manager_is_reentrant(self):
        """Store should work after multiple enter/exit cycles."""
        store = AsyncInMemoryStore()
        async with store:
            await store.upsert('k', 1)
        async with store:
            self.assertEqual(await store.read('k'), 1)


if __name__ == '__main__':
    unittest.main()
