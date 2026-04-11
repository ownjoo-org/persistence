import unittest

from oj_persistence.store.async_sqlite import AsyncSqliteStore
from oj_persistence.store.sqlite import SqliteStore
from test.unit.store.async_contract import AsyncStoreContract


class TestAsyncSqliteStore(AsyncStoreContract, unittest.IsolatedAsyncioTestCase):
    """Full async CRUDL contract against an in-memory SQLite database."""

    async def asyncSetUp(self):
        self.store = AsyncSqliteStore(':memory:')

    async def asyncTearDown(self):
        self.store.sync_store.close()

    async def test_sync_store_is_sqlite_store(self):
        self.assertIsInstance(self.store.sync_store, SqliteStore)

    async def test_context_manager_closes_connection(self):
        store = AsyncSqliteStore(':memory:')
        async with store:
            await store.upsert('k', 1)
            self.assertEqual(await store.read('k'), 1)


if __name__ == '__main__':
    unittest.main()
