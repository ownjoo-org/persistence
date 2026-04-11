import tempfile
import unittest
from pathlib import Path

from oj_persistence.store.async_sqlalchemy_store import AsyncSqlAlchemyStore
from test.unit.store.async_contract import AsyncStoreContract


class TestAsyncSqlAlchemyStore(AsyncStoreContract, unittest.IsolatedAsyncioTestCase):
    """Full async CRUDL contract using SQLite+aiosqlite as the backing database."""

    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.db'
        self.store = AsyncSqlAlchemyStore(f'sqlite+aiosqlite:///{self.path}')
        await self.store.initialize()

    async def asyncTearDown(self):
        await self.store.dispose()
        self._tmpdir.cleanup()


class TestAsyncSqlAlchemyStoreContextManager(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.db'

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_context_manager_initializes_and_disposes(self):
        async with AsyncSqlAlchemyStore(f'sqlite+aiosqlite:///{self.path}') as store:
            await store.upsert('k', 1)
            self.assertEqual(await store.read('k'), 1)

    async def test_persists_across_instances(self):
        async with AsyncSqlAlchemyStore(f'sqlite+aiosqlite:///{self.path}') as store:
            await store.upsert('k', 'v')
        async with AsyncSqlAlchemyStore(f'sqlite+aiosqlite:///{self.path}') as store:
            self.assertEqual(await store.read('k'), 'v')

    async def test_custom_table_name(self):
        async with AsyncSqlAlchemyStore(f'sqlite+aiosqlite:///{self.path}', table='my_cache') as store:
            await store.upsert('k', 1)
            self.assertEqual(await store.read('k'), 1)

    async def test_invalid_table_name_raises(self):
        with self.assertRaises(ValueError):
            AsyncSqlAlchemyStore(f'sqlite+aiosqlite:///{self.path}', table='bad; DROP TABLE')


if __name__ == '__main__':
    unittest.main()
