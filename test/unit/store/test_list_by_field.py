"""
Tests for list_by_field() on SqliteStore, AsyncSqliteStore,
SqlAlchemyStore, and AsyncSqlAlchemyStore.

list_by_field(json_path, value) pushes the filter into SQL so the index
created by add_json_index() is actually exercised.
"""
import tempfile
import unittest
from pathlib import Path

from oj_persistence.store.sqlite import SqliteStore
from oj_persistence.store.sqlalchemy_store import SqlAlchemyStore


# ------------------------------------------------------------------ SqliteStore

class TestSqliteListByField(unittest.TestCase):
    def setUp(self):
        self.store = SqliteStore(':memory:')
        self.store.upsert('1', {'name': 'Alice', 'role': 'admin',  'age': 30})
        self.store.upsert('2', {'name': 'Bob',   'role': 'viewer', 'age': 25})
        self.store.upsert('3', {'name': 'Carol', 'role': 'admin',  'age': 35})

    def tearDown(self):
        self.store.close()

    def test_returns_matching_records(self):
        result = self.store.list_by_field('$.role', 'admin')
        names = sorted(r['name'] for r in result)
        self.assertEqual(names, ['Alice', 'Carol'])

    def test_returns_empty_when_no_match(self):
        self.assertEqual(self.store.list_by_field('$.role', 'superuser'), [])

    def test_integer_field_match(self):
        result = self.store.list_by_field('$.age', 25)
        self.assertEqual(result, [{'name': 'Bob', 'role': 'viewer', 'age': 25}])

    def test_works_after_add_json_index(self):
        self.store.add_json_index('$.role')
        result = self.store.list_by_field('$.role', 'admin')
        self.assertEqual(len(result), 2)

    def test_nested_field(self):
        self.store.upsert('4', {'address': {'city': 'NYC'}})
        self.store.upsert('5', {'address': {'city': 'LA'}})
        result = self.store.list_by_field('$.address.city', 'NYC')
        self.assertEqual(result, [{'address': {'city': 'NYC'}}])

    def test_unsafe_path_raises(self):
        with self.assertRaises(ValueError):
            self.store.list_by_field("$.role'; DROP TABLE store; --", 'x')


# ------------------------------------------------------------------ AsyncSqliteStore

class TestAsyncSqliteListByField(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from oj_persistence.store.async_sqlite import AsyncSqliteStore
        self.store = AsyncSqliteStore(':memory:')
        await self.store.upsert('1', {'name': 'Alice', 'role': 'admin'})
        await self.store.upsert('2', {'name': 'Bob',   'role': 'viewer'})

    async def asyncTearDown(self):
        self.store.sync_store.close()

    async def test_returns_matching_records(self):
        result = await self.store.list_by_field('$.role', 'admin')
        self.assertEqual(result, [{'name': 'Alice', 'role': 'admin'}])

    async def test_returns_empty_when_no_match(self):
        self.assertEqual(await self.store.list_by_field('$.role', 'owner'), [])


# ------------------------------------------------------------------ SqlAlchemyStore

class TestSqlAlchemyListByField(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.db'
        self.store = SqlAlchemyStore(f'sqlite:///{self.path}')
        self.store.upsert('1', {'name': 'Alice', 'role': 'admin',  'age': 30})
        self.store.upsert('2', {'name': 'Bob',   'role': 'viewer', 'age': 25})
        self.store.upsert('3', {'name': 'Carol', 'role': 'admin',  'age': 35})

    def tearDown(self):
        self.store.dispose()
        self._tmpdir.cleanup()

    def test_returns_matching_records(self):
        result = self.store.list_by_field('$.role', 'admin')
        names = sorted(r['name'] for r in result)
        self.assertEqual(names, ['Alice', 'Carol'])

    def test_returns_empty_when_no_match(self):
        self.assertEqual(self.store.list_by_field('$.role', 'superuser'), [])

    def test_integer_field_match(self):
        result = self.store.list_by_field('$.age', 25)
        self.assertEqual(result, [{'name': 'Bob', 'role': 'viewer', 'age': 25}])

    def test_add_json_index_then_list_by_field(self):
        self.store.add_json_index('$.role')
        result = self.store.list_by_field('$.role', 'admin')
        self.assertEqual(len(result), 2)

    def test_unsafe_path_raises(self):
        with self.assertRaises(ValueError):
            self.store.list_by_field("$.x'; DROP TABLE oj_store; --", 'y')


# ------------------------------------------------------------------ AsyncSqlAlchemyStore

class TestAsyncSqlAlchemyListByField(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from oj_persistence.store.async_sqlalchemy_store import AsyncSqlAlchemyStore
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.db'
        self.store = AsyncSqlAlchemyStore(f'sqlite+aiosqlite:///{self.path}')
        await self.store.initialize()
        await self.store.upsert('1', {'name': 'Alice', 'role': 'admin'})
        await self.store.upsert('2', {'name': 'Bob',   'role': 'viewer'})
        await self.store.upsert('3', {'name': 'Carol', 'role': 'admin'})

    async def asyncTearDown(self):
        await self.store.dispose()
        self._tmpdir.cleanup()

    async def test_returns_matching_records(self):
        result = await self.store.list_by_field('$.role', 'admin')
        self.assertEqual(sorted(r['name'] for r in result), ['Alice', 'Carol'])

    async def test_returns_empty_when_no_match(self):
        self.assertEqual(await self.store.list_by_field('$.role', 'owner'), [])

    async def test_add_json_index_then_list_by_field(self):
        await self.store.add_json_index('$.role')
        result = await self.store.list_by_field('$.role', 'admin')
        self.assertEqual(len(result), 2)


if __name__ == '__main__':
    unittest.main()
