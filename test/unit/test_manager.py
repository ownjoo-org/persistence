"""Manager API tests — register, CRUDL, capabilities, refcount sharing."""

from __future__ import annotations

import pathlib
import tempfile
import unittest

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


class TestManagerSync(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.tmp_path = pathlib.Path(self._tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    # ------------------------------------------------------------------ sync API

    def test_register_crud_in_memory(self):
        pm = Manager()
        pm.register('users', InMemory())

        pm.create('users', 'u1', {'name': 'Alice'})
        self.assertEqual(pm.read('users', 'u1'), {'name': 'Alice'})
        pm.upsert('users', 'u1', {'name': 'Charlie'})
        self.assertEqual(pm.read('users', 'u1'), {'name': 'Charlie'})
        pm.delete('users', 'u1')
        self.assertIsNone(pm.read('users', 'u1'))
        pm.close()

    def test_register_crud_sqlite(self):
        pm = Manager()
        pm.register('users', Sqlite(path=str(self.tmp_path / 'app.db')))

        pm.create('users', 'u1', {'name': 'Alice'})
        self.assertEqual(pm.read('users', 'u1'), {'name': 'Alice'})
        pm.upsert('users', 'u1', {'name': 'Charlie'})
        self.assertEqual(pm.read('users', 'u1'), {'name': 'Charlie'})
        pm.delete('users', 'u1')
        self.assertIsNone(pm.read('users', 'u1'))
        pm.close()

    def test_tables_and_exists(self):
        pm = Manager()
        self.assertEqual(pm.tables(), [])
        self.assertFalse(pm.exists('users'))
        pm.register('users', InMemory())
        self.assertEqual(pm.tables(), ['users'])
        self.assertTrue(pm.exists('users'))
        pm.close()

    def test_list_all_and_predicate(self):
        pm = Manager()
        pm.register('users', InMemory())
        pm.create('users', 'u1', {'role': 'admin'})
        pm.create('users', 'u2', {'role': 'user'})
        pm.create('users', 'u3', {'role': 'admin'})
        self.assertEqual(len(pm.list('users')), 3)
        admins = pm.list('users', predicate=lambda v: v['role'] == 'admin')
        self.assertEqual(len(admins), 2)
        pm.close()

    def test_iter_generator(self):
        pm = Manager()
        pm.register('users', InMemory())
        for i in range(5):
            pm.create('users', f'u{i}', {'i': i})
        self.assertEqual(len(list(pm.iter('users'))), 5)
        pm.close()

    # ------------------------------------------------------------------ registration semantics

    def test_register_duplicate_raises(self):
        pm = Manager()
        pm.register('users', InMemory())
        with self.assertRaises(TableAlreadyRegistered):
            pm.register('users', InMemory())
        pm.close()

    def test_register_replace_drops_old(self):
        pm = Manager()
        pm.register('users', Sqlite(path=str(self.tmp_path / 'a.db')))
        pm.create('users', 'u1', {'name': 'Alice'})
        pm.register('users', Sqlite(path=str(self.tmp_path / 'b.db')), replace=True)
        self.assertIsNone(pm.read('users', 'u1'))
        pm.close()

    def test_data_op_on_unregistered_table_raises(self):
        pm = Manager()
        with self.assertRaises(TableNotRegistered):
            pm.read('ghost', 'k')
        pm.close()

    # ------------------------------------------------------------------ capabilities

    def test_pagination_works_on_both_backends(self):
        pm = Manager()
        pm.register('mem', InMemory())
        pm.register('db', Sqlite(path=str(self.tmp_path / 'app.db')))
        for i in range(20):
            pm.create('mem', f'k{i:02d}', {'i': i})
            pm.create('db', f'k{i:02d}', {'i': i})
        self.assertEqual(len(pm.list_page('mem', offset=5, limit=5)), 5)
        self.assertEqual(len(pm.list_page('db', offset=5, limit=5)), 5)
        pm.close()

    def test_required_capability_rejected_at_register(self):
        pm = Manager()
        with self.assertRaises(UnsupportedOperation):
            pm.register(
                'users',
                InMemory(),
                requires=frozenset({Capability.NATIVE_JOIN}),
            )
        pm.close()

    def test_capability_runtime_dispatch(self):
        pm = Manager()
        pm.register('users', InMemory())
        pm.register('db', Sqlite(path=str(self.tmp_path / 'app.db')))
        pm.create('users', 'u1', {'email': 'a@x.com', 'name': 'Alice'})
        pm.create('users', 'u2', {'email': 'b@x.com', 'name': 'Bob'})
        pm.create('db', 'u1', {'email': 'a@x.com'})
        pm.create('db', 'u2', {'email': 'b@x.com'})
        self.assertEqual(len(pm.list_by_field('users', '$.email', 'a@x.com')), 1)
        self.assertEqual(len(pm.list_by_field('db', '$.email', 'a@x.com')), 1)
        pm.close()

    # ------------------------------------------------------------------ refcount sharing

    def test_two_managers_share_sqlite_backend(self):
        db_path = str(self.tmp_path / 'shared.db')
        pm1 = Manager()
        pm2 = Manager()
        pm1.register('users', Sqlite(path=db_path))
        pm2.register('orders', Sqlite(path=db_path))

        b1 = pm1._tables['users']
        b2 = pm2._tables['orders']
        self.assertIs(b1, b2)

        pm1.create('users', 'u1', {'name': 'Alice'})
        pm2.create('orders', 'o1', {'user': 'u1'})

        pm1.close()
        registry = manager_module._BACKENDS.snapshot()
        self.assertTrue(any(rc > 0 for rc in registry.values()))

        self.assertEqual(pm2.read('orders', 'o1'), {'user': 'u1'})
        pm2.close()

    def test_drop_releases_refcount(self):
        pm = Manager()
        db_path = str(self.tmp_path / 'app.db')
        pm.register('a', Sqlite(path=db_path))
        pm.register('b', Sqlite(path=db_path))
        key = ('sqlite', db_path)
        self.assertEqual(manager_module._BACKENDS.snapshot()[key], 2)

        pm.drop('a')
        self.assertEqual(manager_module._BACKENDS.snapshot()[key], 1)

        pm.drop('b')
        self.assertNotIn(key, manager_module._BACKENDS.snapshot())
        pm.close()

    def test_in_memory_does_not_dedupe(self):
        pm = Manager()
        pm.register('a', InMemory())
        pm.register('b', InMemory())
        self.assertIsNot(pm._tables['a'], pm._tables['b'])
        pm.close()

    # ------------------------------------------------------------------ capabilities introspection

    def test_capabilities_query(self):
        pm = Manager()
        pm.register('mem', InMemory())
        pm.register('db', Sqlite(path=str(self.tmp_path / 'app.db')))

        mem_caps = pm.capabilities('mem')
        db_caps = pm.capabilities('db')

        self.assertIn(Capability.PAGINATION, mem_caps)
        self.assertNotIn(Capability.NATIVE_JOIN, mem_caps)
        self.assertIn(Capability.PAGINATION, db_caps)
        self.assertIn(Capability.NATIVE_JOIN, db_caps)
        pm.close()


class TestManagerAsync(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.tmp_path = pathlib.Path(self._tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    async def test_register_crud_async_in_memory(self):
        pm = Manager()
        await pm.aregister('users', InMemory())
        await pm.acreate('users', 'u1', {'name': 'Alice'})
        self.assertEqual(await pm.aread('users', 'u1'), {'name': 'Alice'})
        await pm.aclose()

    async def test_register_crud_async_sqlite(self):
        pm = Manager()
        await pm.aregister('users', Sqlite(path=str(self.tmp_path / 'app.db')))
        await pm.acreate('users', 'u1', {'name': 'Alice'})
        self.assertEqual(await pm.aread('users', 'u1'), {'name': 'Alice'})
        await pm.aclose()

    async def test_aiter(self):
        pm = Manager()
        await pm.aregister('users', InMemory())
        for i in range(5):
            await pm.acreate('users', f'u{i}', {'i': i})
        collected = []
        async for v in pm.aiter('users'):
            collected.append(v)
        self.assertEqual(len(collected), 5)
        await pm.aclose()


if __name__ == '__main__':
    unittest.main()
