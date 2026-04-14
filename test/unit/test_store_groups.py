"""
Tests for GroupRef / StoreRef dataclasses and the group-oriented manager API:
  create_group(), add_table(), configure() → StoreRef, catalog(), group_catalog()

Both PersistenceManager (sync) and AsyncPersistenceManager (async) are covered.
"""
import unittest
import uuid

from oj_persistence.refs import GroupRef, StoreRef


# ---------------------------------------------------------------------------
# Dataclass structure
# ---------------------------------------------------------------------------

class TestGroupRef(unittest.TestCase):
    def test_fields(self):
        ref = GroupRef(group_id='g1', store_type='in_memory')
        self.assertEqual(ref.group_id, 'g1')
        self.assertEqual(ref.store_type, 'in_memory')

    def test_frozen(self):
        ref = GroupRef(group_id='g1', store_type='in_memory')
        with self.assertRaises((AttributeError, TypeError)):
            ref.group_id = 'changed'  # type: ignore[misc]

    def test_equality(self):
        a = GroupRef(group_id='g1', store_type='in_memory')
        b = GroupRef(group_id='g1', store_type='in_memory')
        self.assertEqual(a, b)

    def test_hashable(self):
        ref = GroupRef(group_id='g1', store_type='in_memory')
        {ref}  # must not raise


class TestStoreRef(unittest.TestCase):
    def test_fields(self):
        ref = StoreRef(store_id='s1', group_id='g1', table_id='t1', store_type='sqlite')
        self.assertEqual(ref.store_id, 's1')
        self.assertEqual(ref.group_id, 'g1')
        self.assertEqual(ref.table_id, 't1')
        self.assertEqual(ref.store_type, 'sqlite')

    def test_frozen(self):
        ref = StoreRef(store_id='s1', group_id='g1', table_id='t1', store_type='sqlite')
        with self.assertRaises((AttributeError, TypeError)):
            ref.store_id = 'changed'  # type: ignore[misc]

    def test_equality(self):
        a = StoreRef(store_id='s1', group_id='g1', table_id='t1', store_type='sqlite')
        b = StoreRef(store_id='s1', group_id='g1', table_id='t1', store_type='sqlite')
        self.assertEqual(a, b)

    def test_hashable(self):
        ref = StoreRef(store_id='s1', group_id='g1', table_id='t1', store_type='sqlite')
        {ref}  # must not raise


# ---------------------------------------------------------------------------
# PersistenceManager — sync
# ---------------------------------------------------------------------------

from oj_persistence.manager import PersistenceManager


class TestSyncManagerCreateGroup(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_returns_group_ref(self):
        ref = self.pm.create_group(store_type='in_memory', group_id='ns')
        self.assertIsInstance(ref, GroupRef)

    def test_explicit_group_id_preserved(self):
        ref = self.pm.create_group(store_type='in_memory', group_id='ns')
        self.assertEqual(ref.group_id, 'ns')
        self.assertEqual(ref.store_type, 'in_memory')

    def test_omitted_group_id_generates_uuid(self):
        ref = self.pm.create_group(store_type='in_memory')
        self.assertEqual(len(ref.group_id), 36)
        uuid.UUID(ref.group_id)  # valid UUID4 — must not raise

    def test_group_appears_in_group_catalog(self):
        ref = self.pm.create_group(store_type='in_memory', group_id='ns')
        self.assertEqual(self.pm.group_catalog()['ns'], ref)

    def test_group_catalog_empty_initially(self):
        self.assertEqual(self.pm.group_catalog(), {})


class TestSyncManagerAddTable(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()
        self.pm.create_group(store_type='in_memory', group_id='ns')

    def tearDown(self):
        PersistenceManager._instance = None

    def test_returns_store_ref(self):
        ref = self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        self.assertIsInstance(ref, StoreRef)

    def test_explicit_ids_preserved(self):
        ref = self.pm.add_table(group_id='ns', store_id='users', table_id='users_tbl')
        self.assertEqual(ref.store_id, 'users')
        self.assertEqual(ref.group_id, 'ns')
        self.assertEqual(ref.table_id, 'users_tbl')
        self.assertEqual(ref.store_type, 'in_memory')

    def test_omitted_store_id_generates_uuid(self):
        ref = self.pm.add_table(group_id='ns')
        uuid.UUID(ref.store_id)  # must not raise

    def test_omitted_table_id_is_unique_and_nonempty(self):
        ref1 = self.pm.add_table(group_id='ns')
        ref2 = self.pm.add_table(group_id='ns')
        self.assertTrue(ref1.table_id)
        self.assertNotEqual(ref1.table_id, ref2.table_id)

    def test_store_is_usable_via_store_id(self):
        ref = self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        self.pm.create('users', 'k1', {'name': 'Alice'})
        self.assertEqual(self.pm.read('users', 'k1'), {'name': 'Alice'})

    def test_store_ref_is_usable_via_returned_id(self):
        ref = self.pm.add_table(group_id='ns')  # UUID store_id
        self.pm.create(ref.store_id, 'k1', 'v1')
        self.assertEqual(self.pm.read(ref.store_id, 'k1'), 'v1')

    def test_store_appears_in_catalog(self):
        ref = self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        self.assertEqual(self.pm.catalog()['users'], ref)

    def test_unknown_group_raises(self):
        with self.assertRaises(KeyError):
            self.pm.add_table(group_id='ghost')

    def test_multiple_tables_in_same_group(self):
        r1 = self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        r2 = self.pm.add_table(group_id='ns', store_id='orders', table_id='orders')
        self.assertEqual(r1.group_id, r2.group_id)
        self.assertNotEqual(r1.store_id, r2.store_id)


class TestSyncManagerConfigure(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_returns_store_ref(self):
        ref = self.pm.configure(store_type='in_memory', store_id='cache')
        self.assertIsInstance(ref, StoreRef)

    def test_explicit_store_id_preserved(self):
        ref = self.pm.configure(store_type='in_memory', store_id='cache')
        self.assertEqual(ref.store_id, 'cache')

    def test_omitted_store_id_generates_uuid(self):
        ref = self.pm.configure(store_type='in_memory')
        uuid.UUID(ref.store_id)  # must not raise

    def test_returned_store_id_is_usable(self):
        ref = self.pm.configure(store_type='in_memory')
        self.pm.create(ref.store_id, 'k', 'v')
        self.assertEqual(self.pm.read(ref.store_id, 'k'), 'v')

    def test_store_appears_in_catalog(self):
        ref = self.pm.configure(store_type='in_memory', store_id='cache')
        self.assertIn('cache', self.pm.catalog())

    def test_catalog_empty_initially(self):
        self.assertEqual(self.pm.catalog(), {})

    def test_sqlite_configure_and_use(self):
        ref = self.pm.configure(store_type='sqlite', store_id='data', path=':memory:')
        self.assertEqual(ref.store_type, 'sqlite')
        self.pm.create('data', 'k', {'x': 1})
        self.assertEqual(self.pm.read('data', 'k'), {'x': 1})


# ---------------------------------------------------------------------------
# AsyncPersistenceManager — async
# ---------------------------------------------------------------------------

from oj_persistence.async_manager import AsyncPersistenceManager


class TestAsyncManagerCreateGroup(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    def test_returns_group_ref(self):
        ref = self.pm.create_group(store_type='in_memory', group_id='ns')
        self.assertIsInstance(ref, GroupRef)

    def test_explicit_group_id_preserved(self):
        ref = self.pm.create_group(store_type='in_memory', group_id='ns')
        self.assertEqual(ref.group_id, 'ns')
        self.assertEqual(ref.store_type, 'in_memory')

    def test_omitted_group_id_generates_uuid(self):
        ref = self.pm.create_group(store_type='in_memory')
        uuid.UUID(ref.group_id)  # must not raise

    def test_group_appears_in_group_catalog(self):
        ref = self.pm.create_group(store_type='in_memory', group_id='ns')
        self.assertEqual(self.pm.group_catalog()['ns'], ref)

    def test_group_catalog_empty_initially(self):
        self.assertEqual(self.pm.group_catalog(), {})


class TestAsyncManagerAddTable(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()
        self.pm.create_group(store_type='in_memory', group_id='ns')

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_returns_store_ref(self):
        ref = await self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        self.assertIsInstance(ref, StoreRef)

    async def test_explicit_ids_preserved(self):
        ref = await self.pm.add_table(group_id='ns', store_id='users', table_id='users_tbl')
        self.assertEqual(ref.store_id, 'users')
        self.assertEqual(ref.group_id, 'ns')
        self.assertEqual(ref.table_id, 'users_tbl')
        self.assertEqual(ref.store_type, 'in_memory')

    async def test_omitted_store_id_generates_uuid(self):
        ref = await self.pm.add_table(group_id='ns')
        uuid.UUID(ref.store_id)  # must not raise

    async def test_omitted_table_id_is_unique_and_nonempty(self):
        ref1 = await self.pm.add_table(group_id='ns')
        ref2 = await self.pm.add_table(group_id='ns')
        self.assertTrue(ref1.table_id)
        self.assertNotEqual(ref1.table_id, ref2.table_id)

    async def test_store_is_usable_via_store_id(self):
        ref = await self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        await self.pm.create('users', 'k1', {'name': 'Alice'})
        self.assertEqual(await self.pm.read('users', 'k1'), {'name': 'Alice'})

    async def test_store_ref_is_usable_via_returned_id(self):
        ref = await self.pm.add_table(group_id='ns')
        await self.pm.create(ref.store_id, 'k1', 'v1')
        self.assertEqual(await self.pm.read(ref.store_id, 'k1'), 'v1')

    async def test_store_appears_in_catalog(self):
        ref = await self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        self.assertEqual(self.pm.catalog()['users'], ref)

    async def test_unknown_group_raises(self):
        with self.assertRaises(KeyError):
            await self.pm.add_table(group_id='ghost')

    async def test_multiple_tables_in_same_group(self):
        r1 = await self.pm.add_table(group_id='ns', store_id='users', table_id='users')
        r2 = await self.pm.add_table(group_id='ns', store_id='orders', table_id='orders')
        self.assertEqual(r1.group_id, r2.group_id)
        self.assertNotEqual(r1.store_id, r2.store_id)


class TestAsyncManagerConfigure(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_returns_store_ref(self):
        ref = await self.pm.configure(store_type='in_memory', store_id='cache')
        self.assertIsInstance(ref, StoreRef)

    async def test_explicit_store_id_preserved(self):
        ref = await self.pm.configure(store_type='in_memory', store_id='cache')
        self.assertEqual(ref.store_id, 'cache')

    async def test_omitted_store_id_generates_uuid(self):
        ref = await self.pm.configure(store_type='in_memory')
        uuid.UUID(ref.store_id)  # must not raise

    async def test_returned_store_id_is_usable(self):
        ref = await self.pm.configure(store_type='in_memory')
        await self.pm.create(ref.store_id, 'k', 'v')
        self.assertEqual(await self.pm.read(ref.store_id, 'k'), 'v')

    async def test_store_appears_in_catalog(self):
        ref = await self.pm.configure(store_type='in_memory', store_id='cache')
        self.assertIn('cache', self.pm.catalog())

    async def test_catalog_empty_initially(self):
        self.assertEqual(self.pm.catalog(), {})

    async def test_sqlite_configure_and_use(self):
        ref = await self.pm.configure(store_type='sqlite', store_id='data', path=':memory:')
        self.assertEqual(ref.store_type, 'sqlite')
        await self.pm.create('data', 'k', {'x': 1})
        self.assertEqual(await self.pm.read('data', 'k'), {'x': 1})


if __name__ == '__main__':
    unittest.main()
