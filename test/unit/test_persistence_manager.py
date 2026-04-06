import threading
import unittest
from unittest.mock import MagicMock

from oj_persistence.manager import PersistenceManager
from oj_persistence.store.base import AbstractStore


def make_store() -> AbstractStore:
    return MagicMock(spec=AbstractStore)


class TestPersistenceManagerSingleton(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None

    def tearDown(self):
        PersistenceManager._instance = None

    def test_same_instance_returned(self):
        a = PersistenceManager()
        b = PersistenceManager()
        self.assertIs(a, b)

    def test_concurrent_construction_yields_single_instance(self):
        instances: list[PersistenceManager] = []
        lock = threading.Lock()

        def create():
            pm = PersistenceManager()
            with lock:
                instances.append(pm)

        threads = [threading.Thread(target=create) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(set(id(i) for i in instances)), 1)


class TestManagerCRUDL(unittest.TestCase):
    """Manager CRUDL methods delegate to the correct store."""

    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()
        self.store = make_store()
        self.pm.register('s', self.store)

    def tearDown(self):
        PersistenceManager._instance = None

    def test_create_delegates_to_store(self):
        self.pm.create('s', 'k', 'v')
        self.store.create.assert_called_once_with('k', 'v')

    def test_read_delegates_to_store(self):
        self.pm.read('s', 'k')
        self.store.read.assert_called_once_with('k')

    def test_update_delegates_to_store(self):
        self.pm.update('s', 'k', 'v')
        self.store.update.assert_called_once_with('k', 'v')

    def test_upsert_delegates_to_store(self):
        self.pm.upsert('s', 'k', 'v')
        self.store.upsert.assert_called_once_with('k', 'v')

    def test_delete_delegates_to_store(self):
        self.pm.delete('s', 'k')
        self.store.delete.assert_called_once_with('k')

    def test_list_delegates_to_store(self):
        self.pm.list('s')
        self.store.list.assert_called_once_with(None)

    def test_list_with_predicate_delegates_to_store(self):
        pred = lambda v: True
        self.pm.list('s', pred)
        self.store.list.assert_called_once_with(pred)

    def test_read_returns_store_value(self):
        self.store.read.return_value = 42
        self.assertEqual(self.pm.read('s', 'k'), 42)

    def test_list_returns_store_values(self):
        self.store.list.return_value = [1, 2, 3]
        self.assertEqual(self.pm.list('s'), [1, 2, 3])

    def test_crudl_on_unregistered_store_raises(self):
        with self.assertRaises(KeyError):
            self.pm.create('ghost', 'k', 'v')
        with self.assertRaises(KeyError):
            self.pm.read('ghost', 'k')
        with self.assertRaises(KeyError):
            self.pm.update('ghost', 'k', 'v')
        with self.assertRaises(KeyError):
            self.pm.upsert('ghost', 'k', 'v')
        with self.assertRaises(KeyError):
            self.pm.delete('ghost', 'k')
        with self.assertRaises(KeyError):
            self.pm.list('ghost')


class TestPersistenceManagerRegistry(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None

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
        self.pm.unregister('nonexistent')  # must not raise

    def test_multiple_stores_independent(self):
        a, b = make_store(), make_store()
        self.pm.register('a', a)
        self.pm.register('b', b)
        self.assertIs(self.pm.get_store('a'), a)
        self.assertIs(self.pm.get_store('b'), b)


if __name__ == '__main__':
    unittest.main()
