"""
Integration tests for PersistenceManager.

Correct usage: callers never touch a store directly for data operations.
All reads and writes go through the manager. Stores are only created and
registered (via get_or_create or register); the manager owns all I/O.
"""
import tempfile
import threading
import unittest
from pathlib import Path

from oj_persistence import (
    CsvFileStore,
    IjsonFileStore,
    InMemoryStore,
    NdjsonFileStore,
    PersistenceManager,
)


class TestRegistration(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_get_or_create_returns_store_object(self):
        store = self.pm.get_or_create('s', lambda: InMemoryStore())
        self.assertIsNotNone(store)

    def test_get_or_create_factory_called_once(self):
        calls = []
        self.pm.get_or_create('s', lambda: (calls.append(1), InMemoryStore())[1])
        self.pm.get_or_create('s', lambda: (calls.append(1), InMemoryStore())[1])
        self.assertEqual(len(calls), 1)

    def test_get_or_create_same_name_same_store(self):
        a = self.pm.get_or_create('s', lambda: InMemoryStore())
        b = self.pm.get_or_create('s', lambda: InMemoryStore())
        self.assertIs(a, b)

    def test_different_names_different_stores(self):
        a = self.pm.get_or_create('a', lambda: InMemoryStore())
        b = self.pm.get_or_create('b', lambda: InMemoryStore())
        self.assertIsNot(a, b)


class TestCallerWorkflow(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None
        self._tmpdir.cleanup()

    def test_create_and_read_via_manager_in_memory(self):
        self.pm.get_or_create('cache', lambda: InMemoryStore())
        self.pm.create('cache', 'item:1', {'value': 42})
        self.assertEqual(self.pm.read('cache', 'item:1'), {'value': 42})

    def test_create_and_read_via_manager_ndjson(self):
        self.pm.get_or_create('log', lambda: NdjsonFileStore(self.tmp_path / 'log.ndjson'))
        self.pm.create('log', 'evt:1', {'msg': 'hello'})
        self.assertEqual(self.pm.read('log', 'evt:1'), {'msg': 'hello'})

    def test_create_and_read_via_manager_ijson(self):
        self.pm.get_or_create('cfg', lambda: IjsonFileStore(self.tmp_path / 'cfg.json'))
        self.pm.create('cfg', 'key', 'value')
        self.assertEqual(self.pm.read('cfg', 'key'), 'value')

    def test_create_and_read_via_manager_csv(self):
        self.pm.get_or_create('records', lambda: CsvFileStore(self.tmp_path / 'records.csv'))
        self.pm.create('records', 'row:1', {'name': 'Alice', 'score': '99'})
        self.assertEqual(self.pm.read('records', 'row:1'), {'name': 'Alice', 'score': '99'})

    def test_update_via_manager(self):
        self.pm.get_or_create('s', lambda: InMemoryStore())
        self.pm.create('s', 'k', 'original')
        self.pm.update('s', 'k', 'updated')
        self.assertEqual(self.pm.read('s', 'k'), 'updated')

    def test_upsert_via_manager(self):
        self.pm.get_or_create('s', lambda: InMemoryStore())
        self.pm.upsert('s', 'k', 'first')
        self.pm.upsert('s', 'k', 'second')
        self.assertEqual(self.pm.read('s', 'k'), 'second')

    def test_delete_via_manager(self):
        self.pm.get_or_create('s', lambda: InMemoryStore())
        self.pm.create('s', 'k', 'v')
        self.pm.delete('s', 'k')
        self.assertIsNone(self.pm.read('s', 'k'))

    def test_list_via_manager(self):
        self.pm.get_or_create('s', lambda: InMemoryStore())
        self.pm.create('s', 'a', 1)
        self.pm.create('s', 'b', 2)
        self.assertEqual(sorted(self.pm.list('s')), [1, 2])

    def test_list_with_predicate_via_manager(self):
        self.pm.get_or_create('s', lambda: InMemoryStore())
        self.pm.create('s', 'a', 10)
        self.pm.create('s', 'b', 5)
        self.assertEqual(self.pm.list('s', lambda v: v >= 10), [10])

    def test_data_ops_on_unregistered_store_raise(self):
        with self.assertRaises(KeyError):
            self.pm.create('ghost', 'k', 'v')
        with self.assertRaises(KeyError):
            self.pm.read('ghost', 'k')


class TestStateVisibility(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()
        self.pm.get_or_create('s', lambda: InMemoryStore())

    def tearDown(self):
        PersistenceManager._instance = None

    def test_write_then_read_sees_new_value(self):
        self.pm.create('s', 'k', 'v')
        self.assertEqual(self.pm.read('s', 'k'), 'v')

    def test_update_then_read_sees_updated_value(self):
        self.pm.create('s', 'k', 'old')
        self.pm.update('s', 'k', 'new')
        self.assertEqual(self.pm.read('s', 'k'), 'new')

    def test_delete_then_read_returns_none(self):
        self.pm.create('s', 'k', 'v')
        self.pm.delete('s', 'k')
        self.assertIsNone(self.pm.read('s', 'k'))


class TestMultipleStores(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None
        self._tmpdir.cleanup()

    def test_different_store_types_tracked_simultaneously(self):
        self.pm.get_or_create('mem', lambda: InMemoryStore())
        self.pm.get_or_create('nd',  lambda: NdjsonFileStore(self.tmp_path / 'nd.ndjson'))
        self.pm.get_or_create('ij',  lambda: IjsonFileStore(self.tmp_path / 'ij.json'))
        self.pm.get_or_create('cv',  lambda: CsvFileStore(self.tmp_path / 'cv.csv'))

        self.pm.create('mem', 'k', 'mem-val')
        self.pm.create('nd',  'k', 'nd-val')
        self.pm.create('ij',  'k', 'ij-val')
        self.pm.create('cv',  'k', {'f': 'cv-val'})

        self.assertEqual(self.pm.read('mem', 'k'), 'mem-val')
        self.assertEqual(self.pm.read('nd',  'k'), 'nd-val')
        self.assertEqual(self.pm.read('ij',  'k'), 'ij-val')
        self.assertEqual(self.pm.read('cv',  'k'), {'f': 'cv-val'})

    def test_writes_to_one_store_do_not_affect_another(self):
        self.pm.get_or_create('a', lambda: InMemoryStore())
        self.pm.get_or_create('b', lambda: InMemoryStore())
        self.pm.create('a', 'key', 'in-a')
        self.assertIsNone(self.pm.read('b', 'key'))


class TestConcurrentRegistration(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_concurrent_get_or_create_returns_same_store(self):
        results: list = []
        lock = threading.Lock()

        def caller():
            store = self.pm.get_or_create('shared', lambda: InMemoryStore())
            with lock:
                results.append(store)

        threads = [threading.Thread(target=caller) for _ in range(20)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertEqual(len(set(id(s) for s in results)), 1)


if __name__ == '__main__':
    unittest.main()
