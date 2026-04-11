import tempfile
import threading
import unittest
from pathlib import Path

from oj_persistence.store.sqlite import SqliteStore
from test.unit.store.contract import StoreContract


class TestSqliteStoreInMemory(StoreContract, unittest.TestCase):
    """Full CRUDL contract against an in-memory SQLite database."""

    def setUp(self):
        self.store = SqliteStore(':memory:')

    def tearDown(self):
        self.store.close()


class TestSqliteStoreFile(unittest.TestCase):
    """File-backed behaviour that makes no sense against :memory:."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.db'

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_persists_across_instances(self):
        with SqliteStore(self.path) as store:
            store.upsert('k', 'v')
        with SqliteStore(self.path) as store:
            self.assertEqual(store.read('k'), 'v')

    def test_context_manager_closes_connection(self):
        with SqliteStore(self.path) as store:
            store.upsert('k', 1)
        with SqliteStore(self.path) as store:
            self.assertEqual(store.read('k'), 1)


class TestSqliteStoreJsonIndex(unittest.TestCase):
    def setUp(self):
        self.store = SqliteStore(':memory:')

    def tearDown(self):
        self.store.close()

    def test_add_json_index_does_not_raise(self):
        self.store.add_json_index('$.name')

    def test_add_json_index_idempotent(self):
        self.store.add_json_index('$.name')
        self.store.add_json_index('$.name')  # IF NOT EXISTS — should not raise

    def test_add_json_index_custom_name(self):
        self.store.add_json_index('$.age', index_name='idx_custom_age')

    def test_list_still_works_after_index(self):
        self.store.add_json_index('$.name')
        self.store.upsert('1', {'name': 'Alice', 'age': 30})
        self.store.upsert('2', {'name': 'Bob', 'age': 25})
        result = self.store.list(lambda v: v['age'] >= 30)
        self.assertEqual(result, [{'name': 'Alice', 'age': 30}])


class TestSqliteStoreConcurrency(unittest.TestCase):
    """Thread-safety beyond what StoreContract covers."""

    def test_concurrent_reads_do_not_block_each_other(self):
        store = SqliteStore(':memory:')
        for i in range(10):
            store.upsert(str(i), i)

        results = []
        errors = []

        def reader():
            try:
                results.append(store.read('5'))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        store.close()
        self.assertFalse(errors)
        self.assertTrue(all(r == 5 for r in results))


if __name__ == '__main__':
    unittest.main()
