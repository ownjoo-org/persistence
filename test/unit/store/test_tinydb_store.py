import tempfile
import unittest
from pathlib import Path

from tinydb import where

from oj_persistence.store.tinydb_store import TinyDbStore
from test.unit.store.contract import StoreContract


class TestTinyDbStoreInMemory(StoreContract, unittest.TestCase):
    """Full CRUDL contract against an in-memory TinyDB database."""

    def setUp(self):
        self.store = TinyDbStore()  # no path → MemoryStorage

    def tearDown(self):
        self.store.close()


class TestTinyDbStoreFile(unittest.TestCase):
    """File-backed behaviour."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.json'

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_persists_across_instances(self):
        TinyDbStore(self.path).upsert('k', 'v')
        self.assertEqual(TinyDbStore(self.path).read('k'), 'v')

    def test_context_manager_closes_db(self):
        with TinyDbStore(self.path) as store:
            store.upsert('k', 1)
        with TinyDbStore(self.path) as store:
            self.assertEqual(store.read('k'), 1)


class TestTinyDbStoreQuery(unittest.TestCase):
    """TinyDB-native query() beyond what list(predicate) covers."""

    def setUp(self):
        self.store = TinyDbStore()

    def tearDown(self):
        self.store.close()

    def test_query_returns_matching_values(self):
        self.store.upsert('1', {'name': 'Alice', 'age': 30})
        self.store.upsert('2', {'name': 'Bob', 'age': 25})
        self.store.upsert('3', {'name': 'Carol', 'age': 35})

        results = self.store.query(where('_value')['age'] > 28)
        self.assertEqual(sorted(r['name'] for r in results), ['Alice', 'Carol'])

    def test_query_returns_empty_when_no_match(self):
        self.store.upsert('1', {'name': 'Alice'})
        self.assertEqual(self.store.query(where('_value')['name'] == 'Nobody'), [])

    def test_query_nested_field(self):
        self.store.upsert('1', {'address': {'city': 'NYC'}})
        self.store.upsert('2', {'address': {'city': 'LA'}})

        results = self.store.query(where('_value')['address']['city'] == 'NYC')
        self.assertEqual(results, [{'address': {'city': 'NYC'}}])

    def test_query_does_not_include_key_in_result(self):
        self.store.upsert('k', {'x': 1})
        results = self.store.query(where('_value')['x'] == 1)
        self.assertEqual(results, [{'x': 1}])
        self.assertNotIn('_key', results[0])


if __name__ == '__main__':
    unittest.main()
