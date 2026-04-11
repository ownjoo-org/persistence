import tempfile
import unittest
from pathlib import Path

from oj_persistence.store.sqlalchemy_store import SqlAlchemyStore
from test.unit.store.contract import StoreContract


class TestSqlAlchemyStoreSQLite(StoreContract, unittest.TestCase):
    """Full CRUDL contract using SQLite as the backing database."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.db'
        # timeout gives SQLite time to wait on write locks under thread contention
        self.store = SqlAlchemyStore(f'sqlite:///{self.path}', connect_args={'timeout': 30})

    def tearDown(self):
        self.store.dispose()
        self._tmpdir.cleanup()


class TestSqlAlchemyStorePersistence(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'store.db'

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_persists_across_instances(self):
        with SqlAlchemyStore(f'sqlite:///{self.path}') as store:
            store.upsert('k', 'v')
        with SqlAlchemyStore(f'sqlite:///{self.path}') as store:
            self.assertEqual(store.read('k'), 'v')

    def test_custom_table_name(self):
        with SqlAlchemyStore(f'sqlite:///{self.path}', table='my_cache') as store:
            store.upsert('k', 1)
            self.assertEqual(store.read('k'), 1)

    def test_invalid_table_name_raises(self):
        with self.assertRaises(ValueError):
            SqlAlchemyStore(f'sqlite:///{self.path}', table='bad name; DROP TABLE store')


if __name__ == '__main__':
    unittest.main()
