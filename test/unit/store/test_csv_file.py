"""
CsvFileStore tests.

CSV round-trips all field values as strings, so values here use string fields
throughout. This store does not use StoreContract (which uses typed values).
"""
import tempfile
import threading
import unittest
from pathlib import Path

from oj_persistence.store.csv_file import CsvFileStore


class TestFieldnames(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_inferred_from_first_dict_write(self):
        s = CsvFileStore(self.tmp_path / 'data.csv')
        s.create('a', {'name': 'Alice', 'age': '30'})
        self.assertEqual(s.fieldnames, ['name', 'age'])

    def test_inferred_from_first_tuple_iterable_write(self):
        s = CsvFileStore(self.tmp_path / 'data.csv')
        s.create('a', [('name', 'Alice'), ('age', '30')])
        self.assertEqual(s.fieldnames, ['name', 'age'])

    def test_explicit_fieldnames_at_construction(self):
        s = CsvFileStore(self.tmp_path / 'data.csv', fieldnames=['name', 'age'])
        self.assertEqual(s.fieldnames, ['name', 'age'])

    def test_fieldnames_loaded_from_existing_file(self):
        path = self.tmp_path / 'data.csv'
        s1 = CsvFileStore(path)
        s1.create('a', {'x': '1', 'y': '2'})
        s2 = CsvFileStore(path)
        self.assertEqual(s2.fieldnames, ['x', 'y'])


class TestCreate(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = CsvFileStore(self.tmp_path / 'data.csv')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_create_dict_then_read(self):
        self.store.create('a', {'name': 'Alice', 'age': '30'})
        self.assertEqual(self.store.read('a'), {'name': 'Alice', 'age': '30'})

    def test_create_tuple_iterable_then_read(self):
        self.store.create('a', [('name', 'Alice'), ('age', '30')])
        self.assertEqual(self.store.read('a'), {'name': 'Alice', 'age': '30'})

    def test_create_existing_key_raises(self):
        self.store.create('a', {'name': 'Alice'})
        with self.assertRaises(KeyError):
            self.store.create('a', {'name': 'Bob'})

    def test_create_existing_key_does_not_overwrite(self):
        self.store.create('a', {'name': 'Alice'})
        with self.assertRaises(KeyError):
            self.store.create('a', {'name': 'Bob'})
        self.assertEqual(self.store.read('a'), {'name': 'Alice'})

    def test_create_missing_field_defaults_to_empty_string(self):
        s = CsvFileStore(self.tmp_path / 'fields.csv', fieldnames=['name', 'age'])
        s.create('a', {'name': 'Alice'})
        self.assertEqual(s.read('a'), {'name': 'Alice', 'age': ''})

    def test_create_extra_field_raises(self):
        s = CsvFileStore(self.tmp_path / 'schema.csv', fieldnames=['name'])
        with self.assertRaises(ValueError):
            s.create('a', {'name': 'Alice', 'extra': 'x'})


class TestRead(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = CsvFileStore(self.tmp_path / 'data.csv')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_read_missing_key_returns_none(self):
        self.assertIsNone(self.store.read('missing'))

    def test_read_existing_key(self):
        self.store.create('a', {'val': 'hello'})
        self.assertEqual(self.store.read('a'), {'val': 'hello'})


class TestUpdate(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = CsvFileStore(self.tmp_path / 'data.csv')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_update_existing_key(self):
        self.store.create('a', {'val': 'first'})
        self.store.update('a', {'val': 'second'})
        self.assertEqual(self.store.read('a'), {'val': 'second'})

    def test_update_missing_key_raises(self):
        with self.assertRaises(KeyError):
            self.store.update('nonexistent', {'val': 'x'})

    def test_update_missing_key_does_not_create(self):
        with self.assertRaises(KeyError):
            self.store.update('nonexistent', {'val': 'x'})
        self.assertIsNone(self.store.read('nonexistent'))


class TestUpsert(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = CsvFileStore(self.tmp_path / 'data.csv')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_upsert_creates_when_absent(self):
        self.store.upsert('a', {'val': 'new'})
        self.assertEqual(self.store.read('a'), {'val': 'new'})

    def test_upsert_updates_when_present(self):
        self.store.create('a', {'val': 'old'})
        self.store.upsert('a', {'val': 'new'})
        self.assertEqual(self.store.read('a'), {'val': 'new'})


class TestDelete(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = CsvFileStore(self.tmp_path / 'data.csv')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_delete_removes_key(self):
        self.store.create('a', {'val': '1'})
        self.store.delete('a')
        self.assertIsNone(self.store.read('a'))

    def test_delete_missing_key_is_noop(self):
        self.store.delete('nonexistent')


class TestList(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = CsvFileStore(self.tmp_path / 'data.csv')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_list_returns_all_values(self):
        self.store.create('a', {'n': '1'})
        self.store.create('b', {'n': '2'})
        self.assertEqual(
            sorted(self.store.list(), key=lambda v: v['n']),
            [{'n': '1'}, {'n': '2'}],
        )

    def test_list_with_predicate(self):
        self.store.create('a', {'n': '10'})
        self.store.create('b', {'n': '5'})
        self.assertEqual(self.store.list(lambda v: int(v['n']) >= 10), [{'n': '10'}])

    def test_list_empty_store_returns_empty(self):
        self.assertEqual(self.store.list(), [])


class TestCrosscutting(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_file_has_header_row(self):
        path = self.tmp_path / 'data.csv'
        s = CsvFileStore(path)
        s.create('a', {'name': 'Alice', 'age': '30'})
        first_line = path.read_text().splitlines()[0]
        self.assertEqual(first_line, 'key,name,age')

    def test_persists_across_instances(self):
        path = self.tmp_path / 'data.csv'
        CsvFileStore(path).create('k', {'v': 'hello'})
        self.assertEqual(CsvFileStore(path).read('k'), {'v': 'hello'})

    def test_concurrent_upserts_do_not_corrupt(self):
        s = CsvFileStore(self.tmp_path / 'data.csv', fieldnames=['val'])
        errors: list[Exception] = []

        def worker(key: str, value: str) -> None:
            try:
                s.upsert(key, {'val': value})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(str(i), str(i))) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors)
        self.assertEqual(len(s.list()), 20)


if __name__ == '__main__':
    unittest.main()
