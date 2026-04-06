import tempfile
import threading
import unittest
from pathlib import Path

from oj_persistence.store.flat_file import FlatFileStore


class TestCreate(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = FlatFileStore(self.tmp_path / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_create_then_read(self):
        self.store.create('a', {'x': 1})
        self.assertEqual(self.store.read('a'), {'x': 1})

    def test_create_existing_key_raises(self):
        self.store.create('a', 1)
        with self.assertRaises(KeyError):
            self.store.create('a', 2)

    def test_create_existing_key_does_not_overwrite(self):
        self.store.create('a', 'original')
        with self.assertRaises(KeyError):
            self.store.create('a', 'overwrite')
        self.assertEqual(self.store.read('a'), 'original')


class TestRead(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = FlatFileStore(self.tmp_path / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_read_existing_key(self):
        self.store.create('a', 42)
        self.assertEqual(self.store.read('a'), 42)

    def test_read_missing_key_returns_none(self):
        self.assertIsNone(self.store.read('missing'))


class TestUpdate(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = FlatFileStore(self.tmp_path / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_update_existing_key(self):
        self.store.create('a', 'first')
        self.store.update('a', 'second')
        self.assertEqual(self.store.read('a'), 'second')

    def test_update_missing_key_raises(self):
        with self.assertRaises(KeyError):
            self.store.update('nonexistent', 'value')

    def test_update_missing_key_does_not_create(self):
        with self.assertRaises(KeyError):
            self.store.update('nonexistent', 'value')
        self.assertIsNone(self.store.read('nonexistent'))


class TestUpsert(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = FlatFileStore(self.tmp_path / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_upsert_creates_when_absent(self):
        self.store.upsert('a', 'new')
        self.assertEqual(self.store.read('a'), 'new')

    def test_upsert_updates_when_present(self):
        self.store.create('a', 'old')
        self.store.upsert('a', 'new')
        self.assertEqual(self.store.read('a'), 'new')


class TestDelete(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = FlatFileStore(self.tmp_path / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_delete_removes_key(self):
        self.store.create('a', 1)
        self.store.delete('a')
        self.assertIsNone(self.store.read('a'))

    def test_delete_missing_key_is_noop(self):
        self.store.delete('nonexistent')  # must not raise


class TestList(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = FlatFileStore(self.tmp_path / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_list_returns_all_values(self):
        self.store.create('a', 1)
        self.store.create('b', 2)
        self.assertEqual(sorted(self.store.list()), [1, 2])

    def test_list_with_predicate(self):
        self.store.create('a', 10)
        self.store.create('b', 20)
        self.store.create('c', 5)
        self.assertEqual(sorted(self.store.list(lambda v: v >= 10)), [10, 20])

    def test_list_empty_store_returns_empty(self):
        self.assertEqual(self.store.list(), [])

    def test_list_no_matches_returns_empty(self):
        self.store.create('a', 1)
        self.assertEqual(self.store.list(lambda v: v > 100), [])


class TestCrosscutting(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_unsupported_format_raises(self):
        with self.assertRaisesRegex(ValueError, "Unsupported format"):
            FlatFileStore(self.tmp_path / 'data.xml', fmt='xml')

    def test_creates_file_on_first_write(self):
        path = self.tmp_path / 'subdir' / 'data.json'
        s = FlatFileStore(path)
        self.assertFalse(path.exists())
        s.create('k', 'v')
        self.assertTrue(path.exists())

    def test_persists_across_instances(self):
        path = self.tmp_path / 'shared.json'
        FlatFileStore(path).create('key', 'value')
        self.assertEqual(FlatFileStore(path).read('key'), 'value')

    def test_concurrent_writes_do_not_corrupt(self):
        path = self.tmp_path / 'concurrent.json'
        s = FlatFileStore(path)
        errors: list[Exception] = []

        def worker(key: str, value: int) -> None:
            try:
                s.upsert(key, value)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(str(i), i)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors)
        self.assertEqual(sorted(s.list()), list(range(20)))


if __name__ == '__main__':
    unittest.main()
