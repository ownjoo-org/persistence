"""
Tests for compression support and from_stream() / from_stream_sync()
on all four file-backed stores: FlatFileStore, NdjsonFileStore,
CsvFileStore, IjsonFileStore.
"""
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from oj_persistence.store.csv_file import CsvFileStore
from oj_persistence.store.flat_file import FlatFileStore
from oj_persistence.store.ijson_file import IjsonFileStore
from oj_persistence.store.ndjson_file import NdjsonFileStore


# ──────────────────────────────────────────── helpers

def _gzip_bytes(data: bytes) -> bytes:
    import io
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
        gz.write(data)
    return buf.getvalue()


# ──────────────────────────────────────────── NdjsonFileStore

class TestNdjsonCompression(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.d = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _write_gzip(self, path: Path) -> None:
        lines = [
            json.dumps({'key': 'u1', 'value': {'name': 'Alice', 'role': 'admin'}}),
            json.dumps({'key': 'u2', 'value': {'name': 'Bob', 'role': 'viewer'}}),
        ]
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')

    def test_read_gzip_explicit(self):
        p = self.d / 'data.ndjson.gz'
        self._write_gzip(p)
        store = NdjsonFileStore(p, compression='gzip')
        self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})

    def test_read_gzip_auto_detect(self):
        p = self.d / 'data.ndjson.gz'
        self._write_gzip(p)
        store = NdjsonFileStore(p, compression='auto')
        self.assertEqual(len(store.list()), 2)

    def test_list_gzip(self):
        p = self.d / 'data.ndjson.gz'
        self._write_gzip(p)
        store = NdjsonFileStore(p, compression='gzip')
        names = sorted(r['name'] for r in store.list())
        self.assertEqual(names, ['Alice', 'Bob'])

    def test_upsert_and_read_gzip(self):
        p = self.d / 'data.ndjson.gz'
        store = NdjsonFileStore(p, compression='gzip')
        store.upsert('u1', {'name': 'Alice'})
        store.upsert('u2', {'name': 'Bob'})
        self.assertEqual(store.read('u1'), {'name': 'Alice'})
        self.assertEqual(len(store.list()), 2)

    def test_update_gzip(self):
        p = self.d / 'data.ndjson.gz'
        self._write_gzip(p)
        store = NdjsonFileStore(p, compression='gzip')
        store.update('u1', {'name': 'Alice Updated', 'role': 'superadmin'})
        self.assertEqual(store.read('u1')['name'], 'Alice Updated')

    def test_delete_gzip(self):
        p = self.d / 'data.ndjson.gz'
        self._write_gzip(p)
        store = NdjsonFileStore(p, compression='gzip')
        store.delete('u1')
        self.assertIsNone(store.read('u1'))
        self.assertEqual(len(store.list()), 1)

    def test_invalid_compression_raises(self):
        with self.assertRaises(ValueError):
            NdjsonFileStore(self.d / 'f.ndjson', compression='zip')

    def test_from_stream_sync(self):
        line1 = json.dumps({'key': 'u1', 'value': {'name': 'Alice'}}) + '\n'
        line2 = json.dumps({'key': 'u2', 'value': {'name': 'Bob'}}) + '\n'
        p = self.d / 'data.ndjson'
        store = NdjsonFileStore.from_stream_sync([(line1 + line2).encode()], p)
        self.assertIsInstance(store, NdjsonFileStore)
        self.assertEqual(sorted(r['name'] for r in store.list()), ['Alice', 'Bob'])

    def test_from_stream_sync_gzip(self):
        line1 = json.dumps({'key': 'u1', 'value': {'name': 'Alice'}}) + '\n'
        compressed = _gzip_bytes(line1.encode())
        p = self.d / 'data.ndjson.gz'
        store = NdjsonFileStore.from_stream_sync([compressed], p, compression='auto')
        self.assertEqual(store.read('u1'), {'name': 'Alice'})


class TestNdjsonFromStreamAsync(unittest.IsolatedAsyncioTestCase):
    async def test_from_stream_async(self):
        line1 = json.dumps({'key': 'u1', 'value': {'name': 'Alice'}}) + '\n'
        line2 = json.dumps({'key': 'u2', 'value': {'name': 'Bob'}}) + '\n'

        async def source():
            yield (line1 + line2).encode()

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'data.ndjson'
            store = await NdjsonFileStore.from_stream(source(), p)
            self.assertIsInstance(store, NdjsonFileStore)
            self.assertEqual(sorted(r['name'] for r in store.list()), ['Alice', 'Bob'])

    async def test_from_stream_async_gzip(self):
        line1 = json.dumps({'key': 'u1', 'value': {'name': 'Alice'}}) + '\n'
        compressed = _gzip_bytes(line1.encode())

        async def source():
            yield compressed

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'data.ndjson.gz'
            store = await NdjsonFileStore.from_stream(source(), p, compression='auto')
            self.assertEqual(store.read('u1'), {'name': 'Alice'})


# ──────────────────────────────────────────── CsvFileStore

class TestCsvCompression(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.d = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _write_gzip(self, path: Path) -> None:
        content = 'key,name,role\nu1,Alice,admin\nu2,Bob,viewer\n'
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            f.write(content)

    def test_read_gzip_explicit(self):
        p = self.d / 'data.csv.gz'
        self._write_gzip(p)
        store = CsvFileStore(p, compression='gzip')
        self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})

    def test_read_gzip_auto_detect(self):
        p = self.d / 'data.csv.gz'
        self._write_gzip(p)
        store = CsvFileStore(p, compression='auto')
        self.assertEqual(len(store.list()), 2)

    def test_list_gzip(self):
        p = self.d / 'data.csv.gz'
        self._write_gzip(p)
        store = CsvFileStore(p, compression='gzip')
        names = sorted(r['name'] for r in store.list())
        self.assertEqual(names, ['Alice', 'Bob'])

    def test_upsert_and_read_gzip(self):
        p = self.d / 'data.csv.gz'
        store = CsvFileStore(p, compression='gzip', fieldnames=['name', 'role'])
        store.upsert('u1', {'name': 'Alice', 'role': 'admin'})
        store.upsert('u2', {'name': 'Bob', 'role': 'viewer'})
        self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})

    def test_update_gzip(self):
        p = self.d / 'data.csv.gz'
        self._write_gzip(p)
        store = CsvFileStore(p, compression='gzip')
        store.update('u1', {'name': 'Alice', 'role': 'superadmin'})
        self.assertEqual(store.read('u1')['role'], 'superadmin')

    def test_delete_gzip(self):
        p = self.d / 'data.csv.gz'
        self._write_gzip(p)
        store = CsvFileStore(p, compression='gzip')
        store.delete('u1')
        self.assertIsNone(store.read('u1'))
        self.assertEqual(len(store.list()), 1)

    def test_invalid_compression_raises(self):
        with self.assertRaises(ValueError):
            CsvFileStore(self.d / 'f.csv', compression='zip')

    def test_from_stream_sync(self):
        content = b'key,name,role\nu1,Alice,admin\nu2,Bob,viewer\n'
        p = self.d / 'data.csv'
        store = CsvFileStore.from_stream_sync([content], p)
        self.assertIsInstance(store, CsvFileStore)
        self.assertEqual(sorted(r['name'] for r in store.list()), ['Alice', 'Bob'])

    def test_from_stream_sync_gzip(self):
        content = _gzip_bytes(b'key,name,role\nu1,Alice,admin\n')
        p = self.d / 'data.csv.gz'
        store = CsvFileStore.from_stream_sync([content], p, compression='auto')
        self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})


class TestCsvFromStreamAsync(unittest.IsolatedAsyncioTestCase):
    async def test_from_stream_async(self):
        async def source():
            yield b'key,name,role\n'
            yield b'u1,Alice,admin\n'

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'data.csv'
            store = await CsvFileStore.from_stream(source(), p)
            self.assertIsInstance(store, CsvFileStore)
            self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})

    async def test_from_stream_async_gzip(self):
        content = _gzip_bytes(b'key,name,role\nu1,Alice,admin\n')

        async def source():
            yield content

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'data.csv.gz'
            store = await CsvFileStore.from_stream(source(), p, compression='auto')
            self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})


# ──────────────────────────────────────────── FlatFileStore

class TestFlatFileCompression(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.d = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _write_gzip(self, path: Path) -> None:
        data = {'u1': {'name': 'Alice', 'role': 'admin'}, 'u2': {'name': 'Bob'}}
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(data, f)

    def test_read_gzip_explicit(self):
        p = self.d / 'data.json.gz'
        self._write_gzip(p)
        store = FlatFileStore(p, compression='gzip')
        self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})

    def test_read_gzip_auto_detect(self):
        p = self.d / 'data.json.gz'
        self._write_gzip(p)
        store = FlatFileStore(p, compression='auto')
        self.assertEqual(len(store.list()), 2)

    def test_upsert_and_read_gzip(self):
        p = self.d / 'data.json.gz'
        store = FlatFileStore(p, compression='gzip')
        store.upsert('u1', {'name': 'Alice'})
        store.upsert('u2', {'name': 'Bob'})
        self.assertEqual(store.read('u1'), {'name': 'Alice'})

    def test_update_gzip(self):
        p = self.d / 'data.json.gz'
        self._write_gzip(p)
        store = FlatFileStore(p, compression='gzip')
        store.update('u1', {'name': 'Alice Updated'})
        self.assertEqual(store.read('u1'), {'name': 'Alice Updated'})

    def test_invalid_compression_raises(self):
        with self.assertRaises(ValueError):
            FlatFileStore(self.d / 'f.json', compression='zip')

    def test_from_stream_sync(self):
        data = {'u1': {'name': 'Alice'}, 'u2': {'name': 'Bob'}}
        content = json.dumps(data).encode()
        p = self.d / 'data.json'
        store = FlatFileStore.from_stream_sync([content], p)
        self.assertIsInstance(store, FlatFileStore)
        self.assertEqual(store.read('u1'), {'name': 'Alice'})

    def test_from_stream_sync_gzip(self):
        data = {'u1': {'name': 'Alice'}}
        content = _gzip_bytes(json.dumps(data).encode())
        p = self.d / 'data.json.gz'
        store = FlatFileStore.from_stream_sync([content], p, compression='auto')
        self.assertEqual(store.read('u1'), {'name': 'Alice'})


class TestFlatFileFromStreamAsync(unittest.IsolatedAsyncioTestCase):
    async def test_from_stream_async(self):
        data = {'u1': {'name': 'Alice'}}

        async def source():
            yield json.dumps(data).encode()

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'data.json'
            store = await FlatFileStore.from_stream(source(), p)
            self.assertIsInstance(store, FlatFileStore)
            self.assertEqual(store.read('u1'), {'name': 'Alice'})


# ──────────────────────────────────────────── IjsonFileStore

class TestIjsonCompression(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.d = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _write_gzip(self, path: Path) -> None:
        data = {'u1': {'name': 'Alice', 'role': 'admin'}, 'u2': {'name': 'Bob'}}
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(data, f)

    def test_read_gzip_explicit(self):
        p = self.d / 'data.json.gz'
        self._write_gzip(p)
        store = IjsonFileStore(p, compression='gzip')
        self.assertEqual(store.read('u1'), {'name': 'Alice', 'role': 'admin'})

    def test_read_gzip_auto_detect(self):
        p = self.d / 'data.json.gz'
        self._write_gzip(p)
        store = IjsonFileStore(p, compression='auto')
        self.assertEqual(len(store.list()), 2)

    def test_list_gzip(self):
        p = self.d / 'data.json.gz'
        self._write_gzip(p)
        store = IjsonFileStore(p, compression='gzip')
        names = sorted(r['name'] for r in store.list())
        self.assertEqual(names, ['Alice', 'Bob'])

    def test_upsert_and_read_gzip(self):
        p = self.d / 'data.json.gz'
        store = IjsonFileStore(p, compression='gzip')
        store.upsert('u1', {'name': 'Alice'})
        self.assertEqual(store.read('u1'), {'name': 'Alice'})

    def test_invalid_compression_raises(self):
        with self.assertRaises(ValueError):
            IjsonFileStore(self.d / 'f.json', compression='zip')

    def test_from_stream_sync(self):
        data = {'u1': {'name': 'Alice'}, 'u2': {'name': 'Bob'}}
        content = json.dumps(data).encode()
        p = self.d / 'data.json'
        store = IjsonFileStore.from_stream_sync([content], p)
        self.assertIsInstance(store, IjsonFileStore)
        self.assertEqual(store.read('u1'), {'name': 'Alice'})

    def test_from_stream_sync_gzip(self):
        data = {'u1': {'name': 'Alice'}}
        content = _gzip_bytes(json.dumps(data).encode())
        p = self.d / 'data.json.gz'
        store = IjsonFileStore.from_stream_sync([content], p, compression='auto')
        self.assertEqual(store.read('u1'), {'name': 'Alice'})


class TestIjsonFromStreamAsync(unittest.IsolatedAsyncioTestCase):
    async def test_from_stream_async(self):
        data = {'u1': {'name': 'Alice'}}

        async def source():
            yield json.dumps(data).encode()

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'data.json'
            store = await IjsonFileStore.from_stream(source(), p)
            self.assertIsInstance(store, IjsonFileStore)
            self.assertEqual(store.read('u1'), {'name': 'Alice'})

    async def test_from_stream_async_gzip(self):
        data = {'u1': {'name': 'Alice'}}
        content = _gzip_bytes(json.dumps(data).encode())

        async def source():
            yield content

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'data.json.gz'
            store = await IjsonFileStore.from_stream(source(), p, compression='auto')
            self.assertEqual(store.read('u1'), {'name': 'Alice'})


if __name__ == '__main__':
    unittest.main()
