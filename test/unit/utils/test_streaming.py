import io
import os
import tempfile
import unittest
from pathlib import Path

from oj_persistence.utils.streaming import stream_to_file, stream_to_file_sync


async def _async_iter(chunks):
    for chunk in chunks:
        yield chunk


class TestStreamToFile(unittest.IsolatedAsyncioTestCase):

    async def test_writes_bytes_from_async_iterable(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            await stream_to_file(_async_iter([b'hello ', b'world']), dest)
            self.assertEqual(dest.read_bytes(), b'hello world')

    async def test_writes_bytes_from_sync_iterable(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            await stream_to_file(iter([b'foo', b'bar']), dest)
            self.assertEqual(dest.read_bytes(), b'foobar')

    async def test_writes_bytes_from_file_like(self):
        src = io.BytesIO(b'A' * 200_000)
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            await stream_to_file(src, dest, chunk_size=65536)
            self.assertEqual(dest.read_bytes(), b'A' * 200_000)

    async def test_returns_final_path(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            result = await stream_to_file(_async_iter([b'x']), dest)
            self.assertEqual(result, dest)

    async def test_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'a' / 'b' / 'out.bin'
            await stream_to_file(_async_iter([b'x']), dest)
            self.assertTrue(dest.exists())

    async def test_temp_cleaned_on_failure(self):
        async def bad():
            yield b'partial'
            raise IOError('simulated failure')

        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            with self.assertRaises(IOError):
                await stream_to_file(bad(), dest)
            self.assertEqual(list(Path(d).glob('*.tmp')), [])
            self.assertFalse(dest.exists())

    async def test_debug_flag_leaves_temp_on_failure(self):
        async def bad():
            yield b'partial'
            raise IOError('simulated failure')

        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            with self.assertRaises(IOError):
                await stream_to_file(bad(), dest, debug=True)
            tmp_files = list(Path(d).glob('*.tmp'))
            self.assertEqual(len(tmp_files), 1)
            self.assertEqual(tmp_files[0].read_bytes(), b'partial')

    async def test_env_var_debug_leaves_temp_on_failure(self):
        async def bad():
            yield b'data'
            raise IOError('simulated failure')

        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            os.environ['OJ_PERSISTENCE_DEBUG'] = '1'
            try:
                with self.assertRaises(IOError):
                    await stream_to_file(bad(), dest)
                self.assertEqual(len(list(Path(d).glob('*.tmp'))), 1)
            finally:
                del os.environ['OJ_PERSISTENCE_DEBUG']

    async def test_two_concurrent_failures_get_unique_temps(self):
        import asyncio

        async def bad():
            yield b'data'
            raise IOError('simulated failure')

        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            results = await asyncio.gather(
                stream_to_file(bad(), dest, debug=True),
                stream_to_file(bad(), dest, debug=True),
                return_exceptions=True,
            )
            self.assertTrue(all(isinstance(r, IOError) for r in results))
            self.assertEqual(len(list(Path(d).glob('*.tmp'))), 2)


class TestStreamToFileSync(unittest.TestCase):

    def test_writes_bytes_from_iterable(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            stream_to_file_sync(iter([b'hello ', b'world']), dest)
            self.assertEqual(dest.read_bytes(), b'hello world')

    def test_writes_bytes_from_file_like(self):
        src = io.BytesIO(b'B' * 200_000)
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            stream_to_file_sync(src, dest, chunk_size=65536)
            self.assertEqual(dest.read_bytes(), b'B' * 200_000)

    def test_returns_final_path(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            result = stream_to_file_sync([b'x'], dest)
            self.assertEqual(result, dest)

    def test_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'sub' / 'out.bin'
            stream_to_file_sync([b'x'], dest)
            self.assertTrue(dest.exists())

    def test_temp_cleaned_on_failure(self):
        def bad():
            yield b'partial'
            raise IOError('simulated failure')

        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            with self.assertRaises(IOError):
                stream_to_file_sync(bad(), dest)
            self.assertEqual(list(Path(d).glob('*.tmp')), [])
            self.assertFalse(dest.exists())

    def test_debug_flag_leaves_temp_on_failure(self):
        def bad():
            yield b'partial'
            raise IOError('simulated failure')

        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            with self.assertRaises(IOError):
                stream_to_file_sync(bad(), dest, debug=True)
            tmp_files = list(Path(d).glob('*.tmp'))
            self.assertEqual(len(tmp_files), 1)
            self.assertEqual(tmp_files[0].read_bytes(), b'partial')

    def test_env_var_debug_leaves_temp_on_failure(self):
        def bad():
            yield b'data'
            raise IOError('simulated failure')

        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / 'out.bin'
            os.environ['OJ_PERSISTENCE_DEBUG'] = 'true'
            try:
                with self.assertRaises(IOError):
                    stream_to_file_sync(bad(), dest)
                self.assertEqual(len(list(Path(d).glob('*.tmp'))), 1)
            finally:
                del os.environ['OJ_PERSISTENCE_DEBUG']
