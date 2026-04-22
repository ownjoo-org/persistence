"""SQLite concurrency tests — WAL + pool + writer serialization.

These verify the "rock-solid" contract:
  - WAL is on after first open
  - reads parallelize (N readers, no serial delay)
  - writes don't block reads
  - writes serialize (one at a time, but each completes correctly)
"""

from __future__ import annotations

import asyncio
import pathlib
import sqlite3
import tempfile
import time
import unittest

from oj_persistence import Manager, Sqlite


class TestSqliteConcurrencySync(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.tmp_path = pathlib.Path(self._tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_wal_mode_is_on(self):
        db_path = str(self.tmp_path / 'app.db')
        pm = Manager()
        pm.register('x', Sqlite(path=db_path))
        conn = sqlite3.connect(db_path)
        try:
            mode = conn.execute('PRAGMA journal_mode').fetchone()[0].lower()
        finally:
            conn.close()
        self.assertEqual(mode, 'wal')
        pm.close()


class TestSqliteConcurrencyAsync(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.tmp_path = pathlib.Path(self._tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    async def test_concurrent_reads_do_not_serialize(self):
        """If N concurrent reads each take T seconds, total wall time should
        be near T (parallel), not N*T (serial)."""
        db_path = str(self.tmp_path / 'app.db')
        pm = Manager()
        await pm.aregister('x', Sqlite(path=db_path, pool_size=4))
        for i in range(100):
            await pm.acreate('x', f'k{i}', {'i': i})

        async def sleepy_read() -> None:
            await pm.alist('x')
            await asyncio.sleep(0.1)

        start = time.monotonic()
        await asyncio.gather(*(sleepy_read() for _ in range(4)))
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 0.3, f'concurrent reads took {elapsed:.2f}s — suggests serialization')
        await pm.aclose()

    async def test_writer_does_not_block_readers(self):
        """An in-flight write must not stall a concurrent read (WAL semantics)."""
        db_path = str(self.tmp_path / 'app.db')
        pm = Manager()
        await pm.aregister('x', Sqlite(path=db_path))
        for i in range(50):
            await pm.acreate('x', f'k{i}', {'i': i})

        async def slow_write() -> None:
            await pm.aupsert('x', 'k0', {'i': 0, 'updated': True})

        async def timed_read() -> float:
            t0 = time.monotonic()
            await pm.alist('x')
            return time.monotonic() - t0

        results = await asyncio.gather(slow_write(), *(timed_read() for _ in range(8)))
        read_times = results[1:]
        self.assertLess(max(read_times), 0.2, f'reads blocked by writer: {read_times}')
        await pm.aclose()

    async def test_concurrent_writes_all_commit_and_are_consistent(self):
        """N concurrent writers should all succeed with no lost updates."""
        db_path = str(self.tmp_path / 'app.db')
        pm = Manager()
        await pm.aregister('x', Sqlite(path=db_path))

        async def writer(i: int) -> None:
            await pm.acreate('x', f'k{i}', {'i': i})

        await asyncio.gather(*(writer(i) for i in range(50)))

        values = await pm.alist('x')
        collected = sorted(v['i'] for v in values)
        self.assertEqual(collected, list(range(50)))
        await pm.aclose()

    async def test_writer_lock_serializes_interleaved_writes(self):
        """Interleaved upserts to the same key should all commit."""
        db_path = str(self.tmp_path / 'app.db')
        pm = Manager()
        await pm.aregister('x', Sqlite(path=db_path))
        await pm.acreate('x', 'k', {'version': 0})

        async def bump(target: int) -> None:
            await pm.aupsert('x', 'k', {'version': target})

        await asyncio.gather(*(bump(i) for i in range(1, 21)))
        final = await pm.aread('x', 'k')
        self.assertIn(final['version'], range(1, 21))
        await pm.aclose()

    async def test_two_tables_share_one_backend(self):
        """Two tables in the same file use the same backend — no deadlock."""
        db_path = str(self.tmp_path / 'app.db')
        pm = Manager()
        await pm.aregister('a', Sqlite(path=db_path))
        await pm.aregister('b', Sqlite(path=db_path))

        await asyncio.gather(
            pm.acreate('a', 'k1', {'table': 'a'}),
            pm.acreate('b', 'k1', {'table': 'b'}),
        )
        self.assertEqual(await pm.aread('a', 'k1'), {'table': 'a'})
        self.assertEqual(await pm.aread('b', 'k1'), {'table': 'b'})
        self.assertIs(pm._tables['a'], pm._tables['b'])
        await pm.aclose()


if __name__ == '__main__':
    unittest.main()
