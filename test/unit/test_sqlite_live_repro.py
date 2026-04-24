"""Reproduce the exact failure pattern seen in fast-etl's live pipeline.

Conditions that matter for the bug:

1. Manager registers a table that doesn't yet exist (acreate_table on the backend).
2. Before any writes, an ``alist_page`` is issued against a DIFFERENT table that
   *also* doesn't exist yet — in the live system this is fast-etl's ``/results``
   poll firing while stage-1 runs and the collector's results table hasn't been
   created yet. That read fails silently (OperationalError caught upstream by
   fast-etl's DiskCollector, returned as []).
3. Writes land in the first table.
4. A fresh ``alist_page`` against the first table should see the writes.

What the live system does in between step 2 and step 4 is exactly what
SQLite readers find confusing — a pre-existing reader connection pulled
from the pool saw the database in an empty state, and the Python sqlite3
wrapper may or may not end that implicit read transaction before the next
SELECT on the same connection.

These tests prove the Manager's contract holds under that sequence. If
any of them fail, fast-etl's staged pipeline fails in the same way.
"""

from __future__ import annotations

import asyncio
import pathlib
import sqlite3
import tempfile
import unittest

from oj_persistence import Manager, Sqlite
from oj_persistence.backends.sqlite_backend import SqliteBackend


class TestSqliteLiveRepro(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        from oj_persistence import Manager
        Manager._reset()
        self._tmp = tempfile.mkdtemp()
        self.tmp_path = pathlib.Path(self._tmp)

    def tearDown(self):
        from oj_persistence import Manager
        Manager._reset()
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    async def test_reader_after_failed_read_on_missing_table(self):
        """Reader pool survives a failed read on a missing table, still sees later writes."""
        path = str(self.tmp_path / 'db.sqlite')
        pm = Manager()
        await pm.aregister('users', Sqlite(path=path))

        backend: SqliteBackend = pm._tables['users']  # type: ignore[assignment]
        with self.assertRaises(sqlite3.OperationalError):
            await backend.alist_page('ghost', 0, 100)

        for i in range(5):
            await pm.acreate('users', str(i), {'i': i})

        rows = await pm.alist_page('users', 0, 100)
        self.assertEqual(len(rows), 5)
        await pm.aclose()

    async def test_reader_after_poll_on_empty_table(self):
        """Reader sees writes even after repeated reads on the same empty table."""
        path = str(self.tmp_path / 'db.sqlite')
        pm = Manager()
        await pm.aregister('users', Sqlite(path=path))

        for _ in range(10):
            rows = await pm.alist_page('users', 0, 100)
            self.assertEqual(rows, [])

        for i in range(5):
            await pm.acreate('users', str(i), {'i': i})

        rows = await pm.alist_page('users', 0, 100)
        self.assertEqual(len(rows), 5, f'reader stuck in old snapshot; saw {len(rows)} rows, expected 5')
        await pm.aclose()

    async def test_reader_sees_writes_under_concurrent_poll(self):
        """Simulate UI polling /results concurrent with stage-1 writes."""
        path = str(self.tmp_path / 'db.sqlite')
        pm = Manager()
        await pm.aregister('users', Sqlite(path=path))

        stop = asyncio.Event()

        async def poller() -> None:
            while not stop.is_set():
                await pm.alist_page('users', 0, 100)
                await asyncio.sleep(0.01)

        poll_task = asyncio.create_task(poller())

        for i in range(20):
            await pm.acreate('users', str(i), {'i': i})

        stop.set()
        await poll_task

        for _ in range(5):
            rows = await pm.alist_page('users', 0, 100)
            self.assertEqual(len(rows), 20)
        await pm.aclose()

    async def test_multi_table_stage_reader_after_writes(self):
        """Closest replica of fast-etl's staged pipeline failure."""
        path = str(self.tmp_path / 'db.sqlite')
        pm = Manager()
        await pm.aregister('characters', Sqlite(path=path))
        await pm.aregister('locations', Sqlite(path=path))
        await pm.aregister('episodes', Sqlite(path=path))

        for name in ('rick-and-morty-staged__results',) * 10:
            try:
                await pm.alist_page(name, 0, 100)
            except Exception:
                pass

        async def populate(table: str) -> None:
            for i in range(20):
                await pm.acreate(table, str(i), {'table': table, 'i': i})

        await asyncio.gather(populate('characters'), populate('locations'), populate('episodes'))

        for table in ('characters', 'locations', 'episodes'):
            rows = await pm.alist_page(table, 0, 100)
            self.assertEqual(len(rows), 20, f'{table}: reader returned {len(rows)} rows, expected 20')

        await pm.aclose()

    async def test_raw_backend_reader_pool_after_writes(self):
        """Same as above but against SqliteBackend directly."""
        path = str(self.tmp_path / 'db.sqlite')
        backend = SqliteBackend(path=path, pool_size=4)
        await backend.aopen()
        await backend.acreate_table('users')

        for _ in range(8):
            self.assertEqual(await backend.alist_page('users', 0, 100), [])

        for i in range(5):
            await backend.acreate('users', str(i), {'i': i})

        for _ in range(8):
            rows = await backend.alist_page('users', 0, 100)
            self.assertEqual(len(rows), 5, f'backend reader saw {len(rows)} rows, expected 5')

        await backend.aclose()


if __name__ == '__main__':
    unittest.main()
