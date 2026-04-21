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
import sqlite3

import pytest

from oj_persistence import Manager, Sqlite
from oj_persistence.backends.sqlite_backend import SqliteBackend


# ------------------------------------------------------------------ via the manager

@pytest.mark.asyncio
async def test_reader_after_failed_read_on_missing_table(tmp_path) -> None:
    """Reader pool survives a failed read on a missing table, still sees later writes."""
    path = str(tmp_path / 'db.sqlite')
    pm = Manager()
    await pm.aregister('users', Sqlite(path=path))

    # Step 1: try to read a table that hasn't been registered/created (simulates
    # fast-etl's /results polling before Pipeline.run() gets to collector.configure()).
    backend: SqliteBackend = pm._tables['users']  # type: ignore[assignment]
    # Directly invoke list_page against a non-existent table — raw, like the
    # UI-triggered race. Should raise; we catch.
    with pytest.raises(sqlite3.OperationalError):
        await backend.alist_page('ghost', 0, 100)

    # Step 2: write 5 rows to 'users' (same backend, same file, same pool).
    for i in range(5):
        await pm.acreate('users', str(i), {'i': i})

    # Step 3: read — must see all 5. If the reader pool's connection was
    # "poisoned" by the failed step-1 read, this would return 0.
    rows = await pm.alist_page('users', 0, 100)
    assert len(rows) == 5
    await pm.aclose()


@pytest.mark.asyncio
async def test_reader_after_poll_on_empty_table(tmp_path) -> None:
    """Reader sees writes even after repeated reads on the same table while it was empty."""
    path = str(tmp_path / 'db.sqlite')
    pm = Manager()
    await pm.aregister('users', Sqlite(path=path))

    # Polls on the empty table — each borrows/returns a pool reader.
    for _ in range(10):
        rows = await pm.alist_page('users', 0, 100)
        assert rows == []

    # Writer populates.
    for i in range(5):
        await pm.acreate('users', str(i), {'i': i})

    # Readers must now see all 5.
    rows = await pm.alist_page('users', 0, 100)
    assert len(rows) == 5, f'reader stuck in old snapshot; saw {len(rows)} rows, expected 5'
    await pm.aclose()


@pytest.mark.asyncio
async def test_reader_sees_writes_under_concurrent_poll(tmp_path) -> None:
    """Simulate UI polling /results concurrent with stage-1 writes."""
    path = str(tmp_path / 'db.sqlite')
    pm = Manager()
    await pm.aregister('users', Sqlite(path=path))

    stop = asyncio.Event()

    async def poller() -> None:
        while not stop.is_set():
            await pm.alist_page('users', 0, 100)
            await asyncio.sleep(0.01)

    poll_task = asyncio.create_task(poller())

    # Writer ramps up mid-poll.
    for i in range(20):
        await pm.acreate('users', str(i), {'i': i})

    stop.set()
    await poll_task

    # After writes settle, every subsequent poll must see the full 20 rows.
    for _ in range(5):
        rows = await pm.alist_page('users', 0, 100)
        assert len(rows) == 20
    await pm.aclose()


@pytest.mark.asyncio
async def test_multi_table_stage_reader_after_writes(tmp_path) -> None:
    """Closest replica of fast-etl's staged pipeline failure: read across multiple
    tables after writes by the 'writer' counterpart."""
    path = str(tmp_path / 'db.sqlite')
    pm = Manager()
    await pm.aregister('characters', Sqlite(path=path))
    await pm.aregister('locations', Sqlite(path=path))
    await pm.aregister('episodes', Sqlite(path=path))

    # Simulate UI polling different non-existent tables (poisons the pool).
    for name in ('rick-and-morty-staged__results',) * 10:
        try:
            await pm.alist_page(name, 0, 100)
        except (sqlite3.OperationalError, Exception):
            pass

    # Stage-1 writes concurrently to the three tables.
    async def populate(table: str) -> None:
        for i in range(20):
            await pm.acreate(table, str(i), {'table': table, 'i': i})

    await asyncio.gather(populate('characters'), populate('locations'), populate('episodes'))

    # Stage-2 reads from each. This is where fast-etl gets out=0.
    for table in ('characters', 'locations', 'episodes'):
        rows = await pm.alist_page(table, 0, 100)
        assert len(rows) == 20, f'{table}: reader returned {len(rows)} rows, expected 20'

    await pm.aclose()


# ------------------------------------------------------------------ raw-backend test bypassing the Manager

@pytest.mark.asyncio
async def test_raw_backend_reader_pool_after_writes(tmp_path) -> None:
    """Same as above but against SqliteBackend directly — isolates whether the
    bug is in the backend's pool handling vs something in Manager."""
    path = str(tmp_path / 'db.sqlite')
    backend = SqliteBackend(path=path, pool_size=4)
    await backend.aopen()
    await backend.acreate_table('users')

    # Use each pool reader at least once with an empty-result query.
    for _ in range(8):
        assert await backend.alist_page('users', 0, 100) == []

    # Writer puts 5 rows.
    for i in range(5):
        await backend.acreate('users', str(i), {'i': i})

    # Each subsequent read must see all 5.
    for _ in range(8):
        rows = await backend.alist_page('users', 0, 100)
        assert len(rows) == 5, f'backend reader saw {len(rows)} rows, expected 5'

    await backend.aclose()
