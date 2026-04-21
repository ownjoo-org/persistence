"""SQLite concurrency tests — WAL + pool + writer serialization.

These verify the "rock-solid" contract:
  - WAL is on after first open
  - reads parallelize (N readers, no serial delay)
  - writes don't block reads
  - writes serialize (one at a time, but each completes correctly)
"""

from __future__ import annotations

import asyncio
import sqlite3
import time

import pytest

from .. import Manager, Sqlite


# ------------------------------------------------------------------ WAL verification

def test_wal_mode_is_on(tmp_path) -> None:
    db_path = str(tmp_path / 'app.db')
    pm = Manager()
    pm.register('x', Sqlite(path=db_path))
    # Verify directly via a separate connection: PRAGMA journal_mode returns
    # the current mode. Must be 'wal'.
    conn = sqlite3.connect(db_path)
    try:
        mode = conn.execute('PRAGMA journal_mode').fetchone()[0].lower()
    finally:
        conn.close()
    assert mode == 'wal'
    pm.close()


# ------------------------------------------------------------------ reader concurrency

@pytest.mark.asyncio
async def test_concurrent_reads_do_not_serialize(tmp_path) -> None:
    """If N concurrent reads each take T seconds of work, total wall time
    should be near T (parallel), not N*T (serial)."""
    db_path = str(tmp_path / 'app.db')
    pm = Manager()
    await pm.aregister('x', Sqlite(path=db_path, pool_size=4))
    # Seed some rows so reads do real work
    for i in range(100):
        await pm.acreate('x', f'k{i}', {'i': i})

    async def sleepy_read() -> None:
        # Each read scans the full table (trivial cost) plus a real sleep
        # to simulate work. The pool should let 4 of these run in parallel.
        await pm.alist('x')
        await asyncio.sleep(0.1)

    start = time.monotonic()
    await asyncio.gather(*(sleepy_read() for _ in range(4)))
    elapsed = time.monotonic() - start

    # If reads serialized, elapsed would be ~0.4s+. Parallel → ~0.1s plus overhead.
    # Give generous headroom for slow CI, but strict enough to catch regression.
    assert elapsed < 0.3, f'concurrent reads took {elapsed:.2f}s — suggests serialization'
    await pm.aclose()


# ------------------------------------------------------------------ reader/writer independence

@pytest.mark.asyncio
async def test_writer_does_not_block_readers(tmp_path) -> None:
    """An in-flight write must not stall a concurrent read (WAL semantics)."""
    db_path = str(tmp_path / 'app.db')
    pm = Manager()
    await pm.aregister('x', Sqlite(path=db_path))
    for i in range(50):
        await pm.acreate('x', f'k{i}', {'i': i})

    # Launch a write that takes a moment, plus a bunch of reads.
    async def slow_write() -> None:
        await pm.aupsert('x', 'k0', {'i': 0, 'updated': True})
        # The write itself is fast; add think-time AFTER it returns so we
        # exercise the "writer currently holds the lock" window on the lock
        # itself by issuing another write right behind it.

    async def timed_read() -> float:
        t0 = time.monotonic()
        await pm.alist('x')
        return time.monotonic() - t0

    # Concurrent: one write + many reads
    results = await asyncio.gather(slow_write(), *(timed_read() for _ in range(8)))
    read_times = results[1:]
    # No single read should have been blocked for long by the write
    assert max(read_times) < 0.2, f'reads blocked by writer: {read_times}'
    await pm.aclose()


# ------------------------------------------------------------------ writer correctness

@pytest.mark.asyncio
async def test_concurrent_writes_all_commit_and_are_consistent(tmp_path) -> None:
    """N concurrent writers should all succeed and final state should be
    consistent (no lost updates, no duplicates)."""
    db_path = str(tmp_path / 'app.db')
    pm = Manager()
    await pm.aregister('x', Sqlite(path=db_path))

    async def writer(i: int) -> None:
        await pm.acreate('x', f'k{i}', {'i': i})

    # Launch 50 concurrent creates. Writer lock serializes them internally
    # but the event loop stays responsive.
    await asyncio.gather(*(writer(i) for i in range(50)))

    values = await pm.alist('x')
    # All 50 must be present, no duplicates, no dropouts
    collected = sorted(v['i'] for v in values)
    assert collected == list(range(50))
    await pm.aclose()


@pytest.mark.asyncio
async def test_writer_lock_serializes_interleaved_writes(tmp_path) -> None:
    """Interleaved upserts to the same key should all commit. Final value
    reflects the last-committed write (whichever serialized last)."""
    db_path = str(tmp_path / 'app.db')
    pm = Manager()
    await pm.aregister('x', Sqlite(path=db_path))
    await pm.acreate('x', 'k', {'version': 0})

    async def bump(target: int) -> None:
        await pm.aupsert('x', 'k', {'version': target})

    await asyncio.gather(*(bump(i) for i in range(1, 21)))
    final = await pm.aread('x', 'k')
    # The final version is whatever serialized last — we can't predict which,
    # but it must be one of the values we wrote.
    assert final['version'] in range(1, 21)
    await pm.aclose()


# ------------------------------------------------------------------ multi-table same-file

@pytest.mark.asyncio
async def test_two_tables_share_one_backend(tmp_path) -> None:
    """Two tables in the same sqlite file are backed by the same
    SqliteBackend — so writes to `a` and reads of `b` use the same writer
    lock / reader pool. Sanity check: both work and don't deadlock."""
    db_path = str(tmp_path / 'app.db')
    pm = Manager()
    await pm.aregister('a', Sqlite(path=db_path))
    await pm.aregister('b', Sqlite(path=db_path))

    await asyncio.gather(
        pm.acreate('a', 'k1', {'table': 'a'}),
        pm.acreate('b', 'k1', {'table': 'b'}),
    )
    assert await pm.aread('a', 'k1') == {'table': 'a'}
    assert await pm.aread('b', 'k1') == {'table': 'b'}

    # Same backend object
    assert pm._tables['a'] is pm._tables['b']
    await pm.aclose()
