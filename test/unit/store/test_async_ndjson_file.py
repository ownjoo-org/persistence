"""
Unit tests for AsyncNdjsonFileStore.

Design contract:
  - create/read/update/delete/list delegate immediately to the sync store via thread
  - upsert is buffered in memory; flush occurs on __aexit__ or when batch_size is reached
  - context manager manages the timer task lifecycle
  - sync_store exposes the underlying NdjsonFileStore for registration in PersistenceManager
  - timer flush writes buffered items to disk during idle pauses
"""

import tempfile
import unittest
from pathlib import Path

from oj_persistence.store.async_ndjson_file import AsyncNdjsonFileStore
from oj_persistence.store.ndjson_file import NdjsonFileStore


class TestAsyncNdjsonFileCRUDL(unittest.IsolatedAsyncioTestCase):
    """Non-buffered CRUDL operations — context manager not required."""

    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'data.ndjson'
        self.store = AsyncNdjsonFileStore(self.path)

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_create_then_read(self):
        await self.store.create('a', {'x': 1})
        self.assertEqual(await self.store.read('a'), {'x': 1})

    async def test_create_existing_key_raises(self):
        await self.store.create('a', 1)
        with self.assertRaises(KeyError):
            await self.store.create('a', 2)

    async def test_read_missing_key_returns_none(self):
        self.assertIsNone(await self.store.read('missing'))

    async def test_update_existing_key(self):
        await self.store.create('a', 'first')
        await self.store.update('a', 'second')
        self.assertEqual(await self.store.read('a'), 'second')

    async def test_update_missing_key_raises(self):
        with self.assertRaises(KeyError):
            await self.store.update('nonexistent', 'value')

    async def test_delete_removes_key(self):
        await self.store.create('a', 1)
        await self.store.delete('a')
        self.assertIsNone(await self.store.read('a'))

    async def test_delete_missing_key_is_noop(self):
        await self.store.delete('nonexistent')

    async def test_list_returns_all_values(self):
        await self.store.create('a', 1)
        await self.store.create('b', 2)
        self.assertEqual(sorted(await self.store.list()), [1, 2])

    async def test_list_with_predicate(self):
        await self.store.create('a', 10)
        await self.store.create('b', 20)
        await self.store.create('c', 5)
        self.assertEqual(sorted(await self.store.list(lambda v: v >= 10)), [10, 20])

    async def test_list_empty_store_returns_empty(self):
        self.assertEqual(await self.store.list(), [])


class TestAsyncNdjsonFileBuffering(unittest.IsolatedAsyncioTestCase):
    """Upsert buffering: items accumulate, flush writes to disk."""

    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'data.ndjson'

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_upsert_not_visible_until_flushed(self):
        """Buffered upsert is not on disk until context exit."""
        store = AsyncNdjsonFileStore(self.path, batch_size=100)
        async with store:
            await store.upsert('k', 'v')
            # read the sync store directly — should be empty until flush
            self.assertIsNone(store.sync_store.read('k'))
        # after __aexit__ the buffer is flushed
        self.assertEqual(store.sync_store.read('k'), 'v')

    async def test_context_exit_flushes_remaining_buffer(self):
        store = AsyncNdjsonFileStore(self.path, batch_size=100)
        async with store:
            await store.upsert('a', 1)
            await store.upsert('b', 2)
        self.assertEqual(sorted(store.sync_store.list()), [1, 2])

    async def test_batch_size_triggers_immediate_flush(self):
        """When buffer reaches batch_size items are written before __aexit__."""
        store = AsyncNdjsonFileStore(self.path, batch_size=3)
        async with store:
            await store.upsert('a', 1)
            await store.upsert('b', 2)
            # buffer not yet flushed (only 2 items)
            self.assertIsNone(store.sync_store.read('a'))
            await store.upsert('c', 3)
            # third item hits batch_size — flush fires inline
            self.assertEqual(store.sync_store.read('a'), 1)
            self.assertEqual(store.sync_store.read('b'), 2)
            self.assertEqual(store.sync_store.read('c'), 3)

    async def test_upsert_overwrites_existing_key_after_flush(self):
        store = AsyncNdjsonFileStore(self.path, batch_size=100)
        async with store:
            await store.upsert('k', 'first')
        async with store:
            await store.upsert('k', 'second')
        self.assertEqual(store.sync_store.read('k'), 'second')

    async def test_partial_batch_flushed_on_exit(self):
        """Fewer items than batch_size are still flushed on exit."""
        store = AsyncNdjsonFileStore(self.path, batch_size=50)
        async with store:
            for i in range(10):
                await store.upsert(str(i), i)
        self.assertEqual(sorted(store.sync_store.list()), list(range(10)))


class TestAsyncNdjsonFileLifecycle(unittest.IsolatedAsyncioTestCase):
    """Context manager and sync_store integration."""

    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tmpdir.name) / 'data.ndjson'

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_sync_store_is_ndjson_file_store(self):
        store = AsyncNdjsonFileStore(self.path)
        self.assertIsInstance(store.sync_store, NdjsonFileStore)

    async def test_sync_store_shares_same_path(self):
        store = AsyncNdjsonFileStore(self.path)
        async with store:
            await store.upsert('k', 'v')
        # verify via a fresh NdjsonFileStore pointed at the same path
        self.assertEqual(NdjsonFileStore(self.path).read('k'), 'v')

    async def test_multiple_context_manager_cycles(self):
        """Successive async with blocks accumulate data correctly."""
        store = AsyncNdjsonFileStore(self.path, batch_size=100)
        async with store:
            await store.upsert('a', 1)
        async with store:
            await store.upsert('b', 2)
        self.assertEqual(sorted(store.sync_store.list()), [1, 2])

    async def test_timer_flushes_buffer(self):
        """Timer fires and flushes non-empty buffer without waiting for __aexit__."""
        import asyncio
        store = AsyncNdjsonFileStore(self.path, batch_size=100, flush_interval=0.05)
        async with store:
            await store.upsert('k', 'v')
            self.assertIsNone(store.sync_store.read('k'))  # not yet flushed
            await asyncio.sleep(0.15)                      # let the timer fire
            self.assertEqual(store.sync_store.read('k'), 'v')


if __name__ == '__main__':
    unittest.main()
