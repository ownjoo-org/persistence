"""
Tests for the upsert guard:
  - UpsertNotSupportedError exception class
  - supports_native_upsert property on every store class
  - PersistenceManager.upsert() blocks on inefficient stores unless allow_inefficient=True
  - AsyncPersistenceManager.upsert() same semantics
"""
import unittest

from oj_persistence.exceptions import UpsertNotSupportedError
from oj_persistence.store.async_base import AsyncAbstractStore
from oj_persistence.store.async_in_memory import AsyncInMemoryStore
from oj_persistence.store.async_ndjson_file import AsyncNdjsonFileStore
from oj_persistence.store.async_sqlite import AsyncSqliteStore
from oj_persistence.store.base import AbstractStore
from oj_persistence.store.csv_file import CsvFileStore
from oj_persistence.store.flat_file import FlatFileStore
from oj_persistence.store.in_memory import InMemoryStore
from oj_persistence.store.ndjson_file import NdjsonFileStore
from oj_persistence.store.sqlite import SqliteStore
from oj_persistence.store.versioned import VersionedStore


# ---------------------------------------------------------------------------
# Exception class
# ---------------------------------------------------------------------------

class TestUpsertNotSupportedError(unittest.TestCase):
    def test_is_exception(self):
        self.assertTrue(issubclass(UpsertNotSupportedError, Exception))

    def test_is_not_key_or_value_error(self):
        # Must be catchable specifically — not a subclass of built-in data errors
        self.assertFalse(issubclass(UpsertNotSupportedError, (KeyError, ValueError, TypeError)))

    def test_message_preserved(self):
        err = UpsertNotSupportedError("use create() instead")
        self.assertIn("use create() instead", str(err))


# ---------------------------------------------------------------------------
# supports_native_upsert — efficient stores (default True)
# ---------------------------------------------------------------------------

class TestSupportsNativeUpsertEfficient(unittest.TestCase):
    def test_base_class_has_property(self):
        # Verifies the property exists on the base class and concrete stores
        # that don't override it inherit True (InMemoryStore is the proxy)
        self.assertIn('supports_native_upsert', dir(AbstractStore))

    def test_in_memory_store(self):
        self.assertTrue(InMemoryStore().supports_native_upsert)

    def test_async_in_memory_store(self):
        self.assertTrue(AsyncInMemoryStore().supports_native_upsert)

    def test_sqlite_store(self):
        self.assertTrue(SqliteStore(':memory:').supports_native_upsert)

    def test_async_sqlite_store(self):
        self.assertTrue(AsyncSqliteStore(':memory:').supports_native_upsert)


# ---------------------------------------------------------------------------
# supports_native_upsert — file stores (False)
# ---------------------------------------------------------------------------

class TestSupportsNativeUpsertInefficient(unittest.TestCase):
    def test_flat_file_store(self, tmp_path=None):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            path = f.name
        try:
            store = FlatFileStore(path)
            self.assertFalse(store.supports_native_upsert)
        finally:
            os.unlink(path)

    def test_ndjson_file_store(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = NdjsonFileStore(path)
            self.assertFalse(store.supports_native_upsert)
        finally:
            os.unlink(path)

    def test_csv_file_store(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            path = f.name
        try:
            store = CsvFileStore(path, fieldnames=['key', 'value'])
            self.assertFalse(store.supports_native_upsert)
        finally:
            os.unlink(path)

    def test_async_ndjson_file_store(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = AsyncNdjsonFileStore(path)
            self.assertFalse(store.supports_native_upsert)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# VersionedStore delegates to inner store
# ---------------------------------------------------------------------------

class TestVersionedStoreDelegates(unittest.TestCase):
    def test_versioned_over_efficient_store_is_true(self):
        inner = InMemoryStore()
        self.assertTrue(VersionedStore(inner).supports_native_upsert)

    def test_versioned_over_ndjson_is_false(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            inner = NdjsonFileStore(path)
            self.assertFalse(VersionedStore(inner).supports_native_upsert)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# PersistenceManager.upsert() guard
# ---------------------------------------------------------------------------

from oj_persistence.manager import PersistenceManager


class TestSyncManagerUpsertGuard(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_upsert_efficient_store_succeeds(self):
        store = InMemoryStore()
        self.pm.register('mem', store)
        self.pm.upsert('mem', 'k', 'v')  # must not raise
        self.assertEqual(self.pm.read('mem', 'k'), 'v')

    def test_upsert_inefficient_store_raises(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = NdjsonFileStore(path)
            self.pm.register('ndjson', store)
            with self.assertRaises(UpsertNotSupportedError):
                self.pm.upsert('ndjson', 'k', 'v')
        finally:
            os.unlink(path)

    def test_upsert_inefficient_store_with_allow_inefficient_succeeds(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = NdjsonFileStore(path)
            self.pm.register('ndjson', store)
            self.pm.upsert('ndjson', 'k', {'x': 1}, allow_inefficient=True)
            self.assertEqual(self.pm.read('ndjson', 'k'), {'x': 1})
        finally:
            os.unlink(path)

    def test_upsert_error_message_names_store(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = NdjsonFileStore(path)
            self.pm.register('my_store', store)
            with self.assertRaises(UpsertNotSupportedError) as ctx:
                self.pm.upsert('my_store', 'k', 'v')
            self.assertIn('my_store', str(ctx.exception))
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# AsyncPersistenceManager.upsert() guard
# ---------------------------------------------------------------------------

from oj_persistence.async_manager import AsyncPersistenceManager


class TestAsyncManagerUpsertGuard(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_upsert_efficient_store_succeeds(self):
        store = AsyncInMemoryStore()
        self.pm.register('mem', store)
        await self.pm.upsert('mem', 'k', 'v')
        self.assertEqual(await self.pm.read('mem', 'k'), 'v')

    async def test_upsert_inefficient_store_raises(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = AsyncNdjsonFileStore(path)
            self.pm.register('ndjson', store)
            with self.assertRaises(UpsertNotSupportedError):
                await self.pm.upsert('ndjson', 'k', 'v')
        finally:
            os.unlink(path)

    async def test_upsert_inefficient_store_with_allow_inefficient_succeeds(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = AsyncNdjsonFileStore(path)
            self.pm.register('ndjson', store)
            # Use context manager to guarantee buffer flush before reading back
            async with store:
                await self.pm.upsert('ndjson', 'k', {'x': 1}, allow_inefficient=True)
            self.assertEqual(await self.pm.read('ndjson', 'k'), {'x': 1})
        finally:
            os.unlink(path)

    async def test_upsert_error_message_names_store(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.ndjson', delete=False) as f:
            path = f.name
        try:
            store = AsyncNdjsonFileStore(path)
            self.pm.register('my_store', store)
            with self.assertRaises(UpsertNotSupportedError) as ctx:
                await self.pm.upsert('my_store', 'k', 'v')
            self.assertIn('my_store', str(ctx.exception))
        finally:
            os.unlink(path)


if __name__ == '__main__':
    unittest.main()
