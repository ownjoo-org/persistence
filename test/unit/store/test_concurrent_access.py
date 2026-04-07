"""
Concurrent read/write tests for AbstractStore implementations.

Two levels of verification:

1. True concurrency (InMemoryStore only)
   A _SlowDict is patched onto _data so that dict.get() pauses at a barrier.
   With a shared read lock both reader threads reach the barrier simultaneously.
   With an exclusive lock the second reader is blocked and never reaches the
   barrier → BrokenBarrierError → test fails.

2. Correctness under concurrent load (all stores)
   Many reader and writer threads run simultaneously; we verify:
     - No exceptions are raised.
     - Readers never see a partially-written record.
     - After all writers finish, readers see the final state.
"""
import tempfile
import threading
import time
import unittest
from pathlib import Path

from oj_persistence.store.csv_file import CsvFileStore
from oj_persistence.store.ijson_file import IjsonFileStore
from oj_persistence.store.in_memory import InMemoryStore
from oj_persistence.store.ndjson_file import NdjsonFileStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _SlowDict(dict):
    """
    Drop-in dict replacement that pauses inside get() at a barrier.

    Used to prove that two reader threads can both be inside a read operation
    simultaneously (i.e. the read lock is shared, not exclusive).
    """
    def __init__(self, *args, barrier: threading.Barrier, **kwargs):
        super().__init__(*args, **kwargs)
        self._barrier = barrier

    def get(self, key, *args):
        self._barrier.wait(timeout=2.0)
        return super().get(key, *args)


# Record shape used for partial-write detection
def _make_record(tag: int) -> dict:
    """All fields share the same tag value; any mix indicates a partial write."""
    return {f'f{i}': tag for i in range(8)}


def _is_consistent(record) -> bool:
    if record is None:
        return True
    values = list(record.values())
    return len(set(values)) == 1


# ---------------------------------------------------------------------------
# True concurrency: reads do not block each other
# ---------------------------------------------------------------------------

class TestConcurrentReadsDoNotBlockEachOther(unittest.TestCase):
    def test_two_readers_can_overlap_in_memory_store(self):
        store = InMemoryStore()
        store.create('k', 'v')

        barrier = threading.Barrier(2, timeout=2.0)
        store._data = _SlowDict(store._data, barrier=barrier)

        results: list = []
        errors: list[Exception] = []

        def reader():
            try:
                results.append(store.read('k'))
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=reader)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join(timeout=3)
        t2.join(timeout=3)

        self.assertFalse(errors, f"Reads blocked each other: {errors}")
        self.assertEqual(results, ['v', 'v'])


# ---------------------------------------------------------------------------
# Read/write correctness: consistent values, no deadlock
# ---------------------------------------------------------------------------

class TestReadWriteCorrectness:
    """Mixin — must be combined with unittest.TestCase in concrete subclasses."""

    __test__ = False  # prevent pytest from collecting the base mixin directly

    def test_reader_never_sees_partial_write(self):
        self.store.upsert('k', _make_record(0))
        stop = threading.Event()
        errors: list[str] = []

        def writer():
            tag = 0
            while not stop.is_set():
                tag = 1 - tag
                self.store.upsert('k', _make_record(tag))

        def reader():
            while not stop.is_set():
                v = self.store.read('k')
                if not _is_consistent(v):
                    errors.append(f'partial write detected: {v}')
                    stop.set()

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        time.sleep(0.2)
        stop.set()
        w.join(timeout=2)
        r.join(timeout=2)

        self.assertFalse(errors)

    def test_read_and_write_do_not_deadlock(self):
        self.store.upsert('k', 0)
        errors: list[Exception] = []

        def writer():
            for i in range(50):
                try:
                    self.store.upsert('k', i)
                except Exception as e:
                    errors.append(e)

        def reader():
            for _ in range(50):
                try:
                    self.store.read('k')
                except Exception as e:
                    errors.append(e)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        w.join(timeout=5)
        r.join(timeout=5)

        self.assertFalse(w.is_alive(), "Writer deadlocked")
        self.assertFalse(r.is_alive(), "Reader deadlocked")
        self.assertFalse(errors)

    def test_concurrent_readers_see_no_errors(self):
        for i in range(5):
            self.store.upsert(str(i), _make_record(i))

        errors: list[Exception] = []

        def reader():
            for _ in range(30):
                try:
                    self.store.list()
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertFalse(errors)

    def test_after_write_reader_sees_new_value(self):
        self.store.create('k', 'old')
        done = threading.Event()

        def writer():
            self.store.update('k', 'new')
            done.set()

        threading.Thread(target=writer).start()
        done.wait(timeout=2)

        self.assertEqual(self.store.read('k'), 'new')


# ---------------------------------------------------------------------------
# Concrete test classes — one per store type
# ---------------------------------------------------------------------------

class TestInMemoryStoreConcurrency(TestReadWriteCorrectness, unittest.TestCase):
    __test__ = True

    def setUp(self):
        self.store = InMemoryStore()


class TestNdjsonFileStoreConcurrency(TestReadWriteCorrectness, unittest.TestCase):
    __test__ = True

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.store = NdjsonFileStore(Path(self._tmpdir.name) / 'data.ndjson')

    def tearDown(self):
        self._tmpdir.cleanup()


class TestIjsonFileStoreConcurrency(TestReadWriteCorrectness, unittest.TestCase):
    __test__ = True

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.store = IjsonFileStore(Path(self._tmpdir.name) / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()


class TestCsvFileStoreConcurrency(TestReadWriteCorrectness, unittest.TestCase):
    __test__ = True

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.store = CsvFileStore(Path(self._tmpdir.name) / 'data.csv',
                                  fieldnames=[f'f{i}' for i in range(8)])

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_read_and_write_do_not_deadlock(self):
        row = {f'f{i}': '0' for i in range(8)}
        self.store.upsert('k', row)
        errors: list[Exception] = []

        def writer():
            for i in range(50):
                try:
                    self.store.upsert('k', {f'f{j}': str(i) for j in range(8)})
                except Exception as e:
                    errors.append(e)

        def reader():
            for _ in range(50):
                try:
                    self.store.read('k')
                except Exception as e:
                    errors.append(e)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        w.join(timeout=5)
        r.join(timeout=5)

        self.assertFalse(w.is_alive(), "Writer deadlocked")
        self.assertFalse(r.is_alive(), "Reader deadlocked")
        self.assertFalse(errors)

    # CSV values are string dicts — override the record helpers
    def test_reader_never_sees_partial_write(self):
        record_a = {f'f{i}': '0' for i in range(8)}
        record_b = {f'f{i}': '1' for i in range(8)}
        self.store.upsert('k', record_a)
        stop = threading.Event()
        errors: list[str] = []

        def writer():
            toggle = False
            while not stop.is_set():
                self.store.upsert('k', record_b if toggle else record_a)
                toggle = not toggle

        def reader():
            while not stop.is_set():
                v = self.store.read('k')
                if v is not None:
                    vals = set(v.values())
                    if len(vals) > 1:
                        errors.append(f'partial write: {v}')
                        stop.set()

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        time.sleep(0.2)
        stop.set()
        w.join(timeout=2)
        r.join(timeout=2)

        self.assertFalse(errors)

    def test_after_write_reader_sees_new_value(self):
        self.store.create('k', {'f0': 'old', **{f'f{i}': '' for i in range(1, 8)}})
        done = threading.Event()

        def writer():
            self.store.update('k', {f'f{i}': 'new' for i in range(8)})
            done.set()

        threading.Thread(target=writer).start()
        done.wait(timeout=2)

        self.assertEqual(self.store.read('k'), {f'f{i}': 'new' for i in range(8)})


if __name__ == '__main__':
    unittest.main()
