"""
Concurrent access tests for PersistenceManager.

These tests verify the correct usage model:
  - Threads share only the singleton manager, never a store directly.
  - The manager handles simultaneous requests from multiple threads.
  - Operations on the same store are safe under concurrent access.
  - Operations on different stores are fully independent (no cross-store blocking).
"""
import threading
import time
import unittest

from oj_persistence import InMemoryStore, PersistenceManager


class TestConcurrentReads(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()
        self.pm.get_or_create('store', lambda: InMemoryStore())

    def tearDown(self):
        PersistenceManager._instance = None

    def test_many_threads_read_simultaneously(self):
        self.pm.create('store', 'k', 'v')
        errors: list[Exception] = []
        results: list = []
        lock = threading.Lock()

        def reader():
            try:
                val = self.pm.read('store', 'k')
                with lock:
                    results.append(val)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors)
        self.assertTrue(all(r == 'v' for r in results))

    def test_concurrent_list_calls(self):
        for i in range(10):
            self.pm.create('store', str(i), i)
        errors: list[Exception] = []

        def reader():
            try:
                self.pm.list('store')
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors)


class TestConcurrentReadWrite(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()
        self.pm.get_or_create('store', lambda: InMemoryStore())

    def tearDown(self):
        PersistenceManager._instance = None

    def test_reader_and_writer_do_not_deadlock(self):
        self.pm.upsert('store', 'k', 0)
        errors: list[Exception] = []

        def writer():
            for i in range(50):
                try:
                    self.pm.upsert('store', 'k', i)
                except Exception as e:
                    errors.append(e)

        def reader():
            for _ in range(50):
                try:
                    self.pm.read('store', 'k')
                except Exception as e:
                    errors.append(e)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        w.join(timeout=5)
        r.join(timeout=5)

        self.assertFalse(w.is_alive(), 'writer deadlocked')
        self.assertFalse(r.is_alive(), 'reader deadlocked')
        self.assertFalse(errors)

    def test_reader_never_sees_partial_write(self):
        record_a = {f'f{i}': 0 for i in range(8)}
        record_b = {f'f{i}': 1 for i in range(8)}
        self.pm.upsert('store', 'k', record_a)

        stop = threading.Event()
        errors: list[str] = []

        def writer():
            toggle = False
            while not stop.is_set():
                self.pm.upsert('store', 'k', record_b if toggle else record_a)
                toggle = not toggle

        def reader():
            while not stop.is_set():
                v = self.pm.read('store', 'k')
                if v is not None and len(set(v.values())) > 1:
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

    def test_concurrent_upserts_produce_consistent_final_state(self):
        errors: list[Exception] = []

        def worker(key: str, value: int):
            try:
                self.pm.upsert('store', key, value)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(str(i), i)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors)
        self.assertEqual(sorted(self.pm.list('store')), list(range(20)))


class TestIndependentStores(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None

    def tearDown(self):
        PersistenceManager._instance = None

    def test_threads_on_different_stores_do_not_block_each_other(self):
        pm = PersistenceManager()
        pm.get_or_create('store_a', lambda: InMemoryStore())
        pm.get_or_create('store_b', lambda: InMemoryStore())

        order: list[str] = []
        lock = threading.Lock()
        write_a_started = threading.Event()
        write_a_can_finish = threading.Event()

        def slow_writer_a():
            write_a_started.set()
            write_a_can_finish.wait(timeout=2)
            pm.upsert('store_a', 'k', 'a')
            with lock:
                order.append('a_done')

        def writer_b():
            write_a_started.wait(timeout=2)
            pm.upsert('store_b', 'k', 'b')
            with lock:
                order.append('b_done')
            write_a_can_finish.set()

        t_a = threading.Thread(target=slow_writer_a)
        t_b = threading.Thread(target=writer_b)
        t_a.start()
        t_b.start()
        t_a.join(timeout=3)
        t_b.join(timeout=3)

        self.assertLess(order.index('b_done'), order.index('a_done'))

    def test_reads_on_one_store_do_not_block_writes_on_another(self):
        pm = PersistenceManager()
        pm.get_or_create('readers', lambda: InMemoryStore())
        pm.get_or_create('writers', lambda: InMemoryStore())
        for i in range(5):
            pm.create('readers', str(i), i)

        errors: list[Exception] = []

        def reader():
            for _ in range(30):
                try:
                    pm.list('readers')
                except Exception as e:
                    errors.append(e)

        def writer():
            for i in range(30):
                try:
                    pm.upsert('writers', str(i), i)
                except Exception as e:
                    errors.append(e)

        threads = (
            [threading.Thread(target=reader) for _ in range(5)] +
            [threading.Thread(target=writer) for _ in range(5)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertFalse(any(t.is_alive() for t in threads), 'deadlock detected')
        self.assertFalse(errors)


if __name__ == '__main__':
    unittest.main()
