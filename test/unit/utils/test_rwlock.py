"""
Unit tests for ReadWriteLock.

Key behavioural properties under test:
  1. Multiple readers can hold the lock simultaneously (shared mode).
  2. A writer waits for all active readers to finish.
  3. Readers wait for an active writer to finish.
  4. Only one writer can hold the lock at a time.
  5. No deadlock or starvation under concurrent load.

The barrier technique is used for property 1: two reader threads must both
reach a synchronisation point *while holding the read lock*. With an exclusive
lock only one thread can hold it at a time, so the second reader never reaches
the barrier → BrokenBarrierError. With a shared read lock both threads reach
the barrier and proceed.
"""
import threading
import time
import unittest

from oj_persistence.utils.rwlock import ReadWriteLock


class TestConcurrentReads(unittest.TestCase):
    def setUp(self):
        self.lock = ReadWriteLock()

    def test_two_readers_can_hold_lock_simultaneously(self):
        barrier = threading.Barrier(2, timeout=2.0)
        errors: list[Exception] = []

        def reader():
            try:
                with self.lock.read():
                    barrier.wait()  # both must arrive here while holding the read lock
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=reader)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join(timeout=3)
        t2.join(timeout=3)

        self.assertFalse(errors, f"Reads blocked each other: {errors}")

    def test_many_readers_can_hold_lock_simultaneously(self):
        n = 10
        barrier = threading.Barrier(n, timeout=2.0)
        errors: list[Exception] = []

        def reader():
            try:
                with self.lock.read():
                    barrier.wait()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3)

        self.assertFalse(errors)


class TestWriterExclusivity(unittest.TestCase):
    def setUp(self):
        self.lock = ReadWriteLock()

    def test_write_is_exclusive_no_concurrent_writers(self):
        inside_write = threading.Event()
        order: list[str] = []

        def writer_1():
            with self.lock.write():
                order.append('w1_start')
                inside_write.set()
                time.sleep(0.05)
                order.append('w1_end')

        def writer_2():
            inside_write.wait()  # make sure w1 holds lock before attempting
            with self.lock.write():
                order.append('w2_start')
                order.append('w2_end')

        t1 = threading.Thread(target=writer_1)
        t2 = threading.Thread(target=writer_2)
        t1.start()
        t2.start()
        t1.join(timeout=3)
        t2.join(timeout=3)

        # w1 must complete before w2 starts
        self.assertLess(order.index('w1_end'), order.index('w2_start'))

    def test_writer_blocks_new_readers(self):
        write_started = threading.Event()
        write_can_finish = threading.Event()
        order: list[str] = []

        def writer():
            with self.lock.write():
                order.append('write_start')
                write_started.set()
                write_can_finish.wait(timeout=2)
                order.append('write_end')

        def reader():
            write_started.wait(timeout=2)  # wait until write is in progress
            with self.lock.read():
                order.append('read')

        t_w = threading.Thread(target=writer)
        t_r = threading.Thread(target=reader)
        t_w.start()
        t_r.start()

        time.sleep(0.05)  # give reader time to block on the write lock
        write_can_finish.set()
        t_w.join(timeout=3)
        t_r.join(timeout=3)

        self.assertLess(order.index('write_end'), order.index('read'))


class TestReadersBlockWriter(unittest.TestCase):
    def setUp(self):
        self.lock = ReadWriteLock()

    def test_writer_waits_for_active_readers(self):
        read_started = threading.Event()
        read_can_finish = threading.Event()
        order: list[str] = []

        def reader():
            with self.lock.read():
                order.append('read_start')
                read_started.set()
                read_can_finish.wait(timeout=2)
                order.append('read_end')

        def writer():
            read_started.wait(timeout=2)  # wait until read is in progress
            with self.lock.write():
                order.append('write')

        t_r = threading.Thread(target=reader)
        t_w = threading.Thread(target=writer)
        t_r.start()
        t_w.start()

        time.sleep(0.05)  # give writer time to block on the read lock
        read_can_finish.set()
        t_r.join(timeout=3)
        t_w.join(timeout=3)

        self.assertLess(order.index('read_end'), order.index('write'))


class TestNoDeadlock(unittest.TestCase):
    def setUp(self):
        self.lock = ReadWriteLock()

    def test_concurrent_readers_and_writers_complete_without_deadlock(self):
        errors: list[Exception] = []
        counter = [0]

        def reader():
            try:
                for _ in range(20):
                    with self.lock.read():
                        _ = counter[0]
            except Exception as e:
                errors.append(e)

        def writer():
            try:
                for _ in range(20):
                    with self.lock.write():
                        counter[0] += 1
            except Exception as e:
                errors.append(e)

        threads = (
            [threading.Thread(target=reader) for _ in range(5)] +
            [threading.Thread(target=writer) for _ in range(3)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(counter[0], 60)  # 3 writers x 20 increments


if __name__ == '__main__':
    unittest.main()
