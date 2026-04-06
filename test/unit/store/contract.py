"""
Shared CRUDL contract for AbstractStore implementations.

Inherit StoreContract alongside unittest.TestCase and set self.store in setUp.
Values used here are typed (int, dict) — CSV stores should not use this
mixin since CSV round-trips all field values as strings.
"""
import threading


class StoreContract:
    """Mixin — must be combined with unittest.TestCase in concrete subclasses."""

    # -- create --

    def test_create_then_read(self):
        self.store.create('a', {'x': 1})
        self.assertEqual(self.store.read('a'), {'x': 1})

    def test_create_existing_key_raises(self):
        self.store.create('a', 1)
        with self.assertRaises(KeyError):
            self.store.create('a', 2)

    def test_create_existing_key_does_not_overwrite(self):
        self.store.create('a', 'original')
        with self.assertRaises(KeyError):
            self.store.create('a', 'overwrite')
        self.assertEqual(self.store.read('a'), 'original')

    # -- read --

    def test_read_existing_key(self):
        self.store.create('a', 42)
        self.assertEqual(self.store.read('a'), 42)

    def test_read_missing_key_returns_none(self):
        self.assertIsNone(self.store.read('missing'))

    # -- update --

    def test_update_existing_key(self):
        self.store.create('a', 'first')
        self.store.update('a', 'second')
        self.assertEqual(self.store.read('a'), 'second')

    def test_update_missing_key_raises(self):
        with self.assertRaises(KeyError):
            self.store.update('nonexistent', 'value')

    def test_update_missing_key_does_not_create(self):
        with self.assertRaises(KeyError):
            self.store.update('nonexistent', 'value')
        self.assertIsNone(self.store.read('nonexistent'))

    # -- upsert --

    def test_upsert_creates_when_absent(self):
        self.store.upsert('a', 'new')
        self.assertEqual(self.store.read('a'), 'new')

    def test_upsert_updates_when_present(self):
        self.store.create('a', 'old')
        self.store.upsert('a', 'new')
        self.assertEqual(self.store.read('a'), 'new')

    # -- delete --

    def test_delete_removes_key(self):
        self.store.create('a', 1)
        self.store.delete('a')
        self.assertIsNone(self.store.read('a'))

    def test_delete_missing_key_is_noop(self):
        self.store.delete('nonexistent')

    # -- list --

    def test_list_returns_all_values(self):
        self.store.create('a', 1)
        self.store.create('b', 2)
        self.assertEqual(sorted(self.store.list()), [1, 2])

    def test_list_with_predicate(self):
        self.store.create('a', 10)
        self.store.create('b', 20)
        self.store.create('c', 5)
        self.assertEqual(sorted(self.store.list(lambda v: v >= 10)), [10, 20])

    def test_list_empty_store_returns_empty(self):
        self.assertEqual(self.store.list(), [])

    def test_list_no_matches_returns_empty(self):
        self.store.create('a', 1)
        self.assertEqual(self.store.list(lambda v: v > 100), [])

    # -- concurrency --

    def test_concurrent_upserts_do_not_corrupt(self):
        errors: list[Exception] = []

        def worker(key: str, value: int) -> None:
            try:
                self.store.upsert(key, value)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(str(i), i)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors)
        self.assertEqual(sorted(self.store.list()), list(range(20)))
