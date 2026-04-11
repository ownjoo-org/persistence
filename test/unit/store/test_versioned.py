"""
Unit tests for VersionedStore.

Design contract:
  - Each write (create/update/upsert) appends a new version — old versions are preserved
  - read() / read_latest() return the most recent value for a key
  - list_versions(key) returns all envelopes, oldest first
  - list() returns the latest value per key, optionally filtered
  - Envelopes contain: _key, _seq (monotonically increasing int), _inserted_at (ISO-8601 UTC), _value
  - create() raises KeyError if any version already exists for that key
  - update() raises KeyError if no version exists for that key
  - delete() removes all versions; subsequent reads return None
"""
import unittest

from oj_persistence.store.in_memory import InMemoryStore
from oj_persistence.store.versioned import VersionedStore


def _make_store() -> VersionedStore:
    return VersionedStore(InMemoryStore())


class TestVersionedStoreCreate(unittest.TestCase):
    def test_create_then_read_latest(self):
        s = _make_store()
        s.create('k', {'x': 1})
        self.assertEqual(s.read_latest('k'), {'x': 1})

    def test_read_is_alias_for_read_latest(self):
        s = _make_store()
        s.create('k', 'v')
        self.assertEqual(s.read('k'), s.read_latest('k'))

    def test_create_existing_key_raises(self):
        s = _make_store()
        s.create('k', 1)
        with self.assertRaises(KeyError):
            s.create('k', 2)

    def test_create_does_not_overwrite_existing(self):
        s = _make_store()
        s.create('k', 'original')
        with self.assertRaises(KeyError):
            s.create('k', 'overwrite')
        self.assertEqual(s.read_latest('k'), 'original')

    def test_read_latest_missing_key_returns_none(self):
        self.assertIsNone(_make_store().read_latest('missing'))


class TestVersionedStoreUpsert(unittest.TestCase):
    def test_upsert_creates_first_version(self):
        s = _make_store()
        s.upsert('k', 'v1')
        self.assertEqual(s.read_latest('k'), 'v1')

    def test_upsert_appends_new_version(self):
        s = _make_store()
        s.upsert('k', 'v1')
        s.upsert('k', 'v2')
        s.upsert('k', 'v3')
        self.assertEqual(s.read_latest('k'), 'v3')

    def test_upsert_preserves_old_versions(self):
        s = _make_store()
        s.upsert('k', 'v1')
        s.upsert('k', 'v2')
        versions = s.list_versions('k')
        self.assertEqual(len(versions), 2)

    def test_seq_increments_monotonically(self):
        s = _make_store()
        for i in range(5):
            s.upsert('k', i)
        seqs = [v['_seq'] for v in s.list_versions('k')]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), 5)


class TestVersionedStoreUpdate(unittest.TestCase):
    def test_update_appends_new_version(self):
        s = _make_store()
        s.create('k', 'v1')
        s.update('k', 'v2')
        self.assertEqual(s.read_latest('k'), 'v2')
        self.assertEqual(len(s.list_versions('k')), 2)

    def test_update_missing_key_raises(self):
        with self.assertRaises(KeyError):
            _make_store().update('missing', 'v')

    def test_update_does_not_create(self):
        s = _make_store()
        with self.assertRaises(KeyError):
            s.update('missing', 'v')
        self.assertIsNone(s.read_latest('missing'))


class TestVersionedStoreDelete(unittest.TestCase):
    def test_delete_removes_all_versions(self):
        s = _make_store()
        s.upsert('k', 'v1')
        s.upsert('k', 'v2')
        s.delete('k')
        self.assertIsNone(s.read_latest('k'))
        self.assertEqual(s.list_versions('k'), [])

    def test_delete_missing_key_is_noop(self):
        _make_store().delete('missing')

    def test_delete_only_affects_target_key(self):
        s = _make_store()
        s.upsert('a', 1)
        s.upsert('b', 2)
        s.delete('a')
        self.assertIsNone(s.read_latest('a'))
        self.assertEqual(s.read_latest('b'), 2)


class TestVersionedStoreListVersions(unittest.TestCase):
    def test_list_versions_empty_for_unknown_key(self):
        self.assertEqual(_make_store().list_versions('missing'), [])

    def test_list_versions_sorted_oldest_first(self):
        s = _make_store()
        for v in ['a', 'b', 'c']:
            s.upsert('k', v)
        versions = s.list_versions('k')
        self.assertEqual([v['_value'] for v in versions], ['a', 'b', 'c'])

    def test_list_versions_envelope_shape(self):
        s = _make_store()
        s.upsert('k', {'data': 1})
        env = s.list_versions('k')[0]
        self.assertIn('_key', env)
        self.assertIn('_seq', env)
        self.assertIn('_inserted_at', env)
        self.assertIn('_value', env)
        self.assertEqual(env['_key'], 'k')
        self.assertEqual(env['_value'], {'data': 1})
        self.assertIsInstance(env['_seq'], int)

    def test_list_versions_inserted_at_is_iso8601(self):
        from datetime import datetime
        s = _make_store()
        s.upsert('k', 1)
        ts = s.list_versions('k')[0]['_inserted_at']
        # Should parse without error
        datetime.fromisoformat(ts)


class TestVersionedStoreList(unittest.TestCase):
    def test_list_returns_latest_per_key(self):
        s = _make_store()
        s.upsert('a', 'a1')
        s.upsert('a', 'a2')
        s.upsert('b', 'b1')
        self.assertEqual(sorted(s.list()), ['a2', 'b1'])

    def test_list_with_predicate(self):
        s = _make_store()
        s.upsert('a', {'score': 10})
        s.upsert('a', {'score': 50})   # latest
        s.upsert('b', {'score': 20})
        result = s.list(lambda v: v['score'] >= 20)
        self.assertEqual(sorted(r['score'] for r in result), [20, 50])

    def test_list_empty_store(self):
        self.assertEqual(_make_store().list(), [])

    def test_list_after_delete(self):
        s = _make_store()
        s.upsert('a', 1)
        s.upsert('b', 2)
        s.delete('a')
        self.assertEqual(s.list(), [2])


if __name__ == '__main__':
    unittest.main()
