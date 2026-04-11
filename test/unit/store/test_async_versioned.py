import unittest

from oj_persistence.store.async_in_memory import AsyncInMemoryStore
from oj_persistence.store.async_versioned import AsyncVersionedStore


def _make_store() -> AsyncVersionedStore:
    return AsyncVersionedStore(AsyncInMemoryStore())


class TestAsyncVersionedStore(unittest.IsolatedAsyncioTestCase):
    async def test_upsert_then_read_latest(self):
        s = _make_store()
        await s.upsert('k', 'v1')
        self.assertEqual(await s.read_latest('k'), 'v1')

    async def test_read_is_alias_for_read_latest(self):
        s = _make_store()
        await s.upsert('k', 'v')
        self.assertEqual(await s.read('k'), await s.read_latest('k'))

    async def test_multiple_upserts_preserve_versions(self):
        s = _make_store()
        await s.upsert('k', 'v1')
        await s.upsert('k', 'v2')
        await s.upsert('k', 'v3')
        self.assertEqual(await s.read_latest('k'), 'v3')
        self.assertEqual(len(await s.list_versions('k')), 3)

    async def test_create_existing_key_raises(self):
        s = _make_store()
        await s.create('k', 1)
        with self.assertRaises(KeyError):
            await s.create('k', 2)

    async def test_update_missing_key_raises(self):
        with self.assertRaises(KeyError):
            await _make_store().update('missing', 'v')

    async def test_delete_removes_all_versions(self):
        s = _make_store()
        await s.upsert('k', 'v1')
        await s.upsert('k', 'v2')
        await s.delete('k')
        self.assertIsNone(await s.read_latest('k'))
        self.assertEqual(await s.list_versions('k'), [])

    async def test_list_returns_latest_per_key(self):
        s = _make_store()
        await s.upsert('a', 'a1')
        await s.upsert('a', 'a2')
        await s.upsert('b', 'b1')
        self.assertEqual(sorted(await s.list()), ['a2', 'b1'])

    async def test_list_versions_envelope_shape(self):
        s = _make_store()
        await s.upsert('k', {'x': 1})
        versions = await s.list_versions('k')
        env = versions[0]
        self.assertEqual(env['_key'], 'k')
        self.assertEqual(env['_value'], {'x': 1})
        self.assertIn('_seq', env)
        self.assertIn('_inserted_at', env)

    async def test_seq_increments_monotonically(self):
        s = _make_store()
        for i in range(4):
            await s.upsert('k', i)
        seqs = [v['_seq'] for v in await s.list_versions('k')]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), 4)


if __name__ == '__main__':
    unittest.main()
