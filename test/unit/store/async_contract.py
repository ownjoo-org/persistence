"""
Shared async CRUDL contract for AsyncAbstractStore implementations.

Inherit AsyncStoreContract alongside unittest.IsolatedAsyncioTestCase and
set self.store in asyncSetUp.
"""


class AsyncStoreContract:
    """Mixin — must be combined with unittest.IsolatedAsyncioTestCase."""

    # -- create --

    async def test_create_then_read(self):
        await self.store.create('a', {'x': 1})
        self.assertEqual(await self.store.read('a'), {'x': 1})

    async def test_create_existing_key_raises(self):
        await self.store.create('a', 1)
        with self.assertRaises(KeyError):
            await self.store.create('a', 2)

    async def test_create_existing_key_does_not_overwrite(self):
        await self.store.create('a', 'original')
        with self.assertRaises(KeyError):
            await self.store.create('a', 'overwrite')
        self.assertEqual(await self.store.read('a'), 'original')

    # -- read --

    async def test_read_existing_key(self):
        await self.store.create('a', 42)
        self.assertEqual(await self.store.read('a'), 42)

    async def test_read_missing_key_returns_none(self):
        self.assertIsNone(await self.store.read('missing'))

    # -- update --

    async def test_update_existing_key(self):
        await self.store.create('a', 'first')
        await self.store.update('a', 'second')
        self.assertEqual(await self.store.read('a'), 'second')

    async def test_update_missing_key_raises(self):
        with self.assertRaises(KeyError):
            await self.store.update('nonexistent', 'value')

    async def test_update_missing_key_does_not_create(self):
        with self.assertRaises(KeyError):
            await self.store.update('nonexistent', 'value')
        self.assertIsNone(await self.store.read('nonexistent'))

    # -- upsert --

    async def test_upsert_creates_when_absent(self):
        await self.store.upsert('a', 'new')
        self.assertEqual(await self.store.read('a'), 'new')

    async def test_upsert_updates_when_present(self):
        await self.store.create('a', 'old')
        await self.store.upsert('a', 'new')
        self.assertEqual(await self.store.read('a'), 'new')

    # -- delete --

    async def test_delete_removes_key(self):
        await self.store.create('a', 1)
        await self.store.delete('a')
        self.assertIsNone(await self.store.read('a'))

    async def test_delete_missing_key_is_noop(self):
        await self.store.delete('nonexistent')

    # -- list --

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

    async def test_list_no_matches_returns_empty(self):
        await self.store.create('a', 1)
        self.assertEqual(await self.store.list(lambda v: v > 100), [])
