"""Tests for DynamoDbBackend using moto for offline mocking."""

from __future__ import annotations

import os
import unittest

import boto3
from moto import mock_aws


REGION = 'us-east-1'


class TestDynamoDbBackendBasics(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ.setdefault('AWS_ACCESS_KEY_ID', 'testing')
        os.environ.setdefault('AWS_SECRET_ACCESS_KEY', 'testing')
        os.environ.setdefault('AWS_SECURITY_TOKEN', 'testing')
        os.environ.setdefault('AWS_SESSION_TOKEN', 'testing')
        os.environ.setdefault('AWS_DEFAULT_REGION', REGION)
        self._mock = mock_aws()
        self._mock.start()

    def tearDown(self):
        self._mock.stop()

    async def _make_backend(self, prefix: str = 'test-'):
        from oj_persistence.backends.dynamodb_backend import DynamoDbBackend
        backend = DynamoDbBackend(region=REGION, prefix=prefix)
        await backend.aopen()
        await backend.acreate_table('items')
        return backend

    async def test_upsert_and_read(self):
        b = await self._make_backend()
        await b.aupsert('items', 'key1', {'name': 'Alice'})
        result = await b.aread('items', 'key1')
        self.assertEqual(result, {'name': 'Alice'})

    async def test_read_missing_key_returns_none(self):
        b = await self._make_backend()
        result = await b.aread('items', 'missing')
        self.assertIsNone(result)

    async def test_upsert_overwrites_existing(self):
        b = await self._make_backend()
        await b.aupsert('items', 'k', {'v': 1})
        await b.aupsert('items', 'k', {'v': 2})
        result = await b.aread('items', 'k')
        self.assertEqual(result['v'], 2)

    async def test_delete_removes_key(self):
        b = await self._make_backend()
        await b.aupsert('items', 'to_del', {'x': 1})
        await b.adelete('items', 'to_del')
        result = await b.aread('items', 'to_del')
        self.assertIsNone(result)

    async def test_alist_returns_all_values(self):
        b = await self._make_backend()
        await b.aupsert('items', 'a', {'v': 1})
        await b.aupsert('items', 'b', {'v': 2})
        values = await b.alist('items')
        self.assertEqual(len(values), 2)

    async def test_list_page_returns_items(self):
        b = await self._make_backend()
        for i in range(5):
            await b.aupsert('items', f'key{i}', {'n': i})
        page = await b.alist_page('items', 0, 10)
        self.assertEqual(len(page), 5)

    async def test_table_exists(self):
        b = await self._make_backend()
        self.assertTrue(await b.atable_exists('items'))
        self.assertFalse(await b.atable_exists('nonexistent'))

    async def test_explicit_credentials_accepted(self):
        """DynamoDbBackend works when explicit credentials are provided (moto intercepts them)."""
        from oj_persistence.backends.dynamodb_backend import DynamoDbBackend
        backend = DynamoDbBackend(
            region=REGION,
            prefix='creds-',
            aws_access_key_id='explicit-key',
            aws_secret_access_key='explicit-secret',
        )
        await backend.aopen()
        await backend.acreate_table('things')
        await backend.aupsert('things', 'k1', {'val': 42})
        result = await backend.aread('things', 'k1')
        self.assertEqual(result, {'val': 42})

    async def test_dynamodb_spec_credential_fields(self):
        """DynamoDB dataclass exposes credential fields and they flow through Manager."""
        from oj_persistence import Manager, DynamoDB
        spec = DynamoDB(
            region=REGION,
            prefix='creds-mgr-',
            aws_access_key_id='ak',
            aws_secret_access_key='sk',
        )
        self.assertEqual(spec.aws_access_key_id, 'ak')
        self.assertEqual(spec.aws_secret_access_key, 'sk')

        pm = Manager()
        await pm.aregister('items', spec)
        await pm.aupsert('items', 'x', {'n': 1})
        results = await pm.alist_page('items', 0, 10)
        self.assertEqual(len(results), 1)

    async def test_no_credentials_uses_ambient(self):
        """DynamoDB spec with no credentials defaults to None (ambient IAM/env)."""
        from oj_persistence import DynamoDB
        spec = DynamoDB(region=REGION)
        self.assertIsNone(spec.aws_access_key_id)
        self.assertIsNone(spec.aws_secret_access_key)

    async def test_manager_integration(self):
        """DynamoDB spec wires through Manager.aregister → aupsert → alist_page."""
        from oj_persistence import Manager, DynamoDB
        pm = Manager()
        spec = DynamoDB(region=REGION, prefix='mgr-')
        await pm.aregister('repos', spec)
        await pm.aupsert('repos', '1', {'id': 1, 'name': 'repo-a'})
        await pm.aupsert('repos', '2', {'id': 2, 'name': 'repo-b'})
        results = await pm.alist_page('repos', 0, 10)
        self.assertEqual(len(results), 2)
