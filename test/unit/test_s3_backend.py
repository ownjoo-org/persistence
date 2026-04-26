"""Tests for S3Backend using moto for offline mocking."""

from __future__ import annotations

import json
import os
import unittest

import boto3
from moto import mock_aws


BUCKET = 'test-pipeline-results'
REGION = 'us-east-1'


class TestS3BackendBasics(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ.setdefault('AWS_ACCESS_KEY_ID', 'testing')
        os.environ.setdefault('AWS_SECRET_ACCESS_KEY', 'testing')
        os.environ.setdefault('AWS_SECURITY_TOKEN', 'testing')
        os.environ.setdefault('AWS_SESSION_TOKEN', 'testing')
        os.environ.setdefault('AWS_DEFAULT_REGION', REGION)
        self._mock = mock_aws()
        self._mock.start()
        self._s3 = boto3.client('s3', region_name=REGION)
        self._s3.create_bucket(Bucket=BUCKET)

    def tearDown(self):
        self._mock.stop()

    async def _make_backend(self, prefix: str = 'test/'):
        from oj_persistence.backends.s3_backend import S3Backend
        backend = S3Backend(bucket=BUCKET, prefix=prefix, region=REGION)
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

    async def test_list_page_returns_all_values(self):
        b = await self._make_backend()
        for i in range(5):
            await b.aupsert('items', f'key{i}', {'n': i})
        page = await b.alist_page('items', 0, 10)
        self.assertEqual(len(page), 5)
        ns = {r['n'] for r in page}
        self.assertEqual(ns, {0, 1, 2, 3, 4})

    async def test_list_page_offset_and_limit(self):
        b = await self._make_backend()
        for i in range(10):
            await b.aupsert('items', f'key{i:02d}', {'n': i})
        page = await b.alist_page('items', 3, 4)
        self.assertEqual(len(page), 4)

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

    async def test_data_persists_to_s3(self):
        """After upsert, the NDJSON object exists in S3 under the expected key."""
        b = await self._make_backend(prefix='results/')
        await b.aupsert('repos', 'r1', {'name': 'my-repo'})

        obj = self._s3.get_object(Bucket=BUCKET, Key='results/repos.ndjson')
        content = obj['Body'].read().decode()
        records = [json.loads(line) for line in content.splitlines() if line.strip()]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['pk'], 'r1')
        self.assertEqual(records[0]['value']['name'], 'my-repo')

    async def test_empty_table_list_returns_empty(self):
        b = await self._make_backend()
        result = await b.alist_page('items', 0, 100)
        self.assertEqual(result, [])

    async def test_table_exists(self):
        b = await self._make_backend()
        self.assertTrue(await b.atable_exists('items'))
        self.assertFalse(await b.atable_exists('nonexistent'))

    async def test_manager_integration(self):
        """S3 spec wires through Manager.aregister → aupsert → alist_page."""
        from oj_persistence import Manager, S3
        pm = Manager()
        spec = S3(bucket=BUCKET, prefix='mgr/', region=REGION)
        await pm.aregister('repos', spec)
        await pm.aupsert('repos', '1', {'id': 1, 'name': 'repo-a'})
        await pm.aupsert('repos', '2', {'id': 2, 'name': 'repo-b'})
        results = await pm.alist_page('repos', 0, 10)
        self.assertEqual(len(results), 2)
