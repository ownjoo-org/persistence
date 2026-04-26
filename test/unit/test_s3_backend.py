"""Tests for S3Backend using moto for offline mocking."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws


BUCKET = 'test-pipeline-results'
REGION = 'us-east-1'


def _setup_s3():
    s3 = boto3.client('s3', region_name=REGION)
    s3.create_bucket(Bucket=BUCKET)
    return s3


@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', REGION)


@pytest.fixture
def s3_bucket(aws_credentials):
    with mock_aws():
        s3 = boto3.client('s3', region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        yield s3


async def _make_backend(prefix: str = 'test/'):
    from oj_persistence.backends.s3_backend import S3Backend
    backend = S3Backend(bucket=BUCKET, prefix=prefix, region=REGION)
    await backend.aopen()
    await backend.acreate_table('items')
    return backend


@pytest.mark.asyncio
async def test_upsert_and_read(s3_bucket):
    b = await _make_backend()
    await b.aupsert('items', 'key1', {'name': 'Alice'})
    result = await b.aread('items', 'key1')
    assert result == {'name': 'Alice'}


@pytest.mark.asyncio
async def test_read_missing_key_returns_none(s3_bucket):
    b = await _make_backend()
    result = await b.aread('items', 'missing')
    assert result is None


@pytest.mark.asyncio
async def test_upsert_overwrites_existing(s3_bucket):
    b = await _make_backend()
    await b.aupsert('items', 'k', {'v': 1})
    await b.aupsert('items', 'k', {'v': 2})
    result = await b.aread('items', 'k')
    assert result['v'] == 2


@pytest.mark.asyncio
async def test_list_page_returns_all_values(s3_bucket):
    b = await _make_backend()
    for i in range(5):
        await b.aupsert('items', f'key{i}', {'n': i})
    page = await b.alist_page('items', 0, 10)
    assert len(page) == 5
    ns = {r['n'] for r in page}
    assert ns == {0, 1, 2, 3, 4}


@pytest.mark.asyncio
async def test_list_page_offset_and_limit(s3_bucket):
    b = await _make_backend()
    for i in range(10):
        await b.aupsert('items', f'key{i:02d}', {'n': i})
    page = await b.alist_page('items', 3, 4)
    assert len(page) == 4


@pytest.mark.asyncio
async def test_delete_removes_key(s3_bucket):
    b = await _make_backend()
    await b.aupsert('items', 'to_del', {'x': 1})
    await b.adelete('items', 'to_del')
    result = await b.aread('items', 'to_del')
    assert result is None


@pytest.mark.asyncio
async def test_alist_returns_all_values(s3_bucket):
    b = await _make_backend()
    await b.aupsert('items', 'a', {'v': 1})
    await b.aupsert('items', 'b', {'v': 2})
    values = await b.alist('items')
    assert len(values) == 2


@pytest.mark.asyncio
async def test_data_persists_to_s3(s3_bucket):
    """After upsert, the NDJSON object exists in S3 under the expected key."""
    b = await _make_backend(prefix='results/')
    await b.aupsert('repos', 'r1', {'name': 'my-repo'})

    obj = s3_bucket.get_object(Bucket=BUCKET, Key='results/repos.ndjson')
    content = obj['Body'].read().decode()
    records = [json.loads(line) for line in content.splitlines() if line.strip()]
    assert len(records) == 1
    assert records[0]['pk'] == 'r1'
    assert records[0]['value']['name'] == 'my-repo'


@pytest.mark.asyncio
async def test_empty_table_list_returns_empty(s3_bucket):
    b = await _make_backend()
    result = await b.alist_page('items', 0, 100)
    assert result == []


@pytest.mark.asyncio
async def test_table_exists(s3_bucket):
    b = await _make_backend()
    assert await b.atable_exists('items')
    assert not await b.atable_exists('nonexistent')


@pytest.mark.asyncio
async def test_manager_integration(s3_bucket):
    """S3 spec wires through Manager.aregister → aupsert → alist_page."""
    from oj_persistence import Manager, S3
    pm = Manager()
    spec = S3(bucket=BUCKET, prefix='mgr/', region=REGION)
    await pm.aregister('repos', spec)
    await pm.aupsert('repos', '1', {'id': 1, 'name': 'repo-a'})
    await pm.aupsert('repos', '2', {'id': 2, 'name': 'repo-b'})
    results = await pm.alist_page('repos', 0, 10)
    assert len(results) == 2
