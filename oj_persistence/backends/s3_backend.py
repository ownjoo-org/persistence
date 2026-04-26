"""S3 backend — NDJSON per table, buffered in memory and flushed to S3.

Each logical table maps to ``{prefix}{table}.ndjson`` in the S3 bucket.
Records are stored as newline-delimited JSON: ``{"pk": key, "value": <any>}``.

Operations buffer the table in memory.  Every mutating call (upsert, delete)
re-serializes the table and puts the object to S3 so data is durable after
each write.  ``aopen`` / ``acreate_table`` are lightweight — the NDJSON object
is created lazily on first write.

This makes it suitable for small-to-medium tables (hundreds of records) where
write throughput is low and portability / zero-infrastructure matter.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ..base import Backend, Capability


class S3Backend(Backend):
    capabilities = frozenset({Capability.PAGINATION})

    def __init__(self, bucket: str, prefix: str = '', region: str = 'us-east-1') -> None:
        self._bucket = bucket
        self._prefix = prefix.rstrip('/') + '/' if prefix else ''
        self._region = region
        self._client = None
        self._data: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _get_client(self):
        if self._client is None:
            self._client = boto3.client('s3', region_name=self._region)
        return self._client

    def _key(self, table: str) -> str:
        return f'{self._prefix}{table}.ndjson'

    # ------------------------------------------------------------------ sync internals

    def _read_table_sync(self, table: str) -> dict[str, Any]:
        with self._lock:
            if table in self._data:
                return self._data[table]
        try:
            obj = self._get_client().get_object(Bucket=self._bucket, Key=self._key(table))
            content = obj['Body'].read().decode('utf-8')
            records: dict[str, Any] = {}
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                records[entry['pk']] = entry['value']
        except ClientError as e:
            if e.response['Error']['Code'] in ('NoSuchKey', '404'):
                records = {}
            else:
                raise
        with self._lock:
            self._data.setdefault(table, records)
            return self._data[table]

    def _flush_sync(self, table: str) -> None:
        with self._lock:
            records = self._data.get(table, {})
        ndjson = ''.join(
            json.dumps({'pk': k, 'value': v}) + '\n'
            for k, v in records.items()
        )
        self._get_client().put_object(
            Bucket=self._bucket,
            Key=self._key(table),
            Body=ndjson.encode('utf-8'),
            ContentType='application/x-ndjson',
        )

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        pass

    async def aclose(self) -> None:
        with self._lock:
            self._data.clear()

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        with self._lock:
            self._data.setdefault(table, {})

    async def adrop_table(self, table: str) -> None:
        with self._lock:
            self._data.pop(table, None)
        try:
            await asyncio.to_thread(
                self._get_client().delete_object,
                Bucket=self._bucket,
                Key=self._key(table),
            )
        except ClientError:
            pass

    async def atruncate_table(self, table: str) -> None:
        with self._lock:
            self._data[table] = {}
        await asyncio.to_thread(self._flush_sync, table)

    async def atable_exists(self, table: str) -> bool:
        with self._lock:
            if table in self._data:
                return True
        try:
            await asyncio.to_thread(
                self._get_client().head_object,
                Bucket=self._bucket,
                Key=self._key(table),
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
                return False
            raise

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        def _create():
            records = self._read_table_sync(table)
            with self._lock:
                if key in records:
                    raise KeyError(key)
                records[key] = value
            self._flush_sync(table)
        await asyncio.to_thread(_create)

    async def aread(self, table: str, key: str) -> Any | None:
        records = await asyncio.to_thread(self._read_table_sync, table)
        return records.get(key)

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        def _update():
            records = self._read_table_sync(table)
            with self._lock:
                if key not in records:
                    raise KeyError(key)
                records[key] = value
            self._flush_sync(table)
        await asyncio.to_thread(_update)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        def _upsert():
            records = self._read_table_sync(table)
            with self._lock:
                records[key] = value
            self._flush_sync(table)
        await asyncio.to_thread(_upsert)

    async def adelete(self, table: str, key: str) -> None:
        def _delete():
            records = self._read_table_sync(table)
            with self._lock:
                records.pop(key, None)
            self._flush_sync(table)
        await asyncio.to_thread(_delete)

    async def alist(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        records = await asyncio.to_thread(self._read_table_sync, table)
        values = list(records.values())
        if predicate is not None:
            values = [v for v in values if predicate(v)]
        return values

    async def aiter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> AsyncIterator[Any]:
        values = await self.alist(table, predicate)
        for v in values:
            yield v

    async def alist_page(self, table: str, offset: int, limit: int) -> list[Any]:
        values = await self.alist(table)
        return values[offset:offset + limit]
