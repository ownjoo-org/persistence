"""DynamoDB backend — PAY_PER_REQUEST, native TTL, async via asyncio.to_thread.

Each logical table maps to one DynamoDB table named ``{prefix}{table}``.

Item schema
-----------
- ``pk``    (S) — the record key
- ``value`` (S) — JSON-serialised record value
- ``ttl``   (N) — Unix timestamp (optional); populated when value dict contains
  a ``_ttl`` key (float/int epoch seconds). DynamoDB native TTL must be
  enabled on the ``ttl`` attribute — ``acreate_table`` handles this.

Capabilities: PAGINATION (scan-based offset), NATIVE_UPSERT.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Callable
from typing import Any

from ..base import Backend, Capability

_SAFE_NAME = re.compile(r'^[\w-]+$')


def _check_name(name: str) -> None:
    if not _SAFE_NAME.match(name):
        raise ValueError(
            f'invalid table name {name!r} — only letters, digits, underscores, hyphens allowed'
        )


class DynamoDbBackend(Backend):
    capabilities = frozenset({
        Capability.PAGINATION,
        Capability.NATIVE_UPSERT,
    })

    def __init__(
        self,
        region: str,
        prefix: str = '',
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        self._region = region
        self._prefix = prefix
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._client = None

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        import boto3
        kwargs: dict[str, Any] = {'region_name': self._region}
        if self._endpoint_url:
            kwargs['endpoint_url'] = self._endpoint_url
        if self._aws_access_key_id:
            kwargs['aws_access_key_id'] = self._aws_access_key_id
        if self._aws_secret_access_key:
            kwargs['aws_secret_access_key'] = self._aws_secret_access_key
        self._client = boto3.client('dynamodb', **kwargs)

    async def aclose(self) -> None:
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    # ------------------------------------------------------------------ helpers

    def _table_name(self, table: str) -> str:
        return f'{self._prefix}{table}'

    def _client_op(self, fn: Callable[[], Any]) -> Any:
        assert self._client is not None, 'backend not opened'
        return fn()

    @staticmethod
    def _encode(value: Any) -> dict[str, Any]:
        item: dict[str, Any] = {'value': {'S': json.dumps(value)}}
        if isinstance(value, dict):
            ttl = value.get('_ttl')
            if ttl is not None:
                item['ttl'] = {'N': str(int(ttl))}
        return item

    @staticmethod
    def _decode(item: dict[str, Any]) -> Any:
        return json.loads(item['value']['S'])

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_table_sync, table)

    def _create_table_sync(self, table: str) -> None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None

        try:
            client.create_table(
                TableName=ddb_name,
                KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}],
                BillingMode='PAY_PER_REQUEST',
            )
        except client.exceptions.ResourceInUseException:
            # Table already exists — that's fine.
            pass
        else:
            # Wait until ACTIVE before enabling TTL.
            waiter = client.get_waiter('table_exists')
            waiter.wait(TableName=ddb_name)

        # Enable TTL unconditionally (idempotent — safe to call on existing tables).
        try:
            client.update_time_to_live(
                TableName=ddb_name,
                TimeToLiveSpecification={'Enabled': True, 'AttributeName': 'ttl'},
            )
        except Exception:
            pass  # already enabled or local DynamoDB doesn't support it

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._drop_table_sync, table)

    def _drop_table_sync(self, table: str) -> None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        try:
            client.delete_table(TableName=ddb_name)
            waiter = client.get_waiter('table_not_exists')
            waiter.wait(TableName=ddb_name)
        except client.exceptions.ResourceNotFoundException:
            pass

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._truncate_table_sync, table)

    def _truncate_table_sync(self, table: str) -> None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None

        paginator = client.get_paginator('scan')
        for page in paginator.paginate(TableName=ddb_name, ProjectionExpression='pk'):
            items = page.get('Items', [])
            # Batch delete in groups of 25 (DynamoDB limit).
            for i in range(0, len(items), 25):
                batch = items[i:i + 25]
                client.batch_write_item(
                    RequestItems={
                        ddb_name: [
                            {'DeleteRequest': {'Key': {'pk': item['pk']}}}
                            for item in batch
                        ]
                    }
                )

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        return await asyncio.to_thread(self._table_exists_sync, table)

    def _table_exists_sync(self, table: str) -> bool:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        try:
            client.describe_table(TableName=ddb_name)
            return True
        except client.exceptions.ResourceNotFoundException:
            return False

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_sync, table, key, value)

    def _create_sync(self, table: str, key: str, value: Any) -> None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        item = {'pk': {'S': key}, **self._encode(value)}
        try:
            client.put_item(
                TableName=ddb_name,
                Item=item,
                ConditionExpression='attribute_not_exists(pk)',
            )
        except client.exceptions.ConditionalCheckFailedException:
            raise KeyError(key)

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        return await asyncio.to_thread(self._read_sync, table, key)

    def _read_sync(self, table: str, key: str) -> Any | None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        resp = client.get_item(
            TableName=ddb_name,
            Key={'pk': {'S': key}},
        )
        item = resp.get('Item')
        return self._decode(item) if item else None

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._update_sync, table, key, value)

    def _update_sync(self, table: str, key: str, value: Any) -> None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        item = {'pk': {'S': key}, **self._encode(value)}
        try:
            client.put_item(
                TableName=ddb_name,
                Item=item,
                ConditionExpression='attribute_exists(pk)',
            )
        except client.exceptions.ConditionalCheckFailedException:
            raise KeyError(key)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._upsert_sync, table, key, value)

    def _upsert_sync(self, table: str, key: str, value: Any) -> None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        item = {'pk': {'S': key}, **self._encode(value)}
        client.put_item(TableName=ddb_name, Item=item)

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_sync, table, key)

    def _delete_sync(self, table: str, key: str) -> None:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        client.delete_item(TableName=ddb_name, Key={'pk': {'S': key}})

    async def alist(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        _check_name(table)
        return await asyncio.to_thread(self._list_sync, table, predicate)

    def _list_sync(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None,
    ) -> list[Any]:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        paginator = client.get_paginator('scan')
        values: list[Any] = []
        for page in paginator.paginate(TableName=ddb_name):
            for item in page.get('Items', []):
                v = self._decode(item)
                if predicate is None or predicate(v):
                    values.append(v)
        return values

    async def aiter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> AsyncIterator[Any]:
        values = await self.alist(table, predicate)
        for v in values:
            yield v

    # ------------------------------------------------------------------ optional

    async def alist_page(self, table: str, offset: int, limit: int) -> list[Any]:
        """Offset-based pagination via full scan with skip.

        DynamoDB has no native offset — we scan and discard. Fine for the small
        tables (sessions, task registry) this backend targets.
        """
        _check_name(table)
        return await asyncio.to_thread(self._list_page_sync, table, offset, limit)

    def _list_page_sync(self, table: str, offset: int, limit: int) -> list[Any]:
        ddb_name = self._table_name(table)
        client = self._client
        assert client is not None
        paginator = client.get_paginator('scan')
        results: list[Any] = []
        skipped = 0
        for page in paginator.paginate(TableName=ddb_name):
            for item in page.get('Items', []):
                if skipped < offset:
                    skipped += 1
                    continue
                results.append(self._decode(item))
                if len(results) >= limit:
                    return results
        return results
