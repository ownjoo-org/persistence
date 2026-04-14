from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from oj_persistence.store.async_base import AsyncAbstractStore


class AsyncVersionedStore(AsyncAbstractStore):
    """
    Async equivalent of VersionedStore — wraps any AsyncAbstractStore.

    Every write appends a new version. See VersionedStore for full semantics.
    """

    def __init__(self, store: AsyncAbstractStore) -> None:
        self._store = store

    @property
    def supports_native_upsert(self) -> bool:
        return self._store.supports_native_upsert

    # ------------------------------------------------------------------ internal helpers

    @staticmethod
    def _seq_key(key: str, seq: int) -> str:
        return f'{key}#{seq}'

    @staticmethod
    def _make_envelope(key: str, seq: int, value: Any) -> dict:
        return {
            '_key': key,
            '_seq': seq,
            '_inserted_at': datetime.now(timezone.utc).isoformat(),
            '_value': value,
        }

    async def _all_versions(self, key: str) -> list[dict]:
        records = await self._store.list(
            lambda v: isinstance(v, dict) and v.get('_key') == key
        )
        return sorted(records, key=lambda v: v['_seq'])

    async def _next_seq(self, key: str) -> int:
        versions = await self._all_versions(key)
        return (versions[-1]['_seq'] + 1) if versions else 1

    # ------------------------------------------------------------------ versioned extras

    async def read_latest(self, key: str) -> Any | None:
        """Return the most recent value for key, or None if key has no versions."""
        versions = await self._all_versions(key)
        return versions[-1]['_value'] if versions else None

    async def list_versions(self, key: str) -> list[dict]:
        """Return all envelopes for key, sorted oldest-first."""
        return await self._all_versions(key)

    # ------------------------------------------------------------------ AsyncAbstractStore interface

    async def create(self, key: str, value: Any) -> None:
        if await self._all_versions(key):
            raise KeyError(key)
        await self._store.create(self._seq_key(key, 1), self._make_envelope(key, 1, value))

    async def read(self, key: str) -> Any | None:
        return await self.read_latest(key)

    async def update(self, key: str, value: Any) -> None:
        versions = await self._all_versions(key)
        if not versions:
            raise KeyError(key)
        seq = versions[-1]['_seq'] + 1
        await self._store.upsert(self._seq_key(key, seq), self._make_envelope(key, seq, value))

    async def upsert(self, key: str, value: Any) -> None:
        seq = await self._next_seq(key)
        await self._store.upsert(self._seq_key(key, seq), self._make_envelope(key, seq, value))

    async def delete(self, key: str) -> None:
        for env in await self._all_versions(key):
            await self._store.delete(self._seq_key(key, env['_seq']))

    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        all_envelopes = await self._store.list(
            lambda v: isinstance(v, dict) and '_key' in v and '_seq' in v
        )
        latest: dict[str, dict] = {}
        for env in all_envelopes:
            k = env['_key']
            if k not in latest or env['_seq'] > latest[k]['_seq']:
                latest[k] = env
        values = [env['_value'] for env in latest.values()]
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]
