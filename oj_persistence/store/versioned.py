from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from oj_persistence.store.base import AbstractStore


class VersionedStore(AbstractStore):
    """
    Wraps any AbstractStore to provide versioned records.

    Every write (create / update / upsert) appends a new version rather than
    overwriting. Old versions are preserved and accessible via list_versions().

    Internally, each version is stored under a composite key ``{key}#{seq}``
    with an envelope::

        {
            '_key':         original key,
            '_seq':         monotonically increasing int (1-based),
            '_inserted_at': ISO-8601 UTC timestamp,
            '_value':       the caller's original value,
        }

    The standard AbstractStore methods are re-mapped as follows:

    - read(key)   → read_latest(key): most recent value, or None
    - list()      → latest value per key (de-duplicated)
    - create()    → version 1; raises KeyError if any version already exists
    - update()    → new version; raises KeyError if no version exists yet
    - upsert()    → always appends a new version
    - delete()    → removes ALL versions for the key

    The underlying store is never accessed directly for data — always go
    through the VersionedStore.
    """

    def __init__(self, store: AbstractStore) -> None:
        self._store = store

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

    def _all_versions(self, key: str) -> list[dict]:
        """Return all envelopes for key, sorted oldest-first."""
        records = self._store.list(
            lambda v: isinstance(v, dict) and v.get('_key') == key
        )
        return sorted(records, key=lambda v: v['_seq'])

    def _next_seq(self, key: str) -> int:
        versions = self._all_versions(key)
        return (versions[-1]['_seq'] + 1) if versions else 1

    # ------------------------------------------------------------------ versioned extras

    def read_latest(self, key: str) -> Any | None:
        """Return the most recent value for key, or None if key has no versions."""
        versions = self._all_versions(key)
        return versions[-1]['_value'] if versions else None

    def list_versions(self, key: str) -> list[dict]:
        """Return all envelopes for key, sorted oldest-first."""
        return self._all_versions(key)

    # ------------------------------------------------------------------ AbstractStore interface

    def create(self, key: str, value: Any) -> None:
        if self._all_versions(key):
            raise KeyError(key)
        self._store.create(self._seq_key(key, 1), self._make_envelope(key, 1, value))

    def read(self, key: str) -> Any | None:
        return self.read_latest(key)

    def update(self, key: str, value: Any) -> None:
        versions = self._all_versions(key)
        if not versions:
            raise KeyError(key)
        seq = versions[-1]['_seq'] + 1
        self._store.upsert(self._seq_key(key, seq), self._make_envelope(key, seq, value))

    def upsert(self, key: str, value: Any) -> None:
        seq = self._next_seq(key)
        self._store.upsert(self._seq_key(key, seq), self._make_envelope(key, seq, value))

    def delete(self, key: str) -> None:
        for env in self._all_versions(key):
            self._store.delete(self._seq_key(key, env['_seq']))

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        """Return the latest value per key, optionally filtered by predicate."""
        all_envelopes = self._store.list(
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
