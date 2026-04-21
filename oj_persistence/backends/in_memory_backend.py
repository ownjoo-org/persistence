"""In-memory backend — dict-based, single lock.

Trivial enough that contention isn't worth splitting readers and writers.
Declares ``PAGINATION``, ``FIELD_INDEX`` (via in-Python scan), and
``NATIVE_UPSERT``. No ``NATIVE_JOIN`` — in-memory has no cross-table
primitives, so joins fall back to the Manager's Python path.
"""

from __future__ import annotations

import asyncio
import re
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

from ..base import Backend, Capability

_SAFE_JSON_PATH = re.compile(r'^[\$\.\w\[\]]+$')


def _extract_path(value: Any, path: str) -> Any:
    """Very small subset of JSONPath: ``$.foo.bar`` → nested dict access."""
    if not path.startswith('$'):
        return None
    parts = [p for p in path.lstrip('$').split('.') if p]
    cur = value
    for part in parts:
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


class InMemoryBackend(Backend):
    capabilities = frozenset({
        Capability.PAGINATION,
        Capability.FIELD_INDEX,
        Capability.NATIVE_UPSERT,
    })

    def __init__(self) -> None:
        # table name → dict[key, value]. Each table is insertion-ordered so
        # list_page(offset, limit) gives stable results.
        self._tables: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    async def aopen(self) -> None:
        return  # nothing to open

    async def aclose(self) -> None:
        with self._lock:
            self._tables.clear()

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        with self._lock:
            self._tables.setdefault(table, {})

    async def adrop_table(self, table: str) -> None:
        with self._lock:
            self._tables.pop(table, None)

    async def atruncate_table(self, table: str) -> None:
        with self._lock:
            if table in self._tables:
                self._tables[table].clear()

    async def atable_exists(self, table: str) -> bool:
        with self._lock:
            return table in self._tables

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        with self._lock:
            t = self._require_table(table)
            if key in t:
                raise KeyError(key)
            t[key] = value

    async def aread(self, table: str, key: str) -> Any | None:
        with self._lock:
            t = self._require_table(table)
            return t.get(key)

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        with self._lock:
            t = self._require_table(table)
            if key not in t:
                raise KeyError(key)
            t[key] = value

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        with self._lock:
            t = self._require_table(table)
            t[key] = value

    async def adelete(self, table: str, key: str) -> None:
        with self._lock:
            t = self._require_table(table)
            t.pop(key, None)

    async def alist(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        with self._lock:
            values = list(self._require_table(table).values())
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]

    async def aiter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> AsyncIterator[Any]:
        values = await self.alist(table, predicate)
        for v in values:
            # Yield cooperatively so a long iteration doesn't starve the loop.
            yield v
            await asyncio.sleep(0)

    # ------------------------------------------------------------------ optional

    async def alist_page(self, table: str, offset: int, limit: int) -> list[Any]:
        with self._lock:
            values = list(self._require_table(table).values())
        return values[offset:offset + limit]

    async def alist_by_field(self, table: str, json_path: str, value: Any) -> list[Any]:
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f'unsafe json_path {json_path!r}')
        with self._lock:
            values = list(self._require_table(table).values())
        return [v for v in values if _extract_path(v, json_path) == value]

    async def aadd_index(self, table: str, json_path: str, *, name: str | None = None) -> None:
        # In-memory scan doesn't benefit from persistent indexes; accept the
        # call as a no-op so capability semantics hold.
        return

    # ------------------------------------------------------------------ internals

    def _require_table(self, table: str) -> dict[str, Any]:
        try:
            return self._tables[table]
        except KeyError:
            raise KeyError(f'table {table!r} not created in backend')
