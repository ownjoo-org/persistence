"""CSV backend — one CSV file per table, streaming line-by-line.

Each logical table maps to ``{base_dir}/{table}.csv``. The first column is
always ``key``; remaining columns are the fields of the value dict.

Value contract: values must be flat dicts (or iterables of ``(k, v)`` pairs)
with string-coercible field values — this is a CSV limitation. All values are
read back as ``dict[str, str]``.

Fieldnames (columns beyond ``key``) are inferred from the first value written
to each table and locked in for the lifetime of the file.

Mutations rewrite the file atomically via a temp file. Reads stream line-by-line
without loading the full file into memory.

Capabilities: PAGINATION (scan-based offset).
"""

from __future__ import annotations

import asyncio
import csv
import re
import threading
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

from ..base import Backend, Capability

_SAFE_NAME = re.compile(r'^[\w-]+$')
_KEY_COL = 'key'
_MISSING = object()


def _check_name(name: str) -> None:
    if not _SAFE_NAME.match(name):
        raise ValueError(
            f'invalid table name {name!r} — only letters, digits, underscores, hyphens allowed'
        )


def _to_flat_dict(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    try:
        return {str(k): str(v) for k, v in value}
    except (TypeError, ValueError):
        raise TypeError(f'CSV values must be flat dicts or (k, v) iterables, got {type(value).__name__}')


class CsvBackend(Backend):
    capabilities = frozenset({Capability.PAGINATION})

    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)
        self._locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()
        # per-table fieldnames cache: populated from file header or first write
        self._fieldnames: dict[str, list[str]] = {}
        self._fn_lock = threading.Lock()

    def _lock(self, table: str) -> threading.Lock:
        with self._locks_lock:
            if table not in self._locks:
                self._locks[table] = threading.Lock()
            return self._locks[table]

    def _path(self, table: str) -> Path:
        return self._base / f'{table}.csv'

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._base.mkdir, parents=True, exist_ok=True)

    async def aclose(self) -> None:
        pass

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)
        # File is created on first write so fieldnames can be inferred; no-op here.

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._drop_table_sync, table)

    def _drop_table_sync(self, table: str) -> None:
        self._path(table).unlink(missing_ok=True)
        with self._fn_lock:
            self._fieldnames.pop(table, None)

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._truncate_sync, table)

    def _truncate_sync(self, table: str) -> None:
        with self._lock(table):
            p = self._path(table)
            if not p.exists():
                return
            fieldnames = self._get_fieldnames(table)
            if fieldnames is None:
                p.write_text('', encoding='utf-8')
                return
            with p.open('w', newline='', encoding='utf-8') as f:
                csv.DictWriter(f, fieldnames=[_KEY_COL] + fieldnames).writeheader()

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        return await asyncio.to_thread(self._path(table).exists)

    # ------------------------------------------------------------------ helpers

    def _get_fieldnames(self, table: str) -> list[str] | None:
        """Return cached fieldnames, or read them from the file header."""
        with self._fn_lock:
            if table in self._fieldnames:
                return self._fieldnames[table]
        p = self._path(table)
        if not p.exists():
            return None
        with p.open(newline='', encoding='utf-8') as f:
            header = next(csv.reader(f), None)
        if not header:
            return None
        fn = [col for col in header if col != _KEY_COL]
        with self._fn_lock:
            self._fieldnames[table] = fn
        return fn

    def _init_file(self, table: str, fieldnames: list[str]) -> None:
        """Write an empty CSV with a header. Caller holds the write lock."""
        p = self._path(table)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=[_KEY_COL] + fieldnames).writeheader()
        with self._fn_lock:
            self._fieldnames[table] = fieldnames

    def _rewrite(
        self,
        table: str,
        key: str,
        new_row: dict[str, str] = _MISSING,
        *,
        skip: bool = False,
        append_if_missing: bool = False,
    ) -> bool:
        """Rewrite the file, replacing/skipping the row for ``key``. Returns True if found."""
        p = self._path(table)
        if not p.exists():
            return False
        fieldnames = self._get_fieldnames(table)
        all_cols = [_KEY_COL] + (fieldnames or [])
        tmp = p.with_suffix('.csv.tmp')
        found = False
        with p.open(newline='', encoding='utf-8') as src, \
                tmp.open('w', newline='', encoding='utf-8') as dst:
            reader = csv.DictReader(src)
            writer = csv.DictWriter(dst, fieldnames=all_cols)
            writer.writeheader()
            for row in reader:
                if row[_KEY_COL] == key:
                    found = True
                    if skip:
                        continue
                    row = {_KEY_COL: key, **new_row}
                writer.writerow(row)
            if not found and append_if_missing and new_row is not _MISSING:
                writer.writerow({_KEY_COL: key, **new_row})
        tmp.replace(p)
        return found

    def _append_row(self, table: str, key: str, row: dict[str, str]) -> None:
        fieldnames = self._get_fieldnames(table)
        all_cols = [_KEY_COL] + (fieldnames or [])
        with self._path(table).open('a', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=all_cols).writerow({_KEY_COL: key, **row})

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._create_sync, table, key, value)

    def _create_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            row = _to_flat_dict(value)
            if self._get_fieldnames(table) is None:
                self._init_file(table, list(row.keys()))
            p = self._path(table)
            with p.open(newline='', encoding='utf-8') as f:
                for r in csv.DictReader(f):
                    if r[_KEY_COL] == key:
                        raise KeyError(key)
            self._append_row(table, key, row)

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        return await asyncio.to_thread(self._read_sync, table, key)

    def _read_sync(self, table: str, key: str) -> Any | None:
        p = self._path(table)
        if not p.exists():
            return None
        with p.open(newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                if row[_KEY_COL] == key:
                    return {k: v for k, v in row.items() if k != _KEY_COL}
        return None

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._update_sync, table, key, value)

    def _update_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            row = _to_flat_dict(value)
            if not self._rewrite(table, key, row):
                raise KeyError(key)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        await asyncio.to_thread(self._upsert_sync, table, key, value)

    def _upsert_sync(self, table: str, key: str, value: Any) -> None:
        with self._lock(table):
            row = _to_flat_dict(value)
            if self._get_fieldnames(table) is None:
                self._init_file(table, list(row.keys()))
            self._rewrite(table, key, row, append_if_missing=True)

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        await asyncio.to_thread(self._delete_sync, table, key)

    def _delete_sync(self, table: str, key: str) -> None:
        with self._lock(table):
            self._rewrite(table, key, skip=True)

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
        p = self._path(table)
        if not p.exists():
            return []
        results = []
        with p.open(newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                v = {k: val for k, val in row.items() if k != _KEY_COL}
                if predicate is None or predicate(v):
                    results.append(v)
        return results

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
        _check_name(table)
        return await asyncio.to_thread(self._list_page_sync, table, offset, limit)

    def _list_page_sync(self, table: str, offset: int, limit: int) -> list[Any]:
        p = self._path(table)
        if not p.exists():
            return []
        results: list[Any] = []
        skipped = 0
        with p.open(newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                if skipped < offset:
                    skipped += 1
                    continue
                results.append({k: v for k, v in row.items() if k != _KEY_COL})
                if len(results) >= limit:
                    break
        return results
