"""SQLite backend — WAL mode, single writer, reader pool.

Concurrency model
-----------------
One database file is owned by exactly one ``SqliteBackend`` instance in the
process (enforced at the Manager layer via ``_BackendRegistry``).

Per file::

    - 1 writer connection      (exclusive, serialized by `_writer_lock`)
    - N reader connections     (pool of `pool_size`, acquired via queue.Queue)
    - WAL journal mode         (set once at open time)
    - busy_timeout 5000ms      (retry on SQLITE_BUSY)

Reads never block writes, writes never block reads (WAL semantics). Concurrent
readers are truly concurrent — each uses its own connection from the pool.
Concurrent writers serialize through `_writer_lock`.

Async methods dispatch to a thread via ``asyncio.to_thread`` so the event loop
is never blocked by SQLite I/O or lock contention.
"""

from __future__ import annotations

import asyncio
import json
import queue
import re
import sqlite3
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

from ..base import Backend, Capability

# Allow hyphens in addition to [A-Za-z0-9_] since pipeline / table names often
# contain them (e.g. 'rick-and-morty-staged__results'). All SQL references to
# a table name are double-quoted so hyphenated identifiers are valid SQL.
_SAFE_NAME = re.compile(r'^[\w-]+$')
_SAFE_JSON_PATH = re.compile(r'^[\$\.\w\[\]]+$')


def _check_name(name: str, kind: str = 'table') -> None:
    if not _SAFE_NAME.match(name):
        raise ValueError(
            f'invalid {kind} name {name!r} — only letters, digits, underscores, hyphens allowed'
        )


def _q(name: str) -> str:
    """Quote a safe identifier for SQL. ``name`` must have already passed
    ``_check_name`` so embedding is injection-free; double-quoting then just
    lets hyphenated names parse correctly."""
    return f'"{name}"'


class SqliteBackend(Backend):
    capabilities = frozenset({
        Capability.PAGINATION,
        Capability.FIELD_INDEX,
        Capability.NATIVE_JOIN,
        Capability.NATIVE_UPSERT,
    })

    def __init__(self, path: str, pool_size: int = 4) -> None:
        self._path = path
        self._pool_size = pool_size
        self._writer: sqlite3.Connection | None = None
        self._writer_lock = threading.Lock()
        self._reader_pool: queue.Queue[sqlite3.Connection] = queue.Queue(maxsize=pool_size)
        self._opened = False
        self._open_lock = threading.Lock()

    # ------------------------------------------------------------------ lifecycle

    async def aopen(self) -> None:
        await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        # Idempotent — safe to call on an already-open backend.
        with self._open_lock:
            if self._opened:
                return
            self._writer = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
            # WAL + busy_timeout applied via writer (persists on-disk for WAL).
            # isolation_level=None puts sqlite3 into autocommit for explicit
            # transaction control via BEGIN/COMMIT. We commit per-op in helpers.
            self._writer.execute('PRAGMA journal_mode=WAL')
            self._writer.execute('PRAGMA busy_timeout=5000')
            self._writer.execute('PRAGMA synchronous=NORMAL')  # WAL-safe, faster than FULL

            for _ in range(self._pool_size):
                conn = sqlite3.connect(self._path, check_same_thread=False)
                conn.execute('PRAGMA busy_timeout=5000')
                self._reader_pool.put(conn)
            self._opened = True

    async def aclose(self) -> None:
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        with self._open_lock:
            if not self._opened:
                return
            if self._writer is not None:
                try:
                    self._writer.close()
                except Exception:
                    pass
                self._writer = None
            while not self._reader_pool.empty():
                try:
                    conn = self._reader_pool.get_nowait()
                    conn.close()
                except queue.Empty:
                    break
                except Exception:
                    pass
            self._opened = False

    # ------------------------------------------------------------------ table mgmt

    async def acreate_table(self, table: str) -> None:
        _check_name(table)
        t = _q(table)
        await asyncio.to_thread(self._write, lambda c: (
            c.execute(
                f'CREATE TABLE IF NOT EXISTS {t} '
                f'(key TEXT PRIMARY KEY, value TEXT NOT NULL)'
            )
        ))

    async def adrop_table(self, table: str) -> None:
        _check_name(table)
        t = _q(table)
        await asyncio.to_thread(self._write, lambda c: c.execute(f'DROP TABLE IF EXISTS {t}'))

    async def atruncate_table(self, table: str) -> None:
        _check_name(table)
        t = _q(table)
        await asyncio.to_thread(self._write, lambda c: c.execute(f'DELETE FROM {t}'))

    async def atable_exists(self, table: str) -> bool:
        _check_name(table)
        def _check(c: sqlite3.Connection) -> bool:
            row = c.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            return row is not None
        return await asyncio.to_thread(self._read, _check)

    # ------------------------------------------------------------------ CRUDL

    async def acreate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        t = _q(table)
        payload = json.dumps(value)
        def _op(c: sqlite3.Connection) -> None:
            try:
                c.execute(f'INSERT INTO {t} (key, value) VALUES (?, ?)', (key, payload))
            except sqlite3.IntegrityError:
                raise KeyError(key)
        await asyncio.to_thread(self._write, _op)

    async def aread(self, table: str, key: str) -> Any | None:
        _check_name(table)
        t = _q(table)
        def _op(c: sqlite3.Connection) -> Any | None:
            row = c.execute(f'SELECT value FROM {t} WHERE key = ?', (key,)).fetchone()
            return json.loads(row[0]) if row else None
        return await asyncio.to_thread(self._read, _op)

    async def aupdate(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        t = _q(table)
        payload = json.dumps(value)
        def _op(c: sqlite3.Connection) -> None:
            cursor = c.execute(
                f'UPDATE {t} SET value = ? WHERE key = ?',
                (payload, key),
            )
            if cursor.rowcount == 0:
                raise KeyError(key)
        await asyncio.to_thread(self._write, _op)

    async def aupsert(self, table: str, key: str, value: Any) -> None:
        _check_name(table)
        t = _q(table)
        payload = json.dumps(value)
        def _op(c: sqlite3.Connection) -> None:
            c.execute(
                f'INSERT INTO {t} (key, value) VALUES (?, ?) '
                f'ON CONFLICT(key) DO UPDATE SET value = excluded.value',
                (key, payload),
            )
        await asyncio.to_thread(self._write, _op)

    async def adelete(self, table: str, key: str) -> None:
        _check_name(table)
        t = _q(table)
        await asyncio.to_thread(
            self._write,
            lambda c: c.execute(f'DELETE FROM {t} WHERE key = ?', (key,)),
        )

    async def alist(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        _check_name(table)
        t = _q(table)
        def _op(c: sqlite3.Connection) -> list[Any]:
            rows = c.execute(f'SELECT value FROM {t}').fetchall()
            values = [json.loads(r[0]) for r in rows]
            if predicate is None:
                return values
            return [v for v in values if predicate(v)]
        return await asyncio.to_thread(self._read, _op)

    async def aiter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> AsyncIterator[Any]:
        # First pass: materialize then yield. A streaming cursor-based impl
        # would hold a reader connection for the duration of iteration — fine,
        # but adds complexity; deferring.
        values = await self.alist(table, predicate)
        for v in values:
            yield v

    # ------------------------------------------------------------------ optional

    async def alist_page(self, table: str, offset: int, limit: int) -> list[Any]:
        _check_name(table)
        t = _q(table)
        def _op(c: sqlite3.Connection) -> list[Any]:
            rows = c.execute(
                f'SELECT value FROM {t} ORDER BY key LIMIT ? OFFSET ?',
                (limit, offset),
            ).fetchall()
            return [json.loads(r[0]) for r in rows]
        return await asyncio.to_thread(self._read, _op)

    async def alist_by_field(self, table: str, json_path: str, value: Any) -> list[Any]:
        _check_name(table)
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f'unsafe json_path {json_path!r}')
        t = _q(table)
        def _op(c: sqlite3.Connection) -> list[Any]:
            sql = (
                f"SELECT value FROM {t} "
                f"WHERE json_extract(value, '{json_path}') = ?"
            )
            rows = c.execute(sql, (value,)).fetchall()
            return [json.loads(r[0]) for r in rows]
        return await asyncio.to_thread(self._read, _op)

    async def aadd_index(self, table: str, json_path: str, *, name: str | None = None) -> None:
        _check_name(table)
        if not _SAFE_JSON_PATH.match(json_path):
            raise ValueError(f'unsafe json_path {json_path!r}')
        idx_name = name or 'idx_' + re.sub(r'\W', '_', json_path.lstrip('$').lstrip('.'))
        _check_name(idx_name, kind='index')
        t = _q(table)
        i = _q(idx_name)
        sql = (
            f'CREATE INDEX IF NOT EXISTS {i} '
            f"ON {t}(json_extract(value, '{json_path}'))"
        )
        await asyncio.to_thread(self._write, lambda c: c.execute(sql))

    # ------------------------------------------------------------------ internals

    def _write(self, op: Callable[[sqlite3.Connection], Any]) -> Any:
        """Run ``op`` under the writer lock, wrapped in a transaction."""
        assert self._writer is not None, 'backend not opened'
        with self._writer_lock:
            self._writer.execute('BEGIN')
            try:
                result = op(self._writer)
                self._writer.execute('COMMIT')
                return result
            except BaseException:
                self._writer.execute('ROLLBACK')
                raise

    def _read(self, op: Callable[[sqlite3.Connection], Any]) -> Any:
        """Run ``op`` on a pool-borrowed reader connection, returning it after."""
        conn = self._reader_pool.get()
        try:
            return op(conn)
        finally:
            self._reader_pool.put(conn)
