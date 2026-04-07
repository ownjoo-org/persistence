from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from oj_persistence.store.base import AbstractStore
from oj_persistence.utils.rwlock import ReadWriteLock

_MISSING = object()


class NdjsonFileStore(AbstractStore):
    """
    AbstractStore backed by a Newline-Delimited JSON (NDJSON) file.

    Each record occupies exactly one line:
        {"key": "...", "value": <any JSON-serialisable value>}

    Streaming characteristics:
      - create()  appends a single line — O(1) write, O(n) existence check
      - read()    scans lines until the key is found — O(n), O(1) memory
      - update()  / delete() / upsert() stream to a temp file then rename — O(n), O(1) memory
      - list()    streams all lines, accumulating matches — O(n) scan, O(k) results

    Thread-safe via RLock. Temp files use the same parent directory to
    guarantee atomic rename (same filesystem).
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = ReadWriteLock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _iter_lines(self, fp):
        """Yield parsed records from an open NDJSON file handle."""
        for line in fp:
            line = line.strip()
            if line:
                yield json.loads(line)

    def _append(self, key: str, value: Any) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open('a', encoding='utf-8') as f:
            f.write(json.dumps({'key': key, 'value': value}) + '\n')

    def _rewrite(self, key: str, new_value: Any = _MISSING, *, skip: bool = False) -> bool:
        """
        Stream the file to a temp file, replacing or skipping the record for key.
        Returns True if key was found.
        skip=True  → delete the record.
        new_value  → replace the record's value.
        """
        if not self._path.exists():
            return False
        tmp = Path(str(self._path) + '.tmp')
        found = False
        with self._path.open('r', encoding='utf-8') as src, \
                tmp.open('w', encoding='utf-8') as dst:
            for record in self._iter_lines(src):
                if record['key'] == key:
                    found = True
                    if skip:
                        continue
                    record['value'] = new_value
                dst.write(json.dumps(record) + '\n')
        tmp.replace(self._path)
        return found

    # ------------------------------------------------------------------
    # CRUDL interface
    # ------------------------------------------------------------------

    def create(self, key: str, value: Any) -> None:
        with self._lock.write():
            if self._path.exists():
                with self._path.open('r', encoding='utf-8') as f:
                    for record in self._iter_lines(f):
                        if record['key'] == key:
                            raise KeyError(key)
            self._append(key, value)

    def read(self, key: str) -> Any:
        with self._lock.read():
            if not self._path.exists():
                return None
            with self._path.open('r', encoding='utf-8') as f:
                for record in self._iter_lines(f):
                    if record['key'] == key:
                        return record['value']
            return None

    def update(self, key: str, value: Any) -> None:
        with self._lock.write():
            if not self._rewrite(key, value):
                raise KeyError(key)

    def upsert(self, key: str, value: Any) -> None:
        with self._lock.write():
            if not self._rewrite(key, value):
                self._append(key, value)

    def upsert_many(self, items: list[tuple[str, Any]]) -> None:
        """
        Upsert a batch of (key, value) pairs in a single file pass.

        One scan rewrites existing keys in place; new keys are appended
        in one write at the end — O(file_size + batch_size) instead of
        O(file_size x batch_size) for repeated single upserts.
        """
        if not items:
            return
        updates = dict(items)  # key → value for O(1) lookup
        with self._lock.write():
            found: set[str] = set()
            if self._path.exists():
                tmp = Path(str(self._path) + '.tmp')
                with self._path.open('r', encoding='utf-8') as src, \
                        tmp.open('w', encoding='utf-8') as dst:
                    for record in self._iter_lines(src):
                        k = record['key']
                        if k in updates:
                            record['value'] = updates[k]
                            found.add(k)
                        dst.write(json.dumps(record) + '\n')
                tmp.replace(self._path)
            # Append all keys that were not found (new records)
            new_items = [(k, v) for k, v in items if k not in found]
            if new_items:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open('a', encoding='utf-8') as f:
                    for k, v in new_items:
                        f.write(json.dumps({'key': k, 'value': v}) + '\n')

    def delete(self, key: str) -> None:
        with self._lock.write():
            self._rewrite(key, skip=True)

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._lock.read():
            if not self._path.exists():
                return []
            results = []
            with self._path.open('r', encoding='utf-8') as f:
                for record in self._iter_lines(f):
                    v = record['value']
                    if predicate is None or predicate(v):
                        results.append(v)
            return results
