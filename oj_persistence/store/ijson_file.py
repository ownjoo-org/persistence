from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import ijson

from oj_persistence.store.abstract_file import AbstractFileStore
from oj_persistence.utils.compression import open_binary, open_text


_MISSING = object()


class IjsonFileStore(AbstractFileStore):
    """
    AbstractStore backed by a standard JSON object file, read via ijson.

    The on-disk format is a plain JSON object:
        {
          "key1": <value>,
          "key2": <value>
        }

    Streaming characteristics:
      - read()   streams with ijson, stopping at the first key match — O(n), O(1) memory
      - list()   streams all entries with ijson, accumulating matches — O(n), O(k) results
      - write operations (create/update/upsert/delete) must rewrite the full
        file (standard JSON has no append-friendly structure), streaming
        through ijson on the read side and writing line-by-line on the
        write side — O(n), O(1) memory

    Thread-safe via RLock. Temp files use the same parent directory to
    guarantee atomic rename (same filesystem).

    Compression
    -----------
    Pass compression='gzip', 'bz2', 'lzma', or 'auto' (detect from extension)
    to read and write a compressed file transparently. ijson operates on binary
    streams, so compressed files are opened in binary mode and decompressed
    transparently.
    """

    def __init__(self, path: str | Path, *, compression: str | None = None) -> None:
        super().__init__(path, compression=compression)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_file(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._open_text('w', encoding='utf-8') as f:
            f.write('{}')

    def _rewrite(
        self,
        key: str,
        new_value: Any = _MISSING,
        *,
        skip: bool = False,
        append_if_missing: bool = False,
    ) -> bool:
        """
        Stream the file through ijson, replacing or skipping the record for key.
        Returns True if key was found.
        skip=True            → delete the record.
        new_value provided   → replace the record's value.
        append_if_missing    → if key not found, append it (upsert behaviour).
        """
        tmp = Path(str(self._path) + '.tmp')
        found = False
        with self._open_binary('rb') as src, \
                open_text(tmp, 'w', self._compression, encoding='utf-8') as dst:
            dst.write('{\n')
            first = True
            for k, v in ijson.kvitems(src, ''):
                if k == key:
                    found = True
                    if skip:
                        continue
                    v = new_value
                if not first:
                    dst.write(',\n')
                dst.write(f'  {json.dumps(k)}: {json.dumps(v)}')
                first = False
            if not found and append_if_missing and new_value is not _MISSING:
                if not first:
                    dst.write(',\n')
                dst.write(f'  {json.dumps(key)}: {json.dumps(new_value)}')
            dst.write('\n}')
        tmp.replace(self._path)
        return found

    # ------------------------------------------------------------------
    # CRUDL interface
    # ------------------------------------------------------------------

    def create(self, key: str, value: Any) -> None:
        with self._lock.write():
            if not self._path.exists():
                self._init_file()
            with self._open_binary('rb') as f:
                for k, _ in ijson.kvitems(f, ''):
                    if k == key:
                        raise KeyError(key)
            self._rewrite(key, value, append_if_missing=True)

    def read(self, key: str) -> Any:
        with self._lock.read():
            if not self._path.exists():
                return None
            with self._open_binary('rb') as f:
                for k, v in ijson.kvitems(f, ''):
                    if k == key:
                        return v
            return None

    def update(self, key: str, value: Any) -> None:
        with self._lock.write():
            if not self._path.exists() or not self._rewrite(key, value):
                raise KeyError(key)

    def upsert(self, key: str, value: Any) -> None:
        with self._lock.write():
            if not self._path.exists():
                self._init_file()
            self._rewrite(key, value, append_if_missing=True)

    def delete(self, key: str) -> None:
        with self._lock.write():
            if self._path.exists():
                self._rewrite(key, skip=True)

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._lock.read():
            if not self._path.exists():
                return []
            results = []
            with self._open_binary('rb') as f:
                for _, v in ijson.kvitems(f, ''):
                    if predicate is None or predicate(v):
                        results.append(v)
            return results
