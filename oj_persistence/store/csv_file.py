from __future__ import annotations

import csv
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from oj_persistence.store.abstract_file import AbstractFileStore
from oj_persistence.utils.compression import open_text_csv


_MISSING = object()
_KEY_COL = 'key'


def _to_dict(value: Any) -> dict[str, str]:
    """Normalise a dict or iterable of (k, v) tuples to a flat string dict."""
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, Iterable):
        return {str(k): str(v) for k, v in value}
    raise TypeError(f'value must be a dict or iterable of (k, v) tuples, got {type(value)}')


class CsvFileStore(AbstractFileStore):
    """
    AbstractStore backed by a CSV file.

    The first column is always 'key'. Remaining columns are the value fields.

    Fieldnames (value columns, excluding 'key') may be:
      - supplied at construction: CsvFileStore(path, fieldnames=['a', 'b'])
      - inferred from the first value written (dict keys or tuple-iterable keys)
      - loaded from the header row of an existing file

    Type fidelity: CSV stores all values as strings. Callers are responsible
    for any type conversion on read.

    Streaming: all operations stream line-by-line; the full file is never
    loaded into memory at once. Mutations rewrite via a temp file.

    Thread-safe via ReadWriteLock — concurrent reads allowed; writes exclusive.

    Compression
    -----------
    Pass compression='gzip', 'bz2', 'lzma', or 'auto' (detect from extension)
    to read and write a compressed file transparently.
    """

    def __init__(
        self,
        path: str | Path,
        fieldnames: list[str] | None = None,
        *,
        compression: str | None = None,
    ) -> None:
        super().__init__(path, compression=compression)
        self._fieldnames: list[str] | None = None

        if fieldnames is not None:
            self._fieldnames = list(fieldnames)
        elif self._path.exists():
            self._fieldnames = self._read_fieldnames()

    @property
    def fieldnames(self) -> list[str] | None:
        return self._fieldnames

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _all_columns(self) -> list[str]:
        return [_KEY_COL] + (self._fieldnames or [])

    def _read_fieldnames(self) -> list[str]:
        with self._open_text_csv('r') as f:
            reader = csv.reader(f)
            header = next(reader, None)
        if header is None:
            return []
        return [col for col in header if col != _KEY_COL]

    def _validate_and_normalise(self, value: Any) -> dict[str, str]:
        row = _to_dict(value)
        if self._fieldnames is not None:
            extra = set(row) - set(self._fieldnames)
            if extra:
                raise ValueError(f'Unknown fields {extra}. Expected: {self._fieldnames}')
            return {f: row.get(f, '') for f in self._fieldnames}
        return row

    def _ensure_file(self) -> None:
        """Create the file with a header row if it does not yet exist."""
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._open_text_csv('w') as f:
                csv.DictWriter(f, fieldnames=self._all_columns()).writeheader()

    def _init_fieldnames(self, row: dict[str, str]) -> None:
        """Called on the first write when fieldnames were not pre-specified."""
        self._fieldnames = list(row.keys())
        self._ensure_file()

    def _rewrite(
        self,
        key: str,
        new_row: dict[str, str] = _MISSING,
        *,
        skip: bool = False,
        append_if_missing: bool = False,
    ) -> bool:
        """
        Stream the file to a temp file, replacing or skipping the row for key.
        Returns True if key was found.
        """
        if not self._path.exists():
            return False
        tmp = Path(str(self._path) + '.tmp')
        found = False
        columns = self._all_columns()
        with self._open_text_csv('r') as src, \
                open_text_csv(tmp, 'w', self._compression) as dst:
            reader = csv.DictReader(src)
            writer = csv.DictWriter(dst, fieldnames=columns)
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
        tmp.replace(self._path)
        return found

    def _append(self, key: str, row: dict[str, str]) -> None:
        with self._open_text_csv('a') as f:
            writer = csv.DictWriter(f, fieldnames=self._all_columns())
            writer.writerow({_KEY_COL: key, **row})

    # ------------------------------------------------------------------
    # CRUDL interface
    # ------------------------------------------------------------------

    def create(self, key: str, value: Any) -> None:
        with self._lock.write():
            row = _to_dict(value)
            if self._fieldnames is None:
                self._init_fieldnames(row)
            else:
                self._ensure_file()
            row = self._validate_and_normalise(value)
            with self._open_text_csv('r') as f:
                for r in csv.DictReader(f):
                    if r[_KEY_COL] == key:
                        raise KeyError(key)
            self._append(key, row)

    def read(self, key: str) -> dict[str, str] | None:
        with self._lock.read():
            if not self._path.exists():
                return None
            with self._open_text_csv('r') as f:
                for row in csv.DictReader(f):
                    if row[_KEY_COL] == key:
                        return {k: v for k, v in row.items() if k != _KEY_COL}
            return None

    def update(self, key: str, value: Any) -> None:
        with self._lock.write():
            row = self._validate_and_normalise(value)
            if not self._rewrite(key, row):
                raise KeyError(key)

    def upsert(self, key: str, value: Any) -> None:
        with self._lock.write():
            row = _to_dict(value)
            if self._fieldnames is None:
                self._init_fieldnames(row)
            else:
                self._ensure_file()
            row = self._validate_and_normalise(value)
            self._rewrite(key, row, append_if_missing=True)

    def delete(self, key: str) -> None:
        with self._lock.write():
            self._rewrite(key, skip=True)

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._lock.read():
            if not self._path.exists():
                return []
            results = []
            with self._open_text_csv('r') as f:
                for row in csv.DictReader(f):
                    v = {k: val for k, val in row.items() if k != _KEY_COL}
                    if predicate is None or predicate(v):
                        results.append(v)
            return results
