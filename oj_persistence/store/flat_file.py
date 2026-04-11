from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from oj_persistence.store.abstract_file import AbstractFileStore


class _Serializer:
    """
    Protocol for flat-file serialization strategies.

    Each format (JSON, CSV, TSV, XML, …) implements load() and dump()
    against a file object. Add new formats by subclassing and registering
    in FlatFileStore._SERIALIZERS.

    Contract:
      - load() always returns a dict[str, Any]
      - dump() persists that dict to fp
    """

    def load(self, fp) -> dict[str, Any]:
        raise NotImplementedError

    def dump(self, data: dict[str, Any], fp) -> None:
        raise NotImplementedError


class JsonSerializer(_Serializer):
    def load(self, fp) -> dict[str, Any]:
        return json.load(fp)

    def dump(self, data: dict[str, Any], fp) -> None:
        json.dump(data, fp, indent=2)


class FlatFileStore(AbstractFileStore):
    """
    AbstractStore backed by a single flat file on disk.

    The entire file is loaded into memory on each read and written back on
    each mutation. This is intentional for simplicity; callers that need
    high-frequency writes should batch or use a different backend.

    Thread safety: an RLock guards all load/save cycles so concurrent readers
    and writers within the same process do not corrupt the file.

    Format support is pluggable via _SERIALIZERS. To add a new format,
    subclass _Serializer and register it:
        FlatFileStore._SERIALIZERS['csv'] = MyCsvSerializer()

    Compression
    -----------
    Pass compression='gzip', 'bz2', 'lzma', or 'auto' (detect from extension)
    to read and write a compressed file transparently.
    """

    _SERIALIZERS: ClassVar[dict[str, _Serializer]] = {
        'json': JsonSerializer(),
    }

    def __init__(self, path: str | Path, fmt: str = 'json', *, compression: str | None = None) -> None:
        if fmt not in self._SERIALIZERS:
            raise ValueError(
                f"Unsupported format '{fmt}'. "
                f"Available: {list(self._SERIALIZERS)}"
            )
        super().__init__(path, compression=compression)
        self._serializer = self._SERIALIZERS[fmt]

    # ------------------------------------------------------------------
    # Internal I/O
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        with self._open_text('r', encoding='utf-8') as fp:
            return self._serializer.load(fp)

    def _save(self, data: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._open_text('w', encoding='utf-8') as fp:
            self._serializer.dump(data, fp)

    # ------------------------------------------------------------------
    # CRUDL interface
    # ------------------------------------------------------------------

    def create(self, key: str, value: Any) -> None:
        with self._lock.write():
            data = self._load()
            if key in data:
                raise KeyError(key)
            data[key] = value
            self._save(data)

    def read(self, key: str) -> Any:
        with self._lock.read():
            return self._load().get(key)

    def update(self, key: str, value: Any) -> None:
        with self._lock.write():
            data = self._load()
            if key not in data:
                raise KeyError(key)
            data[key] = value
            self._save(data)

    def upsert(self, key: str, value: Any) -> None:
        with self._lock.write():
            data = self._load()
            data[key] = value
            self._save(data)

    def delete(self, key: str) -> None:
        with self._lock.write():
            data = self._load()
            data.pop(key, None)
            self._save(data)

    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        with self._lock.read():
            data = self._load()
        values = list(data.values())
        if predicate is None:
            return values
        return [v for v in values if predicate(v)]
