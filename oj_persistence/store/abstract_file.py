from __future__ import annotations

from collections.abc import AsyncIterable, Iterable
from pathlib import Path
from typing import Any

from oj_persistence.store.base import AbstractStore
from oj_persistence.utils.compression import (
    open_binary,
    open_text,
    open_text_csv,
    resolve,
)
from oj_persistence.utils.rwlock import ReadWriteLock


class AbstractFileStore(AbstractStore):
    """
    Base class for all file-backed stores.

    Provides:
      - _path, _compression, _lock initialisation
      - _open_text()  / _open_binary()  helpers for the primary store file
      - from_stream() / from_stream_sync() classmethods
    """

    def __init__(self, path: str | Path, *, compression: str | None = None) -> None:
        self._path = Path(path)
        self._compression = resolve(self._path, compression)
        self._lock = ReadWriteLock()

    # ------------------------------------------------------------------ openers

    def _open_text(self, mode: str, **kwargs):
        """Open the store file in text mode (plain or compressed)."""
        return open_text(self._path, mode, self._compression, **kwargs)

    def _open_text_csv(self, mode: str, path: Path | None = None, encoding: str = 'utf-8'):
        """Open a file for CSV access (newline='', plain or compressed)."""
        return open_text_csv(path or self._path, mode, self._compression, encoding=encoding)

    def _open_binary(self, mode: str):
        """Open the store file in binary mode (plain or compressed)."""
        return open_binary(self._path, mode, self._compression)

    # ------------------------------------------------------------------ from_stream

    @classmethod
    async def from_stream(
        cls,
        source: AsyncIterable[bytes] | Iterable[bytes],
        path: str | Path,
        *,
        chunk_size: int = 65536,
        debug: bool = False,
        compression: str | None = None,
        **kwargs: Any,
    ):
        """
        Stream bytes from source to path, then open the file as this store type.

        Parameters
        ----------
        source      : AsyncIterable[bytes], Iterable[bytes], or file-like (.read).
        path        : Destination path on disk.
        chunk_size  : Read size in bytes when source exposes a .read() method.
        debug       : Leave temp files on disk after failures (also set via
                      OJ_PERSISTENCE_DEBUG env var).
        compression : Passed through to the store constructor. 'auto' detects
                      from the file extension.
        **kwargs    : Forwarded to the concrete store constructor (e.g. fieldnames
                      for CsvFileStore, fmt for FlatFileStore).
        """
        from oj_persistence.utils.streaming import stream_to_file
        final_path = await stream_to_file(source, path, chunk_size=chunk_size, debug=debug)
        return cls(final_path, compression=compression, **kwargs)

    @classmethod
    def from_stream_sync(
        cls,
        source: Iterable[bytes],
        path: str | Path,
        *,
        chunk_size: int = 65536,
        debug: bool = False,
        compression: str | None = None,
        **kwargs: Any,
    ):
        """
        Synchronous variant of from_stream.

        Parameters
        ----------
        source      : Iterable[bytes] or file-like (.read).
        path        : Destination path on disk.
        chunk_size  : Read size in bytes when source exposes a .read() method.
        debug       : Leave temp files on disk after failures (also set via
                      OJ_PERSISTENCE_DEBUG env var).
        compression : Passed through to the store constructor.
        **kwargs    : Forwarded to the concrete store constructor.
        """
        from oj_persistence.utils.streaming import stream_to_file_sync
        final_path = stream_to_file_sync(source, path, chunk_size=chunk_size, debug=debug)
        return cls(final_path, compression=compression, **kwargs)
