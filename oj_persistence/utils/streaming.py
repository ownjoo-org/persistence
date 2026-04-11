from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterable, Iterable
from pathlib import Path

_DEBUG_ENV = 'OJ_PERSISTENCE_DEBUG'


def _is_debug(debug: bool) -> bool:
    return debug or os.environ.get(_DEBUG_ENV, '').lower() in ('1', 'true', 'yes')


async def stream_to_file(
    source: AsyncIterable[bytes] | Iterable[bytes],
    path: str | Path,
    *,
    chunk_size: int = 65536,
    debug: bool = False,
) -> Path:
    """
    Stream bytes from source to path atomically.

    Writes to a UUID-named temp file in the same directory, then renames on
    success so the destination never holds partial data.

    Parameters
    ----------
    source      : AsyncIterable[bytes], Iterable[bytes], or any object with a
                  .read(n) method (chunk_size is used in that case).
    path        : Destination path. Parent directories are created if absent.
    chunk_size  : Read size in bytes when source exposes a .read() method.
    debug       : When True (or OJ_PERSISTENCE_DEBUG env var is set), temp files
                  are left on disk after failures for inspection.
    """
    path = Path(path)
    tmp = path.parent / f'{uuid.uuid4()}.tmp'
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tmp.open('wb') as f:
            if hasattr(source, '__aiter__'):
                async for chunk in source:
                    f.write(chunk)
            elif hasattr(source, 'read'):
                while chunk := source.read(chunk_size):
                    f.write(chunk)
            else:
                for chunk in source:
                    f.write(chunk)
        tmp.rename(path)
    except Exception:
        if not _is_debug(debug):
            tmp.unlink(missing_ok=True)
        raise
    return path


def stream_to_file_sync(
    source: Iterable[bytes],
    path: str | Path,
    *,
    chunk_size: int = 65536,
    debug: bool = False,
) -> Path:
    """
    Synchronous variant of stream_to_file.

    Parameters
    ----------
    source      : Iterable[bytes] or any object with a .read(n) method.
    path        : Destination path. Parent directories are created if absent.
    chunk_size  : Read size in bytes when source exposes a .read() method.
    debug       : When True (or OJ_PERSISTENCE_DEBUG env var is set), temp files
                  are left on disk after failures for inspection.
    """
    path = Path(path)
    tmp = path.parent / f'{uuid.uuid4()}.tmp'
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tmp.open('wb') as f:
            if hasattr(source, 'read'):
                while chunk := source.read(chunk_size):
                    f.write(chunk)
            else:
                for chunk in source:
                    f.write(chunk)
        tmp.rename(path)
    except Exception:
        if not _is_debug(debug):
            tmp.unlink(missing_ok=True)
        raise
    return path
