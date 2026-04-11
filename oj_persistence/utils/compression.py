from __future__ import annotations

import bz2
import gzip
import io
import lzma
from pathlib import Path
from typing import IO

_EXTENSION_MAP: dict[str, str] = {
    '.gz': 'gzip',
    '.bz2': 'bz2',
    '.xz': 'lzma',
    '.lzma': 'lzma',
}

_OPENERS = {
    'gzip': gzip.open,
    'bz2': bz2.open,
    'lzma': lzma.open,
}

VALID_COMPRESSION = frozenset(_OPENERS) | {'auto'}


def resolve(path: Path, compression: str | None) -> str | None:
    """
    Resolve a compression value to a concrete codec name (or None).

    'auto'       → detect from path extension (.gz, .bz2, .xz/.lzma)
    None         → no compression
    'gzip' etc.  → validated and returned as-is
    """
    if compression is None:
        return None
    if compression not in VALID_COMPRESSION:
        raise ValueError(
            f"Unknown compression {compression!r}. "
            f"Expected one of: {sorted(VALID_COMPRESSION)} or None."
        )
    if compression == 'auto':
        return _EXTENSION_MAP.get(path.suffix.lower())
    return compression


def open_text(path: Path, mode: str, compression: str | None, **kwargs) -> IO:
    """Open path in text mode, with optional transparent decompression."""
    if compression is None:
        return path.open(mode, **kwargs)
    text_mode = mode if ('t' in mode or 'b' in mode) else mode + 't'
    return _OPENERS[compression](path, text_mode, **kwargs)


def open_text_csv(path: Path, mode: str, compression: str | None, encoding: str = 'utf-8') -> IO:
    """
    Open path for CSV in text mode with newline='', with optional decompression.

    csv.DictReader/Writer require newline='' on the underlying file handle.
    For compressed files we wrap a binary stream in TextIOWrapper to supply it.
    """
    if compression is None:
        return path.open(mode, newline='', encoding=encoding)
    binary_mode = mode.rstrip('t') + 'b'
    raw = _OPENERS[compression](path, binary_mode)
    return io.TextIOWrapper(raw, encoding=encoding, newline='')


def open_binary(path: Path, mode: str, compression: str | None) -> IO:
    """Open path in binary mode, with optional transparent decompression."""
    if compression is None:
        return path.open(mode)
    binary_mode = mode if 'b' in mode else mode + 'b'
    return _OPENERS[compression](path, binary_mode)
