"""Backend interface + capability declarations + backend-config dataclasses.

PROPOSAL — not wired into the package yet. See ./README.md for context.

Consumers never touch anything in this module directly. They hand a ``BackendSpec``
(one of the dataclasses below) to ``Manager.register(table, spec)``, and the
Manager constructs and owns the real ``Backend`` instance internally.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


# ------------------------------------------------------------------ capabilities

class Capability(StrEnum):
    """Features a Backend may declare it supports.

    Capabilities are declared at the class level (``Backend.capabilities``) so the
    Manager can validate at ``register()`` time whether an intended usage pattern
    will work on the chosen backend. No more runtime surprises.
    """
    PAGINATION = 'pagination'       # list_page(offset, limit)
    FIELD_INDEX = 'field_index'     # add_index + list_by_field
    NATIVE_JOIN = 'native_join'     # SQL JOIN pushdown when two tables share a backing file
    NATIVE_UPSERT = 'native_upsert' # in-place upsert without rewriting the whole backing file


# ------------------------------------------------------------------ backend specs

class BackendSpec:
    """Marker base for backend config classes. Subclasses are frozen dataclasses.

    A spec is a cheap, hashable description of "where a table lives and how to
    connect to it." The Manager uses the spec to construct a real ``Backend``
    and to deduplicate connections (two tables with identical ``Sqlite(path=...)``
    specs share a connection internally).
    """


@dataclass(frozen=True)
class Sqlite(BackendSpec):
    path: str | Path = ':memory:'
    pool_size: int = 4  # reader-connection pool size


@dataclass(frozen=True)
class InMemory(BackendSpec):
    pass


@dataclass(frozen=True)
class Ndjson(BackendSpec):
    path: str | Path


@dataclass(frozen=True)
class Redis(BackendSpec):
    url: str
    db: int = 0
    prefix: str = ''


@dataclass(frozen=True)
class SqlAlchemy(BackendSpec):
    url: str  # full SQLAlchemy URL, e.g. "postgresql://user:pass@host/db"


@dataclass(frozen=True)
class TinyDb(BackendSpec):
    path: str | Path


# ------------------------------------------------------------------ backend abstract

class Backend(ABC):
    """Internal contract every backend implements.

    Consumers never see this class. The Manager instantiates exactly one Backend
    per unique connection target (e.g. one per sqlite path, one per redis URL)
    and multiplexes tables onto it.

    Every method is async — the Manager's sync entry points run these on a thread
    pool. Backends that are naturally sync (sqlite) use ``asyncio.to_thread``
    internally; backends that are natively async (redis via aioredis) just await.
    """

    # ---- class-level declarations ----

    capabilities: frozenset[Capability] = frozenset()

    # ---- lifecycle ----

    @abstractmethod
    async def aopen(self) -> None:
        """Acquire the backend's resources (sqlite connections, redis client, etc).

        Called once by the Manager when the first table referencing this backend
        is registered. Idempotent: safe to call on an already-open backend.
        """

    @abstractmethod
    async def aclose(self) -> None:
        """Release all resources. After this returns, the backend is unusable."""

    # ---- table management ----

    @abstractmethod
    async def acreate_table(self, table: str) -> None:
        """Create the backing storage for ``table`` if it doesn't exist."""

    @abstractmethod
    async def adrop_table(self, table: str) -> None:
        """Remove all data for ``table`` and its backing storage (e.g. DROP TABLE)."""

    @abstractmethod
    async def atruncate_table(self, table: str) -> None:
        """Remove all rows from ``table``. The table itself continues to exist."""

    @abstractmethod
    async def atable_exists(self, table: str) -> bool: ...

    # ---- CRUDL ----

    @abstractmethod
    async def acreate(self, table: str, key: str, value: Any) -> None:
        """Insert ``(key, value)``. Raises ``KeyError`` if ``key`` already exists."""

    @abstractmethod
    async def aread(self, table: str, key: str) -> Any | None:
        """Return the value for ``key``, or ``None`` if absent."""

    @abstractmethod
    async def aupdate(self, table: str, key: str, value: Any) -> None:
        """Overwrite an existing entry. Raises ``KeyError`` if ``key`` is absent."""

    @abstractmethod
    async def aupsert(self, table: str, key: str, value: Any) -> None:
        """Create or overwrite.

        Backends without ``NATIVE_UPSERT`` implement this via read+write (which
        may involve rewriting the whole backing file). Consumers who care about
        the cost can check ``Capability.NATIVE_UPSERT`` before calling.
        """

    @abstractmethod
    async def adelete(self, table: str, key: str) -> None:
        """Remove ``key``. No-op if absent."""

    @abstractmethod
    async def alist(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> list[Any]:
        """Materialize all values in ``table`` (filtered by ``predicate`` if given)."""

    @abstractmethod
    def aiter(
        self,
        table: str,
        predicate: Callable[[Any], bool] | None = None,
    ) -> AsyncIterator[Any]:
        """Stream values one at a time. Preferred when the table is large."""

    # ---- optional (capability-gated) ----

    async def alist_page(self, table: str, offset: int, limit: int) -> list[Any]:
        """Pagination. Backends declaring ``Capability.PAGINATION`` must override."""
        raise NotImplementedError

    async def alist_by_field(self, table: str, json_path: str, value: Any) -> list[Any]:
        """Field-indexed lookup. Backends declaring ``Capability.FIELD_INDEX`` must override."""
        raise NotImplementedError

    async def aadd_index(self, table: str, json_path: str, *, name: str | None = None) -> None:
        """Create an index on a JSON field. Required by ``Capability.FIELD_INDEX``."""
        raise NotImplementedError

    async def anative_join(
        self,
        left_table: str,
        right_table: str,
        *args,
        **kwargs,
    ) -> list[tuple[Any, Any]]:
        """Push a JOIN down to the backend. Required by ``Capability.NATIVE_JOIN``."""
        raise NotImplementedError


# ------------------------------------------------------------------ sync facade

class SyncBackend:
    """Thin sync wrapper over a ``Backend``. Constructed by the Manager internally
    so its sync entry points don't have to juggle an event loop per call.

    Each method runs the async counterpart via ``asyncio.run`` on a dedicated
    manager-owned loop (see ``Manager._run_sync``). Users of the Manager never
    instantiate or see this class.
    """
    # Signature-for-signature mirror of Backend. Full impl in manager.py's
    # _run_sync helper rather than here — leaving the concrete sync wrapper as
    # a proposal-level detail.


# ------------------------------------------------------------------ errors

class PersistenceError(Exception):
    """Base class for this library's public errors."""


class TableAlreadyRegistered(PersistenceError):
    """Raised when ``register()`` is called for a table that already exists.

    Pass ``replace=True`` to override. Existing connection/data on the previous
    spec is closed and dropped first.
    """


class TableNotRegistered(PersistenceError):
    """Raised when a data op targets a table the Manager has never heard of."""


class UnsupportedOperation(PersistenceError):
    """Raised at ``register()`` time when a consumer declares they'll need a
    capability the chosen backend doesn't offer, or at call time if the
    operation simply isn't available (and no earlier declaration was made).
    """
