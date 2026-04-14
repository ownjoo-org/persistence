from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any


class AsyncAbstractStore(ABC):
    """
    Async CRUDL interface for persistence backends that support non-blocking I/O.

    Mirrors AbstractStore semantics exactly:
      - create()  raises KeyError if the key already exists
      - update()  raises KeyError if the key does not exist
      - upsert()  always succeeds (create or update)
      - read()    returns None for missing keys (no raise)
      - delete()  is a no-op for missing keys (no raise)
      - list()    returns all values, optionally filtered by predicate

    Implementations should also support use as an async context manager so
    callers can guarantee a final flush/close on exit:

        async with AsyncSomeStore(...) as store:
            await store.upsert('k', v)
        # final flush guaranteed here
    """

    @abstractmethod
    async def create(self, key: str, value: Any) -> None:
        """Store value under key. Raises KeyError if key already exists."""

    @abstractmethod
    async def read(self, key: str) -> Any:
        """Return the value for key, or None if not found."""

    @abstractmethod
    async def update(self, key: str, value: Any) -> None:
        """Update value for an existing key. Raises KeyError if not found."""

    @abstractmethod
    async def upsert(self, key: str, value: Any) -> None:
        """Store value under key, creating or overwriting as needed."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Remove the entry for key. No-op if key does not exist."""

    @abstractmethod
    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        """Return all values, or only those for which predicate(value) is True."""

    @property
    def supports_native_upsert(self) -> bool:
        """
        True when upsert() is implemented as an atomic or native operation.
        False when upsert() requires a full file rewrite.

        The manager blocks upsert() on False stores unless allow_inefficient=True
        is passed.  Subclasses that need a full rewrite must override to False.
        AsyncVersionedStore delegates to its inner store.
        """
        return True

    async def __aenter__(self) -> "AsyncAbstractStore":
        return self

    async def __aexit__(self, *args) -> None:  # noqa: B027 — intentional no-op default; subclasses override to flush
        pass
