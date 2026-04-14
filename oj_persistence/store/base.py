from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any


class AbstractStore(ABC):
    """
    CRUDL interface for all persistence backends.

    Strict create/update semantics:
      - create() raises KeyError if the key already exists
      - update() raises KeyError if the key does not exist
      - upsert() always succeeds (create or update)
      - read()   returns None for missing keys (no raise)
      - delete() is a no-op for missing keys (no raise)
      - list()   returns all values, optionally filtered by predicate

    Implementations must be thread-safe. Async variants are planned for a
    future release to support io_chains integration as an async source or
    subscriber.
    """

    @abstractmethod
    def create(self, key: str, value: Any) -> None:
        """Store value under key. Raises KeyError if key already exists."""

    @abstractmethod
    def read(self, key: str) -> Any:
        """Return the value for key, or None if not found."""

    @abstractmethod
    def update(self, key: str, value: Any) -> None:
        """Update value for an existing key. Raises KeyError if not found."""

    @abstractmethod
    def upsert(self, key: str, value: Any) -> None:
        """Store value under key, creating or overwriting as needed."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove the entry for key. No-op if key does not exist."""

    @abstractmethod
    def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        """Return all values, or only those for which predicate(value) is True."""

    @property
    def supports_native_upsert(self) -> bool:
        """
        True when upsert() is implemented as an atomic or native operation
        (dict assignment, SQL INSERT OR REPLACE, Redis SET, etc.).

        False when upsert() requires a full file rewrite to locate and replace
        an existing record (NDJSON, CSV, flat-file stores).  The manager blocks
        upsert() on False stores unless allow_inefficient=True is passed.

        Subclasses that need a full rewrite must override this to return False.
        VersionedStore delegates to its inner store.
        """
        return True
