from abc import ABC, abstractmethod
from typing import Any, Callable, Optional


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
    def list(self, predicate: Optional[Callable[[Any], bool]] = None) -> list[Any]:
        """Return all values, or only those for which predicate(value) is True."""
