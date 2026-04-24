"""oj-persistence v2 (proposal).

Self-contained proposal package — not imported by oj_persistence's real
public API. Re-export the Manager and spec classes so tests and consumers
can ``from oj_persistence.design.v2 import Manager, Sqlite, InMemory``.
"""

from .base import (
    Backend,
    BackendSpec,
    Capability,
    Csv,
    DynamoDB,
    InMemory,
    Json,
    Ndjson,
    PersistenceError,
    Redis,
    Sqlite,
    SqlAlchemy,
    TableAlreadyRegistered,
    TableNotRegistered,
    TinyDb,
    UnsupportedOperation,
)
from .manager import Manager

__all__ = [
    'Backend',
    'BackendSpec',
    'Capability',
    'Csv',
    'DynamoDB',
    'InMemory',
    'Json',
    'Manager',
    'Ndjson',
    'PersistenceError',
    'Redis',
    'Sqlite',
    'SqlAlchemy',
    'TableAlreadyRegistered',
    'TableNotRegistered',
    'TinyDb',
    'UnsupportedOperation',
]
