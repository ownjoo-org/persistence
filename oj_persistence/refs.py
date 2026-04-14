"""
Public reference types returned by the manager's store-creation API.

Callers hold GroupRef / StoreRef to name stores in subsequent CRUDL calls.
Connection details (file paths, URLs, credentials) are never included here —
they stay inside the manager and are not accessible at runtime.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GroupRef:
    """
    Identifies a backing store group (a database file, an in-memory namespace,
    a Redis connection, etc.).

    Returned by PersistenceManager.create_group() and held as long as the
    caller needs to add new tables to the group.  The group_id is the key
    used in group_catalog().
    """

    group_id: str
    store_type: str


@dataclass(frozen=True)
class StoreRef:
    """
    Identifies a single logical table within a group.

    Returned by PersistenceManager.add_table() and PersistenceManager.configure().
    The store_id is the key used in all CRUDL calls and in catalog().
    """

    store_id: str
    group_id: str
    table_id: str
    store_type: str
