"""Package-level exceptions for oj_persistence."""


class UpsertNotSupportedError(Exception):
    """
    Raised when upsert() is called on a store that requires a full rewrite
    to satisfy the operation, and allow_inefficient=True was not passed.

    Use create() or update() instead, or pass allow_inefficient=True to
    the manager's upsert() if you genuinely need the behaviour and accept
    the performance cost.
    """
