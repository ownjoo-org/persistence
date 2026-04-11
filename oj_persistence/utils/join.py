from __future__ import annotations

from collections.abc import Callable
from typing import Any

VALID_JOIN_TYPES = frozenset({'inner', 'left', 'right', 'outer'})


def apply_join(
    left_vals: list[Any],
    right_vals: list[Any],
    on: Callable[[Any, Any], bool],
    how: str,
    where: Callable[[Any, Any], bool] | None,
) -> list[tuple[Any, Any]]:
    """
    Core join algorithm over two already-fetched value lists.

    Called by both PersistenceManager.join() (sync) and
    AsyncPersistenceManager.join() (async) after each manager fetches
    its store values with the appropriate list() call.

    Parameters
    ----------
    left_vals, right_vals : pre-fetched value lists from each store
    on    : join predicate — on(left_val, right_val) → bool
    how   : 'inner' | 'left' | 'right' | 'outer'
    where : optional post-join filter applied to matched pairs

    Returns
    -------
    list of (left_val, right_val) tuples; unmatched sides are None.
    """
    results: list[tuple[Any, Any]] = []

    if how == 'inner':
        for lv in left_vals:
            for rv in right_vals:
                if on(lv, rv) and (where is None or where(lv, rv)):
                    results.append((lv, rv))

    elif how == 'left':
        for lv in left_vals:
            has_match = False
            for rv in right_vals:
                if on(lv, rv):
                    has_match = True
                    if where is None or where(lv, rv):
                        results.append((lv, rv))
            if not has_match:
                results.append((lv, None))

    elif how == 'right':
        for rv in right_vals:
            has_match = False
            for lv in left_vals:
                if on(lv, rv):
                    has_match = True
                    if where is None or where(lv, rv):
                        results.append((lv, rv))
            if not has_match:
                results.append((None, rv))

    elif how == 'outer':
        matched_right: set[int] = set()
        for lv in left_vals:
            has_match = False
            for i, rv in enumerate(right_vals):
                if on(lv, rv):
                    has_match = True
                    matched_right.add(i)
                    if where is None or where(lv, rv):
                        results.append((lv, rv))
            if not has_match:
                results.append((lv, None))
        for i, rv in enumerate(right_vals):
            if i not in matched_right:
                results.append((None, rv))

    return results
