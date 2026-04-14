"""
Python-path implementation of apply_relation().

Called by both PersistenceManager.relate() and AsyncPersistenceManager.relate()
whenever a same-group native join is not available or not applicable.

All join logic lives here; managers only provide the fetched value lists.
"""
from __future__ import annotations

import operator
from collections.abc import Callable
from typing import Any

from oj_persistence.relation import JoinCondition, Op, Relation
from oj_persistence.utils.join import VALID_JOIN_TYPES

_OP_FN: dict[Op, Any] = {
    Op.EQ: operator.eq,
    Op.NE: operator.ne,
    Op.LT: operator.lt,
    Op.LE: operator.le,
    Op.GT: operator.gt,
    Op.GE: operator.ge,
}


def _evaluate(left_val: Any, right_val: Any, conditions: list[JoinCondition]) -> bool:
    """Return True when all conditions hold between left_val and right_val."""
    for cond in conditions:
        lv = left_val[cond.left_field]
        rv = right_val[cond.right_field]
        if not _OP_FN[cond.op](lv, rv):
            return False
    return True


def _project(value: Any, fields: list[str] | None) -> Any:
    """Return a projected dict if fields is given; otherwise return value unchanged."""
    if fields is None or not isinstance(value, dict):
        return value
    return {k: value[k] for k in fields if k in value}


def apply_relation(
    left_vals: list[Any],
    right_vals: list[Any],
    on: JoinCondition | list[JoinCondition],
    how: str = 'inner',
    left_fields: list[str] | None = None,
    right_fields: list[str] | None = None,
    where: Callable[[Any, Any], bool] | None = None,
) -> list[tuple[Any, Any]]:
    """
    Evaluate a Relation against two pre-fetched value lists.

    Parameters
    ----------
    left_vals, right_vals : pre-fetched from the respective stores
    on           : JoinCondition or list; all conditions ANDed
    how          : 'inner' | 'left' | 'right' | 'outer'
    left_fields  : project left result (None = all)
    right_fields : project right result (None = all)
    where        : optional post-join filter on matched pairs

    Returns
    -------
    list of (left_val, right_val) tuples; unmatched sides are None.
    Projection is applied to non-None sides only.
    """
    if how not in VALID_JOIN_TYPES:
        raise ValueError(f"Invalid how={how!r}. Expected one of: {sorted(VALID_JOIN_TYPES)}")

    conditions = on if isinstance(on, list) else [on]

    def matches(lv: Any, rv: Any) -> bool:
        return _evaluate(lv, rv, conditions)

    results: list[tuple[Any, Any]] = []

    if how == 'inner':
        for lv in left_vals:
            for rv in right_vals:
                if matches(lv, rv) and (where is None or where(lv, rv)):
                    results.append((_project(lv, left_fields), _project(rv, right_fields)))

    elif how == 'left':
        for lv in left_vals:
            has_match = False
            for rv in right_vals:
                if matches(lv, rv):
                    has_match = True
                    if where is None or where(lv, rv):
                        results.append((_project(lv, left_fields), _project(rv, right_fields)))
            if not has_match:
                results.append((_project(lv, left_fields), None))

    elif how == 'right':
        for rv in right_vals:
            has_match = False
            for lv in left_vals:
                if matches(lv, rv):
                    has_match = True
                    if where is None or where(lv, rv):
                        results.append((_project(lv, left_fields), _project(rv, right_fields)))
            if not has_match:
                results.append((None, _project(rv, right_fields)))

    elif how == 'outer':
        matched_right: set[int] = set()
        for lv in left_vals:
            has_match = False
            for i, rv in enumerate(right_vals):
                if matches(lv, rv):
                    has_match = True
                    matched_right.add(i)
                    if where is None or where(lv, rv):
                        results.append((_project(lv, left_fields), _project(rv, right_fields)))
            if not has_match:
                results.append((_project(lv, left_fields), None))
        for i, rv in enumerate(right_vals):
            if i not in matched_right:
                results.append((None, _project(rv, right_fields)))

    return results
