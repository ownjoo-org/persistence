"""
Relation types for cross-store and same-store queries.

Usage
-----
    from oj_persistence.relation import JoinCondition, Op, Relation

    result = pm.relate(Relation(
        left_store='users',
        right_store='orders',
        on=JoinCondition('id', Op.EQ, 'user_id'),
        how='inner',
        left_fields=['name', 'email'],
        right_fields=['amount', 'created_at'],
    ))

JoinCondition.op is inspectable so the manager can translate equi-join
conditions into native SQL when both stores share a backing group.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Op(str, Enum):
    """Comparison operator for a JoinCondition."""
    EQ = '=='
    NE = '!='
    LT = '<'
    LE = '<='
    GT = '>'
    GE = '>='


@dataclass(frozen=True)
class JoinCondition:
    """
    A single field-level join predicate: left_val[left_field] op right_val[right_field].

    Use a list of JoinConditions for compound joins (all conditions are ANDed).
    """
    left_field: str
    op: Op
    right_field: str


@dataclass
class Relation:
    """
    Declarative specification for a relational join across two stores.

    Parameters
    ----------
    left_store   : store_id of the left-hand store (must be registered)
    right_store  : store_id of the right-hand store (must be registered)
    on           : one JoinCondition or a list (all ANDed together)
    how          : 'inner' | 'left' | 'right' | 'outer'
    left_fields  : project the left result to these fields (None = return all)
    right_fields : project the right result to these fields (None = return all)
    where        : optional post-join filter called as where(left_val, right_val);
                   applied only to matched pairs — unmatched sides always pass through
    """
    left_store:   str
    right_store:  str
    on:           JoinCondition | list[JoinCondition]
    how:          str = 'inner'
    left_fields:  list[str] | None = None
    right_fields: list[str] | None = None
    where:        Callable[[Any, Any], bool] | None = field(default=None, compare=False)
