"""
Tests for Op / JoinCondition / Relation types, apply_relation() utility,
and PersistenceManager.relate() / AsyncPersistenceManager.relate().

Covers:
  - type structure and validation
  - Python-path joins (cross-group in-memory stores — always falls back here)
  - all join types (inner / left / right / outer)
  - all operators (EQ, NE, LT, LE, GT, GE)
  - compound JoinConditions (AND semantics)
  - projection (left_fields / right_fields)
  - where filter
  - same-group SQLite native join (sync manager only for now)
  - error cases (unknown store, invalid how)
"""
import unittest

from oj_persistence.relation import JoinCondition, Op, Relation


# ---------------------------------------------------------------------------
# Op enum
# ---------------------------------------------------------------------------

class TestOp(unittest.TestCase):
    def test_all_operators_exist(self):
        for name in ('EQ', 'NE', 'LT', 'LE', 'GT', 'GE'):
            self.assertTrue(hasattr(Op, name), f"Op.{name} missing")

    def test_values_are_strings(self):
        for op in Op:
            self.assertIsInstance(op.value, str)


# ---------------------------------------------------------------------------
# JoinCondition dataclass
# ---------------------------------------------------------------------------

class TestJoinCondition(unittest.TestCase):
    def test_fields(self):
        jc = JoinCondition(left_field='id', op=Op.EQ, right_field='user_id')
        self.assertEqual(jc.left_field, 'id')
        self.assertEqual(jc.op, Op.EQ)
        self.assertEqual(jc.right_field, 'user_id')

    def test_frozen(self):
        jc = JoinCondition('id', Op.EQ, 'user_id')
        with self.assertRaises((AttributeError, TypeError)):
            jc.op = Op.NE  # type: ignore[misc]

    def test_equality(self):
        a = JoinCondition('id', Op.EQ, 'user_id')
        b = JoinCondition('id', Op.EQ, 'user_id')
        self.assertEqual(a, b)

    def test_hashable(self):
        {JoinCondition('id', Op.EQ, 'user_id')}  # must not raise


# ---------------------------------------------------------------------------
# Relation dataclass
# ---------------------------------------------------------------------------

class TestRelation(unittest.TestCase):
    def test_minimal_construction(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        r = Relation(left_store='users', right_store='orders', on=on)
        self.assertEqual(r.left_store, 'users')
        self.assertEqual(r.right_store, 'orders')
        self.assertEqual(r.on, on)
        self.assertEqual(r.how, 'inner')
        self.assertIsNone(r.left_fields)
        self.assertIsNone(r.right_fields)
        self.assertIsNone(r.where)

    def test_compound_on(self):
        conditions = [
            JoinCondition('dept', Op.EQ, 'dept'),
            JoinCondition('level', Op.GE, 'min_level'),
        ]
        r = Relation(left_store='emp', right_store='bands', on=conditions)
        self.assertEqual(r.on, conditions)

    def test_projection(self):
        r = Relation(
            left_store='users', right_store='orders',
            on=JoinCondition('id', Op.EQ, 'user_id'),
            left_fields=['name'], right_fields=['amount'],
        )
        self.assertEqual(r.left_fields, ['name'])
        self.assertEqual(r.right_fields, ['amount'])

    def test_how_variants(self):
        for how in ('inner', 'left', 'right', 'outer'):
            r = Relation('a', 'b', JoinCondition('x', Op.EQ, 'y'), how=how)
            self.assertEqual(r.how, how)


# ---------------------------------------------------------------------------
# apply_relation() — pure Python path
# ---------------------------------------------------------------------------

from oj_persistence.utils.relation import apply_relation


USERS = [
    {'id': 'u1', 'name': 'Alice', 'score': 10},
    {'id': 'u2', 'name': 'Bob',   'score': 5},
    {'id': 'u3', 'name': 'Carol', 'score': 8},
]
ORDERS = [
    {'order_id': 'o1', 'user_id': 'u1', 'amount': 100},
    {'order_id': 'o2', 'user_id': 'u1', 'amount': 200},
    {'order_id': 'o3', 'user_id': 'u2', 'amount': 50},
]


class TestApplyRelation(unittest.TestCase):

    # -- inner join --

    def test_inner_join_eq(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(USERS, ORDERS, on, how='inner')
        self.assertEqual(len(result), 3)
        for u, o in result:
            self.assertEqual(u['id'], o['user_id'])

    def test_inner_join_no_match_returns_empty(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(USERS, [], on, how='inner')
        self.assertEqual(result, [])

    # -- left join --

    def test_left_join_includes_unmatched_left(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(USERS, ORDERS, on, how='left')
        unmatched = [(u, o) for u, o in result if o is None]
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0][0]['name'], 'Carol')

    # -- right join --

    def test_right_join_includes_unmatched_right(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        dangling_order = [{'order_id': 'o9', 'user_id': 'u_unknown', 'amount': 999}]
        result = apply_relation(USERS, dangling_order, on, how='right')
        unmatched = [(u, o) for u, o in result if u is None]
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0][1]['order_id'], 'o9')

    # -- outer join --

    def test_outer_join_includes_both_unmatched(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        left = [{'id': 'u1'}, {'id': 'u_only'}]
        right = [{'user_id': 'u1'}, {'user_id': 'r_only'}]
        result = apply_relation(left, right, on, how='outer')
        left_only  = [(u, o) for u, o in result if o is None]
        right_only = [(u, o) for u, o in result if u is None]
        self.assertEqual(len(left_only), 1)
        self.assertEqual(left_only[0][0]['id'], 'u_only')
        self.assertEqual(len(right_only), 1)
        self.assertEqual(right_only[0][1]['user_id'], 'r_only')

    # -- operators --

    def test_ne_operator(self):
        on = JoinCondition('id', Op.NE, 'user_id')
        result = apply_relation(USERS[:1], ORDERS, on, how='inner')
        # u1 != u1 is False, u1 != u2 ... wait u1 matches o3 (user_id=u2), not o1 or o2
        matched_order_ids = {o['order_id'] for _, o in result}
        self.assertIn('o3', matched_order_ids)
        self.assertNotIn('o1', matched_order_ids)

    def test_lt_operator(self):
        items = [{'val': 3}, {'val': 7}]
        thresholds = [{'max': 5}]
        on = JoinCondition('val', Op.LT, 'max')
        result = apply_relation(items, thresholds, on, how='inner')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]['val'], 3)

    def test_le_operator(self):
        items = [{'val': 5}, {'val': 6}]
        thresholds = [{'max': 5}]
        on = JoinCondition('val', Op.LE, 'max')
        result = apply_relation(items, thresholds, on, how='inner')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]['val'], 5)

    def test_gt_operator(self):
        items = [{'score': 10}, {'score': 3}]
        thresholds = [{'min': 5}]
        on = JoinCondition('score', Op.GT, 'min')
        result = apply_relation(items, thresholds, on, how='inner')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]['score'], 10)

    def test_ge_operator(self):
        items = [{'score': 5}, {'score': 4}]
        thresholds = [{'min': 5}]
        on = JoinCondition('score', Op.GE, 'min')
        result = apply_relation(items, thresholds, on, how='inner')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]['score'], 5)

    # -- compound conditions --

    def test_compound_on_all_must_match(self):
        on = [
            JoinCondition('dept', Op.EQ, 'dept'),
            JoinCondition('level', Op.GE, 'min_level'),
        ]
        employees = [
            {'dept': 'eng', 'level': 3, 'name': 'Alice'},
            {'dept': 'eng', 'level': 1, 'name': 'Bob'},
            {'dept': 'hr',  'level': 5, 'name': 'Carol'},
        ]
        bands = [{'dept': 'eng', 'min_level': 2}]
        result = apply_relation(employees, bands, on, how='inner')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]['name'], 'Alice')

    # -- projection --

    def test_left_fields_projection(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(USERS, ORDERS, on, how='inner', left_fields=['name'])
        for u, o in result:
            self.assertEqual(set(u.keys()), {'name'})
            self.assertIsNotNone(o)  # right side unprojected

    def test_right_fields_projection(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(USERS, ORDERS, on, how='inner', right_fields=['amount'])
        for u, o in result:
            self.assertEqual(set(o.keys()), {'amount'})

    def test_both_fields_projected(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(USERS, ORDERS, on, how='inner',
                                left_fields=['name'], right_fields=['amount'])
        for u, o in result:
            self.assertEqual(set(u.keys()), {'name'})
            self.assertEqual(set(o.keys()), {'amount'})

    def test_projection_none_returns_full_records(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(USERS, ORDERS, on, how='inner')
        for u, o in result:
            self.assertIn('score', u)       # full left record
            self.assertIn('order_id', o)    # full right record

    # -- where filter --

    def test_where_filter_applied_after_join(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = apply_relation(
            USERS, ORDERS, on, how='inner',
            where=lambda u, o: o['amount'] > 100,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][1]['amount'], 200)

    # -- invalid how --

    def test_invalid_how_raises(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        with self.assertRaises(ValueError):
            apply_relation(USERS, ORDERS, on, how='diagonal')


# ---------------------------------------------------------------------------
# PersistenceManager.relate() — sync
# ---------------------------------------------------------------------------

from oj_persistence.manager import PersistenceManager
from oj_persistence.store.in_memory import InMemoryStore


def _seed_sync(pm, users_id, orders_id):
    for u in USERS:
        pm.create(users_id, u['id'], u)
    for o in ORDERS:
        pm.create(orders_id, o['order_id'], o)


class TestSyncManagerRelate(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = PersistenceManager()
        # cross-group: Python path
        self.u_ref = self.pm.configure(store_type='in_memory', store_id='users')
        self.o_ref = self.pm.configure(store_type='in_memory', store_id='orders')
        _seed_sync(self.pm, 'users', 'orders')

    def tearDown(self):
        PersistenceManager._instance = None

    def test_inner_join(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = self.pm.relate(Relation('users', 'orders', on))
        self.assertEqual(len(result), 3)

    def test_left_join(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = self.pm.relate(Relation('users', 'orders', on, how='left'))
        unmatched = [pair for pair in result if pair[1] is None]
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0][0]['name'], 'Carol')

    def test_gt_operator(self):
        items_ref  = self.pm.configure(store_type='in_memory', store_id='items')
        thresh_ref = self.pm.configure(store_type='in_memory', store_id='thresh')
        self.pm.create('items',  'a', {'val': 10})
        self.pm.create('items',  'b', {'val': 3})
        self.pm.create('thresh', 't', {'min': 5})
        on = JoinCondition('val', Op.GT, 'min')
        result = self.pm.relate(Relation('items', 'thresh', on))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]['val'], 10)

    def test_projection(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = self.pm.relate(
            Relation('users', 'orders', on, left_fields=['name'], right_fields=['amount'])
        )
        for u, o in result:
            self.assertEqual(set(u.keys()), {'name'})
            self.assertEqual(set(o.keys()), {'amount'})

    def test_invalid_how_raises(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        with self.assertRaises(ValueError):
            self.pm.relate(Relation('users', 'orders', on, how='diagonal'))

    def test_unknown_store_raises(self):
        on = JoinCondition('id', Op.EQ, 'user_id')
        with self.assertRaises(KeyError):
            self.pm.relate(Relation('users', 'ghost', on))

    def test_same_group_sqlite_native_join(self):
        """Same-group SQLite: manager should delegate to SQL JOIN."""
        pm2 = PersistenceManager()
        g = pm2.create_group(store_type='sqlite', group_id='appdb', path=':memory:')
        u_ref = pm2.add_table(group_id='appdb', store_id='su', table_id='users')
        o_ref = pm2.add_table(group_id='appdb', store_id='so', table_id='orders')

        for u in USERS:
            pm2.create('su', u['id'], u)
        for o in ORDERS:
            pm2.create('so', o['order_id'], o)

        on = JoinCondition('id', Op.EQ, 'user_id')
        result = pm2.relate(Relation('su', 'so', on))
        self.assertEqual(len(result), 3)
        for u, o in result:
            self.assertEqual(u['id'], o['user_id'])


# ---------------------------------------------------------------------------
# AsyncPersistenceManager.relate() — async
# ---------------------------------------------------------------------------

from oj_persistence.async_manager import AsyncPersistenceManager


async def _seed_async(pm, users_id, orders_id):
    for u in USERS:
        await pm.create(users_id, u['id'], u)
    for o in ORDERS:
        await pm.create(orders_id, o['order_id'], o)


class TestAsyncManagerRelate(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None
        self.pm = AsyncPersistenceManager()

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def _seed(self):
        await self.pm.configure(store_type='in_memory', store_id='users')
        await self.pm.configure(store_type='in_memory', store_id='orders')
        await _seed_async(self.pm, 'users', 'orders')

    async def test_inner_join(self):
        await self._seed()
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = await self.pm.relate(Relation('users', 'orders', on))
        self.assertEqual(len(result), 3)

    async def test_left_join(self):
        await self._seed()
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = await self.pm.relate(Relation('users', 'orders', on, how='left'))
        unmatched = [p for p in result if p[1] is None]
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0][0]['name'], 'Carol')

    async def test_gt_operator(self):
        await self.pm.configure(store_type='in_memory', store_id='items')
        await self.pm.configure(store_type='in_memory', store_id='thresh')
        await self.pm.create('items',  'a', {'val': 10})
        await self.pm.create('items',  'b', {'val': 3})
        await self.pm.create('thresh', 't', {'min': 5})
        on = JoinCondition('val', Op.GT, 'min')
        result = await self.pm.relate(Relation('items', 'thresh', on))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]['val'], 10)

    async def test_projection(self):
        await self._seed()
        on = JoinCondition('id', Op.EQ, 'user_id')
        result = await self.pm.relate(
            Relation('users', 'orders', on, left_fields=['name'], right_fields=['amount'])
        )
        for u, o in result:
            self.assertEqual(set(u.keys()), {'name'})
            self.assertEqual(set(o.keys()), {'amount'})

    async def test_invalid_how_raises(self):
        await self._seed()
        on = JoinCondition('id', Op.EQ, 'user_id')
        with self.assertRaises(ValueError):
            await self.pm.relate(Relation('users', 'orders', on, how='diagonal'))

    async def test_unknown_store_raises(self):
        await self._seed()
        on = JoinCondition('id', Op.EQ, 'user_id')
        with self.assertRaises(KeyError):
            await self.pm.relate(Relation('users', 'ghost', on))

    async def test_same_group_sqlite_native_join(self):
        """Same-group SQLite: manager should delegate to SQL JOIN."""
        g = self.pm.create_group(store_type='sqlite', group_id='appdb', path=':memory:')
        u_ref = await self.pm.add_table(group_id='appdb', store_id='su', table_id='users')
        o_ref = await self.pm.add_table(group_id='appdb', store_id='so', table_id='orders')

        for u in USERS:
            await self.pm.create('su', u['id'], u)
        for o in ORDERS:
            await self.pm.create('so', o['order_id'], o)

        on = JoinCondition('id', Op.EQ, 'user_id')
        result = await self.pm.relate(Relation('su', 'so', on))
        self.assertEqual(len(result), 3)
        for u, o in result:
            self.assertEqual(u['id'], o['user_id'])


if __name__ == '__main__':
    unittest.main()
