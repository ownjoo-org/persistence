"""
Integration tests for PersistenceManager.join().

Test data:
  users:  u1=Alice, u2=Bob, u3=Charlie (no orders)
  orders: o1(u1, 100), o2(u2, 200), o3(u1, 50), o4(u99, 75) — o4 is orphaned

Join predicate: user['id'] == order['user_id']

Join type coverage:
  inner  — only matched pairs
  left   — all users, None on right when unmatched
  right  — all orders, None on left when unmatched
  outer  — all rows from both sides

where semantics:
  - Applied only to matched pairs (both sides non-None).
  - Unmatched rows (one side is None) are always included regardless of where.
  - A left row that has on-matches but all are filtered by where is NOT demoted
    to an unmatched (l, None) row — it simply contributes no result rows.
"""
import unittest

from oj_persistence import InMemoryStore, PersistenceManager


def ON(u, o):
    return u['id'] == o['user_id']


def _make_pm():
    manager = PersistenceManager()

    users = manager.get_or_create('users', lambda: InMemoryStore())
    users.create('u1', {'id': 'u1', 'name': 'Alice'})
    users.create('u2', {'id': 'u2', 'name': 'Bob'})
    users.create('u3', {'id': 'u3', 'name': 'Charlie'})  # no matching orders

    orders = manager.get_or_create('orders', lambda: InMemoryStore())
    orders.create('o1', {'user_id': 'u1', 'total': 100})
    orders.create('o2', {'user_id': 'u2', 'total': 200})
    orders.create('o3', {'user_id': 'u1', 'total': 50})
    orders.create('o4', {'user_id': 'u99', 'total': 75})  # no matching user

    return manager


class TestInnerJoin(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = _make_pm()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_returns_only_matched_pairs(self):
        self.assertEqual(len(self.pm.join('users', 'orders', on=ON)), 3)

    def test_each_result_is_a_tuple_of_left_right(self):
        for left, right in self.pm.join('users', 'orders', on=ON):
            self.assertIsNotNone(left)
            self.assertIsNotNone(right)

    def test_matched_values_are_correct(self):
        results = self.pm.join('users', 'orders', on=ON)
        names = sorted(lv['name'] for lv, _ in results)
        totals = sorted(r['total'] for _, r in results)
        self.assertEqual(names, ['Alice', 'Alice', 'Bob'])
        self.assertEqual(totals, [50, 100, 200])

    def test_unmatched_left_excluded(self):
        results = self.pm.join('users', 'orders', on=ON)
        names = [lv['name'] for lv, _ in results]
        self.assertNotIn('Charlie', names)

    def test_unmatched_right_excluded(self):
        results = self.pm.join('users', 'orders', on=ON)
        user_ids = [r['user_id'] for _, r in results]
        self.assertNotIn('u99', user_ids)

    def test_default_how_is_inner(self):
        implicit = self.pm.join('users', 'orders', on=ON)
        explicit = self.pm.join('users', 'orders', on=ON, how='inner')
        self.assertEqual(len(implicit), len(explicit))


class TestLeftJoin(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = _make_pm()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_all_left_rows_represented(self):
        results = self.pm.join('users', 'orders', on=ON, how='left')
        left_names = [lv['name'] for lv, _ in results]
        self.assertIn('Alice', left_names)
        self.assertIn('Bob', left_names)
        self.assertIn('Charlie', left_names)

    def test_unmatched_left_has_none_on_right(self):
        results = self.pm.join('users', 'orders', on=ON, how='left')
        charlie_rows = [(lv, r) for lv, r in results if lv['name'] == 'Charlie']
        self.assertEqual(len(charlie_rows), 1)
        self.assertIsNone(charlie_rows[0][1])

    def test_matched_left_has_all_matching_right_rows(self):
        results = self.pm.join('users', 'orders', on=ON, how='left')
        alice_rows = [(lv, r) for lv, r in results if lv['name'] == 'Alice']
        self.assertEqual(len(alice_rows), 2)

    def test_orphaned_right_row_excluded(self):
        results = self.pm.join('users', 'orders', on=ON, how='left')
        right_user_ids = [r['user_id'] for _, r in results if r is not None]
        self.assertNotIn('u99', right_user_ids)

    def test_total_result_count(self):
        # Alice x2, Bob x1, Charlie x1 (None) = 4
        self.assertEqual(len(self.pm.join('users', 'orders', on=ON, how='left')), 4)


class TestRightJoin(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = _make_pm()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_all_right_rows_represented(self):
        results = self.pm.join('users', 'orders', on=ON, how='right')
        totals = [r['total'] for _, r in results]
        self.assertEqual(sorted(totals), [50, 75, 100, 200])

    def test_unmatched_right_has_none_on_left(self):
        results = self.pm.join('users', 'orders', on=ON, how='right')
        orphan_rows = [(lv, r) for lv, r in results if r['user_id'] == 'u99']
        self.assertEqual(len(orphan_rows), 1)
        self.assertIsNone(orphan_rows[0][0])

    def test_unmatched_left_row_excluded(self):
        results = self.pm.join('users', 'orders', on=ON, how='right')
        left_names = [lv['name'] for lv, _ in results if lv is not None]
        self.assertNotIn('Charlie', left_names)

    def test_total_result_count(self):
        # o1(u1), o2(u2), o3(u1), o4(None) = 4
        self.assertEqual(len(self.pm.join('users', 'orders', on=ON, how='right')), 4)


class TestOuterJoin(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = _make_pm()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_all_matched_pairs_included(self):
        results = self.pm.join('users', 'orders', on=ON, how='outer')
        matched = [(lv, r) for lv, r in results if lv is not None and r is not None]
        self.assertEqual(len(matched), 3)

    def test_unmatched_left_included_with_none_right(self):
        results = self.pm.join('users', 'orders', on=ON, how='outer')
        charlie_rows = [(lv, r) for lv, r in results if lv is not None and lv['name'] == 'Charlie']
        self.assertEqual(len(charlie_rows), 1)
        self.assertIsNone(charlie_rows[0][1])

    def test_unmatched_right_included_with_none_left(self):
        results = self.pm.join('users', 'orders', on=ON, how='outer')
        orphan_rows = [(lv, r) for lv, r in results if r is not None and r['user_id'] == 'u99']
        self.assertEqual(len(orphan_rows), 1)
        self.assertIsNone(orphan_rows[0][0])

    def test_total_result_count(self):
        # matched: 3, unmatched-left: 1 (Charlie), unmatched-right: 1 (o4) = 5
        self.assertEqual(len(self.pm.join('users', 'orders', on=ON, how='outer')), 5)


class TestWhere(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = _make_pm()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_where_filters_matched_pairs(self):
        results = self.pm.join('users', 'orders', on=ON, where=lambda u, o: o['total'] >= 100)
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r['total'] >= 100 for _, r in results))

    def test_where_does_not_affect_unmatched_left_rows(self):
        # Charlie has no orders; where can't touch her
        results = self.pm.join('users', 'orders', on=ON, how='left',
                               where=lambda u, o: o['total'] > 9999)
        charlie_rows = [(lv, r) for lv, r in results if lv['name'] == 'Charlie']
        self.assertEqual(len(charlie_rows), 1)
        self.assertIsNone(charlie_rows[0][1])

    def test_where_does_not_affect_unmatched_right_rows(self):
        results = self.pm.join('users', 'orders', on=ON, how='right',
                               where=lambda u, o: u['name'] == 'NOBODY')
        orphan_rows = [(lv, r) for lv, r in results if r['user_id'] == 'u99']
        self.assertEqual(len(orphan_rows), 1)
        self.assertIsNone(orphan_rows[0][0])

    def test_where_filters_all_matches_does_not_create_unmatched_row(self):
        # Alice has matches, but where filters them all; she should not appear as (Alice, None)
        results = self.pm.join('users', 'orders', on=ON, how='left',
                               where=lambda u, o: o['total'] > 9999)
        alice_rows = [(lv, r) for lv, r in results if lv['name'] == 'Alice']
        self.assertEqual(alice_rows, [])


class TestEdgeCases(unittest.TestCase):
    def setUp(self):
        PersistenceManager._instance = None
        self.pm = _make_pm()

    def tearDown(self):
        PersistenceManager._instance = None

    def test_unknown_store_name_raises(self):
        with self.assertRaises(KeyError):
            self.pm.join('users', 'nonexistent', on=ON)

    def test_unknown_left_store_raises(self):
        with self.assertRaises(KeyError):
            self.pm.join('nonexistent', 'orders', on=ON)

    def test_invalid_how_raises(self):
        with self.assertRaisesRegex(ValueError, 'how'):
            self.pm.join('users', 'orders', on=ON, how='cross')

    def test_empty_left_store_inner_returns_empty(self):
        PersistenceManager._instance = None
        pm = PersistenceManager()
        pm.get_or_create('empty', lambda: InMemoryStore())
        pm.get_or_create('orders', lambda: InMemoryStore()).create('o1', {'user_id': 'u1', 'total': 1})
        self.assertEqual(pm.join('empty', 'orders', on=ON), [])

    def test_empty_right_store_inner_returns_empty(self):
        PersistenceManager._instance = None
        pm = PersistenceManager()
        pm.get_or_create('users', lambda: InMemoryStore()).create('u1', {'id': 'u1', 'name': 'Alice'})
        pm.get_or_create('empty', lambda: InMemoryStore())
        self.assertEqual(pm.join('users', 'empty', on=ON), [])

    def test_empty_left_store_left_join_returns_empty(self):
        PersistenceManager._instance = None
        pm = PersistenceManager()
        pm.get_or_create('empty', lambda: InMemoryStore())
        pm.get_or_create('orders', lambda: InMemoryStore()).create('o1', {'user_id': 'u1', 'total': 1})
        self.assertEqual(pm.join('empty', 'orders', on=ON, how='left'), [])

    def test_empty_right_store_left_join_returns_all_left_as_unmatched(self):
        PersistenceManager._instance = None
        pm = PersistenceManager()
        pm.get_or_create('users', lambda: InMemoryStore()).create('u1', {'id': 'u1', 'name': 'Alice'})
        pm.get_or_create('empty', lambda: InMemoryStore())
        results = pm.join('users', 'empty', on=ON, how='left')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], ({'id': 'u1', 'name': 'Alice'}, None))


if __name__ == '__main__':
    unittest.main()
