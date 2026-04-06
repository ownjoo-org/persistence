import unittest

from oj_persistence.store.in_memory import InMemoryStore
from test.unit.store.contract import StoreContract


class TestInMemoryStore(StoreContract, unittest.TestCase):
    def setUp(self):
        self.store = InMemoryStore()


if __name__ == '__main__':
    unittest.main()
