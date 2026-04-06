import json
import tempfile
import unittest
from pathlib import Path

from oj_persistence.store.ijson_file import IjsonFileStore
from test.unit.store.contract import StoreContract


class TestIjsonFileStore(StoreContract, unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = IjsonFileStore(self.tmp_path / 'data.json')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_file_is_valid_standard_json(self):
        s = IjsonFileStore(self.tmp_path / 'std.json')
        s.create('a', {'x': 1})
        s.create('b', {'x': 2})
        data = json.loads((self.tmp_path / 'std.json').read_text())
        self.assertEqual(data, {'a': {'x': 1}, 'b': {'x': 2}})

    def test_persists_across_instances(self):
        path = self.tmp_path / 'shared.json'
        IjsonFileStore(path).create('k', 'v')
        self.assertEqual(IjsonFileStore(path).read('k'), 'v')


if __name__ == '__main__':
    unittest.main()
