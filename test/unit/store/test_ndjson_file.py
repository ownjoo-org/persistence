import json
import tempfile
import unittest
from pathlib import Path

from oj_persistence.store.ndjson_file import NdjsonFileStore
from test.unit.store.contract import StoreContract


class TestNdjsonFileStore(StoreContract, unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)
        self.store = NdjsonFileStore(self.tmp_path / 'data.ndjson')

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_file_format_is_one_json_object_per_line(self):
        s = NdjsonFileStore(self.tmp_path / 'fmt.ndjson')
        s.create('a', {'x': 1})
        s.create('b', {'x': 2})
        lines = (self.tmp_path / 'fmt.ndjson').read_text().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            json.loads(line)  # each line must be valid JSON

    def test_create_appends_without_rewriting(self):
        """create() appends a line; existing lines are untouched."""
        path = self.tmp_path / 'append.ndjson'
        s = NdjsonFileStore(path)
        s.create('a', 1)
        s.create('b', 2)
        lines = path.read_text().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {'key': 'a', 'value': 1})

    def test_persists_across_instances(self):
        path = self.tmp_path / 'shared.ndjson'
        NdjsonFileStore(path).create('k', 'v')
        self.assertEqual(NdjsonFileStore(path).read('k'), 'v')


if __name__ == '__main__':
    unittest.main()
