# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock
import json
from pandasticsearch.api import Pandasticsearch, Column
from pandasticsearch.filters import Greater


@patch('pandasticsearch.client.urllib.request.urlopen')
def create_ps(mock_urlopen):
    response = Mock()
    dic = {"index": {"mappings": {"type": {"properties": {"a": {"type": "integer"}}}}}}
    response.read.return_value = json.dumps(dic).encode("utf-8")
    mock_urlopen.return_value = response
    return Pandasticsearch("http://localhost:9200", 'xxx')


class TestAPI(unittest.TestCase):
    def test_getitem(self):
        ps = create_ps()
        self.assertTrue(isinstance(ps['a'], Column))
        self.assertEqual(ps[ps['a'] > 2]._filter.build(), {'range': {'a': {'gt': 2}}})

    def test_columns(self):
        ps = create_ps()
        self.assertEqual(ps.columns, ['a'])

    def test_filter(self):
        ps = create_ps()
        self.assertEqual(ps.filter(ps['a'] > 2)._filter.build(), {'range': {'a': {'gt': 2}}})
        self.assertEqual(ps.filter(Greater('a', 2))._filter.build(), {'range': {'a': {'gt': 2}}})


if __name__ == '__main__':
    unittest.main()
