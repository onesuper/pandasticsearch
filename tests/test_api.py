# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock

from pandasticsearch.api import Pandasticsearch, Dim
from pandasticsearch.filters import Greater


def create_ps():
    return Pandasticsearch("http://localhost:9200", 'xxx')


class TestAPI(unittest.TestCase):
    @patch('pandasticsearch.clients.urllib.request.urlopen')
    def test_pandasticsearch_top(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response
        ps = create_ps()

        top = ps.show()
        print(repr(top))
        self.assertEqual(repr(top), 'Select: 1 row')

    def test_getitem(self):
        ps = create_ps()
        self.assertTrue(isinstance(ps['a'], Dim))
        self.assertEqual(ps[ps['a'] > 2]._filter.build(), {'range': {'a': {'gt': 2}}})

    def test_filter(self):
        ps = create_ps()
        self.assertEqual(ps.filter(ps['a'] > 2)._filter.build(), {'range': {'a': {'gt': 2}}})
        self.assertEqual(ps.filter(Greater('a', 2))._filter.build(), {'range': {'a': {'gt': 2}}})


if __name__ == '__main__':
    unittest.main()
