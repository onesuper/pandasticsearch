# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock

from pandasticsearch.api import Pandasticsearch


class TestAPI(unittest.TestCase):
    @patch('pandasticsearch.client.urllib.request.urlopen')
    def test_pandasticsearch_top(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response
        ps = Pandasticsearch("http://localhost:9200", 'xxx')

        top = ps.top()
        print(repr(top))
        assert repr(top) == 'values: [{}]'


if __name__ == '__main__':
    unittest.main()
