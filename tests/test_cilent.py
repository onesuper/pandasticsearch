# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock

from pandasticsearch.client import SqlClient, RestClient


class TestClient(unittest.TestCase):
    @patch('pandasticsearch.client.urllib.request.urlopen')
    def test_sql_client_returns_results(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client = SqlClient("http://localhost:9200")

        query = client.execute("xxxx")
        assert query is not None
        assert query.json is not None
        assert query.result is None

    @patch('pandasticsearch.client.urllib.request.urlopen')
    def test_rest_client_returns_results(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client = RestClient("http://localhost:9200")

        query = client.execute("xxxx")
        assert query is not None
        assert query.json is not None
        assert query.result is None

if __name__ == '__main__':
    unittest.main()
