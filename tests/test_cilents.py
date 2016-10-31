# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock

from pandasticsearch.clients import SqlClient, RestClient


class TestClients(unittest.TestCase):
    @patch('pandasticsearch.clients.urllib.request.urlopen')
    def test_sql_client_returns_results(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client = SqlClient("http://localhost:9200")

        query = client.execute("xxxx")

        print(query)
        print(query.json)
        print(query.result)

        self.assertIsNotNone(query)
        self.assertIsNotNone(query.json)
        self.assertIsNone(query.result)

    @patch('pandasticsearch.clients.urllib.request.urlopen')
    def test_rest_client_returns_results(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client = RestClient("http://localhost:9200")

        query = client.execute("xxxx")

        print(query)
        print(query.json)
        print(query.result)

        self.assertIsNotNone(query)
        self.assertIsNotNone(query.json)
        self.assertIsNone(query.result)


if __name__ == '__main__':
    unittest.main()
