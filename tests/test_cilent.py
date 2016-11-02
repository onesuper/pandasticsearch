# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock

from pandasticsearch.client import RestClient


class TestClients(unittest.TestCase):
    @patch('pandasticsearch.client.urllib.request.urlopen')
    def test_rest_client_returns_results(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client = RestClient("http://localhost:9200")

        json = client.post(data="xxxx")

        print(json)
        self.assertIsNotNone(json)
        self.assertEqual(json, {"hits": {"hits": [{"_source": {}}]}})


if __name__ == '__main__':
    unittest.main()
