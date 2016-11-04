# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock
import json
from pandasticsearch.dataframe import DataFrame, Column
from pandasticsearch.types import Greater, BooleanCond
from pandasticsearch.aggregators import Avg


@patch('pandasticsearch.client.urllib.request.urlopen')
def create_df_from_es(mock_urlopen):
    response = Mock()
    dic = {"index": {"mappings": {"type": {"properties": {"a": {"type": "integer"}}}}}}
    response.read.return_value = json.dumps(dic).encode("utf-8")
    mock_urlopen.return_value = response
    return DataFrame.from_es("http://localhost:9200", 'xxx')


class TestDataframe(unittest.TestCase):
    def test_getitem(self):
        ps = create_df_from_es()
        self.assertTrue(isinstance(ps['a'], Column))

        expr = ps['a'] > 2
        self.assertTrue(isinstance(expr, BooleanCond))
        self.assertTrue(isinstance(ps[expr], DataFrame))
        self.assertEqual(ps[expr]._filter, {'range': {'a': {'gt': 2}}})

    def test_columns(self):
        df = create_df_from_es()
        self.assertEqual(df.columns, ['a'])

    def test_filter(self):
        df = create_df_from_es()

        self.assertEqual((df.filter(df['a'] > 2)).to_dict(),
                         {'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}}, 'size': 20})
        self.assertEqual(df.where(Greater('a', 2)).to_dict(),
                         {'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}}, 'size': 20})

    def test_agg(self):
        df = create_df_from_es()
        self.assertEqual((df.agg(Avg('a'))).to_dict(),
                         {'aggregations': {'avg(a)': {'avg': {'field': 'a'}}}, 'size': 0})

    def test_select(self):
        df = create_df_from_es()
        self.assertEqual(df.select('a').to_dict(),
                         {'_source': {'excludes': [], 'includes': ('a',)}, 'size': 20})

    def test_limit(self):
        df = create_df_from_es()
        self.assertEqual(df.limit(199).to_dict(), {'size': 199})

    def test_complex(self):
        df = create_df_from_es()
        self.assertEqual(df.filter(df['a'] > 2).agg(Avg('a')).select('a').limit(1).to_dict(),
                         {'_source': {'excludes': [], 'includes': ('a',)},
                          'aggregations': {'avg(a)': {'avg': {'field': 'a'}}},
                          'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}},
                          'size': 0})

if __name__ == '__main__':
    unittest.main()
