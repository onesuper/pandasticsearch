# -*- coding: UTF-8 -*-
import unittest
from mock import patch, Mock
import json
from pandasticsearch.dataframe import DataFrame, Column
from pandasticsearch.operators import *


@patch('pandasticsearch.client.urllib.request.urlopen')
def create_df_from_es(mock_urlopen):
    response = Mock()
    dic = {
        "index": {
            "mappings": {
                "doc_type": {
                    "properties": {
                        "a": {
                            "type": "integer"
                        },
                        "b": {
                            "type": "integer"
                        },
                        "c": {"properties": {
                            "d": {"type": "keyword", "ignore_above": 1024},
                            "e": {"type": "keyword", "ignore_above": 1024}
                        }
                        }
                    }
                }
            }
        }
    }
    response.read.return_value = json.dumps(dic).encode("utf-8")
    mock_urlopen.return_value = response
    return DataFrame.from_es(url="http://localhost:9200", index='index', doc_type='doc_type')


@patch('pandasticsearch.client.urllib.request.urlopen')
def create_df_from_es_after_removal_mapping(mock_urlopen):
    """
    https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
    """
    response = Mock()
    dic = {
        "index": {
            "mappings": {
                "properties": {
                    "a": {
                        "type": "integer"
                    },
                    "b": {
                        "type": "integer"
                    },
                    "c": {"properties": {
                        "d": {"type": "keyword", "ignore_above": 1024},
                        "e": {"type": "keyword", "ignore_above": 1024}
                    }
                    }
                }
            }
        }
    }
    response.read.return_value = json.dumps(dic).encode("utf-8")
    mock_urlopen.return_value = response
    return DataFrame.from_es(url="http://localhost:9200", index='index', compat=7)


class TestDataFrame(unittest.TestCase):
    def test_getitem(self):
        df = create_df_from_es()
        self.assertTrue(isinstance(df['a'], Column))
        self.assertTrue(isinstance(df['b'], Column))

        expr = df['a'] > 2
        self.assertTrue(isinstance(expr, BooleanFilter))
        self.assertTrue(isinstance(df[expr], DataFrame))
        self.assertEqual(df[expr]._filter.build(), {'range': {'a': {'gt': 2}}})

    def test_getattr(self):
        df = create_df_from_es()
        self.assertTrue(isinstance(df.a, Column))
        self.assertTrue(isinstance(df.b, Column))

    def test_index(self):
        df = create_df_from_es()
        self.assertEqual(df.index, 'index/doc_type')

    def test_index_after_removal_mapping(self):
        df = create_df_from_es_after_removal_mapping()
        self.assertEqual(df.index, 'index')

    def test_columns(self):
        df = create_df_from_es()
        self.assertEqual(df.columns, ['a', 'b', 'c.d', 'c.e'])

    def test_print_schema(self):
        df = create_df_from_es()
        df.print_schema()

    def test_columns_after_removal_mapping(self):
        df = create_df_from_es_after_removal_mapping()
        self.assertEqual(df.columns, ['a', 'b', 'c.d', 'c.e'])

    def test_print_schema_after_removal_mapping(self):
        df = create_df_from_es_after_removal_mapping()
        df.print_schema()

    def test_init(self):
        df = create_df_from_es()
        self.assertEqual(df.to_dict(), {'size': 20})

    def test_filter(self):
        df = create_df_from_es()

        self.assertEqual((df.filter(df['a'] > 2)).to_dict(),
                         {'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}},
                          'size': 20})

        self.assertEqual((df.filter((df['a'] > 2) & (df.b == 1))).to_dict(),
                         {'query': {'filtered': {'filter': {'bool': {'must': [
                             {'range': {'a': {'gt': 2}}},
                             {'term': {'b': 1}}]}}}},
                             'size': 20})

        self.assertEqual((df.filter(df['a'] > 2).filter(df.b == 1)).to_dict(),
                         {'query': {'filtered': {'filter': {'bool': {'must': [
                             {'range': {'a': {'gt': 2}}},
                             {'term': {'b': 1}}]}}}},
                             'size': 20})

        self.assertEqual(df.where(Greater('a', 2)).to_dict(),
                         {'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}},
                          'size': 20})

        self.assertEqual(df.filter('2016 - doc["age"].value > 1995').to_dict(),
                         {'query': {'filtered': {
                             'filter': {'script': {'script': {'inline': '2016 - doc["age"].value > 1995'}}}}},
                             'size': 20})

    def test_groupby(self):
        df = create_df_from_es()
        self.assertEqual((df.groupby(df.a)).to_dict(),
                         {'aggregations': {'a': {'terms': {'field': 'a', 'size': 20}}}, 'size': 0})

        self.assertEqual((df.groupby(df['a'], df['b'])).to_dict(),
                         {
                             'aggregations': {
                                 'a': {
                                     'aggregations': {
                                         'b': {
                                             'terms': {'field': 'b', 'size': 20}}
                                     },
                                     'terms': {'field': 'a', 'size': 20}}},
                             'size': 0})

    def test_agg(self):
        df = create_df_from_es()
        self.assertEqual((df.agg(MetricAggregator('a', 'avg'))).to_dict(),
                         {'aggregations': {'avg(a)': {'avg': {'field': 'a'}}}, 'size': 0})

    def test_sort(self):
        df = create_df_from_es()
        self.assertEqual((df.sort(df['a'].asc)).to_dict(),
                         {'sort': [{'a': {'order': 'asc'}}], 'size': 20})

        self.assertEqual((df.sort(Sorter('a'), Sorter('b'))).to_dict(),
                         {'sort': [{'a': {'order': 'desc'}},
                                   {'b': {'order': 'desc'}}], 'size': 20})

        self.assertEqual((df.sort('doc["age"].value * 2')).to_dict(),
                         {'sort': [{'_script': {
                             'order': 'desc',
                             'script': 'doc["age"].value * 2',
                             'type': 'number'
                         }}], 'size': 20})

    def test_select(self):
        df = create_df_from_es()
        self.assertEqual(df.select('a').to_dict(),
                         {'_source': {'excludes': [], 'includes': ['a']}, 'size': 20})

        self.assertEqual(df.select(df['a'], df['b']).to_dict(),
                         {'_source': {'excludes': [], 'includes': ['a', 'b']}, 'size': 20})

    def test_limit(self):
        df = create_df_from_es()
        self.assertEqual(df.limit(199).to_dict(), {'size': 199})

    def test_complex(self):
        df = create_df_from_es()

        df2 = df.filter(df['a'] > 2)
        df3 = df2.select('a').limit(30)

        print(df3.to_dict())
        self.assertEqual(df3.to_dict(),
                         {'_source': {'excludes': [], 'includes': ['a']},
                          'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}},
                          'size': 30})

        df4 = df3.groupby('b')
        df5 = df4.agg(MetricAggregator('a', 'avg'))

        print(df5.to_dict())

        self.assertEqual(df5.to_dict(),
                         {'_source': {'excludes': [], 'includes': ['a']},
                          'aggregations': {
                              'b': {
                                  'terms': {'field': 'b', 'size': 20},
                                  'aggregations': {
                                      'avg(a)': {'avg': {'field': 'a'}}}}
                          },
                          'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}},
                          'size': 0})

        df6 = df5.sort(Sorter('a'))

        print(df6.to_dict())
        self.assertEqual(df6.to_dict(),
                         {'_source': {'excludes': [], 'includes': ['a']},
                          'aggregations': {
                              'b': {
                                  'terms': {'field': 'b', 'size': 20},
                                  'aggregations': {
                                      'avg(a)': {'avg': {'field': 'a'}}}}
                          },
                          'query': {'filtered': {'filter': {'range': {'a': {'gt': 2}}}}},
                          'sort': [{'a': {'order': 'desc'}}],
                          'size': 0})

    def test_complex_agg(self):
        df = create_df_from_es()
        df2 = df.groupby(df.b, df.a)

        df3 = df2.agg(MetricAggregator('a', 'avg'))

        self.assertEqual(df3.to_dict(),
                         {
                             'size': 0,
                             'aggregations': {
                                 'b': {
                                     'terms': {'field': 'b', 'size': 20},
                                     'aggregations': {
                                         'a': {
                                             'terms': {'field': 'a', 'size': 20},
                                             'aggregations': {
                                                 'avg(a)': {'avg': {'field': 'a'}}}}
                                     }}}})


if __name__ == '__main__':
    unittest.main()
