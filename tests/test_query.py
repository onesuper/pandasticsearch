# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.query import Select, Agg


class TestQuery(unittest.TestCase):
    def test_select(self):
        select = Select()
        select._result_dict = {
            'hits': {
                'hits': [
                    {'_source': {'a': 1, 'b': 1}},
                    {'_source': {'a': 2, 'b': 2}},
                    {'_source': {'a': 3, 'b': 3}},
                ]
            }
        }
        select.explain_result()
        print(select)
        print(repr(select))
        print(select.to_pandas())

        assert select.result is not None
        assert len(select) == 3

    def test_agg(self):
        agg = Agg()
        agg._result_dict = {
            'aggregations': {
                'agg_key': {
                    'buckets': [
                        {
                            'key': 'a',
                            'f1': {'value': 100}},
                        {
                            'key': 'b',
                            'f1': {'value': 200}},
                    ]
                }
            }
        }

        agg.explain_result()
        print(agg)
        print(repr(agg))
        print(agg.to_pandas())

        assert agg.result is not None
        assert len(agg) == 2

    def test_agg_recursive(self):
        agg = Agg()
        agg._result_dict = {
            'aggregations': {
                'agg_key1': {
                    'buckets': [
                        {
                            'key': 'a',
                            'agg_key2': {
                                'buckets': [
                                    {
                                        'key': 'x',
                                        'f1': {'value': 100},
                                        'f2': {'value': 1}
                                    },
                                    {
                                        'key': 'y',
                                        'f1': {'value': 200},
                                        'f2': {'value': 2}
                                    },
                                ]
                            }
                        },
                        {
                            'key': 'b',
                            'agg_key2': {
                                'buckets': [
                                    {
                                        'key': 'x',
                                        'f1': {'value': 300},
                                        'f2': {'value': 3}
                                    },
                                    {
                                        'key': 'y',
                                        'f1': {'value': 400},
                                        'f2': {'value': 4}
                                    },
                                ]
                            }
                        }
                    ]
                }
            }
        }

        agg.explain_result()
        print(agg)
        print(repr(agg))
        print(agg.to_pandas())
        assert agg.result is not None
        assert len(agg) == 4


if __name__ == '__main__':
    unittest.main()
