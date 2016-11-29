# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.queries import Select, Agg


def create_hits():
    return {
        'hits': {
            'hits': [
                {'_source': {'a': 1, 'b': 1}},
                {'_source': {'a': 2, 'b': 2}},
                {'_source': {'a': 3, 'b': 3}},
            ]
        },
        'took': 1
    }


class TestQueries(unittest.TestCase):
    def test_select_explain_result(self):
        select = Select()
        select._result_dict = create_hits()
        select.explain_result()
        print(select)
        print(repr(select))

        self.assertIsNotNone(select.result)
        self.assertEqual(len(select), 3)

    def test_select_from_dict(self):
        select = Select.from_dict(create_hits())
        print(select)
        print(repr(select))

        self.assertIsNotNone(select.result)
        self.assertEqual(len(select), 3)

    def test_select_result(self):
        select = Select.from_dict(create_hits())
        print(select.result)

        self.assertIsNotNone(select.result)
        self.assertEqual(len(select.result[0]), 2)

    def test_agg_buckets(self):
        agg = Agg()
        agg._result_dict = {
            'took': 1,
            'aggregations': {
                'agg_key': {
                    'buckets': [
                        {
                            'key': 'a',
                            'f1': {'value': 100},
                            'f2': {'value': 1},
                            "doc_count": 12
                        },
                        {
                            'key': 'b',
                            'f1': {'value': 200},
                            'f2': {'value': 2},
                            "doc_count": 13
                        },
                    ]
                }
            }
        }

        agg.explain_result()
        print(agg.result)
        self.assertEqual(agg.result, [{'f1': 100, 'f2': 1, 'doc_count': 12},
                                      {'f1': 200, 'f2': 2, 'doc_count': 13}])
        self.assertEqual(agg.index, [('a',),
                                     ('b',)])

    def test_agg_date_histogram(self):
        agg = Agg()
        agg._result_dict = {
            'took': 3,
            'aggregations': {
                'my_date_histogram': {
                    'buckets': [
                        {
                            'doc_count': 1,
                            'key': 1480392360000,
                            'key_as_string': '2016-11-29T04:06:00.000Z',
                            'f1': {'value': 0.1},
                        },
                        {
                            'doc_count': 1,
                            'key': 1480392420000,
                            'key_as_string': '2016-11-29T04:07:00.000Z',
                            'f1': {'value': 0.2},
                        },
                        {
                            'doc_count': 1,
                            'key': 1480392480000,
                            'key_as_string': '2016-11-29T04:08:00.000Z',
                            'f1': {'value': 0.3},
                        },
                        {
                            'doc_count': 1,
                            'key': 1480392540000,
                            'key_as_string': '2016-11-29T04:09:00.000Z',
                            'f1': {'value': 0.4},
                        },
                        {
                            'doc_count': 1,
                            'key': 1480392600000,
                            'key_as_string': '2016-11-29T04:10:00.000Z',
                            'f1': {'value': 0.5},
                        }
                    ]
                }
            }
        }
        agg.explain_result()
        print(agg.result)
        self.assertEqual(agg.result, [{'f1': 0.1, 'doc_count': 1},
                                      {'f1': 0.2, 'doc_count': 1},
                                      {'f1': 0.3, 'doc_count': 1},
                                      {'f1': 0.4, 'doc_count': 1},
                                      {'f1': 0.5, 'doc_count': 1}])
        self.assertEqual(agg.index, [('2016-11-29T04:06:00.000Z',),
                                     ('2016-11-29T04:07:00.000Z',),
                                     ('2016-11-29T04:08:00.000Z',),
                                     ('2016-11-29T04:09:00.000Z',),
                                     ('2016-11-29T04:10:00.000Z',)])

    def test_agg_nested_buckets(self):
        agg = Agg()
        agg._result_dict = {
            'took': 1,
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
                                        'f2': {'value': 1},
                                        "doc_count": 11
                                    },
                                    {
                                        'key': 'y',
                                        'f1': {'value': 200},
                                        'f2': {'value': 2},
                                        "doc_count": 12
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
                                        'f2': {'value': 3},
                                        "doc_count": 13
                                    },
                                    {
                                        'key': 'y',
                                        'f1': {'value': 400},
                                        'f2': {'value': 4},
                                        "doc_count": 14
                                    },
                                ]
                            }
                        }
                    ]
                }
            }
        }

        agg.explain_result()
        print(agg.result)

        self.assertEqual(agg.result,
                         [{'f1': 100, 'f2': 1, 'doc_count': 11},
                          {'f1': 200, 'f2': 2, 'doc_count': 12},
                          {'f1': 300, 'f2': 3, 'doc_count': 13},
                          {'f1': 400, 'f2': 4, 'doc_count': 14}])
        self.assertEqual(agg.index, [('a', 'x'),
                                     ('a', 'y'),
                                     ('b', 'x'),
                                     ('b', 'y')])


if __name__ == '__main__':
    unittest.main()
