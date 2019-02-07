# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.operators import *


class TestOperators(unittest.TestCase):
    def test_metric_agg(self):
        self.assertEqual(MetricAggregator('x', 'avg').build(), {'avg(x)': {'avg': {'field': 'x'}}})
        self.assertEqual(MetricAggregator('x', 'max').build(), {'max(x)': {'max': {'field': 'x'}}})
        self.assertEqual(MetricAggregator('x', 'max').alias('max_x').build(), {'max_x': {'max': {'field': 'x'}}})

    def test_grouper(self):
        exp = Grouper('a', size=100, include=['x', 'y'])
        self.assertEqual(exp.build(),
                         {'a': {'terms': {'field': 'a', 'size': 100, 'include': ['x', 'y']}}})

    def test_nested_grouper(self):
        exp = Grouper('a', inner=Grouper('b', inner=Grouper('c')))
        self.assertEqual(exp.build(),
                         {
                             'a': {
                                 'terms': {'field': 'a', 'size': 20},
                                 'aggregations': {
                                     'b': {
                                         'terms': {'field': 'b', 'size': 20},
                                         'aggregations': {
                                             'c': {'terms': {'field': 'c', 'size': 20}}}}}}})

    def test_range_grouper(self):
        exp = RangeGrouper('a', [1, 3, 6])
        self.assertEqual(exp.build(), {
            'range(1,3,6)': {'range': {'ranges': [{'to': 3, 'from': 1}, {'to': 6, 'from': 3}], 'field': 'a'}}})

    def test_date_grouper(self):
        exp = DateGrouper('a', '1d', 'm')
        self.assertEqual(exp.build(), {
            'date(a,1d)': {'date_histogram': {'interval': '1d', 'field': 'a', 'format': 'm'}}})

    def test_sorter(self):
        self.assertEqual(Sorter('x').build(), {'x': {'order': 'desc'}})
        self.assertEqual(Sorter('x', mode='avg').build(), {'x': {'order': 'desc', 'mode': 'avg'}})

        self.assertEqual(ScriptSorter('doc["field_name"].value * factor', params={'factor': 1.1}).build(),
                         {'_script': {'type': 'number',
                                      'order': 'desc',
                                      'script': 'doc["field_name"].value * factor',
                                      "params": {
                                          "factor": 1.1
                                      }}})

    def test_leaf_boolean_filter(self):
        self.assertEqual(GreaterEqual('a', 2).build(), {"range": {"a": {"gte": 2}}})
        self.assertEqual(LessEqual('a', 2).build(), {"range": {"a": {"lte": 2}}})
        self.assertEqual(Less('a', 2).build(), {"range": {"a": {"lt": 2}}})
        self.assertEqual(Equal('a', 2).build(), {"term": {"a": 2}})
        exp = Equal('a', 2)
        self.assertEqual((~exp).build()['bool'], {"must_not": {"term": {"a": 2}}})
        self.assertEqual(Greater('a', 2).build(), {"range": {"a": {"gt": 2}}})
        self.assertEqual(IsIn('a', [1, 2, 3]).build(), {'terms': {'a': [1, 2, 3]}})
        self.assertEqual(Like('a', 'a*b').build(), {'wildcard': {'a': 'a*b'}})
        self.assertEqual(Rlike('a', 'a*b').build(), {'regexp': {'a': 'a*b'}})
        self.assertEqual(Startswith('a', 'jj').build(), {'prefix': {'a': 'jj'}})
        self.assertEqual(IsNull('a').build(), {'missing': {'field': 'a'}})
        self.assertEqual(NotNull('a').build(), {'exists': {'field': 'a'}})
        self.assertEqual(ScriptFilter('doc["num1"].value > params.param1', params={'param1': 5}).build(),
                         {'script': {
                             'script': {
                                 'inline': 'doc["num1"].value > params.param1',
                                 'params': {'param1': 5}}}})

    def test_and_filter1(self):
        exp = GreaterEqual('a', 2) & Less('b', 3)
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must': [
                        {'range': {'a': {'gte': 2}}},
                        {'range': {'b': {'lt': 3}}}
                    ]
                }
            })

    def test_and_filter2(self):
        exp = GreaterEqual('a', 2) & Less('b', 3) & Equal('c', 4)
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must': [
                        {'range': {'a': {'gte': 2}}},
                        {'range': {'b': {'lt': 3}}},
                        {'term': {'c': 4}}
                    ]
                }
            })

    def test_and_filter3(self):
        exp = GreaterEqual('a', 2) & (Less('b', 3) & Equal('c', 4))
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must': [
                        {'range': {'b': {'lt': 3}}},
                        {'term': {'c': 4}},
                        {'range': {'a': {'gte': 2}}}
                    ]
                }
            })

    def test_or_filter1(self):
        exp = GreaterEqual('a', 2) | Less('b', 3)
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'should': [
                        {'range': {'a': {'gte': 2}}},
                        {'range': {'b': {'lt': 3}}}
                    ]
                }
            })

    def test_or_filter2(self):
        exp = GreaterEqual('a', 2) | Less('b', 3) | Equal('c', 4)
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'should': [
                        {'range': {'a': {'gte': 2}}},
                        {'range': {'b': {'lt': 3}}},
                        {'term': {'c': 4}}
                    ]
                }
            })

    def test_or_filter3(self):
        exp = GreaterEqual('a', 2) | (Less('b', 3) | Equal('c', 4))
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'should': [
                        {'range': {'b': {'lt': 3}}},
                        {'term': {'c': 4}},
                        {'range': {'a': {'gte': 2}}}
                    ]
                }
            })

    def test_not_filter(self):
        exp = ~GreaterEqual('a', 2)
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must_not': {'range': {'a': {'gte': 2}}}
                }
            })

    def test_not_not_filter(self):
        exp = ~~GreaterEqual('a', 2)

        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must_not': {
                        'bool': {
                            'must_not': {'range': {'a': {'gte': 2}}}
                        }
                    }
                }
            })

    def test_not_and_filter(self):
        exp = ~(GreaterEqual('a', 2) & Less('b', 3))
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must_not': {
                        'bool': {
                            'must': [
                                {'range': {'a': {'gte': 2}}},
                                {'range': {'b': {'lt': 3}}}
                            ]
                        }
                    }
                }
            })

    def test_and_or_filter(self):
        exp = GreaterEqual('a', 2) & (Less('b', 3) | Equal('c', 4))
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must': [
                        {'range': {'a': {'gte': 2}}},
                        {
                            'bool': {
                                'should': [
                                    {'range': {'b': {'lt': 3}}},
                                    {'term': {'c': 4}}
                                ]
                            }
                        }
                    ]
                }
            })

    def test_and_not_or_filter(self):
        exp = GreaterEqual('a', 2) & ~(Less('b', 3) | Equal('c', 4))
        self.assertEqual(
            exp.build(),
            {
                'bool': {
                    'must': [
                        {'range': {'a': {'gte': 2}}},
                        {
                            'bool': {
                                'must_not': {
                                    'bool': {
                                        'should': [
                                            {'range': {'b': {'lt': 3}}},
                                            {'term': {'c': 4}}
                                        ]
                                    }

                                }
                            }
                        }
                    ]
                }
            })


if __name__ == '__main__':
    unittest.main()
