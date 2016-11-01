# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.aggregators import *


class TestAggregators(unittest.TestCase):
    def test_avg(self):
        self.assertEqual(Avg('x').build(), {'avg(x)': {'avg': {'field': 'x'}}})

    def test_max(self):
        self.assertEqual(Max('x').build(), {'max(x)': {'max': {'field': 'x'}}})

    def test_count_star(self):
        self.assertEqual(CountStar().build(), {'count(*)': {'value_count': {'field': '_index'}}})


if __name__ == '__main__':
    unittest.main()
