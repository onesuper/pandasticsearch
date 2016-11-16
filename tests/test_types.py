# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.operators import *
from pandasticsearch.types import Row, Column


class TestSchema(unittest.TestCase):
    def test_row(self):
        row = Row(a=1, b='你好,世界')
        print(repr(row))

        self.assertEqual(row['a'], 1)
        self.assertEqual(row['b'], '你好,世界')
        self.assertEqual(row.as_dict(), {'a': 1, 'b': '你好,世界'})

    def test_column(self):
        col = Column('b')
        self._assert_equal_filter(col > 2, Greater('b', 2))
        self._assert_equal_filter(col >= 2, GreaterEqual('b', 2))
        self._assert_equal_filter(col < 2, Less('b', 2))
        self._assert_equal_filter(col <= 2, LessEqual('b', 2))
        self._assert_equal_filter(col == 2, Equal('b', 2))
        self._assert_equal_filter(col != 2, ~Equal('b', 2))
        self._assert_equal_filter(col.isin([1, 2, 3]), IsIn('b', [1, 2, 3]))
        self._assert_equal_filter(col.like('a*b'), Like('b', 'a*b'))
        self._assert_equal_filter(col.rlike('a*b'), Rlike('b', 'a*b'))
        self._assert_equal_filter(col.startswith('jj'), Startswith('b', 'jj'))
        self._assert_equal_filter(col.isnull, IsNull('b'))
        self._assert_equal_filter(col.notnull, NotNull('b'))

    def _assert_equal_filter(self, x, y):
        self.assertTrue(x, BooleanFilter)
        self.assertTrue(y, BooleanFilter)
        self.assertEqual(x.build(), y.build())


if __name__ == '__main__':
    unittest.main()
