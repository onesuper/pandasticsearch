# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.types import Row


class TestSchema(unittest.TestCase):
    def test_row(self):
        row = Row(a=1, b='你好,世界')
        print(repr(row))

        self.assertEqual(row['a'], 1)
        self.assertEqual(row['b'], '你好,世界')
        self.assertEqual(repr(row), '''Row(a=1,b='你好,世界')''')


if __name__ == '__main__':
    unittest.main()
