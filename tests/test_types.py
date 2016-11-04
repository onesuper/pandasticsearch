# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.types import *


class TestSchema(unittest.TestCase):
    def test_row(self):
        row = Row(a=1, b='你好,世界')
        print(repr(row))

        self.assertEqual(row['a'], 1)
        self.assertEqual(row['b'], '你好,世界')
        self.assertEqual(repr(row), '''Row(a=1,b='你好,世界')''')

    def test_leaves(self):
        self.assertEqual(GreaterEqual('a', 2).build(), {"range": {"a": {"gte": 2}}})
        self.assertEqual(LessEqual('a', 2).build(), {"range": {"a": {"lte": 2}}})
        self.assertEqual(Less('a', 2).build(), {"range": {"a": {"lt": 2}}})
        self.assertEqual(Equal('a', 2).build(), {"term": {"a": 2}})
        exp = Equal('a', 2)
        self.assertEqual((~exp).build()['bool'], {"must_not": {"term": {"a": 2}}})
        self.assertEqual(Greater('a', 2).build(), {"range": {"a": {"gt": 2}}})
        self.assertEqual(IsIn('a', [1, 2, 3]).build(), {'terms': {'a': [1, 2, 3]}})

    def test_and(self):
        self.assertEqual(
            (GreaterEqual('a', 2) & Less('b', 3)).build()['bool'],
            {
                'must': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}}]
            })

        self.assertEqual(
            (GreaterEqual('a', 2) & Less('b', 3) & Equal('c', 4)).build()['bool'],
            {
                'must': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}}]
            })

        self.assertEqual(
            (GreaterEqual('a', 2) & (Less('b', 3) & Equal('c', 4))).build()['bool'],
            {
                'must': [
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}},
                    {'range': {'a': {'gte': 2}}}]
            })

    def test_or(self):
        self.assertEqual(
            (GreaterEqual('a', 2) | Less('b', 3)).build()['bool'],
            {
                'should': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}}]
            })

        self.assertEqual(
            (GreaterEqual('a', 2) | Less('b', 3) | Equal('c', 4)).build()['bool'],
            {
                'should': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}}]
            })

        self.assertEqual(
            (GreaterEqual('a', 2) | (Less('b', 3) | Equal('c', 4))).build()['bool'],
            {
                'should': [
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}},
                    {'range': {'a': {'gte': 2}}}]
            })

    def test_not(self):
        self.assertEqual(
            (~GreaterEqual('a', 2)).build()['bool'],
            {
                'must_not':
                    {'range': {'a': {'gte': 2}}}})

    def test_not_not(self):
        exp = GreaterEqual('a', 2)
        print((~exp).build())
        print((~~exp).build())
        self.assertEqual(
            (~~exp).build()['bool'],
            {
                'must_not': {
                    'bool': {
                        'must_not':
                            {'range': {'a': {'gte': 2}}}}}})

    def test_not_and(self):
        exp = GreaterEqual('a', 2) & Less('b', 3)
        self.assertEqual(
            (~exp).build()['bool'],
            {
                'must_not': {
                    'bool': {
                        'must': [
                            {'range': {'a': {'gte': 2}}},
                            {'range': {'b': {'lt': 3}}}]
                    }
                }
            })

    def test_and_or(self):
        exp = Less('b', 3) | Equal('c', 4)
        actual = GreaterEqual('a', 2) & exp

        self.assertEqual(
            actual.build()['bool'],
            {
                'must': [
                    {'range': {'a': {'gte': 2}}},
                    {
                        'bool': {
                            'should': [
                                {'range': {'b': {'lt': 3}}},
                                {'term': {'c': 4}}
                            ]
                        }
                    }]
            })


if __name__ == '__main__':
    unittest.main()
