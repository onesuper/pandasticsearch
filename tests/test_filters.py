# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.filters import *


class TestFilters(unittest.TestCase):
    def test_leaves(self):
        self.assertEqual((Dim('a') >= 2).build(), {"range": {"a": {"gte": 2}}})
        self.assertEqual((Dim('a') <= 2).build(), {"range": {"a": {"lte": 2}}})
        self.assertEqual((Dim('a') < 2).build(), {"range": {"a": {"lt": 2}}})
        self.assertEqual((Dim('a') == 2).build(), {"term": {"a": 2}})
        self.assertEqual((Dim('a') != 2).build()['bool'], {"must_not": {"term": {"a": 2}}})
        self.assertEqual((Dim('a') > 2).build(), {"range": {"a": {"gt": 2}}})

    def test_and(self):
        self.assertEqual(
            ((Dim('a') >= 2) & (Dim('b') < 3)).build()['bool'],
            {
                'must': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) & (Dim('b') < 3) & (Dim('c') == 4)).build()['bool'],
            {
                'must': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) & ((Dim('b') < 3) & (Dim('c') == 4))).build()['bool'],
            {
                'must': [
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}},
                    {'range': {'a': {'gte': 2}}}]
            })

    def test_or(self):
        self.assertEqual(
            ((Dim('a') >= 2) | (Dim('b') < 3)).build()['bool'],
            {
                'should': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) | (Dim('b') < 3) | (Dim('c') == 4)).build()['bool'],
            {
                'should': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) | ((Dim('b') < 3) | (Dim('c') == 4))).build()['bool'],
            {
                'should': [
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}},
                    {'range': {'a': {'gte': 2}}}]
            })

    def test_not(self):
        self.assertEqual(
            (~(Dim('a') >= 2)).build()['bool'],
            {
                'must_not':
                    {'range': {'a': {'gte': 2}}}})

    def test_not_not(self):
        exp = Dim('a') >= 2
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
        exp = (Dim('a') >= 2) & (Dim('b') < 3)
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
        exp = (Dim('b') < 3) | (Dim('c') == 4)
        actual = (Dim('a') >= 2) & exp
        print(actual.debug_string())

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
