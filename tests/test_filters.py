# -*- coding: UTF-8 -*-
import unittest

from pandasticsearch.filters import *


class TestFilters(unittest.TestCase):
    def test_leaves(self):
        self.assertEqual((Dim('a') >= 2).subtree, {"range": {"a": {"gte": 2}}})
        self.assertEqual((Dim('a') <= 2).subtree, {"range": {"a": {"lte": 2}}})
        self.assertEqual((Dim('a') < 2).subtree, {"range": {"a": {"lt": 2}}})
        self.assertEqual((Dim('a') == 2).subtree, {"term": {"a": 2}})
        self.assertEqual((Dim('a') != 2).subtree, {"must_not": {"term": {"a": 2}}})
        self.assertEqual((Dim('a') > 2).subtree, {"range": {"a": {"gt": 2}}})

    def test_and(self):
        self.assertEqual(
            ((Dim('a') >= 2) & (Dim('b') < 3)).subtree,
            {
                'must': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) & (Dim('b') < 3) & (Dim('c') == 4)).subtree,
            {
                'must': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) & ((Dim('b') < 3) & (Dim('c') == 4))).subtree,
            {
                'must': [
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}},
                    {'range': {'a': {'gte': 2}}}]
            })

    def test_or(self):
        self.assertEqual(
            ((Dim('a') >= 2) | (Dim('b') < 3)).subtree,
            {
                'should': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) | (Dim('b') < 3) | (Dim('c') == 4)).subtree,
            {
                'should': [
                    {'range': {'a': {'gte': 2}}},
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}}]
            })

        self.assertEqual(
            ((Dim('a') >= 2) | ((Dim('b') < 3) | (Dim('c') == 4))).subtree,
            {
                'should': [
                    {'range': {'b': {'lt': 3}}},
                    {'term': {'c': 4}},
                    {'range': {'a': {'gte': 2}}}]
            })

    def test_not(self):
        self.assertEqual(
            (~(Dim('a') >= 2)).subtree,
            {
                'must_not':
                    {'range': {'a': {'gte': 2}}}})

    def test_not_not(self):
        exp = Dim('a') >= 2
        print((~exp).subtree)
        print((~~exp).subtree)
        self.assertEqual(
            (~~exp).subtree,
            {
                'must_not': {
                    'bool': {
                        'must_not':
                            {'range': {'a': {'gte': 2}}}}}})

    def test_not_and(self):
        exp = (Dim('a') >= 2) & (Dim('b') < 3)
        self.assertEqual(
            (~exp).subtree,
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
            actual.subtree,
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
