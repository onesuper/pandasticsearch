# -*- coding: UTF-8 -*-


# Es filter builder for BooleanCond
class BooleanFilter(object):
    def __init__(self, *args):
        self._filter = None

    def __and__(self, x):
        # Combine results
        if isinstance(self, AndFilter):
            self.subtree['must'].append(x.build())
            return self
        elif isinstance(x, AndFilter):
            x.subtree['must'].append(self.subtree)
            return x
        return AndFilter(self, x)

    def __or__(self, x):
        # Combine results
        if isinstance(self, OrFilter):
            self.subtree['should'].append(x.build())
            return self
        elif isinstance(x, OrFilter):
            x.subtree['should'].append(self.subtree)
            return x
        return OrFilter(self, x)

    def __invert__(self):
        return NotFilter(self)

    @property
    def subtree(self):
        if 'bool' in self._filter:
            return self._filter['bool']
        else:
            return self._filter

    def build(self):
        return self._filter


# Binary operator
class AndFilter(BooleanFilter):
    def __init__(self, *args):
        [isinstance(x, BooleanFilter) for x in args]
        super(AndFilter, self).__init__()
        self._filter = {'bool': {'must': [x.build() for x in args]}}


class OrFilter(BooleanFilter):
    def __init__(self, *args):
        [isinstance(x, BooleanFilter) for x in args]
        super(OrFilter, self).__init__()
        self._filter = {'bool': {'should': [x.build() for x in args]}}


class NotFilter(BooleanFilter):
    def __init__(self, x):
        assert isinstance(x, BooleanFilter)
        super(NotFilter, self).__init__()
        self._filter = {'bool': {'must_not': x.build()}}


# LeafBooleanFilter
class GreaterEqual(BooleanFilter):
    def __init__(self, field, value):
        super(GreaterEqual, self).__init__()
        self._filter = {'range': {field: {'gte': value}}}


class Greater(BooleanFilter):
    def __init__(self, field, value):
        super(Greater, self).__init__()
        self._filter = {'range': {field: {'gt': value}}}


class LessEqual(BooleanFilter):
    def __init__(self, field, value):
        super(LessEqual, self).__init__()
        self._filter = {'range': {field: {'lte': value}}}


class Less(BooleanFilter):
    def __init__(self, field, value):
        super(Less, self).__init__()
        self._filter = {'range': {field: {'lt': value}}}


class Equal(BooleanFilter):
    def __init__(self, field, value):
        super(Equal, self).__init__()
        self._filter = {'term': {field: value}}


class IsIn(BooleanFilter):
    def __init__(self, field, value):
        super(IsIn, self).__init__()
        assert isinstance(value, list)
        self._filter = {'terms': {field: value}}


class Like(BooleanFilter):
    def __init__(self, field, value):
        super(Like, self).__init__()
        self._filter = {'wildcard': {field: value}}


class Rlike(BooleanFilter):
    def __init__(self, field, value):
        super(Rlike, self).__init__()
        self._filter = {'regexp': {field: value}}


class Startswith(BooleanFilter):
    def __init__(self, field, value):
        super(Startswith, self).__init__()
        self._filter = {'prefix': {field: value}}


class IsNull(BooleanFilter):
    """
    .. _Find documents missing indexed values
        https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
    """
    def __init__(self, field):
        super(IsNull, self).__init__()
        self._filter = {'bool': {'must_not': {'exists': {'field': field}}}}


class NotNull(BooleanFilter):
    def __init__(self, field):
        super(NotNull, self).__init__()
        self._filter = {'exists': {'field': field}}


class ScriptFilter(BooleanFilter):
    def __init__(self, inline, lang=None, params=None):
        super(ScriptFilter, self).__init__()
        script = {'inline': inline}
        if lang is not None:
            script['lang'] = lang
        if params is not None:
            script['params'] = params
        self._filter = {'script': {'script': script}}