import json


# Bool filter builder
class Filter(object):
    def __init__(self, *args):
        self._filter = None

    def __and__(self, x):
        # Combine results
        if isinstance(self, And):
            self.subtree['must'].append(x.subtree)
            return self
        elif isinstance(x, And):
            x.subtree['must'].append(self.subtree)
            return x
        return And(self, x)

    def __or__(self, x):
        # Combine results
        if isinstance(self, Or):
            self.subtree['should'].append(x.subtree)
            return self
        elif isinstance(x, Or):
            x.subtree['should'].append(self.subtree)
            return x
        return Or(self, x)

    def __invert__(self):
        return Not(self)

    @property
    def subtree(self):
        if 'bool' in self._filter:
            return self._filter['bool']
        else:
            return self._filter

    def build(self):
        return self._filter

    def debug_string(self, indent=4):
        return json.dumps(self._filter, indent=indent)


# Binary operator
class And(Filter):
    def __init__(self, *args):
        [isinstance(x, Filter) for x in args]
        super(And, self).__init__()
        self._filter = {'bool': {'must': [x.build() for x in args]}}


class Or(Filter):
    def __init__(self, *args):
        [isinstance(x, Filter) for x in args]
        super(Or, self).__init__()
        self._filter = {'bool': {'should': [x.build() for x in args]}}


class Not(Filter):
    def __init__(self, x):
        assert isinstance(x, Filter)
        super(Not, self).__init__()
        self._filter = {'bool': {'must_not': x.build()}}


# Leaves
class GreaterEqual(Filter):
    def __init__(self, dim, value):
        super(GreaterEqual, self).__init__()
        self._filter = {'range': {dim: {'gte': value}}}


class Greater(Filter):
    def __init__(self, dim, value):
        super(Greater, self).__init__()
        self._filter = {'range': {dim: {'gt': value}}}


class LessEqual(Filter):
    def __init__(self, dim, value):
        super(LessEqual, self).__init__()
        self._filter = {'range': {dim: {'lte': value}}}


class Less(Filter):
    def __init__(self, dim, value):
        super(Less, self).__init__()
        self._filter = {'range': {dim: {'lt': value}}}


class Equal(Filter):
    def __init__(self, dim, value):
        super(Equal, self).__init__()
        self._filter = {'term': {dim: value}}


class IsIn(Filter):
    def __init__(self, dim, value):
        super(IsIn, self).__init__()
        self._filter = {'terms': {dim: value}}


class Dim(object):
    def __init__(self, dim):
        self._dim = dim

    def __eq__(self, other):
        return Equal(dim=self._dim, value=other)

    def __ne__(self, other):
        return ~Equal(dim=self._dim, value=other)

    def __gt__(self, other):
        return Greater(dim=self._dim, value=other)

    def __lt__(self, other):
        return Less(dim=self._dim, value=other)

    def __ge__(self, other):
        return GreaterEqual(dim=self._dim, value=other)

    def __le__(self, other):
        return LessEqual(dim=self._dim, value=other)

    def isin(self, other):
        return IsIn(dim=self._dim, value=other)
