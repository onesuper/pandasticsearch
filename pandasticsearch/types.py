import json


class Column(object):
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


class Row(tuple):
    """
    The builtin L{DataFrame} row type for accessing before converted into Pandas DataFrame.
    The fields will be sorted by names.

    >>> row = Row(name="Alice", age=12)
    >>> row
    Row(age=12, name='Alice')
    >>> row['name'], row['age']
    ('Alice', 12)
    >>> row.name, row.age
    ('Alice', 12)
    >>> 'name' in row
    True
    >>> 'wrong_key' in row
    """

    def __new__(cls, **kwargs):
        names = sorted(kwargs.keys())
        row = tuple.__new__(cls, [kwargs[n] for n in names])
        row._fields = names
        return row

    def __getitem__(self, name):
        try:
            idx = self._fields.index(name)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(name)
        except ValueError:
            raise ValueError(name)

    def __contains__(self, name):
        return name in self._fields

    def __repr__(self):
        return 'Row({0})'.format(','.join(['{0}={1}'.format(k, repr(v)) for k, v in zip(self._fields, tuple(self))]))


# Es filter builder for BooleanCond
class BooleanCond(object):
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
class And(BooleanCond):
    def __init__(self, *args):
        [isinstance(x, BooleanCond) for x in args]
        super(And, self).__init__()
        self._filter = {'bool': {'must': [x.build() for x in args]}}


class Or(BooleanCond):
    def __init__(self, *args):
        [isinstance(x, BooleanCond) for x in args]
        super(Or, self).__init__()
        self._filter = {'bool': {'should': [x.build() for x in args]}}


class Not(BooleanCond):
    def __init__(self, x):
        assert isinstance(x, BooleanCond)
        super(Not, self).__init__()
        self._filter = {'bool': {'must_not': x.build()}}


# Leaves
class GreaterEqual(BooleanCond):
    def __init__(self, dim, value):
        super(GreaterEqual, self).__init__()
        self._filter = {'range': {dim: {'gte': value}}}


class Greater(BooleanCond):
    def __init__(self, dim, value):
        super(Greater, self).__init__()
        self._filter = {'range': {dim: {'gt': value}}}


class LessEqual(BooleanCond):
    def __init__(self, dim, value):
        super(LessEqual, self).__init__()
        self._filter = {'range': {dim: {'lte': value}}}


class Less(BooleanCond):
    def __init__(self, dim, value):
        super(Less, self).__init__()
        self._filter = {'range': {dim: {'lt': value}}}


class Equal(BooleanCond):
    def __init__(self, dim, value):
        super(Equal, self).__init__()
        self._filter = {'term': {dim: value}}


class IsIn(BooleanCond):
    def __init__(self, dim, value):
        super(IsIn, self).__init__()
        assert isinstance(value, list)
        self._filter = {'terms': {dim: value}}
