from pandasticsearch.filters import *


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
