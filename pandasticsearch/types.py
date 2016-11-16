# -*- coding: UTF-8 -*-

from pandasticsearch.operators import *
import six


class Column(object):
    def __init__(self, field):
        self._field = field

    def field_name(self):
        return self._field

    def __eq__(self, other):
        return Equal(field=self._field, value=other)

    def __ne__(self, other):
        return ~Equal(field=self._field, value=other)

    def __gt__(self, other):
        return Greater(field=self._field, value=other)

    def __lt__(self, other):
        return Less(field=self._field, value=other)

    def __ge__(self, other):
        return GreaterEqual(field=self._field, value=other)

    def __le__(self, other):
        return LessEqual(field=self._field, value=other)

    def isin(self, other):
        return IsIn(field=self._field, value=other)

    def like(self, other):
        return Like(field=self._field, value=other)

    def rlike(self, other):
        return Rlike(field=self._field, value=other)

    def startswith(self, other):
        return Startswith(field=self._field, value=other)

    def ranges(self, range_list):
        return RangeGrouper(field=self._field, range_list=range_list)

    @property
    def isnull(self):
        return IsNull(field=self._field)

    @property
    def notnull(self):
        return NotNull(field=self._field)

    @property
    def desc(self, mode=None):
        return Sorter(self._field, mode=mode)

    @property
    def asc(self, mode=None):
        return Sorter(self._field, order='asc', mode=mode)

    @property
    def max(self):
        return MetricAggregator(self._field, 'max')

    @property
    def min(self):
        return MetricAggregator(self._field, 'min')

    @property
    def avg(self):
        return MetricAggregator(self._field, 'avg')

    @property
    def sum(self):
        return MetricAggregator(self._field, 'sum')

    @property
    def value_count(self):
        return MetricAggregator(self._field, 'value_count')

    count = value_count

    @property
    def cardinality(self):
        return MetricAggregator(self._field, 'cardinality')

    distinct_count = cardinality

    @property
    def percentiles(self):
        return MetricAggregator(self._field, 'percentiles')

    @property
    def percentile_ranks(self):
        return MetricAggregator(self._field, 'percentile_ranks')

    @property
    def stats(self):
        return MetricAggregator(self._field, 'stats')

    @property
    def extended_stats(self):
        return MetricAggregator(self._field, 'extended_stats')


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
        return 'Row('+','.join(['{0}={1}'.format(k, Row._stringfy(v)) for k, v in zip(self._fields, tuple(self))])+')'

    @classmethod
    def _stringfy(cls, v):
        b = six.StringIO()
        b.write(repr(v))
        return b.getvalue()

    def as_dict(self):
        return dict((x, y) for x, y in zip(self._fields, self))
