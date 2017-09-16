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

    def isin(self, values):
        """
        Returns a :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        :param values:  A list of values to filter terms
        :return: :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        df.filter(df.gender.isin(['male', 'female'])
        """
        return IsIn(field=self._field, value=values)

    def like(self, wildcard):
        """
        Returns a :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        :param str wildcard: The wildcard to filter the column with.
        :return: :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        >>> df.filter(df.name.like('A*'))
        """
        return Like(field=self._field, value=wildcard)

    def rlike(self, regexp):
        """
        Returns a :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        :param str regexp: The regular expression to filter the column with.
        :return: :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        >>> df.filter(df.name.rlike('A.l.e'))
        """
        return Rlike(field=self._field, value=regexp)

    def startswith(self, substr):
        """
        Returns a :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        :param str substr: The sub string to filter the column with.
        :return: :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`

        >>> df.filter(df.name.startswith('Al')
        """
        return Startswith(field=self._field, value=substr)

    def ranges(self, values):
        """
        Returns a :class:`Grouper <pandasticsearch.operators.Grouper>`

        :param values: A list of numeric values
        :return: :class:`Grouper <pandasticsearch.operators.Grouper>`

        >>> df.groupby(df.age.ranges([10,12,14]))
        """
        return RangeGrouper(field=self._field, range_list=values)

    def date_interval(self, interval, format='yyyy/MM/dd HH:mm:ss'):
        """
        Returns a :class:`Grouper <pandasticsearch.operators.Grouper>`

        :param interval: A string indicating date interval
        :param format: Date format string
        :return: :class:`Grouper <pandasticsearch.operators.Grouper>`

        >>> df.groupby(df.date_interval('1d'))
        """
        return DateGrouper(field=self._field, interval=interval, format=format)

    def terms(self, limit=20, include=None, exclude=None):
        """
        Returns a :class:`Grouper <pandasticsearch.operators.Grouper>`

        :param limit: limit the number of terms to be aggregated (default 20)
        :param include: the exact term to be included
        :param exclude: the exact term to be excluded

        :return: :class:`Grouper <pandasticsearch.operators.Grouper>`

        >>> df.groupby(df.age.terms(limit=10, include=[1, 2, 3]))
        """
        return Grouper(field=self._field, size=limit, include=include, exclude=exclude)

    @property
    def isnull(self):
        """
        :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>` to indicate the null column value

        :return: :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`
        """
        return IsNull(field=self._field)

    @property
    def notnull(self):
        """
        :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>` to indicate the non-null column value

        :return: :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>`
        """
        return NotNull(field=self._field)

    @property
    def desc(self, mode=None):
        """
        Descending :class:`Sorter <pandasticsearch.operators.Sorter>`

        :return: :class:`Sorter <pandasticsearch.operators.Sorter>`

        >>> df.orderyby(df.age.desc)
        """
        return Sorter(self._field, mode=mode)

    @property
    def asc(self, mode=None):
        """
        Ascending :class:`Sorter <pandasticsearch.operators.Sorter>`

        :return: :class:`Sorter <pandasticsearch.operators.Sorter>`

        >>> df.orderyby(df.age.asc)
        """
        return Sorter(self._field, order='asc', mode=mode)

    @property
    def max(self):
        """
        Max aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.max)
        """
        return MetricAggregator(self._field, 'max')

    @property
    def min(self):
        """
        Min aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.min)
        """
        return MetricAggregator(self._field, 'min')

    @property
    def avg(self):
        """
        Avg aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.avg)
        """
        return MetricAggregator(self._field, 'avg')

    @property
    def sum(self):
        """
        Sum aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.sum)
        """
        return MetricAggregator(self._field, 'sum')

    @property
    def value_count(self):
        """
        Value count aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.value_count)
        """
        return MetricAggregator(self._field, 'value_count')

    count = value_count

    @property
    def cardinality(self):
        """
        Distince aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.cardinality)
        >>> df.groupby(df.gender).agg(df.age.distinct_count)
        """
        return MetricAggregator(self._field, 'cardinality')

    distinct_count = cardinality

    @property
    def percentiles(self):
        """
        Percentile aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.percentiles)
        """
        return MetricAggregator(self._field, 'percentiles')

    @property
    def percentile_ranks(self):
        """
        Percentile ranks aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.percentile_ranks)
        """
        return MetricAggregator(self._field, 'percentile_ranks')

    @property
    def stats(self):
        """
        Stats aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.stats)
        """
        return MetricAggregator(self._field, 'stats')

    @property
    def extended_stats(self):
        """
        Extended stats aggregator

        :return: :class:`Aggregator <pandasticsearch.operators.Aggregator>`

        >>> df.groupby(df.gender).agg(df.age.extended_stats)
        """
        return MetricAggregator(self._field, 'extended_stats')


class Row(tuple):
    """
    The builtin :class:`DataFrame <pandasticsearch.dataframe.DataFrame>` row type for accessing before converted into Pandas DataFrame.
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
        return 'Row(' + ','.join(
            ['{0}={1}'.format(k, Row._stringfy(v)) for k, v in zip(self._fields, tuple(self))]) + ')'

    @classmethod
    def _stringfy(cls, v):
        b = six.StringIO()
        b.write(repr(v))
        return b.getvalue()

    def as_dict(self):
        return dict((x, y) for x, y in zip(self._fields, self))
