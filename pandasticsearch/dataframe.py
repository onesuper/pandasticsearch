# -*- coding: UTF-8 -*-

from pandasticsearch.client import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.operators import *
from pandasticsearch.types import Column
from pandasticsearch.types import Row

import json
import six
import copy
import sys


class DataFrame(object):
    """
    A :class:`DataFrame` treats index and documents in Elasticsearch as named columns and rows.

    >>> from pandasticsearch import DataFrame
    >>> df = DataFrame.from_es('http://localhost:9200', index='people')

    It can be converted to Pandas object for subsequent analysis.
    >>> df.to_pandas()
    """

    def __init__(self, client, mapping, **kwargs):
        cols = []
        for index, mappings in six.iteritems(mapping):
            self._index = index
            for _, properties in six.iteritems(mappings['mappings']):
                for k, _ in six.iteritems(properties['properties']):
                    cols.append(k)

        if self._index is None:
            raise Exception('No index in [{0}]'.format(mapping))

        if len(cols) == 0:
            raise Exception('0 columns found in [{0}]'.format(self._index))

        self._client = client
        self._columns = sorted(cols)
        self._mapping = mapping
        self._filter = kwargs.get('filter', None)
        self._groupby = kwargs.get('groupby', None)
        self._aggregation = kwargs.get('aggregation', None)
        self._sort = kwargs.get('sort', None)
        self._projection = kwargs.get('projection', None)
        self._limit = kwargs.get('limit', None)
        self._last_query = None

    @staticmethod
    def from_es(url, index, doc_type=None):
        """
        Creates an :class:`DataFrame` object by providing the URL of ElasticSearch node and the name of the index.

        :param str url: URL of the node connected to
        :param str index: The name of the index
        :param str doc_type: The type of the document
        :return: DataFrame object for accessing
        :rtype: DataFrame
        """
        # get mapping structure from server
        if doc_type is None:
            mapping_endpoint = index
        else:
            mapping_endpoint = index + '/_mapping/' + doc_type

        mapping = RestClient(url, mapping_endpoint).get()

        if doc_type is None:
            endpoint = index + '/_search'
        else:
            endpoint = index + '/' + doc_type + '/_search'
        return DataFrame(RestClient(url, endpoint), mapping)

    def __getattr__(self, name):
        """
        Returns the :class:`Column` denoted by ``name``.
        """
        if name not in self.columns:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name))
        return Column(name)

    def __getitem__(self, item):
        if isinstance(item, six.string_types):
            if item not in self._columns:
                raise TypeError('Column does not exist: [{0}]'.format(item))
            return Column(item)
        elif isinstance(item, BooleanFilter):
            self._filter = item.build()
            return self
        else:
            raise TypeError('Unsupported expr: [{0}]'.format(item))

    def filter(self, condition):
        """
        Filters rows using a given condition

        where() is an alias for filter().

        :param condition: BooleanCond object

        >>> df.filter(df['age'] < 13).collect()
        [Row(age=12,gender='female',name='Alice'), Row(age=11,gender='male',name='Bob')]
        """
        assert isinstance(condition, BooleanFilter)
        return DataFrame(self._client, self._mapping,
                         filter=condition.build(),
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=self._limit)

    where = filter

    def select(self, *cols):
        """
        Projects a set of columns and returns a new L{DataFrame}

        :param cols: list of column names or L{Column}.

        >>> df.filter(df['age'] < 25).select('name', 'age').collect()
        [Row(age=12,name='Alice'), Row(age=11,name='Bob'), Row(age=13,name='Leo')]
        """
        columns = []
        for col in cols:
            if isinstance(col, six.string_types):
                columns.append(getattr(self, col))
            elif isinstance(col, Column):
                columns.append(col)
            else:
                raise TypeError('{0} is supposed to be str or Column'.format(col))
        return DataFrame(self._client, self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=columns,
                         sort=self._sort,
                         limit=self._limit)

    def limit(self, num):
        """
        Limits the result count to the number specified.
        """
        assert isinstance(num, int)
        assert num >= 1
        return DataFrame(self._client, self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=num)

    def groupby(self, *cols):
        columns = []

        if len(cols) == 1 and isinstance(cols[0], RangeGrouper):
            groupby = cols[0].build()
        else:
            for col in cols:
                if isinstance(col, six.string_types):
                    columns.append(getattr(self, col))
                elif isinstance(col, Column):
                    columns.append(col)
                else:
                    raise TypeError('{0} is supposed to be str or Column'.format(col))

            names = [col.field_name() for col in columns]
            groupby = Grouper.from_list(names).build()

        return DataFrame(self._client, self._mapping,
                         filter=self._filter,
                         groupby=groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=self.limit)

    def agg(self, *aggs):
        """
        Aggregate on the entire DataFrame without groups.
        :param aggs: aggregate functions

        >>> df[df['gender'] == 'male'].agg(df['age'].avg).collect()
        [Row(avg(age)=12)]
        """
        aggregation = {}
        for agg in aggs:
            assert isinstance(agg, Aggregator)
            aggregation.update(agg.build())

        return DataFrame(self._client, self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=self._limit)

    def sort(self, *cols):
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        :param cols: list of :class:`Column`to sort by.

        orderby() is an alias for sort().

        >>> df.sort(df['age'].asc).collect()
        [Row(age=11,name='Bob'), Row(age=12,name='Alice'), Row(age=13,name='Leo')]
        """
        sorts = []
        for col in cols:
            assert isinstance(col, Sorter)
            sorts.append(col.build())

        return DataFrame(self._client, self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=sorts,
                         limit=self._limit)

    orderby = sort

    def _execute(self):
        res_dict = self._client.post(data=self._build_query())
        if self._aggregation is None and self._groupby is None:
            query = Select.from_dict(res_dict)
        else:
            query = Agg.from_dict(res_dict)
        return query

    def collect(self):
        """
        Returns all the records as a list of Row.

        :return: list of L{Row}

        >>> df.collect()
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        """
        query = self._execute()
        return [Row(**v) for v in query.result]

    def to_pandas(self):
        """
        Export to a Pandas DataFrame object.
        :return: The DataFrame representing the query result

        >>> df[df['gender'] == 'male'].agg(Avg('age')).to_pandas()
            avg(age)
        0        12
        """
        query = self._execute()
        return query.to_pandas()

    def count(self):
        """
        Returns the number of rows in this index/type.
        >>> df.count()
        2
        """
        _df = self.agg(MetricAggregator('_index', 'value_count'))
        return _df.collect()[0]['count(*)']

    def show(self, n=10000, truncate=15):
        """
        Prints the first ``n`` rows to the console.

        :param n:  Number of rows to show.
        :param truncate:  Number of words to be truncated for each column.

        >>> df.filter(df['age'] < 25).select('name').show(3)
        +------+
        | name |
        +------+
        | Alice|
        | Bob  |
        | Leo  |
        +------+
        """
        assert n > 0

        if self._aggregation is not None:
            raise TypeError('show() is not allowed for aggregation. use collect() instead')

        query = self._execute()

        if self._projection:
            cols = [col.field_name() for col in self._projection]
        else:
            cols = self._columns
        sys.stdout.write(query.result_as_tabular(cols, n, truncate))
        sys.stdout.write('time: {0}ms\n'.format(query.millis_taken))

    def __repr__(self):
        return "DataFrame[%s]" % (", ".join("%s" % c for c in self._columns))

    def print_debug(self):
        """
        Return a indented JSON string returned by the Elasticsearch Server
        """
        sys.stdout.write(json.dumps(self._client.post(data=self._build_query()), indent=4))

    def to_dict(self):
        """
        Converts the current :class:`DataFrame` to Elasticsearch search dictionary.
        :return: a dictionary which obeys the Elasticsearch RESTful protocol
        """
        return self._build_query()

    def print_schema(self):
        """
        Prints out the schema in the tree format.

        >>> df.print_schema()
        index_name
        |-- type_name
          |-- experience :  {'type': 'integer'}
          |-- id :  {'type': 'string'}
          |-- mobile :  {'index': 'not_analyzed', 'type': 'string'}
          |-- regions :  {'index': 'not_analyzed', 'type': 'string'}
        """
        for index, mappings in six.iteritems(self._mapping):
            sys.stdout.write('{0}\n'.format(index))
            for typ, properties in six.iteritems(mappings['mappings']):
                sys.stdout.write('|--{0}\n'.format(typ))
                for k, v in six.iteritems(properties['properties']):
                    sys.stdout.write('  |--{0}: {1}\n'.format(k, v))

    @property
    def columns(self):
        """
        Returns all column names as a list.
        :return: column names as a list

        >>> df.columns
        ['age', 'name']
        """
        return self._columns

    @property
    def schema(self):
        return self._mapping

    def _build_query(self):
        query = dict()

        if self._limit:
            query['size'] = self._limit
        else:
            query['size'] = 20

        if self._groupby and not self._aggregation:
            query['aggregations'] = self._groupby
            query['size'] = 0

        if self._aggregation:
            if self._groupby is None:
                query['aggregations'] = self._aggregation
                query['size'] = 0

            else:
                agg = copy.deepcopy(self._groupby)
                # insert aggregator to the inner-most grouper
                inner_most = agg
                while True:
                    key = list(inner_most.keys())[0]
                    if 'aggregations' in inner_most[key]:
                        inner_most = inner_most[key]['aggregations']
                    else:
                        break
                key = list(inner_most.keys())[0]
                inner_most[key]['aggregations'] = self._aggregation
                query['aggregations'] = agg
                query['size'] = 0

        if self._filter:
            query['query'] = {'filtered': {'filter': self._filter}}

        if self._projection:
            query['_source'] = {"includes": [col.field_name() for col in self._projection], "excludes": []}

        if self._sort:
            query['sort'] = self._sort
        self._last_query = query
        return query
