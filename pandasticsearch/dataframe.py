from pandasticsearch.client import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.aggregators import Aggregator, CountStar
from pandasticsearch.types import Column, BooleanCond
from pandasticsearch.types import Row

import json
import six
import sys


class DataFrame(object):
    """
    A :class:`DataFrame` treats index and documents in Elasticsearch as named columns and rows.

    >>> from pandasticsearch import DataFrame
    >>> df = DataFrame('http://localhost:9200', index='company')
    """

    def __init__(self, client, columns, schema, **kwargs):
        self._client = client
        self._columns = columns
        self._schema = schema
        self._filter = kwargs.get('filter', None)
        self._aggregation = kwargs.get('aggregation', None)
        self._projection = kwargs.get('projection', None)
        self._limit = kwargs.get('limit', None)
        self._last_query = None

    @staticmethod
    def from_es(url, index, type=None):
        # setup the columns(properties)
        if type is None:
            mapping_endpoint = index
        else:
            mapping_endpoint = index + '/_mapping/' + type

        schema = RestClient(url, mapping_endpoint).get()

        cols = []
        for _, mappings in six.iteritems(schema):
            for _, properties in six.iteritems(mappings['mappings']):
                for k, _ in six.iteritems(properties['properties']):
                    cols.append(k)

        if len(cols) == 0:
            raise Exception('0 columns found in [{0}]'.format(index))

        if type is None:
            endpoint = index + '/_search'
        else:
            endpoint = index + '/' + type + '/_search'
        return DataFrame(RestClient(url, endpoint), cols, schema)

    def __getattr__(self, name):
        """Returns the :class:`Column` denoted by ``name``.
        >>> df.select(df.age).collect()
        [Row(age=2), Row(age=5)]
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
        elif isinstance(item, BooleanCond):
            self._filter = item.build()
            return self
        else:
            raise TypeError('Unsupported expr: [{0}]'.format(item))

    def filter(self, condition):
        """
        Filters rows using a given condition

        where() is an alias for filter().

        :param condition: BooleanCond object
        """
        assert isinstance(condition, BooleanCond)
        return DataFrame(self._client, self._columns, self._schema,
                         filter=condition.build(),
                         aggregation=self._aggregation,
                         projection=self._projection,
                         limit=self._limit)

    where = filter

    def select(self, *cols):
        """
        Projects a set of columns and returns a new L{DataFrame}

        :param cols: list of column names or L{Column}.
        """
        project = {"includes": cols, "excludes": []}
        return DataFrame(self._client, cols, self._schema,
                         filter=self._filter,
                         aggregation=self._aggregation,
                         projection=project,
                         limit=self._limit)

    def limit(self, num):
        """
        Limits the result count to the number specified.
        """
        assert isinstance(num, int)
        assert num >= 1
        return DataFrame(self._client, self._columns, self._schema,
                         filter=self._filter,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         limit=num)

    def agg(self, *aggs):
        """
        Aggregate on the entire DataFrame without groups.
        :param aggs: aggregate functions
        """
        aggregation = {}
        for agg in aggs:
            assert isinstance(agg, Aggregator)
            aggregation.update(agg.build())
        return DataFrame(self._client, self._columns, self._schema,
                         filter=self._filter,
                         aggregation=aggregation,
                         projection=self._projection,
                         limit=self._limit)

    def collect(self):
        """
        Returns all the records as a list of Row.
        >>> df.collect()
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]

        :return: list of L{Row}
        """
        res_dict = self._client.post(data=self._build_query())
        if self._aggregation is not None:
            query = Agg.from_dict(res_dict)
        else:
            query = Select.from_dict(res_dict)

        return [Row(**v) for v in query.result]

    def to_pandas(self):
        """
        Export to a Pandas DataFrame object.
        :return: The DataFrame representing the query result
        """
        res_dict = self._client.post(data=self._build_query())
        if self._aggregation is not None:
            query = Agg.from_dict(res_dict)
        else:
            query = Select.from_dict(res_dict)
        return query.to_pandas()

    def count(self):
        """
        Returns the number of rows in this index/type.
        >>> df.count()
        2
        """
        return self.agg(CountStar())[0]['count(*)']

    def show(self, n=10):
        """
        Prints the first ``n`` rows to the console.

        :param n:  Number of rows to show.
        """
        assert n > 0
        res_dict = self._client.post(data=self._build_query())
        if self._aggregation is not None:
            query = Agg.from_dict(res_dict)
        else:
            query = Select.from_dict(res_dict)

        cols = self._columns
        widths = []
        tavnit = '|'
        separator = '+'

        for col in cols:
            maxlen = len(col)
            for kv in query.result[:n]:
                if col in kv:
                    s = str(kv[col])
                else:
                    s = '(NULL)'
                if len(s) > maxlen: maxlen = len(s)
            widths.append(min(maxlen, 15))

        for w in widths:
            tavnit += " %-" + "%ss |" % (w,)
            separator += '-' * w + '--+'

        sys.stdout.write(separator + '\n')
        sys.stdout.write(tavnit % tuple(cols) + '\n')
        sys.stdout.write(separator + '\n')
        for kv in query.result[:n]:
            row = []
            for col in cols:
                if col in kv:
                    row.append(str(kv[col]))
                else:
                    row.append('(NULL)')
            sys.stdout.write(tavnit % tuple(row) + '\n')
        sys.stdout.write(separator + '\n')

    def __repr__(self):
        return "DataFrame[%s]" % (", ".join("%s" % c for c in self._columns))

    def print_debug(self):
        """
        Return a indented JSON string returned by the Elasticsearch Server
        """
        sys.stdout.write(json.dumps(self._build_query(), indent=4))

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
        for index, mappings in six.iteritems(self._schema):
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
        return self._schema

    def _build_query(self):
        query = {}

        if self._limit:
            query['size'] = self._limit
        else:
            query['size'] = 20

        if self._aggregation:
            query['aggregations'] = self._aggregation
            query['size'] = 0

        if self._filter:
            query['query'] = {'filtered': {'filter': self._filter}}

        if self._projection:
            query['_source'] = self._projection

        self._last_query = query
        return query
