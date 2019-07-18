# -*- coding: UTF-8 -*-

from pandasticsearch.client import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.operators import *
from pandasticsearch.types import Column, Row
from pandasticsearch.errors import DataFrameException

import json
import six
import sys
import copy

_unbound_index_err = DataFrameException('DataFrame is not bound to ES index')

_count_aggregator = MetricAggregator('_index', 'value_count', alias='count').build()


class DataFrame(object):
    """
    A :class:`DataFrame` treats index and documents in Elasticsearch as named columns and rows.

    >>> from pandasticsearch import DataFrame
    >>> df = DataFrame.from_es('http://localhost:9200', index='people')

    Customizing the endpoint of the ElasticSearch:

    >>> from pandasticsearch import DataFrame
    >>> from pandasticsearch.client import RestClient
    >>> df = DataFrame(client=RestClient('http://host:port/v2/_search',), index='people')

    It can be converted to Pandas object for subsequent analysis:

    >>> df.to_pandas()
    """

    def __init__(self, **kwargs):
        self._client = kwargs.get('client', None)
        self._mapping = kwargs.get('mapping', None)
        self._doc_type = kwargs.get('doc_type', None)
        self._index = kwargs.get('index', None)
        self._compat = kwargs.get('compat', 2)
        self._filter = kwargs.get('filter', None)
        self._groupby = kwargs.get('groupby', None)
        self._aggregation = kwargs.get('aggregation', None)
        self._sort = kwargs.get('sort', None)
        self._projection = kwargs.get('projection', None)
        self._limit = kwargs.get('limit', None)
        self._last_query = None

    @property
    def index(self):
        """
        Returns the index name.

        :return: string as the name

        >>> df.index
        people/children
        """
        if self._index is None:
            return None
        return self._index + '/' + self._doc_type if self._doc_type else self._index

    @property
    def columns(self):
        """
        Returns all column names as a list.

        :return: column names as a list

        >>> df.columns
        ['age', 'name']
        """
        return sorted(self._get_cols(self._mapping)) if self._mapping else None

    @property
    def schema(self):
        """
        Returns the schema(mapping) of the index/type as a dictionary.
        """
        return self._mapping

    @staticmethod
    def from_es(**kwargs):
        """
        Creates an :class:`DataFrame <DataFrame>` object by providing the URL of ElasticSearch node and the name of the index.

        :param str url: URL of the node connected to (default: 'http://localhost:9200')
        :param str index: The name of the index
        :param str doc_type: The type of the document
        :param str compat: The compatible ES version (an integer number)
        :return: DataFrame object for accessing
        :rtype: DataFrame

        >>> from pandasticsearch import DataFrame
        >>> df = DataFrame.from_es('http://localhost:9200', index='people')
        """

        doc_type = kwargs.get('doc_type', None)
        index = kwargs.get('index', None)
        url = kwargs.get('url', 'http://localhost:9200')
        compat = kwargs.get('compat', 2)
        username = kwargs.get('username', None)
        password = kwargs.get('password', None)
        verify_ssl = kwargs.get('verify_ssl', True)

        if index is None:
            raise ValueError('Index name must be specified')

        mapping = RestClient(url, index, username, password, verify_ssl).get()

        if doc_type is None:
            endpoint = index + '/_search'
        else:
            endpoint = index + '/' + doc_type + '/_search'
        return DataFrame(client=RestClient(url, endpoint, username, password, verify_ssl),
                         mapping=mapping, index=index, doc_type=doc_type, compat=compat)

    def __getattr__(self, name):
        """
        Returns a :class:`types.Column <pandasticsearch.types.Column>` object denoted by ``name``.
        """
        if name not in self.columns:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name))
        return Column(name)

    def __getitem__(self, item):
        if isinstance(item, six.string_types):
            if item not in self.columns:
                raise TypeError('Column does not exist: [{0}]'.format(item))
            return Column(item)
        elif isinstance(item, BooleanFilter):
            self._filter = item
            return self
        else:
            raise TypeError('Unsupported expr: [{0}]'.format(item))

    def filter(self, condition):
        """
        Filters rows using a given condition.

        where() is an alias for filter().

        :param condition: :class:`BooleanFilter <pandasticsearch.operators.BooleanFilter>` object or a string

        >>> df.filter(df['age'] < 13).collect()
        [Row(age=12,gender='female',name='Alice'), Row(age=11,gender='male',name='Bob')]
        """

        if isinstance(condition, six.string_types):
            _filter = ScriptFilter(condition)
        elif isinstance(condition, BooleanFilter):
            _filter = condition
        else:
            raise TypeError('{0} is supposed to be str or BooleanFilter'.format(condition))

        # chaining filter treated as AND
        if self._filter is not None:
            _filter = (self._filter & _filter)

        return DataFrame(client=self._client,
                         index=self._index,
                         doc_type=self._doc_type,
                         mapping=self._mapping,
                         filter=_filter,
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=self._limit,
                         compat=self._compat)

    where = filter

    def select(self, *cols):
        """
        Projects a set of columns and returns a new :class:`DataFrame <DataFrame>`

        :param cols: list of column names or :class:`Column <pandasticsearch.types.Column>`.

        >>> df.filter(df['age'] < 25).select('name', 'age').collect()
        [Row(age=12,name='Alice'), Row(age=11,name='Bob'), Row(age=13,name='Leo')]
        """
        projection = []
        for col in cols:
            if isinstance(col, six.string_types):
                projection.append(getattr(self, col))
            elif isinstance(col, Column):
                projection.append(col)
            else:
                raise TypeError('{0} is supposed to be str or Column'.format(col))
        return DataFrame(client=self._client,
                         index=self._index,
                         doc_type=self._doc_type,
                         mapping=self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=projection,
                         sort=self._sort,
                         limit=self._limit,
                         compat=self._compat)

    def limit(self, num):
        """
        Limits the result count to the number specified.
        """
        assert isinstance(num, int)
        assert num >= 1
        return DataFrame(client=self._client,
                         index=self._index,
                         doc_type=self._doc_type,
                         mapping=self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=num,
                         compat=self._compat)

    def groupby(self, *cols):
        """
        Returns a new :class:`DataFrame <DataFrame>` object grouped by the specified column(s).

        :param cols: A list of column names, :class:`Column <pandasticsearch.types.Column>` or :class:`Grouper <pandasticsearch.operators.Grouper>` objects
        """
        columns = []
        if len(cols) == 1 and isinstance(cols[0], Grouper):
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

        return DataFrame(client=self._client,
                         index=self._index,
                         doc_type=self._doc_type,
                         mapping=self._mapping,
                         filter=self._filter,
                         groupby=groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=self.limit,
                         compat=self._compat)

    def agg(self, *aggs):
        """
        Aggregate on the entire DataFrame without groups.

        :param aggs: a list of :class:`Aggregator <pandasticsearch.operators.Aggregator>` objects

        >>> df[df['gender'] == 'male'].agg(df['age'].avg).collect()
        [Row(avg(age)=12)]
        """
        aggregation = {}
        for agg in aggs:
            assert isinstance(agg, Aggregator)
            aggregation.update(agg.build())

        return DataFrame(client=self._client,
                         index=self._index,
                         doc_type=self._doc_type,
                         mapping=self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=aggregation,
                         projection=self._projection,
                         sort=self._sort,
                         limit=self._limit,
                         compat=self._compat)

    def sort(self, *cols):
        """
        Returns a new :class:`DataFrame <DataFrame>` object sorted by the specified column(s).

        :param cols: A list of column names, :class:`Column <pandasticsearch.types.Column>` or :class:`Sorter <pandasticsearch.operators.Sorter>`.

        orderby() is an alias for sort().

        >>> df.sort(df['age'].asc).collect()
        [Row(age=11,name='Bob'), Row(age=12,name='Alice'), Row(age=13,name='Leo')]
        """
        sorts = []
        for col in cols:
            if isinstance(col, six.string_types):
                sorts.append(ScriptSorter(col).build())
            elif isinstance(col, Sorter):
                sorts.append(col.build())
            else:
                raise TypeError('{0} is supposed to be str or Sorter'.format(col))

        return DataFrame(client=self._client,
                         index=self._index,
                         doc_type=self._doc_type,
                         mapping=self._mapping,
                         filter=self._filter,
                         groupby=self._groupby,
                         aggregation=self._aggregation,
                         projection=self._projection,
                         sort=sorts,
                         limit=self._limit,
                         compat=self._compat)

    orderby = sort

    def _execute(self):
        if self._client is None:
            raise _unbound_index_err

        res_dict = self._client.post(data=self._build_query())
        if self._aggregation is None and self._groupby is None:
            query = Select()
            query.explain_result(res_dict)
        else:
            query = Agg.from_dict(res_dict)
        return query

    def collect(self):
        """
        Returns all the records as a list of Row.

        :return: list of :class:`Row <pandasticsearch.types.Row>`

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
        Returns a list of numbers indicating the count for each group

        >>> df.groupby(df.gender).count()
        [2, 1]
        """
        df = DataFrame(client=self._client,
                       index=self._index,
                       doc_type=self._doc_type,
                       mapping=self._mapping,
                       filter=self._filter,
                       groupby=self._groupby,
                       aggregation=_count_aggregator,
                       projection=self._projection,
                       sort=self._sort,
                       limit=self._limit,
                       compat=self._compat)
        return df

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

        if self._aggregation:
            raise DataFrameException('show() is not allowed for aggregation. use collect() instead')

        query = self._execute()

        if self._projection:
            cols = [col.field_name() for col in self._projection]
        else:
            cols = self.columns

        if cols is None:
            raise _unbound_index_err

        sys.stdout.write(query.result_as_tabular(cols, n, truncate))
        sys.stdout.write('time: {0}ms\n'.format(query.millis_taken))

    def __repr__(self):
        if self.columns is None:
            return "DataFrame(Unbound)"
        return "DataFrame[%s]" % (", ".join("%s" % c for c in self.columns))

    def print_debug(self):
        """
        Post the query to the Elasticsearch Server and prints out the result it returned
        """
        if self._client is None:
            raise _unbound_index_err
        sys.stdout.write(json.dumps(self._client.post(data=self._build_query()), indent=4))

    def to_dict(self):
        """
        Converts the current :class:`DataFrame <DataFrame>` object to Elasticsearch search dictionary.

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
        if self._index is None:
            return

        sys.stdout.write('{0}\n'.format(self._index))
        index_name = list(self._mapping.keys())[0]
        if self._compat >= 7:
            json_obj = self._mapping[index_name]["mappings"]["properties"]
            sys.stdout.write(self.resolve_schema(json_obj))
        else:
            if self._doc_type is not None:
                json_obj = self._mapping[index_name]["mappings"][self._doc_type]["properties"]
                sys.stdout.write(self.resolve_schema(json_obj))
            else:
                raise DataFrameException('Please specify mapping for ES version under 7')

    def resolve_schema(self, json_prop, res_schema="", depth=1):
        for field in json_prop:
            if "properties" in json_prop[field]:
                res_schema += "{}|--{}:\n".format('  ' * depth, field)
                res_schema = self.resolve_schema(json_prop[field]["properties"],
                                                 res_schema, depth=depth+1)
            else:
                res_schema += "{}|--{}: {}\n".format('  ' * depth, field, json_prop[field])
        return res_schema

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
            assert isinstance(self._filter, BooleanFilter)
            if self._compat >= 5:
                query['query'] = {'bool': {'filter': self._filter.build()}}
            else:
                query['query'] = {'filtered': {'filter': self._filter.build()}}

        if self._projection:
            query['_source'] = {"includes": [col.field_name() for col in self._projection], "excludes": []}

        if self._sort:
            query['sort'] = self._sort
        self._last_query = query
        return query

    def _get_cols(self, mapping):
        index = list(mapping.keys())[0]
        cols = self._get_mappings(mapping, index)

        if len(cols) == 0:
            raise DataFrameException('0 columns found in mapping')
        return cols

    @classmethod
    def resolve_mappings(cls, json_map):
        prop = []
        for field in json_map:
            nested_props = []
            if "properties" in json_map[field]:
                nested_props = cls.resolve_mappings(json_map[field]["properties"])
            if len(nested_props) == 0:
                prop.append(field)
            else:
                for nested_prop in nested_props:
                    prop.append("{}.{}".format(field, nested_prop))
        return prop

    def _get_mappings(self, json_map, index_name):
        if self._compat >= 7:
            return DataFrame.resolve_mappings(json_map[index_name]["mappings"]["properties"])
        else:
            if self._doc_type is not None:
                return DataFrame.resolve_mappings(json_map[index_name]["mappings"][self._doc_type]["properties"])
            else:
                raise DataFrameException('Please specify mapping for ES version under 7')
