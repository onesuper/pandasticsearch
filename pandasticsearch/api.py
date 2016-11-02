from pandasticsearch.client import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.aggregators import Aggregator, CountStar
from pandasticsearch.errors import ColumnExprException
from pandasticsearch.filters import Filter
from pandasticsearch.types import Column

import json
import six


class Pandasticsearch(object):
    def __init__(self, url, index, type=None):
        if type is None:
            endpoint = index + '/_search'
        else:
            endpoint = index + '/' + type + '/_search'
        self._client = RestClient(url, endpoint)

        # setup the columns(properties)
        if type is None:
            mapping_endpoint = index
        else:
            mapping_endpoint = index + '/_mapping/' + type

        mapping_client = RestClient(url, mapping_endpoint)
        self._mapping_json = mapping_client.get()

        self._columns = []
        for _, mappings in six.iteritems(self._mapping_json):
            for _, properties in six.iteritems(mappings['mappings']):
                for k, _ in six.iteritems(properties['properties']):
                    self._columns.append(k)

        self._filter = None
        self._last_query = None

    def __getitem__(self, item):
        if isinstance(item, six.string_types):
            if item not in self._columns:
                raise ColumnExprException('Column does not exist: [{0}]'.format(item))
            return Column(item)
        elif isinstance(item, Filter):
            self._filter = item
            return self
        else:
            raise ColumnExprException('Unsupported expr: [{0}]'.format(item))

    def filter(self, filter=None):
        self._filter = filter
        return self

    def show(self, size=10):
        query = self._build_query(filter=self._filter, size=size)
        return Select.from_dict(self._client.post(data=query))

    def aggregate(self, *args):
        query = self._build_query(aggs=args, filter=self._filter, size=0)
        return Agg.from_dict(self._client.post(data=query))

    def count(self):
        """
        Returns the number of rows in this index/type.
        >>> ps.count()
        2
        """
        return self.aggregate(CountStar())[0]['count(*)']

    def _build_query(self, aggs=None, filter=None, size=None):
        query = {}

        if aggs is not None and len(aggs) > 0:
            agg_dict = {}
            for agg in aggs:
                assert isinstance(agg, Aggregator)
                agg_dict.update(agg.build())
            query['aggregations'] = agg_dict

        if filter is not None:
            assert isinstance(filter, Filter)
            query['query'] = {'filtered': {'filter': filter.build()}}
        else:
            query['query'] = {"match_all": {}}

        if size is not None:
            query['size'] = size

        self._last_query = query
        return query

    def debug_string(self):
        """
        Return a indented JSON string returned by the Elasticsearch Server
        """
        return json.dumps(self._last_query, indent=4)

    def print_schema(self):
        """
        Prints out the schema in the tree format.
        >>> ps.print_schema()
        index_name
        |-- type_name
          |-- experience :  {'type': 'integer'}
          |-- id :  {'type': 'string'}
          |-- mobile :  {'index': 'not_analyzed', 'type': 'string'}
          |-- regions :  {'index': 'not_analyzed', 'type': 'string'}
        """
        for index, mappings in six.iteritems(self._mapping_json):
            print(index)
            for typ, properties in six.iteritems(mappings['mappings']):
                print('|--', typ)
                for k, v in six.iteritems(properties['properties']):
                    print('  |--', k, ': ', v)

    @property
    def columns(self):
        """
        Returns all column names as a list.
        :return: column names as a list
        >>> ps.columns
        ['age', 'name']
        """
        return self._columns
