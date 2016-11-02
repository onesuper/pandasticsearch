from pandasticsearch.clients import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.aggregators import Aggregator
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
        self._filter = None
        self._last_query = None

    def __getitem__(self, item):
        if isinstance(item, six.string_types):
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
        return self._client.execute(query, Select())

    def aggregate(self, *args):
        query = self._build_query(aggs=args, filter=self._filter, size=0)
        return self._client.execute(query, Agg())

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

    def debug_string(self, indent=4):
        return json.dumps(self._last_query, indent=indent)



