from pandasticsearch.clients import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.filters import Filter, Equal, GreaterEqual, Greater, Less, LessEqual
from pandasticsearch.aggregators import Aggregator

import json


class Pandasticsearch(object):
    def __init__(self, url, index, type=None):
        if type is None:
            endpoint = index + '/_search'
        else:
            endpoint = index + '/' + type + '/_search'
        self._client = RestClient(url, endpoint)
        self._filter = None
        self._last_query = None

    def where(self, filter=None):
        self._filter = filter
        return self

    def top(self, size=10):
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
