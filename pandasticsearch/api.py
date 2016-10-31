from pandasticsearch.clients import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.filters import Filter
from pandasticsearch.utils.metric import metric_agg

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
        assert isinstance(filter, Filter)
        self._filter = filter.build()
        return self

    def top(self, size=10):
        self._last_query = Pandasticsearch._build_query(filter=self._filter, size=size)
        return self._client.execute(self._last_query, Select())

    def aggregate(self, agg_func):
        self._last_query = Pandasticsearch._build_query(agg=agg_func, filter=self._filter, size=0)
        return self._client.execute(self._last_query, Agg())

    @staticmethod
    def _build_query(agg=None, filter=None, size=None):
        query = {}
        if agg is not None:
            query['aggs'] = agg
        if size is not None:
            query['size'] = size
        if filter is not None:
            query['query'] = {'filtered': {'filter': filter}}
        else:
            query['query'] = {"match_all": {}}
        return query

    def debug_string(self, indent=4):
        return json.dumps(self._last_query, indent=indent)

    def distinct_count(self, field):
        """
        Calculates an approximate count of distinct values.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        agg = metric_agg('cardinality', field)
        self._last_query = Pandasticsearch._build_query(agg=agg, filter=self._filter, size=0)
        return self._client.execute(self._last_query, Agg())

    def value_count(self, field):
        """
        Counts the number of values that are extracted from the aggregated documents.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        agg = metric_agg('value_count', field)
        self._last_query = Pandasticsearch._build_query(agg, filter=self._filter, size=0)
        return self._client.execute(self._last_query, Agg())

    def percentiles(self, field, percents=None):
        """
        Calculates one or more percentiles over numeric values extracted from the aggregated documents.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        agg = metric_agg('percentiles', field, params={'percents': percents})
        self._last_query = Pandasticsearch._build_query(agg=agg, filter=self._filter, size=0)
        return self._client.execute(self._last_query, Agg())

    def percentile_ranks(self, field, values=None):
        """
        Calculates one or more percentile ranks over numeric values extracted from the aggregated documents.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        agg = metric_agg('percentile_ranks', field, params={'values': values})
        self._last_query = Pandasticsearch._build_query(agg=agg, filter=self._filter, size=0)
        return self._client.execute(self._last_query, Agg())
