from pandasticsearch.client import RestClient
from pandasticsearch.query import Agg, Select

matchall_query = {"query": {"match_all": {}}}


class Pandasticsearch(object):
    def __init__(self, url, index, type=None):
        if type is None:
            endpoint = index + '/_search'
        else:
            endpoint = index + '/' + type + '/_search'
        self._client = RestClient(url, endpoint)

    def top(self, size=10):
        dic = matchall_query.copy()
        dic['size'] = size
        return self._client.execute(dic, Select())

    def distinct_count(self, field):
        """
        Calculates an approximate count of distinct values.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        return self._client.execute(Pandasticsearch._metric_agg_query('cardinality', field), Agg())

    def value_count(self, field):
        """
        Counts the number of values that are extracted from the aggregated documents.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        return self._client.execute(Pandasticsearch._metric_agg_query('value_count', field), Agg())

    def percentiles(self, field, percents=None):
        """
        Calculates one or more percentiles over numeric values extracted from the aggregated documents.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        return self._client.execute(
            Pandasticsearch._metric_agg_query('percentiles', field, params={'percents': percents}), Agg())

    def percentile_ranks(self, field, values=None):
        """
        Calculates one or more percentile ranks over numeric values extracted from the aggregated documents.
        :param str field: The field to be performed metric-aggregation on
        :return: an Agg object containing the aggregated value
        :rtype: Agg
        """
        return self._client.execute(
            Pandasticsearch._metric_agg_query('percentile_ranks', field, params={'values': values}), Agg())

    @classmethod
    def _metric_agg_query(cls, agg_type, field, rename=None, params=None):
        if agg_type in ('value_count', 'cardinality', 'percentiles', 'percentile_ranks'):
            pass
        else:
            raise NotImplementedError('type={0} is not supported for metric agg'.format(agg_type))

        if rename is None:
            rename = '{0}({1})'.format(agg_type, field)

        agg_field = dict()
        agg_field['field'] = field
        if params is not None:
            agg_field.update(params)

        dic = {'size': 0, 'aggs': {rename: {agg_type: agg_field}}}
        print(dic)
        return dic
