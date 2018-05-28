# -*- coding: UTF-8 -*-

_metric_aggs = ('avg', 'min', 'max', 'cardinality', 'value_count', 'sum',
                'percentiles', 'percentile_ranks', 'stats', 'extended_stats')


class Aggregator(object):
    def __init__(self, field):
        self._field = field

    def build(self):
        pass


class MetricAggregator(Aggregator):
    def __init__(self, field, agg_type, alias=None, params=None):
        super(MetricAggregator, self).__init__(field)
        self._agg_type = agg_type
        self._alias = alias
        self._params = params

    def alias(self, alias):
        self._alias = alias
        return self

    def build(self):
        if self._agg_type not in _metric_aggs:
            raise Exception('Not support metric aggregator: {0}'.format(self._type))

        if self._alias is None:
            name = '{0}({1})'.format(self._agg_type, self._field)
        else:
            name = self._alias

        agg_field = dict()
        agg_field['field'] = self._field
        if self._params is not None:
            agg_field.update(self._params)
        return {name: {self._agg_type: agg_field}}

