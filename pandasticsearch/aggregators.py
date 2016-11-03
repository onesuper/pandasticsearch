class Aggregator(object):
    def __init__(self, field):
        self._field = field
        self._aggregator = None

    def build(self):
        return self._aggregator


class MetricAggregator(Aggregator):
    def _metric_agg(self, agg_type, metric_rename=None, params=None):
        if metric_rename is None:
            metric_rename = '{0}({1})'.format(agg_type, self._field)

        agg_field = dict()
        agg_field['field'] = self._field
        if params is not None:
            agg_field.update(params)

        return {metric_rename: {agg_type: agg_field}}


class Avg(MetricAggregator):
    def __init__(self, field):
        super(Avg, self).__init__(field)
        self._aggregator = self._metric_agg('avg')


class Min(MetricAggregator):
    def __init__(self, field):
        super(Min, self).__init__(field)
        self._aggregator = self._metric_agg('min')


class Max(MetricAggregator):
    def __init__(self, field):
        super(Max, self).__init__(field)
        self._aggregator = self._metric_agg('max')


class Cardinality(MetricAggregator):
    def __init__(self, field):
        super(Cardinality, self).__init__(field)
        self._aggregator = self._metric_agg('cardinality')


class ValueCount(MetricAggregator):
    def __init__(self, field):
        super(ValueCount, self).__init__(field)
        self._aggregator = self._metric_agg('value_count')


class CountStar(MetricAggregator):
    def __init__(self):
        super(MetricAggregator, self).__init__('_index')
        self._aggregator = self._metric_agg('value_count', metric_rename='count(*)')


class Percentiles(MetricAggregator):
    def __init__(self, field, percents=None):
        super(Percentiles, self).__init__(field)
        self._aggregator = self._metric_agg('percentiles', params={'percents': percents})


class PercentileRanks(MetricAggregator):
    def __init__(self, field, values=None):
        super(PercentileRanks, self).__init__(field)
        self._aggregator = self._metric_agg('percentile_ranks', params={'values': values})
