# -*- coding: UTF-8 -*-

_metric_aggs = ('avg', 'min', 'max', 'cardinality', 'value_count', 'sum',
                'percentiles', 'percentile_ranks', 'stats', 'extended_stats')

_sort_mode = ('min', 'max', 'sum', 'avg', 'median')


class Aggregator(object):
    def __init__(self, field):
        self._field = field

    def build(self):
        pass


class Grouper(object):
    def __init__(self, field, size=20, inner=None, include=None, exclude=None):
        self._field = field
        self._size = size
        self._inner = inner
        self._include = include
        self._exclude = exclude

    @staticmethod
    def from_list(l):
        if len(l) == 1:
            return Grouper(l[0])
        return Grouper(l[0], inner=Grouper.from_list(l[1:]))

    def build(self):
        terms = {'field': self._field, 'size': self._size}
        if self._exclude is not None:
            assert isinstance(self._exclude, list)
            terms['exclude'] = self._exclude
        if self._include is not None:
            assert isinstance(self._include, list)
            terms['include'] = self._include

        agg = {"terms": terms}

        if self._inner is not None:
            agg["aggregations"] = self._inner.build()

        return {self._field: agg}


class RangeGrouper(Grouper):
    def __init__(self, field, range_list):
        assert isinstance(range_list, list)
        super(RangeGrouper, self).__init__(field)
        self._field = field
        self._range_list = range_list

    def build(self):
        ranges = []
        starts = self._range_list[:-1]
        ends = self._range_list[1:]
        for start, end in zip(starts, ends):
            ranges.append({'from': start, 'to': end})

        name = 'range(' + ','.join([str(x) for x in self._range_list]) + ')'
        return {name: {'range': {'field': self._field, 'ranges': ranges}}}


class DateGrouper(Grouper):
    def __init__(self, field, interval, format):
        super(DateGrouper, self).__init__(field)
        self._field = field
        self._interval = interval
        self._format = format

    def build(self):
        name = 'date({0},{1})'.format(self._field, self._interval)
        return {name: {'date_histogram': {
            'field': self._field,
            'interval': self._interval,
            'format': self._format,
        }}}


class Scriptor(object):
    def __init__(self, inline, lang=None, params=None):
        self._inline = inline
        self._lang = lang
        self._params = params

    def build(self):
        script = {'inline': self._inline}
        if self._lang:
            script['lang'] = self._lang
        if self._params:
            script['params'] = self._params
        return {'script': script}


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


class Sorter(object):
    def __init__(self, field, order='desc', mode=None):
        self._field = field
        self._order = order
        self._mode = mode

    def build(self):
        sort = {}
        if self._mode is not None:
            if self._mode not in _sort_mode:
                raise Exception('Not support sort mode: {0}'.format(self._mode))
            sort['mode'] = self._mode
        sort['order'] = self._order
        return {self._field: sort}


class ScriptSorter(object):
    def __init__(self, script, order='desc', params=None):
        self._order = order
        self._script = script
        self._params = params

    def build(self):
        script = {'script': self._script, 'type': 'number', 'order': self._order}
        if self._params:
            script['params'] = self._params
        return {'_script': script}


# Es filter builder for BooleanCond
class BooleanFilter(object):
    def __init__(self, *args):
        self._filter = None

    def __and__(self, x):
        # Combine results
        if isinstance(self, AndFilter):
            self.subtree['must'].append(x.subtree)
            return self
        elif isinstance(x, AndFilter):
            x.subtree['must'].append(self.subtree)
            return x
        return AndFilter(self, x)

    def __or__(self, x):
        # Combine results
        if isinstance(self, OrFilter):
            self.subtree['should'].append(x.subtree)
            return self
        elif isinstance(x, OrFilter):
            x.subtree['should'].append(self.subtree)
            return x
        return OrFilter(self, x)

    def __invert__(self):
        return NotFilter(self)

    @property
    def subtree(self):
        if 'bool' in self._filter:
            return self._filter['bool']
        else:
            return self._filter

    def build(self):
        return self._filter


# Binary operator
class AndFilter(BooleanFilter):
    def __init__(self, *args):
        [isinstance(x, BooleanFilter) for x in args]
        super(AndFilter, self).__init__()
        self._filter = {'bool': {'must': [x.build() for x in args]}}


class OrFilter(BooleanFilter):
    def __init__(self, *args):
        [isinstance(x, BooleanFilter) for x in args]
        super(OrFilter, self).__init__()
        self._filter = {'bool': {'should': [x.build() for x in args]}}


class NotFilter(BooleanFilter):
    def __init__(self, x):
        assert isinstance(x, BooleanFilter)
        super(NotFilter, self).__init__()
        self._filter = {'bool': {'must_not': x.build()}}


# LeafBooleanFilter
class GreaterEqual(BooleanFilter):
    def __init__(self, field, value):
        super(GreaterEqual, self).__init__()
        self._filter = {'range': {field: {'gte': value}}}


class Greater(BooleanFilter):
    def __init__(self, field, value):
        super(Greater, self).__init__()
        self._filter = {'range': {field: {'gt': value}}}


class LessEqual(BooleanFilter):
    def __init__(self, field, value):
        super(LessEqual, self).__init__()
        self._filter = {'range': {field: {'lte': value}}}


class Less(BooleanFilter):
    def __init__(self, field, value):
        super(Less, self).__init__()
        self._filter = {'range': {field: {'lt': value}}}


class Equal(BooleanFilter):
    def __init__(self, field, value):
        super(Equal, self).__init__()
        self._filter = {'term': {field: value}}


class IsIn(BooleanFilter):
    def __init__(self, field, value):
        super(IsIn, self).__init__()
        assert isinstance(value, list)
        self._filter = {'terms': {field: value}}


class Like(BooleanFilter):
    def __init__(self, field, value):
        super(Like, self).__init__()
        self._filter = {'wildcard': {field: value}}


class Rlike(BooleanFilter):
    def __init__(self, field, value):
        super(Rlike, self).__init__()
        self._filter = {'regexp': {field: value}}


class Startswith(BooleanFilter):
    def __init__(self, field, value):
        super(Startswith, self).__init__()
        self._filter = {'prefix': {field: value}}


class IsNull(BooleanFilter):
    def __init__(self, field):
        super(IsNull, self).__init__()
        self._filter = {'missing': {'field': field}}


class NotNull(BooleanFilter):
    def __init__(self, field):
        super(NotNull, self).__init__()
        self._filter = {'exists': {'field': field}}


class ScriptFilter(BooleanFilter):
    def __init__(self, inline, lang=None, params=None):
        super(ScriptFilter, self).__init__()
        self._filter = {'script': Scriptor(inline, lang, params).build()}
