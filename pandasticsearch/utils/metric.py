def metric_agg(agg_type, field, metric_rename=None, params=None):
    if agg_type in ('value_count', 'cardinality', 'percentiles', 'percentile_ranks', 'avg'):
        pass
    else:
        raise NotImplementedError('type={0} is not supported for metric agg'.format(agg_type))

    if metric_rename is None:
        metric_rename = '{0}({1})'.format(agg_type, field)

    agg_field = dict()
    agg_field['field'] = field
    if params is not None:
        agg_field.update(params)

    return {metric_rename: {agg_type: agg_field}}


def max(field):
    agg_func = metric_agg('avg', field)
    return agg_func
