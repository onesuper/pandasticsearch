# -*- coding: UTF-8 -*-

import collections
import json
import six

from pandasticsearch.errors import NoSuchDependencyException


class Query(collections.MutableSequence):
    def __init__(self):
        super(Query, self).__init__()
        self._values = None
        self._result_dict = None
        self._took_millis = None

    def explain_result(self, result=None):
        if result is not None:
            assert isinstance(result, dict)
            self._result_dict = result
            self._took_millis = self._result_dict['took']

    def to_pandas(self):
        """
        Export the current query result to a Pandas DataFrame object.
        """
        raise NotImplementedError('implemented in subclass')

    def print_json(self):
        indented_json = json.dumps(self._result_dict, sort_keys=True, separators=(',', ': '), indent=4,
                                   ensure_ascii=False)
        print(indented_json)

    @property
    def result(self):
        return self._values

    @property
    def millis_taken(self):
        return self._took_millis

    @property
    def json(self):
        """
        Gets the original JSON representation returned by Elasticsearch REST API
        :return: The JSON string indicating the query result
        :rtype: string
        """
        return json.dumps(self._result_dict)

    def insert(self, index, value):
        self._values.insert(index, value)

    def append(self, value):
        self._values.append(value)

    def __str__(self):
        return str(self._values)

    def __len__(self):
        return len(self._values)

    def __delitem__(self, index):
        del self._values[index]

    def __setitem__(self, index, value):
        self._values[index] = value

    def __getitem__(self, index):
        return self._values[index]


class Select(Query):
    def __init__(self):
        super(Select, self).__init__()

    def resolve_fields(self, row):
        fields = {}
        for field in row:
            nested_fields = {}
            if isinstance(row[field], dict):
                nested_fields = self.resolve_fields(row[field])
                for n_field, val in nested_fields.items():
                    fields["{}.{}".format(field, n_field)] = val
            else:
                fields[field] = row[field]
        return fields

    def explain_result(self, result=None):
        super(Select, self).explain_result(result)
        rows = []
        for hit in self._result_dict['hits']['hits']:
            row = {}
            for k in hit.keys():
                if k == '_source':
                    solved_fields = self.resolve_fields(hit['_source'])
                    row.update(solved_fields)
                elif k.startswith('_'):
                    row[k] = hit[k]
            rows.append(row)
        self._values = rows

    def to_pandas(self):
        try:
            import pandas
        except ImportError:
            raise NoSuchDependencyException('this method requires pandas library')
        if self._values:
            df = pandas.DataFrame(data=self._values)
            return df

    @staticmethod
    def from_dict(d):
        query = Select()
        query.explain_result(d)
        return query

    @classmethod
    def _stringfy_value(cls, value):
        b = six.StringIO()
        if value:
            b.write(repr(value))
        else:
            b.write('(NULL)')
        return b.getvalue()

    def result_as_tabular(self, cols, n, truncate=20):
        b = six.StringIO()
        widths = []
        tavnit = '|'
        separator = '+'

        for col in cols:
            maxlen = len(col)
            for kv in self.result[:n]:
                if col in kv:
                    s = Select._stringfy_value(kv[col])
                else:
                    s = '(NULL)'
                if len(s) > maxlen:
                    maxlen = len(s)
            widths.append(min(maxlen, truncate))

        for w in widths:
            tavnit += ' %-' + '%ss |' % (w,)
            separator += '-' * w + '--+'

        b.write(separator + '\n')
        b.write(tavnit % tuple(cols) + '\n')
        b.write(separator + '\n')
        for kv in self.result[:n]:
            row = []
            for col in cols:
                if col in kv:
                    s = Select._stringfy_value(kv[col])
                    if len(s) > truncate:
                        s = s[:truncate - 3] + '...'
                else:
                    s = '(NULL)'
                row.append(s)
            b.write(tavnit % tuple(row) + '\n')
        b.write(separator + '\n')
        return b.getvalue()


class Agg(Query):
    def __init__(self):
        super(Agg, self).__init__()
        self._index_names = None
        self._indexes = None

    def explain_result(self, result=None):
        super(Agg, self).explain_result(result)
        tuples = list(Agg._process_agg(self._result_dict['aggregations']))
        assert len(tuples) > 0
        self._index_names = list(tuples[0][0])
        self._values = []
        self._indexes = []
        for t in tuples:
            _, index, row = t
            self._values.append(row)
            if len(index) > 0:
                self._indexes.append(index)

    @property
    def index(self):
        return self._indexes

    def to_pandas(self):
        try:
            import pandas
        except ImportError:
            raise NoSuchDependencyException('this method requires pandas library')
        if self._values is not None:
            if len(self._indexes) > 0:
                index = pandas.MultiIndex.from_tuples(self._indexes, names=self._index_names)
                df = pandas.DataFrame(data=self._values, index=index)
            else:
                df = pandas.DataFrame(data=self._values)
            return df

    @classmethod
    def _process_agg(cls, bucket, indexes=(), names=()):
        """
        Recursively extract agg values
        :param bucket: a bucket contains either sub-buckets or a bunch of aggregated values
        :return: a list of tuples: (index_name, index_tuple, row)
        """
        # for each agg, yield a row
        row = {}
        for k, v in bucket.items():
            if isinstance(v, dict):
                if 'buckets' in v:
                    for sub_bucket in v['buckets']:

                        if 'key_as_string' in sub_bucket:
                            key = sub_bucket['key_as_string']
                        else:
                            key = sub_bucket['key']
                        for x in Agg._process_agg(sub_bucket,
                                                  indexes + (key,),
                                                  names + (k,)):
                            yield x
                elif 'value' in v:
                    row[k] = v['value']
                elif 'values' in v:  # percentiles
                    row = v['values']
                else:
                    row.update(v)  # stats
            else:
                if k == 'doc_count':  # count docs
                    row['doc_count'] = v

        if len(row) > 0:
            yield (names, indexes, row)

    @staticmethod
    def from_dict(d):
        agg = Agg()
        agg.explain_result(d)
        return agg
