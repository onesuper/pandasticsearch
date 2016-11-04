import collections
import json
import pandas


class Query(collections.MutableSequence):
    def __init__(self):
        super(Query, self).__init__()
        self._values = None
        self._result_dict = None

    def explain_result(self, result=None):
        if result is not None:
            assert isinstance(result, dict)
            self._result_dict = result

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

    def explain_result(self, result=None):
        super(Select, self).explain_result(result)
        self._values = [hit['_source'] for hit in self._result_dict['hits']['hits']]

    def to_pandas(self):
        if self._values:
            df = pandas.DataFrame(data=self._values)
            return df

    @staticmethod
    def from_dict(d):
        query = Select()
        query.explain_result(d)
        return query


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
            self.append(row)
            if len(index) > 0:
                self._indexes.append(index)

    def to_pandas(self):
        print(self._indexes, self._values)
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
                        for x in Agg._process_agg(sub_bucket, indexes + (sub_bucket['key'],), names + (k,)):
                            yield x
                elif 'value' in v:
                    row[k] = v['value']
                elif 'values' in v:  # percentiles
                    row = v['values']

        if len(row) > 0:
            yield (names, indexes, row)

    @staticmethod
    def from_dict(d):
        agg = Agg()
        agg.explain_result(d)
        return agg
