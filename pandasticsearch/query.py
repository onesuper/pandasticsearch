import collections
import json
import pandas

from pandasticsearch.exc import NoSuchDependency, ParseResultException


class Query(collections.MutableSequence):
    """
    Query objects are produced by Elasticsearch clients and can be used for exporting query results into
    pandas.DataFrame objects for subsequent analysis.
    """

    def __init__(self):
        super(Query, self).__init__()
        self._values = None
        self._result_dict = None

    def parse_json(self, json_str):
        res = json.loads(json_str)
        self._result_dict = res

    def explain_result(self, result=None):
        if result is not None:
            assert isinstance(result, dict)
            self._result_dict = result

    def to_pandas(self):
        """
        Export the current query result to a Pandas DataFrame object.
        :return: The DataFrame representing the query result
        :rtype: DataFrame
        :raise: NotImplementedError
        """
        raise NotImplementedError('implemented in subclass')

    def pretty(self, indent=None):
        """
        Prettify the result (requires pygements package)
        :return: The formatted string
        :rtype: string
        :raise: ImportError
        """
        try:
            from pygments import highlight, lexers, formatters
            indented = json.dumps(self._result_dict, sort_keys=True, separators=(',', ': '), indent=indent,
                                  ensure_ascii=False)
            return highlight(indented, lexers.JsonLexer(), formatters.TerminalFormatter())
        except ImportError:
            raise NoSuchDependency('pretty() method requires pygements package')

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

    def __repr__(self):
        return 'values: {0}'.format(self._values)

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
        self._values = []

    def explain_result(self, result=None):
        super(Select, self).explain_result(result)
        self._values = [hit['_source'] for hit in self._result_dict['hits']['hits']]

    def to_pandas(self):
        if self._values:
            df = pandas.DataFrame(data=self._values)
            return df


class Agg(Query):
    def __init__(self):
        super(Agg, self).__init__()
        self._index_names = []
        self._indexes = []
        self._values = []

    def explain_result(self, result=None):
        super(Agg, self).explain_result(result)
        key, value = list(self._result_dict['aggregations'].items())[0]
        tuples = list(Agg._process_buckets(key, value))
        assert len(tuples) > 0
        self._index_names = tuples[0][0]
        for t in tuples:
            _, index, row = t
            self.append(row)
            self._indexes.append(index)

    def to_pandas(self):
        if self._values:
            index = pandas.MultiIndex.from_tuples(self._indexes, names=self._index_names)
            df = pandas.DataFrame(data=self._values, index=index)
            return df

    def __repr__(self):
        return 'index_names: {0}\nindexes: {1}\n'.format(
            self._index_names, self._indexes) + super(Agg, self).__repr__()

    @classmethod
    def _process_buckets(cls, key, value, indexes=(), names=()):
        """
        Recursively extract bucket values
        :param key: aggregation key
        :param value: either a dictionary contains sub-aggregation or a dictionary contains field value
        :return: a list of tuples: (index_names, indexes, row)
        """
        for bucket in value['buckets']:
            row = {}
            for k, v in bucket.items():
                if isinstance(v, dict):
                    if 'buckets' in v:
                        for x in Agg._process_buckets(k, v, indexes + (bucket['key'],), names + (key,)):
                            yield x
                    elif 'value' in v:
                        row[k] = v['value']
                    else:
                        pass
            if len(row) > 0:
                yield (names + (key,), indexes + (bucket['key'],), row)
