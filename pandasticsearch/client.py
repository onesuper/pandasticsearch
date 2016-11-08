# -*- coding: UTF-8 -*-

import json
import sys
from six.moves import urllib

from pandasticsearch.errors import ServerDefinedException


class RestClient(object):
    """
    RestClient talks to Elasticsearch cluster through native RESTful API.
    Returns Query objects that can be used to export to pandas.DataFrame objects for subsequent analysis.
    :param str url: URL of Broker node in the Elasticsearch cluster
    :param str endpoint: Endpoint that Broker listens for queries on

    :Example:
    >>> from pandasticsearch import RestClient
    >>> client = RestClient('http://localhost:9200', 'index/type/_search')
    >>> result_dict = client.post("query":{"match_all":{}}})
    """

    def __init__(self, url, endpoint=''):
        self.url = url
        self.endpoint = endpoint

    def _prepare_url(self):
        if self.url.endswith('/'):
            url = self.url + self.endpoint
        else:
            url = self.url + '/' + self.endpoint
        return url

    def get(self, **kwargs):
        try:
            url = self._prepare_url()
            params = kwargs.get('params', None)

            if params is not None:
                url = '{0}?{1}'.format(url, urllib.parse.urlencode(params))

            req = urllib.request.Request(url=url)
            res = urllib.request.urlopen(req)
            data = res.read().decode("utf-8")
            res.close()
        except urllib.error.HTTPError:
            _, e, _ = sys.exc_info()
            reason = None
            if e.code != 200:
                try:
                    reason = json.loads(e.read().decode("utf-8"))
                except (ValueError, AttributeError, KeyError):
                    pass
                else:
                    reason = reason.get('error', None)

            raise ServerDefinedException(reason)
        else:
            return json.loads(data)

    def post(self, **kwargs):
        try:
            url = self._prepare_url()
            data = kwargs.get('data', None)
            params = kwargs.get('params', None)

            if params is not None:
                url = '{0}?{1}'.format(url, urllib.parse.urlencode(params))

            req = urllib.request.Request(url=url, data=json.dumps(data).encode('utf-8'),
                                         headers={'Content-Type': 'application/json'})
            res = urllib.request.urlopen(req)
            data = res.read().decode("utf-8")
            res.close()
        except urllib.error.HTTPError:
            _, e, _ = sys.exc_info()
            reason = None
            if e.code != 200:
                try:
                    reason = json.loads(e.read().decode("utf-8"))
                except (ValueError, AttributeError, KeyError):
                    pass
                else:
                    reason = reason.get('error', None)

            raise ServerDefinedException(reason)
        else:
            return json.loads(data)
