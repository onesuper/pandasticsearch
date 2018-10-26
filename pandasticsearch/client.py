# -*- coding: UTF-8 -*-

import json
import sys
import base64
import ssl
from six.moves import urllib

from pandasticsearch.errors import ServerDefinedException


class RestClient(object):
    """
    RestClient talks to Elasticsearch cluster through native RESTful API.
    """

    def __init__(self, url, endpoint='', username=None, password=None, verify_ssl=True):
        """
        Initialize the RESTful from the keyword arguments.

        :param str url: URL of Broker node in the Elasticsearch cluster
        :param str endpoint: Endpoint that Broker listens for queries on
        """
        self.url = url
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl

    def _prepare_url(self):
        if self.url.endswith('/'):
            url = self.url + self.endpoint
        else:
            url = self.url + '/' + self.endpoint
        return url

    def get(self, params=None):
        """
        Sends a GET request to Elasticsearch.

        :param optional params: Dictionary to be sent in the query string.
        :return: The response as a dictionary.

        >>> from pandasticsearch import RestClient
        >>> client = RestClient('http://localhost:9200', '_mapping/index')
        >>> print(client.get())
        """
        try:
            url = self._prepare_url()
            username = self.username
            password = self.password
            verify_ssl = self.verify_ssl

            if params is not None:
                url = '{0}?{1}'.format(url, urllib.parse.urlencode(params))

            req = urllib.request.Request(url=url)

            if username is not None and password is not None:
                base64creds = base64.b64encode('%s:%s' % (username,password))
                req.add_header("Authorization", "Basic %s" % base64creds)
            
            if verify_ssl is False:
                context = ssl._create_unverified_context()
                res = urllib.request.urlopen(req, context=context)
            else:
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

    def post(self, data, params=None):
        """
        Sends a POST request to Elasticsearch.

        :param data: The json data to send in the body of the request.
        :param optional params: Dictionary to be sent in the query string.
        :return: The response as a dictionary.

        >>> from pandasticsearch import RestClient
        >>> client = RestClient('http://localhost:9200', 'index/type/_search')
        >>> print(client.post(data={"query":{"match_all":{}}}))
        """
        try:
            url = self._prepare_url()
            username = self.username
            password = self.password
            verify_ssl = self.verify_ssl

            if params is not None:
                url = '{0}?{1}'.format(url, urllib.parse.urlencode(params))

            req = urllib.request.Request(url=url, data=json.dumps(data).encode('utf-8'),
                                         headers={'Content-Type': 'application/json'})

            if username is not None and password is not None:
                base64creds = base64.b64encode('%s:%s' % (username,password))
                req.add_header("Authorization", "Basic %s" % base64creds)
            
            if verify_ssl is False:
                context = ssl._create_unverified_context()
                res = urllib.request.urlopen(req, context=context)
            else:
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
