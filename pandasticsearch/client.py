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

    def __init__(self, host, username=None, password=None, verify_ssl=True):
        """
        Initialize the RESTful from the keyword arguments.

        :param str host: Host URL of Broker node in the Elasticsearch cluster
        """
        self.host = host
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl

    def _prepare_url(self, path):
        if self.host.endswith('/'):
            url = self.host + path
        else:
            if path.startswith('/'):
                url = self.host + path
            else:
                url = self.host + '/' + path
        return url

    def get(self, path, params=None):
        """
        Sends a GET request to Elasticsearch.

        :param path: path: path of the verb and resource, e.g. /index
        :param optional params: Dictionary to be sent in the query string.
        :return: The response as a dictionary.

        >>> from pandasticsearch import RestClient
        >>> client = RestClient('http://localhost:9200')
        >>> print(client.get())
        """
        try:
            url = self._prepare_url(path)
            username = self.username
            password = self.password
            verify_ssl = self.verify_ssl

            if params is not None:
                url = '{0}?{1}'.format(url, urllib.parse.urlencode(params))

            req = urllib.request.Request(url=url)

            if username is not None and password is not None:
                s = '%s:%s' % (username, password)
                base64creds = base64.b64encode(s.encode('utf-8')).decode('utf-8')
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

    def post(self, path, data, params=None):
        """
        Sends a POST request to Elasticsearch.

        :param path: The path for the verb and resource
        :param data: The json data to send in the body of the request.
        :param optional params: Dictionary to be sent in the query string.
        :return: The response as a dictionary.

        >>> from pandasticsearch import RestClient
        >>> client = RestClient('http://localhost:9200')
        >>> print(client.post(path='/index/_search', data={"query":{"match_all":{}}}))
        """
        try:
            url = self._prepare_url(path)
            username = self.username
            password = self.password
            verify_ssl = self.verify_ssl

            if params is not None:
                url = '{0}?{1}'.format(url, urllib.parse.urlencode(params))

            req = urllib.request.Request(url=url, data=json.dumps(data).encode('utf-8'),
                                         headers={'Content-Type': 'application/json'})

            if username is not None and password is not None:
                s = '%s:%s' % (username, password)
                base64creds = base64.b64encode(s.encode('utf-8')).decode('utf-8')
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
