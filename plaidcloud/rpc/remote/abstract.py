# coding=utf-8

from __future__ import absolute_import
from six.moves.urllib.parse import urlparse
from plaidcloud.rpc.remote.connect import Connect

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'


class Abstract(object):

    def __init__(self, auth, uri=None, verify_ssl=None, proxy_url=None,
                 proxy_user=None, proxy_password=None):
        """Initializes Handle object settings"""
        self._proxy_url = None
        self._proxy_auth = None
        raise NotImplementedError()

    def open_web_socket(self, auth, callback_type, uri=None,
                        on_open=None, verify_ssl=None, proxy_url=None,
                        proxy_user=None, proxy_password=None):

        if not on_open:
            on_open = self.on_open

        if not uri.endswith('/socket'):
            uri = '{}/socket'.format(uri)

        # Open a websocket connection
        self.ws = Connect(
            auth=auth,
            callback_on_message=self.on_message,
            callback_on_error=self.on_error,
            callback_on_close=self.on_close,
            callback_on_open=on_open,
            callback_type=callback_type,
            uri=uri,
            verify_ssl=verify_ssl,
            proxy_url=proxy_url,
            proxy_user=proxy_user,
            proxy_password=proxy_password,
        )

    def _get_proxy_settings(self):
        parsed_uri = urlparse(self._proxy_url)

        proxy = ("{proxy_scheme}://{proxy_auth}@{proxy_url}").format(
            proxy_scheme=parsed_uri.scheme,
            proxy_auth=self._proxy_auth,
            proxy_url='{}{}'.format(parsed_uri.netloc, parsed_uri.path),
        )

        proxy_settings = {"https": proxy, "http": proxy}

        return proxy_settings

    def on_open(self, ws):
        raise NotImplementedError()

    def on_close(self, ws):
        ws.keep_running = False

    def on_message(self, ws, message):
        pass

    def on_error(self, ws, error):
        ws.close()
        raise error

    def send(self, msg):
        self.ws.send(msg)

    def close(self):
        self.ws.close()
