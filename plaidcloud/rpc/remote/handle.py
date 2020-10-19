# coding=utf-8

from __future__ import absolute_import
from __future__ import print_function
import warnings
from plaidcloud.rpc.remote.abstract import Abstract
from plaidcloud.rpc.remote.connect import quick_connect, send_as_json

__author__ = 'Adams Tower'
__maintainer__ = 'Adams Tower <adams.tower@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'

warnings.simplefilter('always', DeprecationWarning)
warnings.warn(
    'plaidtools.remote.handle uses a deprecated Websocket connection. Consider using \
            a plaidtools.connection.jsonrpc.SimpleRPC object instead',
    DeprecationWarning
)


def quick_request(auth, cloud, method, resource, data=None, action=None, uri=None, verify_ssl=None):
    def request(ws):
        send_as_json(ws, {
            'method': method,
            'resource': resource,
            'cloud': cloud,
            'data': data,
            'action': action,
        })
        return ws.recv()

    return quick_connect(
        auth=auth,
        callback_type=u'handle',
        run=request,
        uri=uri,
        verify_ssl=verify_ssl,
    )


class Handle(Abstract):
    # ADT2017: I'm kind of skeptical this will be useful, since handles are
    # http/rest based, but maybe if you had to do a ton of them.

    def __init__(self, auth, uri=None, verify_ssl=None,
                 proxy_url=None, proxy_user=None, proxy_password=None):
        """Initializes Handle object settings
            >>> from yaml import load
            >>> from plaidtools.remote.auth import oauth2_auth
            >>> config = load(open('/home/plaid/src/plaidtools/plaidtools/tests/test_config_example.yaml', 'r'))
            >>> handle = Handle(oauth2_auth(config['auth_token']), 'uri=https://localhost', verify_ssl=False)
            Handle proxy object created.
        """

        self.open_web_socket(
            auth=auth,
            callback_type=u'handle',
            uri=uri,
            verify_ssl=verify_ssl,
            proxy_url=proxy_url,
            proxy_user=proxy_user,
            proxy_password=proxy_password
        )

        print('Handle proxy object created.')

    def on_open(self, ws):
        raise NotImplementedError()

    def on_close(self, ws):
        raise NotImplementedError()

    def on_message(self, ws, message):
        raise NotImplementedError()

    def on_error(self, ws, error):
        raise NotImplementedError()
