# coding=utf-8

from __future__ import absolute_import
from __future__ import print_function
import orjson as json
import warnings

from time import time

from plaidcloud.rpc.remote.abstract import Abstract

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'

DEFAULT_TIMEOUT = 300  # seconds


class JsonRpc(Abstract):

    def __init__(self, auth, uri=None, verify_ssl=None, proxy_url=None, proxy_user=None, proxy_password=None, timeout=DEFAULT_TIMEOUT):
        """Initializes Log object settings

        Examples:
            >>> from yaml import load
            >>> from plaidtools.remote.auth import oauth2_auth
            >>> config = load(open('/home/plaid/src/plaidtools/plaidtools/tests/test_config_example.yaml', 'r'))
            >>> rpc = JsonRpc(oauth2_auth(config['auth_token']), 'uri=https://localhost', verify_ssl=False)
            JsonRpc proxy object created
        """

        warnings.simplefilter('always', DeprecationWarning)
        warnings.warn(
            'plaidtools.remote.jsonrpc.JsonRpc uses a deprecated Websocket connection. Consider using \
                    a plaidtools.connection.jsonrpc.SimpleRPC object instead',
            DeprecationWarning
        )

        self.open_web_socket(
            auth=auth,
            callback_type=u'jsonrpc',
            uri=uri,
            verify_ssl=verify_ssl,
            proxy_url=proxy_url,
            proxy_user=proxy_user,
            proxy_password=proxy_password
        )

        self.id = 0
        self.timeout = timeout
        self.responses = {}

        print('JsonRpc proxy object created')

    def on_open(self, ws):
        pass

    def on_close(self, ws):
        ws.close()

    def on_message(self, ws, message):
        print(message)
        response = json.loads(message)

        if not isinstance(response, dict):
            print(message)
        else:
            id = response.get('id')

            if id is not None:
                self.responses[id] = response

    def on_error(self, ws, error):
        raise Exception(error)

    def set_timeout(self, timeout):
        self.timeout = timeout

    def _get_next_id(self):
        id = self.id + 1
        self.id = id

        return id

    def clear_response_queue(self):
        self.responses = {}

    def get_all_responses(self):
        return self.responses

    def get_timeout(self):
        return self.timeout

    def send(self, msg):
        id = self._get_next_id()

        msg['jsonrpc'] = '2.0'  # Must be this and nothing else
        msg['version'] = '1'  # PlaidCloud only has a version 1 at this time
        msg['id'] = id
        self.ws.send(msg)

        # Wait for the response to this request
        start_time = time()
        timeout = self.timeout

        # Block while waiting so this is like a regular RPC type operation
        # TODO see if there is a way to return something that the calling program can use without blocking here
        while True:
            if time() - start_time > timeout:
                break

            if id in self.responses:
                response = self.responses[id]
                del self.responses[id]

                return response

        raise Exception('Request Timed Out. Try increasing the timeout perhaps.')
