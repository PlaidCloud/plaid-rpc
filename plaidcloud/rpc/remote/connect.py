# coding=utf-8

from __future__ import absolute_import
from __future__ import print_function
import websocket
import ssl
import orjson as json
import threading
from time import sleep

from functools import partial
import logging
import warnings

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'

logging.basicConfig()

warnings.simplefilter('always', DeprecationWarning)
warnings.warn(
    'plaidtools.remote.connect uses a deprecated Websocket connection. Consider using \
            a plaidtools.connection.jsonrpc.SimpleRPC object instead',
    DeprecationWarning
)


def quick_connect(auth, callback_type, run, uri=None, verify_ssl=None,
                  proxy_url=None, proxy_user=None, proxy_password=None):
    """Connect to a websocket, run a callback, and then close the websocket."""

    if uri is None:
        uri = 'plaidcloud.com/socket'
    elif not uri.endswith('/socket'):
        uri = '{}/socket'.format(uri)

    if verify_ssl is None:
        if 'plaidcloud.com' in uri:
            verify_ssl = True
        else:
            verify_ssl = False

    connect_uri = 'wss://{}'.format(uri)

    try:
        headers = auth.get_package()
    except:
        raise Exception("Auth parameter must be an Auth object")

    headers['callback-type'] = str(callback_type)

    if proxy_url is not None:
        proxy_auth = '{}:{}'.format(proxy_user, proxy_password)
    else:
        proxy_auth = None

    if verify_ssl:
        sslopt = None
    else:
        sslopt = {
            "cert_reqs": ssl.CERT_NONE,
            "check_hostname": False
        }

    print('~~~~~~~~~~~ about to open websocket connection ~~~~~~~~~~~~~~')

    ws = websocket.create_connection(
        connect_uri,
        http_proxy_auth=proxy_auth,
        http_proxy_host=proxy_url,
        sslopt=sslopt,
        header=headers
    )

    print('~~~~~~~~~~~ connection opened. Getting response. ~~~~~~~~~~~~~~')

    opening_response = ws.recv()

    print('~~~~~~~~~~~ connection opened. Waiting for callback response ~~~~~~~~~~~~~~')
    rval = run(ws)
    print('~~~~~~~~~~~ cClosing the connection ~~~~~~~~~~~~~~')
    ws.close()

    return rval


def request(ws, msg, returnjson=True):
    """Sends a message as json, and returns the response."""
    send_as_json(ws, msg)
    if returnjson:
        return json.loads(ws.recv())
    else:
        return ws.recv()


def request_cb(msg, returnjson=True):
    """Generates a request function suitable for use as a callback in quick_connect"""
    return partial(request, msg=msg, returnjson=returnjson)


def requests(ws, msg_map, returnjson=True):
    """Sends a dict of messages as json, and returns a dict of responses. The
    output dict will have the same keys as the input dict."""
    return {
        key: request(ws, msg, returnjson=returnjson)
        for key, msg in msg_map.items()
    }


def requests_cb(msg_map, returnjson=True):
    """Generates a requests function suitable for use as a callback in quick_connect"""
    return partial(requests, msg_map=msg_map, returnjson=returnjson)


def send_as_json(ws, msg):
    """Packages up message elements and sends to remote connection socket"""
    message = json.dumps(msg)
    ws.send(message)


class Connect(object):

    def __init__(self, auth, callback_on_message, callback_on_error,
                 callback_on_close, callback_on_open, callback_type, uri=None,
                 verify_ssl=None, proxy_url=None, proxy_user=None,
                 proxy_password=None):
        """Initializes Connect object settings"""

        if uri is None:
            uri = "plaidcloud.com/socket"
        elif not uri.endswith('/socket'):
            uri = '{}/socket'.format(uri)

        if verify_ssl is None:
            if "plaidcloud.com" in uri:
                verify_ssl = True
            else:
                verify_ssl = False

        connect_uri = 'wss://{}'.format(uri)

        # Turn back on for debug mode
        # websocket.enableTrace(True)

        try:
            headers = auth.get_package()
        except:
            raise Exception("Auth parameter must be an Auth object")

        headers['callback-type'] = str(callback_type)

        self.ws = websocket.WebSocketApp(
            connect_uri,
            on_message=callback_on_message,
            on_error=callback_on_error,
            on_close=callback_on_close,
            header=headers
        )
        self.ws.on_open = callback_on_open

        if proxy_url is not None:
            proxy_auth = '{}:{}'.format(proxy_user, proxy_password)
        else:
            proxy_auth = None

        if verify_ssl:
            sslopt = None
        else:
            sslopt = {
                "cert_reqs": ssl.CERT_NONE,
                "check_hostname": False
            }

        rfk = {
            'ping_interval': 10,
            'http_proxy_host': proxy_url,
            'http_proxy_auth': proxy_auth,
            'sslopt': sslopt,
        }

        wst = None
        wst = threading.Thread(target=self.ws.run_forever, kwargs=rfk)
        wst.daemon = True
        wst.start()

        timeout = 5
        while self.ws.sock is None or (not self.ws.sock.connected and timeout):
            #print('connecting...')
            sleep(1)
            timeout -= 1

    def send(self, msg):
        """Packages up message elements and sends to remote connection socket"""

        send_as_json(self.ws, msg)

    def close(self):
        #self.wst.join()
        #self.wst = None

        if self.ws.sock.connected:
            self.ws.close()
