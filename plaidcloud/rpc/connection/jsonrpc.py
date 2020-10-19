# coding=utf-8

from __future__ import absolute_import
from __future__ import print_function

import os
import tempfile
import urllib3
import requests
from requests.adapters import HTTPAdapter
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry
import warnings
import orjson as json

from toolz.dicttoolz import assoc

from plaidcloud.rpc.remote.json_rpc_handler import unsupported_object_json_encoder
from plaidcloud.rpc.remote.rpc_tools import PlainRPCCommon
from plaidcloud.rpc.remote.rpc_common import RPCError, WARNING_CODE

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__author__ = 'Paul Morel'
__credits__ = ['Paul Morel', 'Adams Tower']
__maintainer__ = 'Adams Tower <adams.tower@tartansolutions.com>'
__copyright__ = '© Copyright 2019, Tartan Solutions, Inc'
__license__ = 'Proprietary'

STREAM_ENDPOINTS = {
    'analyze/query/download_csv',
    'analyze/query/download_dataframe',
}

download_folder = os.path.join(tempfile.gettempdir(), "plaid/download")

if not os.path.exists(download_folder):
    os.makedirs(download_folder)


def get(token, uri=None, verify_ssl=None, proxy_url=None, proxy_user=None, proxy_password=None):
    """Convenience Wrapper for JSON-RPC Connection"""

    warnings.simplefilter('always', DeprecationWarning)
    warnings.warn(
        'plaidtools.connection.jsonrpc.get uses a deprecated Websocket connection. Consider using \
                a plaidtools.connection.jsonrpc.SimpleRPC object instead',
        DeprecationWarning
    )

    from plaidtools.remote.jsonrpc import JsonRpc
    from plaidtools.remote.auth import oauth2_auth

    auth_object = oauth2_auth(token)

    return JsonRpc(
        auth=auth_object,
        uri=uri,
        verify_ssl=verify_ssl,
        proxy_url=proxy_url,
        proxy_user=proxy_user,
        proxy_password=proxy_password,
    )


def http_json_rpc(token=None, uri=None, verify_ssl=None, json_data=None, workspace=None, proxies=None, fire_and_forget=False):
    """
    Sends a json_rpc request over http.

    Returns:
        dict: The decoded response from the server.
    Args:
        token (str): oauth2 token
        uri (str): the server uri to connect to
        verify_ssl (bool): passed to requests. flag to check the server's certs, or not.
        json_data (json-encodable object): the payload to send
        workspace (int): workspace to connect to. If None, let the server connect to the default workspace for your user or token
        proxies (dict): Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
        fire_and_forget (bool,optional): return from the method after the request is sent (not wait for response)
    """
    def auth_header():
        if workspace:
            return "Bearer_{}_ws{}".format(token, workspace)
        else:
            return "Bearer_{}".format(token)

    def streamable():
        if json_data and json_data.get('method') in STREAM_ENDPOINTS:
            return True
        return False

    if token:
        headers = {
            "Authorization": auth_header(),
            "Content-Type": "application/json",
        }
    else:
        headers = {
            "Content-Type": "application/json",
        }
    payload = json.dumps(assoc(json_data, 'id', 0), default=unsupported_object_json_encoder, option=json.OPT_NAIVE_UTC | json.OPT_NON_STR_KEYS)

    def get_session():
        if fire_and_forget:
            return FuturesSession()
        return requests.sessions.Session()

    with get_session() as session:
        retry = Retry(connect=5, backoff_factor=0.5, status_forcelist=[500, 502, 504], method_whitelist=['POST'])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        if streamable():
            handle, file_name = tempfile.mkstemp(dir=download_folder, prefix="download_", suffix=".tmp")
            os.close(handle)  # Can't control the access mode, so close this one and open another.
            with open(file_name, 'wb', buffering=1) as tmp_file:
                with session.post(uri, headers=headers, data=payload, verify=verify_ssl, proxies=proxies,
                                  allow_redirects=False, stream=True) as data:
                    for chunk in data.iter_content(chunk_size=None):
                        tmp_file.write(chunk)
            return file_name
        elif fire_and_forget:
            r_future = session.post(uri, headers=headers, data=payload, verify=verify_ssl, proxies=proxies,
                                    allow_redirects=False)

            # Adding a callback that will raise an exception if there was a problem with the request
            def on_request_complete(request_future):
                try:
                    response = request_future.result()
                    response.raise_for_status()
                except:
                    print(f'Exception for method {json_data.get("method")}')
                    raise

            r_future.add_done_callback(on_request_complete)
        else:
            try:
                response = session.post(uri, headers=headers, data=payload, verify=verify_ssl, proxies=proxies,
                                        allow_redirects=False)
                response.raise_for_status()
                result = response.json()
                return result
            except Exception as e:
                print(f'Exception for method {json_data.get("method")}')
                raise


class SimpleRPC(PlainRPCCommon):
    """Call remote rpc methods with a dot based interface, almost as if they
    were simply functions in modules.

    Example:
    rpc = SimpleRPC(token, uri=uri, verify_ssl=verify_ssl, workspace=workspace)
    scopes = rpc.identity.me.scopes()
    """
    def __init__(self, token, uri=None, verify_ssl=None, workspace=None, proxies=None, check_allow_transmit=None):
        verify_ssl = bool(verify_ssl)

        def call_rpc(method_path, params, fire_and_forget=False):
            response = http_json_rpc(
                token, uri, verify_ssl,
                {
                    'jsonrpc': '2.0',
                    'method': method_path,
                    'params': params,
                },
                workspace=workspace,
                proxies=proxies,
                fire_and_forget=fire_and_forget,
            )
            if response:
                if isinstance(response, str):
                    return response
                if response.get('ok'):
                    return response['result']
                else:
                    error = response['error']
                    if error.get('code') == WARNING_CODE:
                        raise Warning(error.get('message'))
                    else:
                        raise RPCError(
                            error['message'],
                            data=error.get('data'),
                            code=error.get('code'),
                        )

        super(SimpleRPC, self).__init__(call_rpc, check_allow_transmit)
