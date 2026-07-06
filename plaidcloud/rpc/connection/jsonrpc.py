# coding=utf-8

import contextlib
import os
import tempfile
import threading

import urllib3
import urllib3.exceptions
import requests
from requests.adapters import HTTPAdapter
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry
import orjson as json
from packaging import version
from urllib.parse import urljoin

from toolz.dicttoolz import assoc

from plaidcloud.rpc.orjson import unsupported_object_json_encoder
from plaidcloud.rpc.remote.rpc_tools import PlainRPCCommon
from plaidcloud.rpc.remote.rpc_common import RPCError, WARNING_CODE
from plaidcloud.rpc.telemetry import inject_trace_context

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__author__ = 'Paul Morel'
__credits__ = ['Paul Morel', 'Adams Tower']
__maintainer__ = 'Adams Tower <adams.tower@tartansolutions.com>'
__copyright__ = '© Copyright 2019-2023, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'

STREAM_ENDPOINTS = {
    'analyze/query/download_csv',
    'analyze/query/download_dataframe',
    'document/view/download_stream',
}

download_folder = os.path.join(tempfile.gettempdir(), "plaid/download")

if not os.path.exists(download_folder):  # pragma: no cover
    os.makedirs(download_folder)


_rpc_context = threading.local()
_shared_session = None

# urllib3 keeps a fixed-size connection pool per host; once it's full, extra
# concurrent requests open a connection that urllib3 then *discards* on return
# (logging "Connection pool is full"), forcing a fresh TCP+TLS handshake — the
# exact cost the shared session exists to avoid. SimpleRPC is called from worker
# pools that routinely exceed 10 in-flight requests, so the default needs to be
# high enough to cover them. Override via PLAIDCLOUD_RPC_POOL_MAXSIZE.
DEFAULT_POOL_MAXSIZE = 32


def _pool_maxsize():
    try:
        return max(1, int(os.environ.get('PLAIDCLOUD_RPC_POOL_MAXSIZE', DEFAULT_POOL_MAXSIZE)))
    except (TypeError, ValueError):
        return DEFAULT_POOL_MAXSIZE


def _get_shared_session():
    """Lazily build a module-level requests.Session for standard (non-streaming, non-fire-and-forget)
    RPC calls. Reusing one session amortises TCP+TLS handshakes across calls.

    The mounted adapter holds a single RPCRetry that reads check_allow_transmit from the
    thread-local _rpc_context, so per-call cancellation still works on the shared session.

    Pool size is controlled by PLAIDCLOUD_RPC_POOL_MAXSIZE (default 32). If concurrent
    in-flight RPCs exceed pool_maxsize, urllib3 logs "Connection pool is full" and
    discards the surplus connection on return, which defeats the shared-session win.
    """
    global _shared_session
    if _shared_session is None:
        pool_size = _pool_maxsize()
        session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=RPCRetry(),
            pool_connections=pool_size,
            pool_maxsize=pool_size,
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        _shared_session = session
    return _shared_session


@contextlib.contextmanager
def _get_session(*, shared=False, futures=False, retry_obj=0):
    """Yield a requests Session configured for an RPC call.

    - shared=True: yield the lazily-built module-level Session. `futures` and `retry_obj`
      are ignored; the shared session has its own RPCRetry that honours per-call
      cancellation via the thread-local. NOT closed on exit — it is reused across calls.
    - futures=True: yield a fresh FuturesSession with `retry_obj` on its adapter.
    - default: yield a fresh requests.Session with `retry_obj` on its adapter.

    Fresh sessions are closed on context exit.
    """
    if shared:
        yield _get_shared_session()
        return
    session = FuturesSession() if futures else requests.Session()
    try:
        adapter = HTTPAdapter(max_retries=retry_obj)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        yield session
    finally:
        session.close()


def http_json_rpc(token=None, uri=None, verify_ssl=None, json_data=None, proxies=None,
                  fire_and_forget=False, check_allow_transmit=None, retry=True, headers=None):
    """
    Sends a json_rpc request over http.

    Returns:
        dict: The decoded response from the server.
    Args:
        token (str): oauth2 token
        uri (str): the server uri to connect to
        verify_ssl (bool): passed to requests. flag to check the server's certs, or not.
        json_data (json-encodable object): the payload to send
        proxies (dict): Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
        fire_and_forget (bool,optional): return from the method after the request is sent (not wait for response)
        check_allow_transmit (callable, optional): For use in retry, callable method to see if retries are still valid to send
        retry (bool, optional): Whether or not to use retry at all, default True
        headers (dict, optional): Custom headers to send with the RPC
    """
    def auth_header():
        return "Bearer {}".format(token)

    def streamable():
        if json_data and json_data.get('method') in STREAM_ENDPOINTS:
            return True
        return False

    headers = headers or {}
    headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = auth_header()

    # Inject W3C trace context so the RPC server can stitch this hop into the caller's
    # trace. Runs in the calling coroutine before any FuturesSession/thread handoff, so it
    # covers both sync and fire_and_forget paths. No-op when no tracer provider/active span.
    inject_trace_context(headers)

    payload = json.dumps(assoc(json_data, 'id', 0), default=unsupported_object_json_encoder, option=json.OPT_NAIVE_UTC | json.OPT_NON_STR_KEYS)

    if streamable():
        with _get_session(retry_obj=0) as session:
            handle, file_name = tempfile.mkstemp(dir=download_folder, prefix="download_", suffix=".tmp")
            os.close(handle)  # Can't control the access mode, so close this one and open another.
            with open(file_name, 'wb') as tmp_file:
                with session.post(uri, headers=headers, data=payload, verify=verify_ssl, proxies=proxies,
                                  allow_redirects=False, stream=True) as response:
                    response.raise_for_status()
                    try:
                        result = response.json()
                        if isinstance(result, dict):
                            return result
                    except Exception:  # JSONDecodeError: Should be this, but which library? json or simplejson - depends what is installed
                        pass
                    for chunk in response.iter_content(chunk_size=None):
                        tmp_file.write(chunk)
            return file_name
    elif fire_and_forget:
        retry_obj = RPCRetry(check_allow_transmit=check_allow_transmit) if retry else 0
        with _get_session(futures=True, retry_obj=retry_obj) as session:
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
    elif not retry:
        # Caller wants no retries. Fresh session keeps the shared session's retry config from leaking in.
        with _get_session(retry_obj=0) as session:
            try:
                response = session.post(uri, headers=headers, data=payload, verify=verify_ssl,
                                        proxies=proxies, allow_redirects=False)
                response.raise_for_status()
                return response.json()
            except Exception:
                print(f'Exception for method {json_data.get("method")}')
                raise
    else:
        with _get_session(shared=True) as session:
            _rpc_context.check_allow_transmit = check_allow_transmit
            try:
                response = session.post(uri, headers=headers, data=payload, verify=verify_ssl,
                                        proxies=proxies, allow_redirects=False)
                response.raise_for_status()
                return response.json()
            except Exception:
                print(f'Exception for method {json_data.get("method")}')
                raise
            finally:
                _rpc_context.check_allow_transmit = None


class RPCRetry(Retry):
    def __init__(self, *args, check_allow_transmit=None, **kwargs):
        """
        Args:
            check_allow_transmit (callable, optional): Method to call to check if retries are still to be made
                This can be used to prevent retry of RPC methods once a workflow has been cancelled and the RPC fails
        """
        kwargs.update(dict(
            allowed_methods=['POST'],
            status_forcelist=[500, 502, 504],
            backoff_factor=0.1,
        ))
        if 'connect' not in kwargs:
            kwargs['connect'] = 5
        super(RPCRetry, self).__init__(*args, **kwargs)
        self.__check_allow_transmit = check_allow_transmit

    def new(self, **kw):
        kw.update(dict(check_allow_transmit=self.__check_allow_transmit))
        return super(RPCRetry, self).new(**kw)

    @property
    def allow_transmit(self):
        # Instance-level callback (passed at construction) takes precedence; otherwise fall
        # through to the per-call callback set on the thread-local by http_json_rpc, so a
        # shared RPCRetry on the module-level session can still honour cancellation.
        if self.__check_allow_transmit:
            return self.__check_allow_transmit()
        check = getattr(_rpc_context, 'check_allow_transmit', None)
        if check:
            return check()
        return True

    def increment(self, *args, **kwargs):
        if self.history:
            print(f'Hit Retry, Request History Looks Like: {self.history}')
        if not self.allow_transmit:
            raise Exception('No more retries, RPC method has been cancelled')
        return super(RPCRetry, self).increment(*args, **kwargs)


class SimpleRPC(PlainRPCCommon):
    """Call remote rpc methods with a dot based interface, almost as if they
    were simply functions in modules.

    Example:
    rpc = SimpleRPC(token, uri=uri, verify_ssl=verify_ssl, workspace=workspace)
    scopes = rpc.identity.me.scopes()
    """
    def __init__(self, token, uri=None, verify_ssl=None, workspace=None, proxies=None, check_allow_transmit=None,
                 retry=True, headers=None):
        verify_ssl = bool(verify_ssl)
        self.__rpc_uri = uri
        self.__verify_ssl = verify_ssl
        self.__auth_token = token

        def call_rpc(method_path, params, fire_and_forget=False):
            if callable(token):
                http_token = token()
            else:
                http_token = token
            response = http_json_rpc(
                http_token, urljoin(uri, method_path), verify_ssl,
                {
                    'jsonrpc': '2.0',
                    'method': method_path,
                    'params': params,
                },
                proxies=proxies,
                fire_and_forget=fire_and_forget,
                check_allow_transmit=check_allow_transmit,
                retry=retry,
                headers=headers,
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

    @property
    def verify_ssl(self):
        return self.__verify_ssl

    @property
    def rpc_uri(self):
        return self.__rpc_uri

    @rpc_uri.setter
    def rpc_uri(self, uri: str):
        self.__rpc_uri = uri

    @property
    def auth_token(self):
        if callable(self.__auth_token):
            return self.__auth_token()
        return self.__auth_token

    @auth_token.setter
    def auth_token(self, token: str):
        self.__auth_token = token
