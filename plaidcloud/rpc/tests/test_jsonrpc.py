#!/usr/bin/env python
# coding=utf-8

import os
import tempfile
from unittest import mock

import pytest

from plaidcloud.rpc.connection.jsonrpc import (
    RPCRetry, SimpleRPC, http_json_rpc, STREAM_ENDPOINTS,
)
from plaidcloud.rpc.remote.rpc_common import RPCError, WARNING_CODE


class TestRPCRetry:

    def test_default_initialization(self):
        retry = RPCRetry()
        assert 'POST' in retry.allowed_methods
        assert 500 in retry.status_forcelist
        assert 502 in retry.status_forcelist
        assert 504 in retry.status_forcelist

    def test_allow_transmit_default_true(self):
        retry = RPCRetry()
        assert retry.allow_transmit is True

    def test_allow_transmit_with_checker_true(self):
        retry = RPCRetry(check_allow_transmit=lambda: True)
        assert retry.allow_transmit is True

    def test_allow_transmit_with_checker_false(self):
        retry = RPCRetry(check_allow_transmit=lambda: False)
        assert retry.allow_transmit is False

    def test_new_preserves_check_allow_transmit(self):
        checker = lambda: True
        retry = RPCRetry(check_allow_transmit=checker)
        new_retry = retry.new()
        assert new_retry.allow_transmit is True

    def test_custom_connect_retries(self):
        retry = RPCRetry(connect=10)
        assert retry.connect == 10

    def test_default_connect_retries(self):
        retry = RPCRetry()
        assert retry.connect == 5


class TestSimpleRPC:

    def test_verify_ssl_property(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com', verify_ssl=True)
            assert rpc.verify_ssl is True

    def test_verify_ssl_false(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com', verify_ssl=False)
            assert rpc.verify_ssl is False

    def test_rpc_uri_property(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com/rpc')
            assert rpc.rpc_uri == 'https://example.com/rpc'

    def test_rpc_uri_setter(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com/rpc')
            rpc.rpc_uri = 'https://new.example.com/rpc'
            assert rpc.rpc_uri == 'https://new.example.com/rpc'

    def test_auth_token_string(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC('my_token', uri='https://example.com')
            assert rpc.auth_token == 'my_token'

    def test_auth_token_callable(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC(lambda: 'dynamic_token', uri='https://example.com')
            assert rpc.auth_token == 'dynamic_token'

    def test_auth_token_setter(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC('old_token', uri='https://example.com')
            rpc.auth_token = 'new_token'
            assert rpc.auth_token == 'new_token'

    def test_dot_access_creates_namespace(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com')
            ns = rpc.analyze
            assert ns is not None


def _mk_response(json_body=None, status_code=200, content_chunks=None):
    """Helper building a fake requests.Response-like mock."""
    resp = mock.MagicMock()
    resp.status_code = status_code
    resp.raise_for_status = mock.Mock()
    if json_body is not None:
        resp.json = mock.Mock(return_value=json_body)
    if content_chunks is not None:
        resp.iter_content = mock.Mock(return_value=iter(content_chunks))
    resp.__enter__ = mock.Mock(return_value=resp)
    resp.__exit__ = mock.Mock(return_value=False)
    return resp


class TestHttpJsonRpc:

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_basic_call_returns_result(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = _mk_response({'ok': True, 'result': 42})
        mock_session_cls.return_value = mock_session

        result = http_json_rpc(
            token='tok', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': 'some/method', 'params': {}, 'jsonrpc': '2.0'},
            retry=False,
        )
        assert result == {'ok': True, 'result': 42}

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_token_sets_bearer_header(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = _mk_response({'ok': True})
        mock_session_cls.return_value = mock_session

        http_json_rpc(
            token='abc', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': 'm', 'params': {}}, retry=False,
        )
        headers = mock_session.post.call_args[1]['headers']
        assert headers['Authorization'] == 'Bearer abc'

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_no_token_no_auth_header(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = _mk_response({'ok': True})
        mock_session_cls.return_value = mock_session

        http_json_rpc(
            token=None, uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': 'm', 'params': {}}, retry=False,
        )
        headers = mock_session.post.call_args[1]['headers']
        assert 'Authorization' not in headers

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_exception_reraises(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.side_effect = RuntimeError('network broke')
        mock_session_cls.return_value = mock_session

        with pytest.raises(RuntimeError, match='network broke'):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=False,
            )

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.FuturesSession')
    def test_fire_and_forget_uses_futures(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        r_future = mock.MagicMock()
        mock_session.post.return_value = r_future
        mock_session_cls.return_value = mock_session

        http_json_rpc(
            token='t', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': 'm', 'params': {}},
            fire_and_forget=True, retry=False,
        )
        r_future.add_done_callback.assert_called_once()

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_streamable_returns_tempfile_on_non_json(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        # Non-JSON stream response
        stream_resp = _mk_response(content_chunks=[b'hello ', b'world'])
        stream_resp.json.side_effect = Exception('not json')
        mock_session.post.return_value = stream_resp
        mock_session_cls.return_value = mock_session

        stream_method = next(iter(STREAM_ENDPOINTS))
        result = http_json_rpc(
            token='t', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': stream_method, 'params': {}}, retry=False,
        )
        assert isinstance(result, str)
        with open(result, 'rb') as fp:
            contents = fp.read()
        assert contents == b'hello world'
        os.remove(result)

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_streamable_returns_json_if_present(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        stream_resp = _mk_response(json_body={'ok': True, 'result': 'json'})
        mock_session.post.return_value = stream_resp
        mock_session_cls.return_value = mock_session

        stream_method = next(iter(STREAM_ENDPOINTS))
        result = http_json_rpc(
            token='t', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': stream_method, 'params': {}}, retry=False,
        )
        assert result == {'ok': True, 'result': 'json'}

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_custom_headers_merged(self, mock_session_cls):
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = _mk_response({'ok': True})
        mock_session_cls.return_value = mock_session

        http_json_rpc(
            token='t', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': 'm', 'params': {}}, retry=False,
            headers={'X-Custom': 'yes'},
        )
        headers = mock_session.post.call_args[1]['headers']
        assert headers['X-Custom'] == 'yes'
        assert headers['Content-Type'] == 'application/json'


class TestSimpleRPCCallPath:

    def test_ok_result(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc',
                        return_value={'ok': True, 'result': 'data'}):
            rpc = SimpleRPC('token', uri='https://example.com')
            result = rpc.analyze.project.list()
            assert result == 'data'

    def test_error_raises_rpc_error(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc',
                        return_value={'ok': False, 'error': {'message': 'bad', 'code': -1, 'data': None}}):
            rpc = SimpleRPC('token', uri='https://example.com')
            with pytest.raises(RPCError):
                rpc.analyze.project.list()

    def test_warning_code_raises_warning(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc',
                        return_value={'ok': False, 'error': {'message': 'soft', 'code': WARNING_CODE}}):
            rpc = SimpleRPC('token', uri='https://example.com')
            with pytest.raises(Warning):
                rpc.analyze.project.list()

    def test_string_response_returned(self):
        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc',
                        return_value='/tmp/download_file'):
            rpc = SimpleRPC('token', uri='https://example.com')
            result = rpc.analyze.query.download_csv()
            assert result == '/tmp/download_file'

    def test_callable_token_is_invoked(self):
        calls = {'n': 0}

        def token_provider():
            calls['n'] += 1
            return f'tok-{calls["n"]}'

        with mock.patch('plaidcloud.rpc.connection.jsonrpc.http_json_rpc',
                        return_value={'ok': True, 'result': 'x'}) as mock_http:
            rpc = SimpleRPC(token_provider, uri='https://example.com')
            rpc.analyze.method.call()
            assert mock_http.call_args[0][0] == 'tok-1'


class TestRPCRetryIncrement:

    def test_increment_raises_when_transmit_blocked(self):
        retry = RPCRetry(check_allow_transmit=lambda: False)
        with pytest.raises(Exception, match='RPC method has been cancelled'):
            retry.increment()

    def test_increment_ok_when_allowed(self):
        # allow_transmit True, call increment() directly — it increments state
        retry = RPCRetry(check_allow_transmit=lambda: True, total=5)
        # Will raise MaxRetryError or similar, but should NOT raise the cancelled message
        try:
            retry.increment()
        except Exception as e:
            assert 'cancelled' not in str(e)

    def test_increment_with_history_prints(self, capsys):
        """Covers the `if self.history: print(...)` branch."""
        retry = RPCRetry(check_allow_transmit=lambda: True, total=5)
        # Populate history
        from urllib3.util.retry import RequestHistory
        retry.history = (RequestHistory('GET', 'https://e.com', None, 500, None),)
        try:
            retry.increment()
        except Exception:
            pass
        captured = capsys.readouterr()
        assert 'Hit Retry' in captured.out


class TestHttpJsonRpcRetryTrue:

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.requests.sessions.Session')
    def test_http_json_rpc_with_retry_true(self, mock_session_cls):
        """Covers the retry=True branch that builds an RPCRetry object."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = _mk_response({'ok': True, 'result': 1})
        mock_session_cls.return_value = mock_session

        result = http_json_rpc(
            token='t', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': 'm', 'params': {}}, retry=True,
        )
        assert result == {'ok': True, 'result': 1}


class TestHttpJsonRpcFireAndForgetException:

    @mock.patch('plaidcloud.rpc.connection.jsonrpc.FuturesSession')
    def test_fire_and_forget_callback_handles_exception(self, mock_session_cls):
        """Test the on_request_complete callback inside fire_and_forget."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        r_future = mock.MagicMock()
        mock_session.post.return_value = r_future
        mock_session_cls.return_value = mock_session

        http_json_rpc(
            token='t', uri='https://example.com/rpc', verify_ssl=True,
            json_data={'method': 'm', 'params': {}},
            fire_and_forget=True, retry=False,
        )
        # Simulate the callback being called with a failed future
        assert r_future.add_done_callback.called
        callback = r_future.add_done_callback.call_args[0][0]

        fail_future = mock.MagicMock()
        failing_resp = mock.MagicMock()
        failing_resp.raise_for_status.side_effect = RuntimeError('HTTP 500')
        fail_future.result.return_value = failing_resp

        with pytest.raises(RuntimeError):
            callback(fail_future)
