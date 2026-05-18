#!/usr/bin/env python
# coding=utf-8

import contextlib
import os
from unittest import mock

import pytest
import requests

from plaidcloud.rpc.connection import jsonrpc
from plaidcloud.rpc.connection.jsonrpc import (
    RPCRetry, SimpleRPC, http_json_rpc, STREAM_ENDPOINTS,
    _get_session, _get_shared_session, _rpc_context,
)
from plaidcloud.rpc.remote.rpc_common import RPCError, WARNING_CODE


@pytest.fixture(autouse=True)
def _reset_shared_session():
    """The module-level shared session is built once and cached. Reset it between tests
    so a mocked Session in one test doesn't leak into the next."""
    jsonrpc._shared_session = None
    _rpc_context.check_allow_transmit = None
    yield
    jsonrpc._shared_session = None
    _rpc_context.check_allow_transmit = None


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


def _patched_get_session(mock_session):
    """Build a contextmanager that yields the given mock session, for patching _get_session."""
    @contextlib.contextmanager
    def fake(*args, **kwargs):
        yield mock_session
    return fake


# -------------------------------------------------------------------- RPCRetry

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

    def test_allow_transmit_reads_thread_local_when_no_checker(self):
        """Shared-session RPCRetry has no per-instance checker; it must fall through to
        the thread-local set by http_json_rpc for the in-flight call."""
        retry = RPCRetry()
        _rpc_context.check_allow_transmit = lambda: False
        try:
            assert retry.allow_transmit is False
        finally:
            _rpc_context.check_allow_transmit = None

    def test_allow_transmit_instance_checker_wins_over_thread_local(self):
        retry = RPCRetry(check_allow_transmit=lambda: True)
        _rpc_context.check_allow_transmit = lambda: False
        try:
            assert retry.allow_transmit is True
        finally:
            _rpc_context.check_allow_transmit = None

    def test_new_preserves_check_allow_transmit(self):
        def checker():
            return True
        retry = RPCRetry(check_allow_transmit=checker)
        new_retry = retry.new()
        assert new_retry.allow_transmit is True

    def test_custom_connect_retries(self):
        retry = RPCRetry(connect=10)
        assert retry.connect == 10

    def test_default_connect_retries(self):
        retry = RPCRetry()
        assert retry.connect == 5


class TestRPCRetryIncrement:

    def test_increment_raises_when_transmit_blocked(self):
        retry = RPCRetry(check_allow_transmit=lambda: False)
        with pytest.raises(Exception, match='RPC method has been cancelled'):
            retry.increment()

    def test_increment_ok_when_allowed(self):
        retry = RPCRetry(check_allow_transmit=lambda: True, total=5)
        try:
            retry.increment()
        except Exception as e:
            assert 'cancelled' not in str(e)

    def test_increment_with_history_prints(self, capsys):
        retry = RPCRetry(check_allow_transmit=lambda: True, total=5)
        from urllib3.util.retry import RequestHistory
        retry.history = (RequestHistory('GET', 'https://e.com', None, 500, None),)
        try:
            retry.increment()
        except Exception:
            pass
        captured = capsys.readouterr()
        assert 'Hit Retry' in captured.out


# --------------------------------------------------------------- _get_session

class TestGetSession:
    """Direct tests for the session/adapter helper."""

    def test_fresh_session_default(self):
        with _get_session() as s:
            assert isinstance(s, requests.Session)
            assert not isinstance(s, jsonrpc.FuturesSession)
            # Adapter mounted with default retry=0
            assert s.get_adapter('https://x').max_retries.total == 0

    def test_fresh_session_custom_retry(self):
        retry = RPCRetry()
        with _get_session(retry_obj=retry) as s:
            assert s.get_adapter('https://x').max_retries is retry

    def test_fresh_session_is_closed_on_exit(self):
        captured = {}
        with _get_session() as s:
            captured['session'] = s
            captured['close'] = s.close = mock.Mock(wraps=s.close)
        captured['close'].assert_called_once()

    def test_fresh_session_closed_on_exception(self):
        captured = {}
        with pytest.raises(RuntimeError):
            with _get_session() as s:
                captured['close'] = s.close = mock.Mock(wraps=s.close)
                raise RuntimeError('boom')
        captured['close'].assert_called_once()

    def test_futures_session(self):
        from requests_futures.sessions import FuturesSession
        with _get_session(futures=True) as s:
            assert isinstance(s, FuturesSession)

    def test_shared_session_is_reused(self):
        with _get_session(shared=True) as s1:
            pass
        with _get_session(shared=True) as s2:
            pass
        assert s1 is s2

    def test_shared_session_not_closed_on_exit(self):
        with _get_session(shared=True) as s:
            s.close = mock.Mock(wraps=s.close)
        s.close.assert_not_called()

    def test_shared_session_uses_rpc_retry(self):
        with _get_session(shared=True) as s:
            adapter = s.get_adapter('https://x')
            assert isinstance(adapter.max_retries, RPCRetry)


# ------------------------------------------------------- _get_shared_session

class TestGetSharedSession:

    def test_first_call_builds_session(self):
        assert jsonrpc._shared_session is None
        s = _get_shared_session()
        assert s is not None
        assert isinstance(s, requests.Session)

    def test_subsequent_calls_return_same_instance(self):
        s1 = _get_shared_session()
        s2 = _get_shared_session()
        assert s1 is s2

    def test_adapter_has_pool_settings(self):
        s = _get_shared_session()
        adapter = s.get_adapter('https://x')
        # Default sizing — see DEFAULT_POOL_MAXSIZE / PLAIDCLOUD_RPC_POOL_MAXSIZE
        assert adapter._pool_connections == jsonrpc.DEFAULT_POOL_MAXSIZE
        assert adapter._pool_maxsize == jsonrpc.DEFAULT_POOL_MAXSIZE

    def test_pool_maxsize_env_var_override(self, monkeypatch):
        monkeypatch.setenv('PLAIDCLOUD_RPC_POOL_MAXSIZE', '64')
        s = _get_shared_session()
        adapter = s.get_adapter('https://x')
        assert adapter._pool_maxsize == 64
        assert adapter._pool_connections == 64

    def test_pool_maxsize_invalid_env_var_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv('PLAIDCLOUD_RPC_POOL_MAXSIZE', 'not-a-number')
        s = _get_shared_session()
        adapter = s.get_adapter('https://x')
        assert adapter._pool_maxsize == jsonrpc.DEFAULT_POOL_MAXSIZE

    def test_pool_maxsize_zero_clamped_to_one(self, monkeypatch):
        monkeypatch.setenv('PLAIDCLOUD_RPC_POOL_MAXSIZE', '0')
        s = _get_shared_session()
        adapter = s.get_adapter('https://x')
        assert adapter._pool_maxsize == 1


# ------------------------------------------------------------- SimpleRPC ctor

class TestSimpleRPC:

    def test_verify_ssl_property(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com', verify_ssl=True)
            assert rpc.verify_ssl is True

    def test_verify_ssl_false(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com', verify_ssl=False)
            assert rpc.verify_ssl is False

    def test_rpc_uri_property(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com/rpc')
            assert rpc.rpc_uri == 'https://example.com/rpc'

    def test_rpc_uri_setter(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com/rpc')
            rpc.rpc_uri = 'https://new.example.com/rpc'
            assert rpc.rpc_uri == 'https://new.example.com/rpc'

    def test_auth_token_string(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC('my_token', uri='https://example.com')
            assert rpc.auth_token == 'my_token'

    def test_auth_token_callable(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC(lambda: 'dynamic_token', uri='https://example.com')
            assert rpc.auth_token == 'dynamic_token'

    def test_auth_token_setter(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC('old_token', uri='https://example.com')
            rpc.auth_token = 'new_token'
            assert rpc.auth_token == 'new_token'

    def test_dot_access_creates_namespace(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc'):
            rpc = SimpleRPC('token', uri='https://example.com')
            ns = rpc.analyze
            assert ns is not None


# -------------------------------------------------------------- http_json_rpc
# All branches mock _get_session, which is the single seam for session creation
# after the refactor. This decouples tests from the underlying Session/Adapter
# wiring and exercises the routing logic in http_json_rpc.

class TestHttpJsonRpcFreshSession:
    """retry=False branch: fresh session, no retries."""

    def test_basic_call_returns_result(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True, 'result': 42})
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            result = http_json_rpc(
                token='tok', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'some/method', 'params': {}, 'jsonrpc': '2.0'},
                retry=False,
            )
        assert result == {'ok': True, 'result': 42}

    def test_token_sets_bearer_header(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True})
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            http_json_rpc(
                token='abc', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=False,
            )
        assert session.post.call_args[1]['headers']['Authorization'] == 'Bearer abc'

    def test_no_token_no_auth_header(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True})
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            http_json_rpc(
                token=None, uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=False,
            )
        assert 'Authorization' not in session.post.call_args[1]['headers']

    def test_exception_reraises(self):
        session = mock.MagicMock()
        session.post.side_effect = RuntimeError('network broke')
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            with pytest.raises(RuntimeError, match='network broke'):
                http_json_rpc(
                    token='t', uri='https://example.com/rpc', verify_ssl=True,
                    json_data={'method': 'm', 'params': {}}, retry=False,
                )

    def test_custom_headers_merged(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True})
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=False,
                headers={'X-Custom': 'yes'},
            )
        headers = session.post.call_args[1]['headers']
        assert headers['X-Custom'] == 'yes'
        assert headers['Content-Type'] == 'application/json'

    def test_routes_with_retry_obj_zero(self):
        """Verify the retry=False path requests a fresh session with retry_obj=0."""
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True})
        captured = {}

        @contextlib.contextmanager
        def fake(*args, **kwargs):
            captured.update(kwargs)
            yield session

        with mock.patch.object(jsonrpc, '_get_session', fake):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=False,
            )
        assert captured == {'retry_obj': 0}


class TestHttpJsonRpcSharedSession:
    """retry=True branch: shared session, RPCRetry honours per-call cancellation
    via the thread-local."""

    def test_returns_result(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True, 'result': 1})
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            result = http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=True,
            )
        assert result == {'ok': True, 'result': 1}

    def test_requests_shared_session(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True})
        captured = {}

        @contextlib.contextmanager
        def fake(*args, **kwargs):
            captured.update(kwargs)
            yield session

        with mock.patch.object(jsonrpc, '_get_session', fake):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=True,
            )
        assert captured == {'shared': True}

    def test_check_allow_transmit_set_on_thread_local_during_call(self):
        """The whole point of the shared-session+thread-local design: a single shared
        RPCRetry instance must see the per-call cancellation callback during the call,
        and the slot must be cleared afterwards."""
        seen = {}
        session = mock.MagicMock()

        def capture_during_post(*args, **kwargs):
            seen['during'] = getattr(_rpc_context, 'check_allow_transmit', None)
            return _mk_response({'ok': True})

        session.post.side_effect = capture_during_post

        def checker():
            return True

        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}}, retry=True,
                check_allow_transmit=checker,
            )
        assert seen['during'] is checker
        # Cleared after the call returns
        assert getattr(_rpc_context, 'check_allow_transmit', None) is None

    def test_thread_local_cleared_on_exception(self):
        session = mock.MagicMock()
        session.post.side_effect = RuntimeError('boom')
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            with pytest.raises(RuntimeError):
                http_json_rpc(
                    token='t', uri='https://example.com/rpc', verify_ssl=True,
                    json_data={'method': 'm', 'params': {}}, retry=True,
                    check_allow_transmit=lambda: True,
                )
        assert getattr(_rpc_context, 'check_allow_transmit', None) is None


class TestHttpJsonRpcFireAndForget:

    def test_uses_futures_session(self):
        session = mock.MagicMock()
        r_future = mock.MagicMock()
        session.post.return_value = r_future
        captured = {}

        @contextlib.contextmanager
        def fake(*args, **kwargs):
            captured.update(kwargs)
            yield session

        with mock.patch.object(jsonrpc, '_get_session', fake):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}},
                fire_and_forget=True, retry=False,
            )
        assert captured['futures'] is True
        assert captured['retry_obj'] == 0
        r_future.add_done_callback.assert_called_once()

    def test_retry_true_passes_rpc_retry_instance(self):
        session = mock.MagicMock()
        session.post.return_value = mock.MagicMock()
        captured = {}

        @contextlib.contextmanager
        def fake(*args, **kwargs):
            captured.update(kwargs)
            yield session

        def checker():
            return True

        with mock.patch.object(jsonrpc, '_get_session', fake):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}},
                fire_and_forget=True, retry=True,
                check_allow_transmit=checker,
            )
        assert captured['futures'] is True
        assert isinstance(captured['retry_obj'], RPCRetry)

    def test_callback_handles_exception(self):
        session = mock.MagicMock()
        r_future = mock.MagicMock()
        session.post.return_value = r_future
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': 'm', 'params': {}},
                fire_and_forget=True, retry=False,
            )
        callback = r_future.add_done_callback.call_args[0][0]
        fail_future = mock.MagicMock()
        failing_resp = mock.MagicMock()
        failing_resp.raise_for_status.side_effect = RuntimeError('HTTP 500')
        fail_future.result.return_value = failing_resp
        with pytest.raises(RuntimeError):
            callback(fail_future)


class TestHttpJsonRpcStreamable:

    def test_returns_tempfile_on_non_json(self):
        session = mock.MagicMock()
        stream_resp = _mk_response(content_chunks=[b'hello ', b'world'])
        stream_resp.json.side_effect = Exception('not json')
        session.post.return_value = stream_resp
        stream_method = next(iter(STREAM_ENDPOINTS))
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            result = http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': stream_method, 'params': {}}, retry=False,
            )
        assert isinstance(result, str)
        with open(result, 'rb') as fp:
            assert fp.read() == b'hello world'
        os.remove(result)

    def test_returns_json_if_present(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True, 'result': 'json'})
        stream_method = next(iter(STREAM_ENDPOINTS))
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            result = http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data={'method': stream_method, 'params': {}}, retry=False,
            )
        assert result == {'ok': True, 'result': 'json'}


# ---------------------------------------------------------- SimpleRPC call path

class TestSimpleRPCCallPath:

    def test_ok_result(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc',
                               return_value={'ok': True, 'result': 'data'}):
            rpc = SimpleRPC('token', uri='https://example.com')
            assert rpc.analyze.project.list() == 'data'

    def test_error_raises_rpc_error(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc',
                               return_value={'ok': False, 'error': {'message': 'bad', 'code': -1, 'data': None}}):
            rpc = SimpleRPC('token', uri='https://example.com')
            with pytest.raises(RPCError):
                rpc.analyze.project.list()

    def test_warning_code_raises_warning(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc',
                               return_value={'ok': False, 'error': {'message': 'soft', 'code': WARNING_CODE}}):
            rpc = SimpleRPC('token', uri='https://example.com')
            with pytest.raises(Warning):
                rpc.analyze.project.list()

    def test_string_response_returned(self):
        with mock.patch.object(jsonrpc, 'http_json_rpc',
                               return_value='/tmp/download_file'):
            rpc = SimpleRPC('token', uri='https://example.com')
            assert rpc.analyze.query.download_csv() == '/tmp/download_file'

    def test_callable_token_is_invoked(self):
        calls = {'n': 0}

        def token_provider():
            calls['n'] += 1
            return f'tok-{calls["n"]}'

        with mock.patch.object(jsonrpc, 'http_json_rpc',
                               return_value={'ok': True, 'result': 'x'}) as mock_http:
            rpc = SimpleRPC(token_provider, uri='https://example.com')
            rpc.analyze.method.call()
            assert mock_http.call_args[0][0] == 'tok-1'
