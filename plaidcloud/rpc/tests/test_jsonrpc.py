#!/usr/bin/env python
# coding=utf-8

import contextlib
import io
import json
import os
import socket
import ssl
import sys
import threading
import time
from unittest import mock

import pytest
import requests
import trustme
import urllib3

from plaidcloud.rpc.connection import jsonrpc
from plaidcloud.rpc.connection.jsonrpc import (
    DownloadCancel, RPCRetry, SimpleRPC, cancellable_downloads, http_json_rpc, http_rest, STREAM_ENDPOINTS,
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


def _mk_response(json_body=None, status_code=200):
    """Helper building a fake requests.Response-like mock."""
    resp = mock.MagicMock()
    resp.status_code = status_code
    resp.raise_for_status = mock.Mock()
    if json_body is not None:
        resp.json = mock.Mock(return_value=json_body)
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
        assert set(retry.allowed_methods) == {'GET', 'POST', 'PUT', 'DELETE'}
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


@pytest.fixture
def download_folder(tmp_path, monkeypatch):
    """Streamed downloads land in a folder of the test's own, so a file left behind shows."""
    monkeypatch.setattr(jsonrpc, 'download_folder', str(tmp_path))
    return tmp_path


def _download_response(body, content_type='text/plain; charset=utf-8', status_code=200, missing=0,
                       enforce_content_length=True):
    """A real Response streaming `body`, a BytesIO, as plaid's JSON-RPC endpoint sends a streamed result.

    `missing` bytes beyond the body in its Content-Length stand for a body cut off part way;
    `enforce_content_length=False` reads it as urllib3 1.26 does, ending quietly.
    """
    response = requests.Response()
    response.status_code = status_code
    response.headers['Content-Type'] = content_type
    response.raw = urllib3.HTTPResponse(
        body=body, status=status_code, preload_content=False,
        headers={'Content-Length': str(len(body.getvalue()) + missing)},
        enforce_content_length=enforce_content_length,
    )
    return response


def _download(response):
    session = mock.create_autospec(requests.Session, instance=True)
    session.post.return_value = response
    with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
        return http_json_rpc(token='t', uri='https://example.com/rpc', verify_ssl=True,
                             json_data={'method': next(iter(STREAM_ENDPOINTS)), 'params': {}})


class _BodyThatMustBeStreamed(io.BytesIO):
    """A body that records each read reaching more than `ahead` bytes past what the files in `folder` hold."""

    def __init__(self, content, folder, ahead):
        super().__init__(content)
        self.size, self.folder, self.ahead = len(content), folder, ahead
        self.reads_ahead_of_the_file = []

    def read(self, size=-1):
        end = self.size if size is None or size < 0 else min(self.tell() + size, self.size)
        written = sum(path.stat().st_size for path in self.folder.iterdir())
        if end - written > self.ahead:
            self.reads_ahead_of_the_file.append((end, written))
        return super().read(size)


class TestHttpJsonRpcStreamable:

    def test_the_body_is_written_to_a_file_whose_path_is_returned(self, download_folder):
        path = _download(_download_response(io.BytesIO(b'a,b\n1,2\n')))
        assert os.path.dirname(path) == str(download_folder)
        with open(path, 'rb') as file:
            assert file.read() == b'a,b\n1,2\n'

    def test_a_json_rpc_envelope_is_decoded_and_leaves_no_file(self, download_folder):
        envelope = {'ok': False, 'error': {'message': 'No such table', 'code': -1}}
        assert _download(_download_response(io.BytesIO(json.dumps(envelope).encode()), 'application/json')) == envelope
        assert os.listdir(download_folder) == []

    def test_a_large_body_is_written_as_it_arrives_rather_than_read_whole_first(self, download_folder):
        chunk = jsonrpc.REST_DOWNLOAD_CHUNK_BYTES
        body = _BodyThatMustBeStreamed(b'a,b,c\n' * (8 * chunk // 6), download_folder, ahead=2 * chunk)
        path = _download(_download_response(body))
        assert body.reads_ahead_of_the_file == []
        assert os.path.getsize(path) == body.size

    def test_an_http_error_leaves_no_file(self, download_folder):
        with pytest.raises(requests.HTTPError):
            _download(_download_response(io.BytesIO(b'Bad Gateway'), status_code=502))
        assert os.listdir(download_folder) == []

    @pytest.mark.parametrize('enforce_content_length', [True, False], ids=['urllib3 2', 'urllib3 1.26'])
    def test_a_body_cut_off_part_way_raises_and_leaves_no_file(self, download_folder, enforce_content_length):
        with pytest.raises(requests.exceptions.ChunkedEncodingError):
            _download(_download_response(io.BytesIO(b'a,b\n1,2\n'), missing=100,
                                         enforce_content_length=enforce_content_length))
        assert os.listdir(download_folder) == []


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


# ------------------------------------------------------------- request ids
# Every call used to go out with a hard-coded id of 0, which made the server's
# paired Start/Finish log lines unmatchable.

class TestRequestId:

    @staticmethod
    def _sent_id(session):
        return json.loads(session.post.call_args[1]['data'])['id']

    def _post(self, session, json_data):
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            http_json_rpc(
                token='t', uri='https://example.com/rpc', verify_ssl=True,
                json_data=json_data, retry=False,
            )
        return self._sent_id(session)

    def test_id_differs_between_calls(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True})
        first = self._post(session, {'method': 'm', 'params': {}, 'jsonrpc': '2.0'})
        second = self._post(session, {'method': 'm', 'params': {}, 'jsonrpc': '2.0'})
        assert first != second

    def test_caller_supplied_id_is_kept(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True})
        assert self._post(session, {'method': 'm', 'params': {}, 'id': 762}) == 762

    def test_simple_rpc_sends_a_usable_id(self):
        session = mock.MagicMock()
        session.post.return_value = _mk_response({'ok': True, 'result': 'x'})
        with mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session)):
            SimpleRPC('token', uri='https://example.com', retry=False).analyze.project.list()
        assert self._sent_id(session) not in (None, 0)


# ------------------------------------------------------------------ http_rest

def _rest_response(status_code=200, content=b''):
    """A real Response, so `ok`, `content` and `text` behave as they do off the wire."""
    response = requests.Response()
    response.status_code = status_code
    response._content = content
    response.encoding = 'utf-8'
    return response


class _BodyOnTheWire(io.BytesIO):
    """A response's body as the socket delivers it, recording the size of each read urllib3 asks of it."""

    def __init__(self, content):
        super().__init__(content)
        self.reads = []

    def read(self, size=-1):
        self.reads.append(size)
        return super().read(size)


def _streamed_response(content, status_code=200, content_length=None, enforce_content_length=True):
    """A real Response over a real urllib3 response, read through `raw.stream()` as one sent with stream=True is.

    A `content_length` beyond `content` stands for a body cut off part way, which urllib3 2 raises for;
    `enforce_content_length=False` reads it as urllib3 1.26 does, ending quietly.
    """
    response = requests.Response()
    response.status_code = status_code
    response.raw = urllib3.HTTPResponse(
        body=_BodyOnTheWire(content), status=status_code, preload_content=False,
        headers={'Content-Length': str(len(content) if content_length is None else content_length)},
        enforce_content_length=enforce_content_length,
    )
    response.encoding = 'utf-8'
    return response


@contextlib.contextmanager
def _rest_session(response):
    """Patch _get_session to yield a Session returning `response`; also yields the kwargs each open used."""
    session = mock.create_autospec(requests.Session, instance=True)
    session.request.return_value = response
    opened = []

    @contextlib.contextmanager
    def fake(**kwargs):
        opened.append(kwargs)
        yield session

    with mock.patch.object(jsonrpc, '_get_session', fake):
        yield session, opened


def _send_rest(**kwargs):
    call = {'token': 'tok', 'uri': 'https://example.com/json-rpc/', 'http_method': 'GET', 'path': 'analyze/dimension/dimensions'}
    return http_rest(**{**call, **kwargs})


class TestHttpRest:

    def test_request_goes_under_rest_root_on_the_rpc_host(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest(verify_ssl=True)
        args, kwargs = session.request.call_args
        assert args == ('GET', 'https://example.com/rest/v1/analyze/dimension/dimensions')
        assert kwargs['verify'] is True
        assert kwargs['allow_redirects'] is False

    def test_leading_slash_on_path_is_tolerated(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest(path='/analyze/dimension/dimensions')
        assert session.request.call_args[0][1] == 'https://example.com/rest/v1/analyze/dimension/dimensions'

    def test_token_sets_bearer_header(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest(token='abc')
        assert session.request.call_args[1]['headers']['Authorization'] == 'Bearer abc'

    def test_no_token_no_auth_header(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest(token=None)
        assert 'Authorization' not in session.request.call_args[1]['headers']

    def test_body_is_sent_as_json(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest(http_method='POST', body={'children': ['a', 'b']})
        kwargs = session.request.call_args[1]
        assert json.loads(kwargs['data']) == {'children': ['a', 'b']}
        assert kwargs['headers']['Content-Type'] == 'application/json'

    def test_no_body_sends_no_data_or_content_type(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest()
        kwargs = session.request.call_args[1]
        assert kwargs['data'] is None
        assert 'Content-Type' not in kwargs['headers']

    def test_params_are_passed_as_query_string(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest(params={'children': ['a', 'b']})
        assert session.request.call_args[1]['params'] == {'children': ['a', 'b']}

    def test_custom_headers_are_sent_without_mutating_the_callers_dict(self):
        headers = {'X-Custom': '1'}
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest(headers=headers)
        assert session.request.call_args[1]['headers']['X-Custom'] == '1'
        assert headers == {'X-Custom': '1'}

    def test_trace_context_is_injected(self):
        def inject(headers):
            headers['traceparent'] = '00-abc-def-01'

        with (
            _rest_session(_rest_response(content=b'{}')) as (session, _),
            mock.patch.object(jsonrpc, 'inject_trace_context', side_effect=inject),
        ):
            _send_rest()
        assert session.request.call_args[1]['headers']['traceparent'] == '00-abc-def-01'

    def test_json_body_is_decoded(self):
        with _rest_session(_rest_response(content=b'{"children": ["a"]}')):
            assert _send_rest() == {'children': ['a']}

    def test_empty_body_returns_none(self):
        with _rest_session(_rest_response(status_code=204)):
            assert _send_rest(http_method='DELETE') is None

    def test_error_raises_rpc_error_with_status_and_detail(self):
        with (
            _rest_session(_rest_response(status_code=422, content=b'{"detail": "Name is required"}')),
            pytest.raises(RPCError) as raised,
        ):
            _send_rest()
        assert raised.value.code == 422
        assert str(raised.value) == '422: Name is required'

    @pytest.mark.parametrize('content', [b'Bad Gateway', b'{"message": "nope"}', b'["nope"]'])
    def test_error_without_a_detail_carries_the_body(self, content):
        with (
            _rest_session(_rest_response(status_code=502, content=content)),
            pytest.raises(RPCError) as raised,
        ):
            _send_rest()
        assert str(raised.value) == f'502: {content.decode()}'

    def test_a_redirect_raises_rather_than_reading_as_an_empty_response(self):
        with (
            _rest_session(_rest_response(status_code=302)),
            pytest.raises(RPCError) as raised,
        ):
            _send_rest()
        assert raised.value.code == 302

    @pytest.mark.parametrize('retry', [True, False])
    def test_shared_session_only_when_retrying(self, retry):
        with _rest_session(_rest_response(content=b'{}')) as (_, opened):
            _send_rest(retry=retry)
        assert opened == [{'shared': retry}]

    def test_check_allow_transmit_is_visible_to_retries_then_cleared(self):
        check = mock.Mock(return_value=True)
        seen = []

        def request(*args, **kwargs):
            seen.append(_rpc_context.check_allow_transmit)
            return _rest_response(content=b'{}')

        with _rest_session(_rest_response()) as (session, _):
            session.request.side_effect = request
            _send_rest(check_allow_transmit=check)
        assert seen == [check]
        assert _rpc_context.check_allow_transmit is None

    def test_check_allow_transmit_cleared_when_the_request_raises(self):
        with _rest_session(_rest_response()) as (session, _):
            session.request.side_effect = requests.ConnectionError('down')
            with pytest.raises(requests.ConnectionError):
                _send_rest(check_allow_transmit=lambda: True)
        assert _rpc_context.check_allow_transmit is None

    def test_a_response_to_decode_is_not_streamed(self):
        with _rest_session(_rest_response(content=b'{}')) as (session, _):
            _send_rest()
        assert session.request.call_args[1]['stream'] is False

    @pytest.mark.parametrize('retry', [True, False])
    def test_to_file_streams_the_body_into_the_file_a_chunk_at_a_time(self, tmp_path, retry):
        body = bytes(range(256)) * 10_000
        target = str(tmp_path / 'package.parquet')
        response = _streamed_response(body)
        with _rest_session(response) as (session, _):
            assert _send_rest(path='document/view/download-file', to_file=target, retry=retry) == target
        assert session.request.call_args[1]['stream'] is True
        assert (tmp_path / 'package.parquet').read_bytes() == body
        assert set(response.raw._fp.reads) == {jsonrpc.REST_DOWNLOAD_CHUNK_BYTES}
        assert os.listdir(tmp_path) == ['package.parquet']

    def test_to_file_replaces_a_file_already_at_the_path_once_the_body_is_whole(self, tmp_path):
        target = tmp_path / 'package.parquet'
        target.write_bytes(b'the copy from the last run')
        with _rest_session(_streamed_response(b'PAR1 this run PAR1')):
            assert _send_rest(to_file=target) == target
        assert target.read_bytes() == b'PAR1 this run PAR1'
        assert os.listdir(tmp_path) == ['package.parquet']

    def test_to_file_writes_no_file_for_an_error_and_raises_its_detail(self, tmp_path):
        target = tmp_path / 'package.parquet'
        with (
            _rest_session(_streamed_response(b'{"detail": "No such document"}', status_code=404)),
            pytest.raises(RPCError) as raised,
        ):
            _send_rest(to_file=str(target))
        assert raised.value.code == 404
        assert str(raised.value) == '404: No such document'
        assert os.listdir(tmp_path) == []

    @pytest.mark.parametrize('enforce_content_length', [True, False], ids=['urllib3 2', 'urllib3 1.26'])
    def test_a_body_cut_off_part_way_leaves_the_file_already_there_and_closes_the_response(
            self, tmp_path, enforce_content_length):
        target = tmp_path / 'package.parquet'
        target.write_bytes(b'the copy from the last run')
        chunk = jsonrpc.REST_DOWNLOAD_CHUNK_BYTES
        response = _streamed_response(b'PAR1' * chunk, content_length=8 * chunk,
                                      enforce_content_length=enforce_content_length)
        response.close = mock.Mock(wraps=response.close)
        with _rest_session(response), pytest.raises(requests.exceptions.ChunkedEncodingError):
            _send_rest(to_file=str(target))
        assert target.read_bytes() == b'the copy from the last run'
        assert os.listdir(tmp_path) == ['package.parquet']
        response.close.assert_called_once_with()


class TestSimpleRPCCallRest:

    def test_forwards_the_connections_settings(self):
        check = mock.Mock(return_value=True)
        with mock.patch.object(jsonrpc, 'http_rest', return_value={'ok': 1}) as mock_rest:
            rpc = SimpleRPC(lambda: 'tok', uri='https://example.com/json-rpc/', verify_ssl=True,
                            proxies={'https': 'proxy'}, check_allow_transmit=check, retry=False, headers={'h': 'v'})
            result = rpc.call_rest('POST', 'analyze/dimension/node', params={'p': 1}, body={'b': 2})
        assert result == {'ok': 1}
        assert mock_rest.call_args == mock.call(
            'tok', 'https://example.com/json-rpc/', 'POST', 'analyze/dimension/node', True,
            params={'p': 1}, body={'b': 2}, proxies={'https': 'proxy'}, check_allow_transmit=check,
            retry=False, headers={'h': 'v'}, to_file=None,
        )

    def test_forwards_the_file_to_write_the_body_to(self):
        with mock.patch.object(jsonrpc, 'http_rest', return_value='/tmp/package.parquet') as mock_rest:
            rpc = SimpleRPC('tok', uri='https://example.com/json-rpc/')
            result = rpc.call_rest('GET', 'document/view/download-file', params={'path': 'p'},
                                   to_file='/tmp/package.parquet')
        assert result == '/tmp/package.parquet'
        assert mock_rest.call_args.kwargs['to_file'] == '/tmp/package.parquet'

    def test_string_token_is_sent_as_is(self):
        with mock.patch.object(jsonrpc, 'http_rest') as mock_rest:
            SimpleRPC('tok', uri='https://example.com/json-rpc/').call_rest('GET', 'x')
        assert mock_rest.call_args[0][0] == 'tok'

    def test_nothing_is_sent_once_transmit_is_disallowed(self):
        with mock.patch.object(jsonrpc, 'http_rest') as mock_rest:
            rpc = SimpleRPC('tok', uri='https://example.com/json-rpc/', check_allow_transmit=lambda: False)
            assert rpc.call_rest('GET', 'x') is None
        mock_rest.assert_not_called()

    def test_is_not_an_rpc_namespace_before_init(self):
        assert object.__new__(SimpleRPC).call_rest is None


# --- cancelling a streamed download ---

def _stalling_server(reply=b'', tls=None):
    """A server that reads the request, sends `reply` and then never answers more; returns (uri, close).

    With `tls`, a server SSLContext, it speaks HTTPS.
    """
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen()
    held = []
    release = threading.Event()

    def serve():
        with contextlib.suppress(OSError):
            conn, _ = server.accept()
            held.append(conn)
            if tls:
                conn = tls.wrap_socket(conn, server_side=True)
                held.append(conn)
            conn.recv(65536)
            conn.sendall(reply)
            release.wait(10)

    threading.Thread(target=serve, daemon=True).start()

    def close():
        release.set()
        for conn in held:
            conn.close()
        server.close()

    return f'{"https" if tls else "http"}://127.0.0.1:{server.getsockname()[1]}/', close


def _tls_server_context():
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    trustme.CA().issue_cert('127.0.0.1').configure_cert(context)
    return context


def _listener_that_never_accepts():
    """The kernel completes the TCP handshake into the backlog, so a client's TLS handshake waits for good."""
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen()
    return f'https://127.0.0.1:{server.getsockname()[1]}/', server.close


def _listener_whose_backlog_is_full():
    """Linux drops a SYN to a listener whose accept queue is full, so a client's TCP connect waits."""
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen(0)
    filler = socket.create_connection(server.getsockname())

    def close():
        filler.close()
        server.close()

    return f'http://127.0.0.1:{server.getsockname()[1]}/', close


def _download_in_thread(uri, cancel, proxies=None):
    outcome = []

    def run():
        try:
            with cancellable_downloads(cancel):
                http_json_rpc(uri=uri, verify_ssl=False, proxies=proxies,
                              json_data={'method': next(iter(STREAM_ENDPOINTS)), 'params': {}})
            outcome.append(None)
        except Exception as err:  # noqa: BLE001 - hand any failure back to the asserting thread
            outcome.append(err)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def _wait_until(condition, seconds=3):
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, 'timed out waiting'
        time.sleep(0.02)


@pytest.mark.parametrize('scheme', ['http', 'https'])
def test_cancel_unblocks_a_download_stalled_before_headers(scheme):
    uri, close = _stalling_server(tls=_tls_server_context() if scheme == 'https' else None)
    cancel = DownloadCancel()
    thread, outcome = _download_in_thread(uri, cancel)
    try:
        time.sleep(0.5)
        assert thread.is_alive()  # waiting on the server
        cancel.cancel()
        thread.join(3)
        assert not thread.is_alive()
        assert isinstance(outcome[0], requests.exceptions.ConnectionError)
    finally:
        close()


def test_cancelling_before_the_connection_exists_still_ends_the_download():
    uri, close = _stalling_server()
    cancel = DownloadCancel()
    cancel.cancel()
    thread, outcome = _download_in_thread(uri, cancel)
    try:
        thread.join(3)
        assert not thread.is_alive()
        assert isinstance(outcome[0], requests.exceptions.ConnectionError)
    finally:
        close()


def test_cancel_unblocks_a_download_stalled_behind_a_proxy():
    proxy, close = _stalling_server()  # accepts the proxied request and never answers
    cancel = DownloadCancel()
    thread, outcome = _download_in_thread('http://plaid.invalid/', cancel, proxies={'http': proxy})
    try:
        time.sleep(0.5)
        assert thread.is_alive()
        cancel.cancel()
        thread.join(3)
        assert not thread.is_alive()
        assert isinstance(outcome[0], requests.exceptions.ConnectionError)
    finally:
        close()


def test_a_download_cancelled_mid_body_leaves_no_file(download_folder):
    headers_and_a_first_chunk = (b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\n'
                                 b'Transfer-Encoding: chunked\r\n\r\n6\r\na,b,c\n\r\n')
    uri, close = _stalling_server(reply=headers_and_a_first_chunk)
    cancel = DownloadCancel()
    thread, outcome = _download_in_thread(uri, cancel)
    try:
        _wait_until(lambda: os.listdir(download_folder))
        cancel.cancel()
        thread.join(3)
        assert not thread.is_alive()
        assert isinstance(outcome[0], requests.RequestException)
        assert os.listdir(download_folder) == []
    finally:
        close()


@pytest.mark.parametrize('stuck_listener', [
    pytest.param(_listener_whose_backlog_is_full, id='tcp connect',
                 marks=pytest.mark.skipif(sys.platform != 'linux', reason='Linux drops a SYN to a full backlog')),
    pytest.param(_listener_that_never_accepts, id='tls handshake'),
])
def test_a_cancel_while_connecting_ends_the_download_at_the_connect_timeout(stuck_listener, monkeypatch):
    monkeypatch.setattr(jsonrpc, 'DOWNLOAD_CONNECT_TIMEOUT', 1)
    uri, close = stuck_listener()
    cancel = DownloadCancel()
    thread, outcome = _download_in_thread(uri, cancel)
    try:
        time.sleep(0.3)
        assert thread.is_alive()
        cancel.cancel()
        thread.join(3)
        assert not thread.is_alive()
        assert isinstance(outcome[0], requests.exceptions.Timeout)
    finally:
        close()
