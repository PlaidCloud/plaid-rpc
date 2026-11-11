"""download_csv and download_arrow over REST, and PlaidXLConnect's refusal to use /json-rpc. Mock transports only."""

import contextlib
import io
import json
import os
import pathlib
import threading
import time
from importlib.metadata import version
from unittest import mock

import pytest
import requests

from plaidcloud.rpc.connection import jsonrpc
from plaidcloud.rpc.connection.jsonrpc import (
    DownloadCancel,
    RPCCancelledError,
    SimpleRPC,
    cancellable_downloads,
    http_json_rpc,
)
from plaidcloud.rpc.remote.rpc_common import RPCError
from plaidcloud.rpc.rpc_connect import PlaidXLConnect
from plaidcloud.rpc.tests.test_jsonrpc import (
    _download_response,
    _listener_that_never_accepts,
    _patched_get_session,
    _stalling_server,
    _wait_until,
    download_folder,  # noqa: F401 - a fixture
)

URI = 'https://example.com/json-rpc'
NOT_FOUND = {'detail': 'Not Found'}


def _json_response(status, body, headers=None):
    response = _download_response(io.BytesIO(json.dumps(body).encode()), 'application/json', status)
    response.headers.update(headers or {})
    return response


def _session(*responses):
    session = mock.create_autospec(requests.Session, instance=True)
    session.request.side_effect = list(responses)
    return session


@pytest.fixture
def fail_json_rpc():
    with mock.patch.object(jsonrpc, 'http_json_rpc', side_effect=AssertionError('/json-rpc was called')) as m:
        yield m


def _patch_session(session):
    return mock.patch.object(jsonrpc, '_get_session', _patched_get_session(session))


class TestDownloadCsvOverRest:

    def test_the_body_is_written_to_a_temp_file_whose_path_is_returned(self, download_folder, fail_json_rpc):  # noqa: F811
        session = _session(_download_response(io.BytesIO(b'a,b\n1,2\n'), 'text/csv'))
        with _patch_session(session):
            path = SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p', table_name='t', query='q')
        assert os.path.dirname(path) == str(download_folder)
        assert pathlib.Path(path).read_bytes() == b'a,b\n1,2\n'
        assert os.listdir(download_folder) == [os.path.basename(path)]
        args, kwargs = session.request.call_args
        assert args == ('POST', 'https://example.com/rest/v1/analyze/query/csv')
        assert kwargs['timeout'] == (jsonrpc.DOWNLOAD_CONNECT_TIMEOUT, None)
        assert kwargs['stream'] is True
        assert json.loads(kwargs['data']) == {'project_id': 'p', 'table_name': 't', 'query': 'q'}

    def test_download_arrow_is_routed_the_same_way(self, download_folder, fail_json_rpc):  # noqa: F811
        session = _session(_download_response(io.BytesIO(b'ARROW'), 'application/vnd.apache.arrow.stream'))
        columns = [{'name': 'a', 'dtype': 'text'}]
        with _patch_session(session):
            path = SimpleRPC('tok', uri=URI).analyze.query.download_arrow(project_id='p', query='q', columns=columns)
        assert pathlib.Path(path).read_bytes() == b'ARROW'
        args, kwargs = session.request.call_args
        assert args == ('POST', 'https://example.com/rest/v1/analyze/query/arrow')
        assert json.loads(kwargs['data'])['columns'] == columns

    def test_the_session_is_a_fresh_cancellable_one_with_no_retries(self, download_folder):  # noqa: F811
        cancel = DownloadCancel()
        seen = []
        real = jsonrpc._get_session

        @contextlib.contextmanager
        def spy(**kwargs):
            with real(**kwargs) as session:
                seen.append((kwargs, session.get_adapter(URI)))
                session.request = mock.Mock(return_value=_download_response(io.BytesIO(b'a\n'), 'text/csv'))
                yield session

        with mock.patch.object(jsonrpc, '_get_session', spy), cancellable_downloads(cancel):
            SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
        [(kwargs, adapter)] = seen
        assert kwargs['cancel'] is cancel
        assert not kwargs['shared']
        assert isinstance(adapter, jsonrpc._CancellableAdapter)
        assert adapter.max_retries.total == 0

    def test_a_body_cut_off_part_way_raises_and_leaves_no_file(self, download_folder, fail_json_rpc):  # noqa: F811
        session = _session(_download_response(io.BytesIO(b'a,b\n1,2\n'), 'text/csv', missing=100))
        with _patch_session(session), pytest.raises(requests.exceptions.ChunkedEncodingError):
            SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
        assert os.listdir(download_folder) == []

    def test_a_route_the_tenant_lacks_falls_back_to_json_rpc_for_a_plain_connection(self, download_folder):  # noqa: F811
        session = _session(_json_response(404, NOT_FOUND))
        with _patch_session(session), mock.patch.object(jsonrpc, 'http_json_rpc', return_value='/tmp/file') as json_rpc:
            assert SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p') == '/tmp/file'
        assert json_rpc.call_args.args[3]['method'] == 'analyze/query/download_csv'
        assert os.listdir(download_folder) == []

    def test_a_real_404_raises_the_message_json_rpc_gave(self, download_folder, fail_json_rpc):  # noqa: F811
        body = {'detail': 'Project p not found', 'type': 'NotFoundError', 'data': {'code': 'project_not_found'}}
        with _patch_session(_session(_json_response(404, body))), pytest.raises(RPCError) as raised:
            SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
        assert str(raised.value) == 'NotFoundError: Project p not found'
        assert raised.value.code == 404
        assert os.listdir(download_folder) == []

    def test_a_warning_is_raised_as_a_warning(self, download_folder, fail_json_rpc):  # noqa: F811
        with _patch_session(_session(_json_response(409, {'detail': 'heads up', 'type': 'Warning'}))), \
                pytest.raises(Warning, match='heads up'):
            SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')


class TestRateLimitedDownload:

    @pytest.fixture
    def sleep(self):
        with mock.patch.object(threading.Event, 'wait', return_value=False) as sleep:
            yield sleep

    def test_a_429_before_the_body_is_retried_after_retry_after(self, download_folder, sleep):  # noqa: F811
        session = _session(_json_response(429, {'detail': 'slow down'}, {'Retry-After': '2'}),
                           _download_response(io.BytesIO(b'a\n'), 'text/csv'))
        with _patch_session(session):
            path = SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
        assert pathlib.Path(path).read_bytes() == b'a\n'
        sleep.assert_called_once_with(2.0)

    def test_retries_are_capped_at_three(self, download_folder, sleep):  # noqa: F811
        session = _session(*[_json_response(429, {'detail': 'slow down'}, {'Retry-After': '1'}) for _ in range(5)])
        with _patch_session(session), pytest.raises(RPCError) as raised:
            SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
        assert raised.value.code == 429
        assert session.request.call_count == 4
        assert sleep.call_count == 3

    def test_total_wait_is_capped_at_120_seconds(self, download_folder, sleep):  # noqa: F811
        session = _session(*[_json_response(429, {'detail': 'slow down'}, {'Retry-After': '70'}) for _ in range(5)])
        with _patch_session(session), pytest.raises(RPCError):
            SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
        assert session.request.call_count == 2
        sleep.assert_called_once_with(70.0)

    @pytest.mark.parametrize('retry_after', [None, 'Wed, 21 Oct 2026 07:28:00 GMT'])
    def test_a_missing_or_dated_retry_after_waits_a_second(self, download_folder, sleep, retry_after):  # noqa: F811
        first = _json_response(429, {'detail': 'slow down'}, {'Retry-After': retry_after} if retry_after else None)
        session = _session(first, _download_response(io.BytesIO(b'a\n'), 'text/csv'))
        with _patch_session(session):
            SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
        sleep.assert_called_once_with(1.0)

    def test_a_cancel_during_the_wait_ends_it_at_once_without_a_second_request(self, download_folder):  # noqa: F811
        session = _session(_json_response(429, {'detail': 'slow down'}, {'Retry-After': '30'}),
                           _download_response(io.BytesIO(b'a\n'), 'text/csv'))
        cancel = DownloadCancel()
        outcome = []

        def run():
            try:
                with _patch_session(session), cancellable_downloads(cancel):
                    SimpleRPC('tok', uri=URI).analyze.query.download_csv(project_id='p')
            except Exception as err:  # noqa: BLE001
                outcome.append(err)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        _wait_until(lambda: session.request.call_count == 1)
        cancel.cancel()
        thread.join(3)
        assert not thread.is_alive()
        assert isinstance(outcome[0], RPCCancelledError)
        assert session.request.call_count == 1
        assert os.listdir(download_folder) == []

    def test_check_allow_transmit_is_asked_again_before_a_retry(self, download_folder, sleep):  # noqa: F811
        session = _session(_json_response(429, {'detail': 'slow down'}, {'Retry-After': '1'}),
                           _download_response(io.BytesIO(b'a\n'), 'text/csv'))
        rpc = SimpleRPC('tok', uri=URI, check_allow_transmit=lambda: session.request.call_count < 1)
        with _patch_session(session), pytest.raises(RPCCancelledError):
            rpc.analyze.query.download_csv(project_id='p')
        assert session.request.call_count == 1
        assert os.listdir(download_folder) == []

    def test_a_token_provider_is_asked_for_each_attempt(self, download_folder, sleep):  # noqa: F811
        tokens = iter(['tok-1', 'tok-2'])
        session = _session(_json_response(429, {'detail': 'slow down'}, {'Retry-After': '1'}),
                           _download_response(io.BytesIO(b'a\n'), 'text/csv'))
        with _patch_session(session):
            SimpleRPC(lambda: next(tokens), uri=URI).analyze.query.download_csv(project_id='p')
        sent = [call.kwargs['headers']['Authorization'] for call in session.request.call_args_list]
        assert sent == ['Bearer tok-1', 'Bearer tok-2']

    def test_other_requests_are_never_retried_on_a_429(self, sleep):
        session = _session(_json_response(429, {'detail': 'slow down'}, {'Retry-After': '1'}))
        with _patch_session(session), pytest.raises(RPCError):
            SimpleRPC('tok', uri=URI).analyze.table.exists(project_id='p', table_id='t')
        assert session.request.call_count == 1
        sleep.assert_not_called()


def _download_in_thread(uri, cancel):
    outcome = []

    def run():
        try:
            with cancellable_downloads(cancel):
                SimpleRPC('tok', uri=uri, verify_ssl=False).analyze.query.download_csv(project_id='p')
            outcome.append(None)
        except Exception as err:  # noqa: BLE001 - hand any failure back to the asserting thread
            outcome.append(err)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


class TestDownloadCancelAndTimeout:

    def test_cancel_unblocks_a_download_stalled_before_headers(self, download_folder):  # noqa: F811
        uri, close = _stalling_server()
        cancel = DownloadCancel()
        thread, outcome = _download_in_thread(uri, cancel)
        try:
            time.sleep(0.3)
            assert thread.is_alive()
            cancel.cancel()
            thread.join(3)
            assert not thread.is_alive()
            assert isinstance(outcome[0], requests.exceptions.ConnectionError)
            assert os.listdir(download_folder) == []
        finally:
            close()

    def test_a_download_cancelled_mid_body_leaves_no_file(self, download_folder):  # noqa: F811
        headers_and_a_first_chunk = (b'HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\n'
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

    def test_a_download_stuck_connecting_ends_at_the_connect_timeout(self, download_folder, monkeypatch):  # noqa: F811
        monkeypatch.setattr(jsonrpc, 'DOWNLOAD_CONNECT_TIMEOUT', 1)
        uri, close = _listener_that_never_accepts()
        thread, outcome = _download_in_thread(uri, DownloadCancel())
        try:
            thread.join(3)
            assert not thread.is_alive()
            assert isinstance(outcome[0], requests.exceptions.Timeout)
            assert os.listdir(download_folder) == []
        finally:
            close()


class TestPlaidXLConnectIsRestOnly:

    @staticmethod
    def connect():
        return PlaidXLConnect(rpc_uri=URI, auth_token='tok')

    def test_a_routed_call_goes_over_rest_with_the_client_header(self, fail_json_rpc):
        session = _session(_json_response(200, True))
        with _patch_session(session):
            assert self.connect().analyze.table.exists(project_id='p', table_id='t') is True
        args, kwargs = session.request.call_args
        assert args == ('GET', 'https://example.com/rest/v1/analyze/table/exists')
        assert kwargs['headers']['X-PlaidCloud-Client'] == f'plaidxl/{version("plaidcloud-rpc")}'

    def test_a_route_the_tenant_lacks_is_32601(self, fail_json_rpc):
        with _patch_session(_session(_json_response(404, NOT_FOUND))), pytest.raises(RPCError) as raised:
            self.connect().analyze.table.exists(project_id='p', table_id='t')
        assert raised.value.code == -32601

    def test_a_method_with_no_route_is_32601(self, fail_json_rpc):
        with pytest.raises(RPCError) as raised:
            self.connect().analyze.table.not_a_routed_method(project_id='p')
        assert raised.value.code == -32601

    def test_an_adapter_refusal_is_32602(self, fail_json_rpc):
        with pytest.raises(RPCError) as raised:
            self.connect().analyze.workflow.queue(project_id='p', workflow_id='w', variables=[{'id': 'v'}])
        assert raised.value.code == -32602

    def test_download_csv_streams_to_a_file(self, download_folder, fail_json_rpc):  # noqa: F811
        session = _session(_download_response(io.BytesIO(b'a,b\n1,2\n'), 'text/csv'))
        with _patch_session(session):
            path = self.connect().analyze.query.download_csv(project_id='p', table_name='t')
        assert pathlib.Path(path).read_bytes() == b'a,b\n1,2\n'

    def test_download_arrow_streams_to_a_file(self, download_folder, fail_json_rpc):  # noqa: F811
        session = _session(_download_response(io.BytesIO(b'ARROW'), 'application/vnd.apache.arrow.stream'))
        with _patch_session(session):
            path = self.connect().analyze.query.download_arrow(project_id='p', query='q', columns=[])
        assert pathlib.Path(path).read_bytes() == b'ARROW'

    def test_a_missing_download_route_is_32601_and_leaves_no_file(self, download_folder, fail_json_rpc):  # noqa: F811
        with _patch_session(_session(_json_response(404, NOT_FOUND))), pytest.raises(RPCError) as raised:
            self.connect().analyze.query.download_arrow(project_id='p', query='q', columns=[])
        assert raised.value.code == -32601
        assert os.listdir(download_folder) == []


def test_http_json_rpc_leaves_the_callers_headers_alone():
    headers = {'X-PlaidCloud-Client': 'x'}
    session = mock.create_autospec(requests.Session, instance=True)
    session.post.return_value = mock.Mock(json=lambda: {'ok': True, 'result': 1})
    with _patch_session(session):
        http_json_rpc(token='t', uri=URI, verify_ssl=True, json_data={'method': 'm', 'params': {}}, headers=headers)
    assert headers == {'X-PlaidCloud-Client': 'x'}
    assert session.post.call_args.kwargs['headers']['X-PlaidCloud-Client'] == 'x'
