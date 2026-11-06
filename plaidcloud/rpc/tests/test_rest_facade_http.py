"""SimpleRPC's REST facade against a real local HTTP server, so the retry, error and fallback behavior of the
whole transport is exercised and not a mocked `http_rest`."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from plaidcloud.rpc.connection import jsonrpc
from plaidcloud.rpc.connection.jsonrpc import SimpleRPC
from plaidcloud.rpc.remote.rpc_common import RPCError

JSON_RPC_OK = (200, {'ok': True, 'result': 'rpc'})


class Server:
    def __init__(self):
        self.seen = []  # (verb, path, body, Authorization)
        self.answer = lambda verb, path, body: (404, {'detail': 'Not Found'})
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def handle_request(self):
                length = int(self.headers.get('Content-Length') or 0)
                raw = self.rfile.read(length)
                body = json.loads(raw) if raw else None
                outer.seen.append((self.command, self.path, body, self.headers.get('Authorization')))
                status, payload = outer.answer(self.command, self.path, body)
                data = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_PUT = do_DELETE = handle_request

            def log_message(self, *args):
                pass

        self.httpd = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.uri = f'http://127.0.0.1:{self.httpd.server_port}/json-rpc'
        threading.Thread(target=self.httpd.serve_forever, kwargs={'poll_interval': 0.01}, daemon=True).start()

    def paths(self):
        return [path for _, path, _, _ in self.seen]


@pytest.fixture
def server():
    jsonrpc._shared_session = jsonrpc._shared_rest_session = None
    srv = Server()
    yield srv
    srv.httpd.shutdown()
    srv.httpd.server_close()
    jsonrpc._shared_session = jsonrpc._shared_rest_session = None


@pytest.fixture
def rpc(server):
    return SimpleRPC('tok', uri=server.uri)


def rest_only(status, payload):
    """Answers every REST path with `payload`, and /json-rpc with a success."""
    def answer(verb, path, body):
        return (status, payload) if path.startswith('/rest/v1/') else JSON_RPC_OK
    return answer


def test_a_5xx_on_a_write_is_not_retried_and_raises_rpc_error(server, rpc):
    server.answer = rest_only(500, {'detail': 'Internal server error', 'code': 500})
    with pytest.raises(RPCError) as raised:
        rpc.analyze.workflow.queue(project_id='p', workflow_id='w')
    assert len(server.seen) == 1
    assert raised.value.code == 500
    assert str(raised.value) == 'Internal server error'


def test_a_refusal_keeps_its_rpc_code_and_message(server, rpc):
    server.answer = rest_only(400, {'detail': 'Table is locked.', 'code': -1001, 'type': 'AccessRefusedError'})
    with pytest.raises(RPCError) as raised:
        rpc.analyze.table.touch(project_id='p', table_id='t')
    assert raised.value.code == -1001
    assert str(raised.value) == 'Table is locked.'


def test_an_analyze_error_keeps_its_type_in_the_message(server, rpc):
    # workflow-runner decides a failure is the user's by looking for 'AnalyzeError' in the message
    server.answer = rest_only(422, {'detail': 'column x is missing', 'code': 422, 'type': 'AnalyzeError'})
    with pytest.raises(RPCError) as raised:
        rpc.analyze.workflow.queue(project_id='p', workflow_id='w')
    assert 'AnalyzeError' in str(raised.value)
    assert 'column x is missing' in str(raised.value)


@pytest.mark.parametrize('body', [
    {'detail': 'Workflow is already running', 'code': -1000, 'type': 'Warning'},
    {'detail': 'Workflow is already running', 'request_id': 'r'},  # a server before errors were typed
])
def test_a_warning_is_raised_as_a_warning(server, rpc, body):
    server.answer = rest_only(409, body)
    with pytest.raises(Warning, match='Workflow is already running'):
        rpc.analyze.workflow.start(project_id='p', workflow_id='w')


def test_publish_sample_statuses_come_back_as_results(server, rpc):
    status = {'status': 'running', 'dataset_id': 'd'}
    server.answer = rest_only(425, {'detail': 'running', 'data': status})
    assert rpc.analyze.onboarding.publish_sample(dataset_id='d') == status


def test_fire_and_forget_is_sent_before_the_call_returns(server, rpc):
    server.answer = rest_only(200, None)
    assert rpc.analyze.workflow.set_step_state(
        project_id='p', workflow_id='w', step_id='s', state='skipped', fire_and_forget=True) is None
    assert server.seen[0][:2] == ('PUT', '/rest/v1/analyze/workflow/step-state')


def test_fire_and_forget_swallows_a_failure(server, rpc):
    server.answer = rest_only(500, {'detail': 'boom'})
    assert rpc.analyze.workflow.log(project_id='p', workflow_id='w', message='m', fire_and_forget=True) is None
    assert len(server.seen) == 1


@pytest.mark.parametrize('body', [
    {'detail': 'Not Found', 'data': {'code': 'route_not_found'}, 'request_id': 'r'},
    {'detail': 'Not Found'},  # a server before the tag
])
def test_a_missing_route_falls_back_to_json_rpc(server, rpc, body):
    server.answer = lambda verb, path, _: (404, body) if path.startswith('/rest/v1/') else JSON_RPC_OK
    assert rpc.analyze.table.exists(project_id='p', table_id='t') == 'rpc'
    assert server.paths()[-1] == '/analyze/table/exists'


def test_a_missing_route_falls_back_for_fire_and_forget(server, rpc):
    server.answer = lambda verb, path, _: (404, {'detail': 'Not Found'}) if path.startswith('/rest/v1/') else JSON_RPC_OK
    rpc.analyze.workflow.log(project_id='p', workflow_id='w', message='m', fire_and_forget=True)
    assert any(path.startswith('/analyze/workflow/log') for path in server.paths())


@pytest.mark.parametrize('body', [
    {'detail': 'Table t not found', 'request_id': 'r'},  # a route's own 404
    {'detail': 'Not Found', 'request_id': 'r'},  # same words, but the route answered
    '<html>404</html>',
])
def test_a_routes_own_404_is_an_error_not_a_fallback(server, rpc, body):
    server.answer = rest_only(404, body)
    with pytest.raises(RPCError) as raised:
        rpc.analyze.table.exists(project_id='p', table_id='t')
    assert raised.value.code == 404
    assert server.paths() == ['/rest/v1/analyze/table/exists?project_id=p&table_id=t']


def test_with_json_rpc_gone_the_fallback_is_an_rpc_error(server, rpc):
    server.answer = lambda verb, path, _: (404, {'detail': 'Not Found'})
    with pytest.raises(RPCError) as raised:
        rpc.analyze.table.exists(project_id='p', table_id='t')
    assert raised.value.code == -32601
    assert 'analyze/table/exists' in str(raised.value)


def test_a_routed_call_sends_the_bearer_token_and_body(server, rpc):
    server.answer = rest_only(200, {'queued': True})
    assert rpc.analyze.workflow.queue(project_id='p', workflow_id='w', run_id='r') == {'queued': True}
    assert server.seen == [('POST', '/rest/v1/analyze/workflow/queue',
                            {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r'}, 'Bearer tok')]


def test_a_get_5xx_ends_as_an_rpc_error_not_a_retry_error(server):
    # A GET is retried on a 5xx; the last one must still surface as the server's error
    jsonrpc._shared_rest_session = None
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(jsonrpc, 'REST_RETRY', {**jsonrpc.REST_RETRY, 'total': 2, 'backoff_factor': 0})
        patch.setattr(jsonrpc.RPCRetry, '__init__', _fast_retry_init(jsonrpc.RPCRetry.__init__), raising=True)
        server.answer = rest_only(502, {'detail': 'bad gateway'})
        with pytest.raises(RPCError) as raised:
            SimpleRPC('tok', uri=server.uri).analyze.table.exists(project_id='p', table_id='t')
    assert raised.value.code == 502
    assert len(server.seen) == 3


def _fast_retry_init(original):
    def init(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self.backoff_factor = 0
    return init


def test_a_get_500_is_not_retried(server, rpc):
    server.answer = rest_only(500, {'detail': 'ProgrammingError: bad sql', 'code': -32603})
    with pytest.raises(RPCError) as raised:
        rpc.analyze.table.exists(project_id='p', table_id='t')
    assert len(server.seen) == 1
    assert raised.value.code == -32603


def test_a_json_rpc_http_error_other_than_missing_is_raised(server, rpc):
    server.answer = lambda verb, path, _: (400, {'detail': 'bad'})
    with pytest.raises(Exception) as raised:
        rpc.analyze.query.query(project_id='p', query='select 1')
    assert not isinstance(raised.value, RPCError)
