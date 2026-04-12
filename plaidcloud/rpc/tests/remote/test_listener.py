#!/usr/bin/env python
# coding=utf-8

from unittest import mock

import pytest

from plaidcloud.rpc.remote.listener import AuthCodeListener, get_auth_code


class TestAuthCodeListener:
    """Unit test the do_GET method by directly invoking it on a mock instance."""

    def _make_handler(self, path, state=None):
        # Construct without calling BaseHTTPRequestHandler.__init__
        # (that would try to handle a request on a socket)
        handler = AuthCodeListener.__new__(AuthCodeListener)
        handler.path = path
        handler.listen_path = '/callback'
        handler.state = state
        handler.keep_listening = True
        handler.send_response = mock.Mock()
        return handler

    def test_path_not_matching_sends_404(self):
        handler = self._make_handler('/otherpath?code=abc')
        handler.do_GET()
        handler.send_response.assert_any_call(404)

    def test_matching_path_with_correct_state(self):
        handler = self._make_handler(
            '/callback?code=abc&state=xyz',
            state=['xyz'],
        )
        handler.do_GET()
        assert handler.code == ['abc']
        assert handler.keep_listening is False
        handler.send_response.assert_any_call(200)

    def test_state_mismatch_sends_401(self):
        handler = self._make_handler(
            '/callback?code=abc&state=xyz',
            state=['abc'],  # different state
        )
        handler.do_GET()
        handler.send_response.assert_any_call(401)


class TestGetAuthCode:

    @mock.patch('plaidcloud.rpc.remote.listener.TCPServer')
    def test_get_auth_code(self, mock_tcpserver_cls):
        mock_server = mock.MagicMock()
        mock_handler_cls = mock.MagicMock()
        # Make keep_listening False so the loop exits immediately
        mock_handler_cls.keep_listening = False
        mock_handler_cls.code = 'authcode_123'
        mock_server.RequestHandlerClass = mock_handler_cls
        mock_tcpserver_cls.return_value = mock_server

        result = get_auth_code()
        assert result == 'authcode_123'
