#!/usr/bin/env python
# coding=utf-8

from unittest import mock

import pytest

from plaidcloud.rpc import rpc_connect
from plaidcloud.rpc.rpc_connect import Connect, PlaidXLConnect


class TestPlaidXLConnect:
    """PlaidXLConnect doesn't call PlaidConfig filesystem logic, so it's testable."""

    def test_basic_instantiation(self):
        rpc = PlaidXLConnect(
            rpc_uri='https://example.com/rpc',
            auth_token='test-token',
            workspace_id='ws1',
            project_id='p1',
        )
        assert rpc.rpc_uri == 'https://example.com/rpc'
        assert rpc.auth_token == 'test-token'
        assert rpc.workspace_uuid == 'ws1'
        assert rpc.project_id == 'p1'


class TestConnect:
    """Connect inherits from PlaidConfig which reads env/files.
    Tests exercise only pieces that don't require a fully-wired environment."""

    @mock.patch('plaidcloud.rpc.rpc_connect.PlaidConfig.__init__')
    def test_init_local_with_auto_initialize_false(self, mock_config_init):
        mock_config_init.return_value = None
        # Simulate the config being set up without env vars.
        with mock.patch.object(Connect, 'initialize') as mock_init, \
             mock.patch.object(Connect, 'ready') as mock_ready:
            connect = Connect.__new__(Connect)
            connect.is_local = True
            Connect.__init__(connect, auto_initialize=False)
            # is_local=True and auto_initialize=False means neither is called
            mock_init.assert_not_called()

    @mock.patch('plaidcloud.rpc.rpc_connect.PlaidConfig.__init__')
    def test_init_local_with_auto_initialize_true(self, mock_config_init):
        """Covers the elif self.is_local and auto_initialize branch."""
        mock_config_init.return_value = None

        def set_local(self, *args, **kwargs):
            self.is_local = True

        mock_config_init.side_effect = set_local
        with mock.patch.object(Connect, 'initialize') as mock_init:
            connect = Connect(auto_initialize=True)
            mock_init.assert_called_once()

    @mock.patch('plaidcloud.rpc.rpc_connect.SimpleRPC.__init__')
    def test_ready_calls_simple_rpc_init(self, mock_srpc_init):
        mock_srpc_init.return_value = None
        connect = Connect.__new__(Connect)
        connect.auth_token = 'tok'
        connect.rpc_uri = 'https://example.com'
        connect.allow_transmit_func = lambda: True
        connect.ready()
        mock_srpc_init.assert_called_once()

    @mock.patch('plaidcloud.rpc.rpc_connect.PlaidConfig.__init__')
    def test_init_remote_calls_ready(self, mock_config_init):
        mock_config_init.return_value = None
        with mock.patch.object(Connect, 'ready') as mock_ready:
            connect = Connect.__new__(Connect)
            connect.is_local = False
            Connect.__init__(connect)
            mock_ready.assert_called_once()

    def test_auth_token_from_auth_code_error_raises(self):
        connect = Connect.__new__(Connect)
        connect.client_id = 'c'
        connect.client_secret = 's'
        connect.token_uri = 'https://example.com/token'
        fake_response = mock.MagicMock()
        fake_response.status_code = 500
        fake_response.reason = 'Server Error'
        fake_response.text = 'boom'
        with mock.patch('plaidcloud.rpc.rpc_connect.requests.post',
                        return_value=fake_response):
            with pytest.raises(Exception, match='Error requesting'):
                connect.auth_token_from_auth_code('code', client_credentials=True)

    def test_get_auth_token_error_raises(self):
        connect = Connect.__new__(Connect)
        connect.client_id = 'c'
        connect.client_secret = 's'
        connect.token_uri = 'https://example.com/token'
        fake_response = mock.MagicMock()
        fake_response.status_code = 500
        fake_response.reason = 'Server Error'
        fake_response.text = 'boom'
        with mock.patch('plaidcloud.rpc.rpc_connect.requests.post',
                        return_value=fake_response):
            with pytest.raises(Exception, match='Error requesting'):
                connect.get_auth_token()

    def test_get_auth_token_success(self):
        connect = Connect.__new__(Connect)
        connect.client_id = 'c'
        connect.client_secret = 's'
        connect.token_uri = 'https://example.com/token'
        fake_response = mock.MagicMock()
        fake_response.status_code = 200
        fake_response.json = mock.Mock(return_value={'access_token': 'abc'})
        with mock.patch('plaidcloud.rpc.rpc_connect.requests.codes.get', return_value=200), \
             mock.patch('plaidcloud.rpc.rpc_connect.requests.post',
                        return_value=fake_response):
            token = connect.get_auth_token()
        assert token == 'abc'

    def test_auth_token_from_auth_code_success(self, tmp_path):
        cfg_file = tmp_path / 'plaid.conf'
        cfg_file.write_text('auth_token: old\n')
        connect = Connect.__new__(Connect)
        connect.client_id = 'c'
        connect.client_secret = 's'
        connect.token_uri = 'https://example.com/token'
        connect.cfg_path = str(cfg_file)
        connect._C = {'config': {}}
        fake_response = mock.MagicMock()
        fake_response.status_code = 200
        fake_response.json = mock.Mock(return_value={'access_token': 'new_token'})
        with mock.patch('plaidcloud.rpc.rpc_connect.requests.codes.get', return_value=200), \
             mock.patch('plaidcloud.rpc.rpc_connect.requests.post',
                        return_value=fake_response):
            connect.auth_token_from_auth_code('the_code', client_credentials=True)
        assert connect.auth_token == 'new_token'

    def test_auth_code_callback(self, tmp_path):
        cfg_file = tmp_path / 'plaid.conf'
        cfg_file.write_text('auth_token: old\n')
        connect = Connect.__new__(Connect)
        connect.client_id = 'c'
        connect.client_secret = 's'
        connect.token_uri = 'https://example.com/token'
        connect.cfg_path = str(cfg_file)
        connect._C = {'config': {}}
        fake_response = mock.MagicMock()
        fake_response.status_code = 200
        fake_response.json = mock.Mock(return_value={'access_token': 'tok'})
        with mock.patch('plaidcloud.rpc.rpc_connect.requests.codes.get', return_value=200), \
             mock.patch('plaidcloud.rpc.rpc_connect.requests.post',
                        return_value=fake_response), \
             mock.patch.object(Connect, 'ready') as mock_ready:
            connect.auth_code_callback('mycode')
        assert connect.auth_code == 'mycode'
        mock_ready.assert_called_once()

    def test_initialize_with_auth_token(self):
        connect = Connect.__new__(Connect)
        connect.auth_token = 'existing'
        with mock.patch.object(Connect, 'ready') as mock_ready:
            connect.initialize()
        mock_ready.assert_called_once()

    def test_initialize_client_credentials(self):
        connect = Connect.__new__(Connect)
        connect.auth_token = ''
        connect.grant_type = 'client_credentials'
        with mock.patch.object(Connect, 'ready') as mock_ready, \
             mock.patch.object(Connect, 'get_auth_token', return_value='tok'):
            connect.initialize()
        assert connect.auth_token == 'tok'

    def test_initialize_no_code_or_token(self):
        connect = Connect.__new__(Connect)
        connect.auth_token = ''
        connect.grant_type = 'code'
        connect.auth_code = None
        with mock.patch.object(Connect, 'request_oauth_token') as mock_req:
            connect.initialize()
        mock_req.assert_called_once()

    def test_initialize_with_auth_code(self):
        connect = Connect.__new__(Connect)
        connect.auth_token = ''
        connect.grant_type = 'code'
        connect.auth_code = 'some_code'
        with mock.patch.object(Connect, 'ready') as mock_ready, \
             mock.patch.object(Connect, 'auth_token_from_auth_code') as mock_exchange:
            connect.initialize()
        mock_exchange.assert_called_once_with('some_code')
        mock_ready.assert_called_once()
