#!/usr/bin/env python
# coding=utf-8

import time
from unittest import mock

import jwt
import pytest

from plaidcloud.rpc.create_oauth_token import (
    token_is_expired,
    create_oauth_token,
    refresh_token,
)


def _make_jwt(payload):
    """Helper to create a JWT with the given payload."""
    return jwt.encode(payload, 'secret', algorithm='HS256')


class TestTokenIsExpired:

    def test_expired_token(self):
        token = _make_jwt({'exp': int(time.time()) - 3600})
        assert token_is_expired(token) is True

    def test_valid_token(self):
        token = _make_jwt({'exp': int(time.time()) + 3600})
        assert token_is_expired(token) is False

    def test_no_exp_field(self):
        token = _make_jwt({'sub': 'user'})
        assert token_is_expired(token) is False


class TestCreateOauthToken:

    def test_invalid_grant_type_raises(self):
        with pytest.raises(Exception, match='Invalid grant type'):
            create_oauth_token('invalid_grant', 'client_id', 'secret')

    def test_password_grant_missing_username_raises(self):
        with pytest.raises(Exception, match='Missing username or password'):
            create_oauth_token('password', 'client_id', 'secret')

    def test_password_grant_missing_password_raises(self):
        with pytest.raises(Exception, match='Missing username or password'):
            create_oauth_token('password', 'client_id', 'secret', username='user')

    @mock.patch('plaidcloud.rpc.create_oauth_token.requests.Session')
    def test_client_credentials_grant(self, mock_session_cls):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {'access_token': 'abc123'}
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session

        result = create_oauth_token(
            'client_credentials', 'client_id', 'secret',
            uri='https://auth.example.com/', retry=False,
        )
        assert result == {'access_token': 'abc123'}
        mock_session.post.assert_called_once()
        call_kwargs = mock_session.post.call_args
        assert 'realms/PlaidCloud/protocol/openid-connect/token' in call_kwargs[0][0]

    @mock.patch('plaidcloud.rpc.create_oauth_token.requests.Session')
    def test_password_grant(self, mock_session_cls):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {'access_token': 'token'}
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session

        result = create_oauth_token(
            'password', 'client_id', 'secret',
            username='user', password='pass',
            uri='https://auth.example.com/', retry=False,
        )
        assert result == {'access_token': 'token'}
        call_kwargs = mock_session.post.call_args
        assert call_kwargs[1]['data']['username'] == 'user'


class TestRefreshToken:

    @mock.patch('plaidcloud.rpc.create_oauth_token.requests.Session')
    def test_refresh_token_success(self, mock_session_cls):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {'access_token': 'new_token', 'refresh_token': 'new_refresh'}
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session

        result = refresh_token(
            'refresh_token', 'client_id', 'old_refresh',
            uri='https://auth.example.com/', retry=False,
        )
        assert result['access_token'] == 'new_token'

    @mock.patch('plaidcloud.rpc.create_oauth_token.requests.Session')
    def test_refresh_token_uri_trailing_slash_stripped(self, mock_session_cls):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {}
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session

        refresh_token('refresh_token', 'client_id', 'rt', uri='https://auth.example.com///', retry=False)
        call_args = mock_session.post.call_args
        url = call_args[0][0]
        assert '///' not in url
        assert url.startswith('https://auth.example.com/realms/')

    @mock.patch('plaidcloud.rpc.create_oauth_token.requests.Session')
    def test_refresh_token_retry_true(self, mock_session_cls):
        """Covers the retry=True branch that builds a Retry object."""
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {'access_token': 'tok'}
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session
        result = refresh_token('refresh_token', 'cid', 'rt', retry=True)
        assert result['access_token'] == 'tok'


class TestCreateOauthTokenRetry:

    @mock.patch('plaidcloud.rpc.create_oauth_token.requests.Session')
    def test_create_oauth_token_retry_true(self, mock_session_cls):
        """Covers the retry=True branch in create_oauth_token."""
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {'access_token': 'abc'}
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.Mock(return_value=mock_session)
        mock_session.__exit__ = mock.Mock(return_value=False)
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session

        result = create_oauth_token(
            'client_credentials', 'cid', 'secret', retry=True,
        )
        assert result == {'access_token': 'abc'}
