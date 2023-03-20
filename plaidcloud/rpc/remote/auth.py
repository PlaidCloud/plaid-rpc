# coding=utf-8

from plaidcloud.rpc import utc

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017-2023, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'


def user_auth(*args, **kwargs):
    auth = Auth()
    auth.user(*args, **kwargs)
    return auth


def agent_auth(*args, **kwargs):
    auth = Auth()
    auth.agent(*args, **kwargs)
    return auth


def transform_auth(*args, **kwargs):
    auth = Auth()
    auth.transform(*args, **kwargs)
    return auth


def oauth2_auth(*args, **kwargs):
    auth = Auth()
    auth.oauth2(*args, **kwargs)
    return auth


class Auth(object):

    def __init__(self):
        """Initializes Auth object settings"""
        self.status_message = ''

        # Private variables
        self._status_message = None
        self._auth_status = None
        self._auth_method = None
        self._private_key = None
        self._public_key = None
        self._mfa = None
        self._attempts = 0

    def user(self, user_name, password, multi_factor=None):
        """Used for User based connections requiring login credentials"""
        self.set_method('user')
        self._set_public_key(user_name)
        self._set_private_key(password)
        self._set_multi_factor(multi_factor)

    def agent(self, public_key, private_key, auth_method='agent'):
        """Used for PlaidLink agent based connections requireing the key information in PlaidCloud"""
        self.set_method(auth_method)
        self._set_public_key(public_key)
        self._set_private_key(private_key)

    def transform(self, task_id, session_id):
        """Transform based connection requiring the transform task_id and session_id"""
        self.set_method('transform')
        self._set_public_key(task_id)
        self._set_private_key(session_id)

    def oauth2(self, token):
        """oAuth2 based authentication connection"""
        self.set_method('oauth2')
        self._set_public_key(token)

    def is_ok(self):
        return self._auth_status == 'ok'

    def get_status_message(self):
        return self._status_message

    def get_attempts(self):
        return int(self._attempts)

    def get_method(self):
        return self._auth_method

    def get_auth_status(self):
        return self._auth_status

    def set_status_message(self, value):
        self._status_message = str(value)

    def set_method(self, value):
        if value in ('user', 'agent', 'transform', 'oauth2'):
            self._auth_method = value
        else:
            raise Exception("Invalid Authentication Method Specified")

    def set_status(self, value):
        if value in ('setup', 'ready', 'ok', 'fail'):
            self.status = value
        else:
            raise Exception("Invalid Authentication Status Specified")

    def _set_private_key(self, value):
        self._private_key = str(value)

    def _set_public_key(self, value):
        self._public_key = str(value)

    def _set_multi_factor(self, value):
        self._mfa = value

    def _get_private_key(self):
        return self._private_key

    def _get_public_key(self):
        return self._public_key

    def _get_mfa(self):
        return self._mfa

    def _increment_attempts(self):
        self._attempts += 1

    def get_package(self):
        package = {
            'PlaidCloud-Auth-Method': str(self.get_method()),
            'PlaidCloud-Key': str(self._get_public_key()),
            'PlaidCloud-Pass': str(self._get_private_key()),
            'PlaidCloud-MFA': str(self._get_mfa()),
            'PlaidCloud-Timestamp': str(utc.timestamp())
        }

        self._increment_attempts()

        return package
