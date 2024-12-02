#!/usr/bin/env python
# coding=utf-8

"""Basic class that allows for a handler that wraps the plaid RPC API.
   Gracefully handles oauth token generation."""

from urllib.parse import urlencode
import yaml
import requests
import uuid
from concurrent.futures import ThreadPoolExecutor

from plaidcloud.rpc.connection.jsonrpc import SimpleRPC
from plaidcloud.rpc.remote import listener
from plaidcloud.rpc.config import PlaidConfig, PlaidXLConfig

__author__ = 'Charlie Laymon'
__maintainer__ = 'Charlie Laymon <charlie.laymon@tartansolutions.com>'
__copyright__ = '© Copyright 2019-2023, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'


class Connect(PlaidConfig, SimpleRPC):

    """API wrapper class

    Also wraps a configuration object so that configuration is available within the same object

    Example:
        > rpc = Connect()
        > rpc.analyze.query.get_dataframe_from_select(project_id=project_id, query='SELECT 1;')
        [{'SELECT 1': 1}]
    """

    def __init__(self, config_path=None, callable_listener=listener, auto_initialize=True, check_allow_transmit=lambda: True):
        """Sets up initial data

        Args:
            config_path (str, optional): path to config file. Defaults to None
            callable_listener (object): A python module containing a method called get_auth_code, which
                returns an auth code after receiving a response from PlaidCloud. This will
                be run in a separate thread.
            auto_initialize (bool): If True, initialize will automatically be called. If False,
                it must be called manually
            allow_transmit (func): Function to be called before making RPC calls to see if the call should actually
                be made. It must take no args and return a boolean value.
        """

        self.callable_listener = callable_listener
        # CRL 2022 Due to bizarre behavior caused by inheriting from SimpleRPC, we must always pass a function
        # to check_allow_transmit. Otherwise, when PlainRPCCommon checks for the existence of `self.__check_allow_transmit`,
        # it will find (and try to call) a `Namespace` object and fail. Not sure when this behavior was introduced, but
        # ensuring that we pass a function (not None) corrects this behavior.
        self.allow_transmit_func = check_allow_transmit
        PlaidConfig.__init__(self, config_path=config_path)
        if not self.is_local:
            self.ready()
        elif self.is_local and auto_initialize:
            self.initialize()

    def request_oauth_token(self):
        """Requests a Plaid Oauth token using the auth_code flow"""

        # Step 0: Set up a listener to await the redirect from PlaidCloud after step 1
        # TODO set up a listener here. Probably use flask?

        # Step 1: Request an auth code from plaidcloud
        state = str(uuid.uuid4())
        token_params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'scope': 'analyze document identity',
            'state': state
        }
        final_token_uri = self.token_uri + '?' + urlencode(token_params)
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(self.callable_listener.get_auth_code)

        def future_callback(f):
            self.auth_code_callback(f.result())
        future.add_done_callback(future_callback)
        # Now send the request to invoke the listener
        requests.get(final_token_uri)

    def auth_code_callback(self, auth_code):
        """Called when the auth code listener receives a code

        Args:
            auth_code (str): The auth code returned by PlaidCloud"""

        self.auth_code = auth_code
        self.auth_token_from_auth_code(auth_code)
        self.ready()

    def auth_token_from_auth_code(self, authorization_code, client_credentials=False):
        """Exchanges an authorization code for an auth token."""
        post_data = {
            'grant_type': 'client_credentials' if client_credentials else 'authorization_code',
            'client_id': str(self.client_id),
            'client_secret': str(self.client_secret),
        }
        if not client_credentials:
            post_data['code'] = str(authorization_code),
        result = requests.post(self.token_uri, data=post_data)

        if result.status_code != requests.codes.get('ok'):
            raise Exception('Error requesting oauth token from PlaidCloud. Reason: {} ({})'.format(result.reason, result.text))

        self.auth_token = result.json()['access_token']

        # Save token to the config
        self.config['auth_token'] = self.auth_token
        with open(self.cfg_path, 'w') as config_file:
            config_file.write(yaml.safe_dump(self.config))

    def get_auth_token(self):
        post_data = {
            'grant_type': 'client_credentials',
            'client_id': str(self.client_id),
            'client_secret': str(self.client_secret)
        }
        result = requests.post(self.token_uri, data=post_data)
        if result.status_code != requests.codes.get('ok'):
            raise Exception('Error requesting oauth token from PlaidCloud. Reason: {} ({})'.format(result.reason, result.text))

        return result.json()['access_token']

    def initialize(self):
        """Connects to PlaidCloud. If need be, this also sends the user to PlaidCloud to request a token"""

        if self.auth_token:
            self.ready()
        elif self.grant_type == "client_credentials":
            self.auth_token = self.get_auth_token
            self.ready()
        else:
            if not self.auth_code:
                # No code OR token. Run through the whole auth process
                self.request_oauth_token()
            else:
                # We can skip to requesting the token.
                self.auth_token_from_auth_code(self.auth_code)
                self.ready()

    def ready(self):
        """Call SimpleRPC __init__ once we have an auth token"""
        SimpleRPC.__init__(self, self.auth_token, uri=self.rpc_uri, check_allow_transmit=self.allow_transmit_func)


class PlaidXLConnect(SimpleRPC, PlaidXLConfig):
    """Connection class for PlaidXL that takes params in the constructor

    Also wraps a configuration object so that configuration is available within the same object

    Example:
        > pxlrpc = PlaidXLConnect(rpc_uri='https://127.0.0.1/json-rpc', auth_token='mytoken', workspace_id='sometenantid')
        > pxlrpc.analyze.query.get_dataframe_from_select(project_id=project_id, query='SELECT 1;')
        [{'SELECT 1': 1}]
    """

    def __init__(self, *, rpc_uri: str, auth_token: str, workspace_id: str = '', project_id: str = ''):
        PlaidXLConfig.__init__(self, rpc_uri=rpc_uri, auth_token=auth_token, workspace_id=workspace_id, project_id=project_id)
        SimpleRPC.__init__(self, self.auth_token, uri=self.rpc_uri)
