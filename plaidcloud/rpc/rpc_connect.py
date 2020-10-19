#!/usr/bin/env python
# coding=utf-8

"""Basic class that allows for a handler that wraps the plaid RPC API.
   Gracefully handles oauth token generation."""

from __future__ import absolute_import

import os
import yaml
import orjson as json
import requests
import uuid
from concurrent.futures import ThreadPoolExecutor

import six
import six.moves.urllib_parse
from plaidcloud.rpc.connection.jsonrpc import SimpleRPC
from plaidcloud.rpc.remote import listener
from plaidcloud.rpc.config import PlaidConfig

__author__ = 'Charlie Laymon'
__maintainer__ = 'Charlie Laymon <charlie.laymon@tartansolutions.com>'
__copyright__ = '© Copyright 2019-2020, Tartan Solutions, Inc'
__license__ = 'Proprietary'


class Connect(SimpleRPC, PlaidConfig):

    """API wrapper class

    Also wraps a configuration object so that configuration is available within the same object

    Example:
        > rpc = Connect()
        > rpc.analyze.query.get_dataframe_from_select(project_id=project_id, query='SELECT 1;')
        [{'SELECT 1': 1}]
    """

    def __init__(self, config_path=None, callable_listener=listener, auto_initialize=True):
        """Sets up initial data

        Args:
            config_path (str, optional): path to config file. Defaults to None
            callable_listener (object): A python module containing a method called get_auth_code, which
                returns an auth code after receiving a response from PlaidCloud. This will
                be run in a separate thread.
            auto_initialize (bool): If True, initialize will automatically be called. If False,
                it must be called manually
        """

        self.callable_listener = callable_listener
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
        final_token_uri = self.token_uri + '?' + six.moves.urllib_parse.urlencode(token_params)
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

    def auth_token_from_auth_code(self, authorization_code):
        """Exchanges an authorization code for an auth token."""
        post_data = {
            'grant_type': 'authorization_code',
            'client_id': str(self.client_id),
            'client_secret': str(self.client_secret),
            'code': str(authorization_code),
        }
        result = requests.post(self.token_uri, data=post_data)

        if result.status_code != requests.codes.get('ok'):
            raise Exception('Error requesting oauth token from PlaidCloud. Reason: {} ({})'.format(result.reason, result.text))

        self.auth_token = result.json()['access_token']

        # Save token to the config
        self.config['auth_token'] = self.auth_token
        with open(self.cfg_path, 'w') as config_file:
            config_file.write(yaml.safe_dump(self.config))

    def initialize(self):
        """Connects to PlaidCloud. If need be, this also sends the user to PlaidCloud to request a token"""

        if not self.auth_token:
            if not self.auth_code:
                # No code OR token. Run through the whole auth process
                self.request_oauth_token()
            else:
                # We can skip to requesting the token.
                self.auth_token_from_auth_code(self.auth_code)
                self.ready()
        else:
            self.ready()

    def ready(self):
        """Call SimpleRPC __init__ once we have an auth token"""
        SimpleRPC.__init__(self, self.auth_token, uri=self.rpc_uri, workspace=self.workspace_id)
