#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import
from __future__ import print_function
import requests
from six.moves import input


def create_oauth_token(grant_type, client_id, client_secret, workspace_id=None, scopes='', username=None, password=None, uri='https://plaidcloud.com/oauth/token', proxy_settings=None):
    """Attempts to create an Oauth2 auth token using the provided data

    This is designed primarily for password-type grants, but should work for client credentials as well.
    Auth code and implicit grants require a more complex process that involves setting up a redirect URI,
    The aim of this method is to provide a shortcut for creating password/client credentials grants

    Args:
        grant_type (str): Which type of grant to create the token for ('password' or 'client_credentials')
        client_id (str): The ID of the client to associate with this token
        client_secret (str): The secret of the client to associate with this token
        workspace_id (int): The ID of the workspace to authorize this token to access
        scope (str, optional): Space-delimited list of scopes. Currently unused
        username (str, optional): Username to use with password-type grants
        password (str, optional): Password to use with password-type grants
        uri (str, optional): The URI to request a token from. Defaults to prod
        proxy_settings (dict, optional): Proxy settings to pass to requests.post

    Returns:
        dict: The response from PlaidCloud after requesting a token.
    """

    # First, do some sanity checking.
    if grant_type not in ['password', 'client_credentials']:
        raise Exception('Invalid grant type. This method is valid for password and client credentials grants only.')

    if grant_type == 'password' and None in [username, password]:
        raise Exception('Missing username or password for password-type grant')

    post_data = {
        'grant_type': grant_type,
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scopes,
        'token_data': None,
    }

    if grant_type == 'password':
        post_data['username'] = username
        post_data['password'] = password

    if workspace_id is not None:
        token_data = {'workspace_id': workspace_id}
        post_data['token_data'] = token_data

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.post(uri, data=post_data, headers=headers, proxies=proxy_settings, verify=False)

    if response.status_code == requests.codes['ok']:
        token = response.json()
        return token
    else:
        raise Exception("Cannot get oAuth token for this client.")


def main():
    """ A simple script to get an auth token from the command line
    """
    grant_type = None
    username = None
    password = None
    while grant_type not in ['password', 'client_credentials']:
        grant_type = str(input('Enter grant type to use for this token ("password" or "client_credentials"): ')).strip()

    client_id = str(input('Enter PlaidCloud Client ID: ')).strip()
    client_secret = str(input('Enter PlaidCloud Client Secret: ')).strip()
    workspace_id = str(input('Enter the workspace ID to use with this token: ')).strip()

    if grant_type == 'password':
        username = str(input('Enter username: ')).strip()
        password = str(input('Enter password: ')).strip()

    print(('Received token from PlaidCloud: {}'.format(create_oauth_token(grant_type, client_id, client_secret, workspace_id=workspace_id, username=username, password=password))))


if __name__ == '__main__':
    main()
