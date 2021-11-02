#!/usr/bin/env python
# coding=utf-8

import requests

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2021, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

def create_oauth_token(grant_type, client_id, client_secret, scopes='openid', username=None, password=None, uri='https://plaidcloud.com/', realm="PlaidCloud", proxy_settings=None):
    """Attempts to create an Oauth2 auth token using the provided data

    This is designed primarily for password-type grants, but should work for client credentials as well.
    Auth code and implicit grants require a more complex process that involves setting up a redirect URI,
    The aim of this method is to provide a shortcut for creating password/client credentials grants

    Args:
        grant_type (str): Which type of grant to create the token for ('password' or 'client_credentials')
        client_id (str): The ID of the client to associate with this token
        client_secret (str): The secret of the client to associate with this token
        scope (str, optional): Space-delimited list of scopes. Defaults to openid
        username (str, optional): Username to use with password-type grants
        password (str, optional): Password to use with password-type grants
        uri (str, optional): The plaidcloud instance to request a token from. Defaults to prod
        realm (str, optional): The keycloak realm to use.
        proxy_settings (dict, optional): Proxy settings to pass to requests.post

    Returns:
        dict: The response from PlaidCloud after requesting a token.
    """

    # First, do some sanity checking.
    if grant_type not in ['password', 'client_credentials']:
        raise Exception('Invalid grant type. This method is valid for password and client credentials grants only.')

    if grant_type == 'password' and None in [username, password]:
        raise Exception('Missing username or password for password-type grant')

    token_url = f"{uri}/auth/realms/{realm}/protocol/openid-connect/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    # Not using the keycloak client library in order to preserve proxy settings.
    if grant_type == "client_credentials":
        payload = {
            "grant_type": "client_credentials",
            "scope": scopes,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        token = requests.post(token_url, headers=headers, data=payload, proxies=proxy_settings)
    else:
        payload = {
            "grant_type": "password",
            "scope": scopes,
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username,
            "password": password,
        }
        token = requests.post(token_url, headers=headers, data=payload, proxies=proxy_settings)
    token.raise_for_status()
    token = token.json()
    return token["access_token"]


def main():
    """ A simple script to get an auth token from the command line
    """
    # This ideally be needed anymore. If it is, it will need to be updated for keycloak.
    pass
    """grant_type = None
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
    """


if __name__ == '__main__':
    main()
