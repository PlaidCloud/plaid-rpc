# coding=utf-8
# pylint: skip-file

from __future__ import absolute_import
from __future__ import print_function
from plaidcloud.rpc.remote.abstract import Abstract

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'


class QueueListener(Abstract):

    def __init__(self, auth, uri=None, verify_ssl=None, proxy_url=None,
                 proxy_user=None, proxy_password=None):
        """Initializes Queue object settings

        Examples:
            >>> from yaml import load
            >>> from plaidtools.remote.auth import oauth2_auth
            >>> config = load(open('/home/plaid/src/plaidtools/plaidtools/tests/test_config_example.yaml', 'r'))
            >>> queue = QueueListener(oauth2_auth(config['auth_token']), 'uri=https://localhost', verify_ssl=False)
            Queue proxy object created.
        """

        # Open a websocket connection
        self.open_web_socket(
            auth=auth,
            callback_type=u'queue_listen',
            uri=uri,
            verify_ssl=verify_ssl,
            proxy_url=proxy_url,
            proxy_user=proxy_user,
            proxy_password=proxy_password
        )

        print('Queue proxy object created.')

    def on_open(self, ws):
        ws.send('ping')

    def on_close(self, ws):
        self.logger.debug('Closing Connection')

    def on_message(self, ws, message):
        try:
            self.logger.debug('RECEIVED - Message from PlaidCloud: {}'.format(message))
            self._execute_task(message)
        except Exception as e:
            # Nacks the message. Use this if there is an issue with processing the data
            ws.send('ack')  # Yes, ack the message even though it failed or this could run forever.  Log error.
            self.logger.exception(e)
        else:
            # Acks the message.
            # Use this _after_ processing the data successfully in order to tell the
            # RabbitMQ server so that it can forget about it.
            ws.send('ack')

    def on_error(self, ws, error):
        self.logger.exception(error)

    def _execute_task(self, message):
        container = self._create_container(message, u'queue')
        container.call_resource(container.url, container.method, 'queue')

        # In case a resource has performed an update to the config
        # we reset the config. If no change is made it doesn't hurt
        # anything. This should actually work without this specific
        # setter since config is passed by reference.
        self.config = container.config

        # See if the system exit has been requested
        if container.method == 'restart':
            reload_config = True
        elif container.method == 'exit':
            self.logger.debug('Agent queue listener is shutting down on request.')
