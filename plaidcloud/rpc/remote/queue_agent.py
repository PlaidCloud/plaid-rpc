# coding=utf-8

from __future__ import absolute_import
from plaidcloud.rpc.remote.abstract import Abstract
from plaidcloud.rpc.remote.connect import quick_connect, send_as_json

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'


def quick_add(auth, cloud, agent_id, resource, method, data=None, action=None,
              uri=None, verify_ssl=None):
    """Adds a message to an agent queue, over a websocket."""

    def add(ws):
        send_as_json(ws, {
            'method': 'post',
            'resource': 'message',
            'params': {
                'cloud': cloud,
                'agent_id': agent_id,
                'resource': resource,
                'method': method,
                'data': data,
                'action': action,
            }
        })

    quick_connect(
        auth=auth,
        callback_type=u'queue_agent',
        run=add,
        uri=uri,
        verify_ssl=verify_ssl,
    )


class QueueAgent(Abstract):

    def __init__(self, auth, on_open=None, uri=None, verify_ssl=None,
                 proxy_url=None, proxy_user=None, proxy_password=None):
        """Initializes Queue object settings"""

        def real_on_open(ws):
            return on_open(self, ws)

        # Open a websocket connection
        self.open_web_socket(
            auth=auth,
            callback_type=u'queue_agent',
            on_open=real_on_open,
            uri=uri,
            verify_ssl=verify_ssl,
            proxy_url=proxy_url,
            proxy_user=proxy_user,
            proxy_password=proxy_password,
        )

    def add(self, cloud, agent_id, resource, method, data=None, action=None):
        """Adds message to queue, remotely

        @type   resource:   str
        @param  resource:   Plaid resource to request

        @type   cloud:   int
        @param  cloud:   Cloud ID

        @type   method:   str
        @param  method:   HTTP Method (get, post, put, delete, head)

        @type   data:   dict
        @param  data:   Data dict

        @type   action:   dict
        @param  action:   Action dict
        """

        self.send({
            'method': 'post',
            'resource': 'message',
            'params': {
                'cloud': cloud,
                'agent_id': agent_id,
                'resource': resource,
                'method': method,
                'data': data,
                'action': action,
            }
        })
