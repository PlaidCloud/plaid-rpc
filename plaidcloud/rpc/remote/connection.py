# coding=utf-8

from __future__ import absolute_import
__author__ = 'Adams Tower'
__maintainer__ = 'Adams Tower <adams.tower@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'

from plaidcloud.rpc.remote.connect import quick_connect, request_cb, requests_cb


def get_connection_params(auth, cloud, connection, base_uri=None, verify_ssl=None):
    """Returns connection_params from a connection id"""

    return quick_connect(
        auth=auth,
        callback_type=u'connection',
        run=request_cb({
            'method': 'get',
            'resource': 'connection',
            'cloud': cloud,
            'connection': connection,
        }),
        uri=base_uri,  # Let the connect method add the /socket path
        verify_ssl=verify_ssl,
    )


def get_connection_params_map(auth, cloud, connection_map, base_uri=None, verify_ssl=None):
    """Given a keyed map of connection ids, returns a similarly keyed map of
    connection params."""

    return quick_connect(
        auth=auth,
        callback_type=u'connection',
        run=requests_cb({
            key: {
                'method': 'get',
                'resource': 'connection',
                'cloud': cloud,
                'connection': connection,
            }
            for key, connection in connection_map.items()
        }),
        uri=base_uri,  # Let the connect method add the /socket path
        verify_ssl=verify_ssl,
    )
