#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

from plaidcloud.rpc.remote.json_rpc_server import get_callable_object, BASE_MODULE_PATH


__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2018-2020, Tartan Solutions, Inc'
__license__ = 'Proprietary'


def _create_rpc_args(method, params):
    return {
        'id': 0,
        'method': method,
        'params': params,
        'jsonrpc': '2.0',
    }


def get_auth_id(workspace_id, member_id, scopes):
    return {
        'workspace': workspace_id,
        'user': member_id,
        'scopes': scopes,
    }


def direct_rpc(auth_id, method, params):
    callable_object, required_scope, default_error = get_callable_object(method, version=1, base_path=BASE_MODULE_PATH, logger=None)
    result = callable_object(auth_id=auth_id, **params)
    return result


class PlainRPCCommon(object):
    """ A superclass for things like DirectRPC and SimpleRPC that are intended
    to create a dot based api for various kinds of rpc call.
    """
    call_rpc = None

    def __init__(self, call_rpc, check_allow_transmit=None):
        """You will override this in subclasses. Your constructor must create a
        self.call_rpc member, containing a function that accepts method and
        params, makes an rpc call, and returns the result or raises an error.
        Usually you'll be closing over connection information provided to your
        constructor. See DirectRPC and SimpleRPC for examples.
        """
        self.__check_allow_transmit = check_allow_transmit
        self.call_rpc = call_rpc

    def __getattr__(self, key):
        return Namespace(self, self.call_rpc, [key])

    @property
    def allow_transmit(self):
        if self.__check_allow_transmit:
            return self.__check_allow_transmit()
        return True


class Namespace(object):
    __rpc_object = None
    call_rpc = None
    __prefix = ''

    def __init__(self, rpc_object, call_rpc, prefix):
        self.__rpc_object = rpc_object
        self.call_rpc = call_rpc
        self.__prefix = prefix

    def __getattr__(self, key):
        return Object(self, self.call_rpc, self.__prefix+[key])

    @property
    def rpc(self):
        return self.__rpc_object


class Object(object):
    __rpc_namespace = None
    call_rpc = None
    __prefix = ''

    def __init__(self, rpc_namespace, call_rpc, prefix):
        self.__rpc_namespace = rpc_namespace
        self.call_rpc = call_rpc
        self.__prefix = prefix

    def __getattr__(self, key):
        method_path = '/'.join(self.__prefix + [key])

        def callable_method(fire_and_forget=False, **params):
            if not self.namespace.rpc.allow_transmit:
                return
            return self.call_rpc(method_path, params, fire_and_forget=fire_and_forget)

        return callable_method

    @property
    def namespace(self):
        return self.__rpc_namespace


class DirectRPC(PlainRPCCommon):
    """Call local rpc methods with a dot based interface, almost as if they were
    simply functions in modules.

    Example:
    rpc = DirectRPC(auth_id=auth_id)
    scopes = rpc.identity.me.scopes()
    """
    def __init__(self, workspace_id=None, user_id=None, scopes=None, auth_id=None):
        if not auth_id:
            auth_id = get_auth_id(workspace_id, user_id, scopes)

        def call_rpc(method_path, params, fire_and_forget=False):
            # fire_and_forget does nothing here.
            return direct_rpc(auth_id, method_path, params)

        super(DirectRPC, self).__init__(call_rpc)
