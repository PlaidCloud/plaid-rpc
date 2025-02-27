#!/usr/bin/env python
# coding=utf-8

import asyncio
import itertools
import os
import types
import tempfile

from plaidcloud.rpc.remote.json_rpc_server import get_callable_object, BASE_MODULE_PATH


__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2018-2021, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'


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


def direct_rpc(auth_id, method, params, logger=None, sequence=None):
    callable_object, required_scope, default_error, is_streamed, use_thread = get_callable_object(
        method, version=1, base_path=BASE_MODULE_PATH, logger=logger
    )
    if logger:
        logger.info(f'Start "{method}" {sequence if sequence is not None else ""}')
    try:
        if asyncio.iscoroutinefunction(callable_object):
            result = asyncio.get_event_loop().run_until_complete(callable_object(auth_id=auth_id, **params))
        else:
            result = callable_object(auth_id=auth_id, **params)
        if isinstance(result, types.GeneratorType):
            download_folder = os.path.join(tempfile.gettempdir(), "plaid/download")
            handle, file_name = tempfile.mkstemp(dir=download_folder, prefix="download_", suffix=".tmp")
            os.close(handle)  # Can't control the access mode, so close this one and open another.
            with open(file_name, 'wb', buffering=1) as tmp_file:
                for chunk in result:
                    tmp_file.write(chunk)
            return file_name
        return result
    finally:
        if logger:
            logger.info(f'Finish "{method}" {sequence if sequence is not None else ""}')


async def direct_rpc_async(auth_id, method, params, logger=None, sequence=None):
    callable_object, required_scope, default_error, is_streamed, use_thread = get_callable_object(
        method, version=1, base_path=BASE_MODULE_PATH, logger=logger
    )
    if logger:
        logger.info(f'Start "{method}" {sequence if sequence is not None else ""}')
    try:
        if asyncio.iscoroutinefunction(callable_object):
            if use_thread:
                def run_in_thread():
                    return asyncio.get_event_loop().run_until_complete(callable_object(auth_id=auth_id, **params))
                result = await asyncio.to_thread(run_in_thread)
            else:
                result = await callable_object(auth_id=auth_id, **params)
        else:
            result = callable_object(auth_id=auth_id, **params)
        if isinstance(result, types.GeneratorType):
            download_folder = os.path.join(tempfile.gettempdir(), "plaid/download")
            handle, file_name = tempfile.mkstemp(dir=download_folder, prefix="download_", suffix=".tmp")
            os.close(handle)  # Can't control the access mode, so close this one and open another.
            with open(file_name, 'wb', buffering=1) as tmp_file:
                for chunk in result:
                    tmp_file.write(chunk)
            return file_name
        return result
    finally:
        if logger:
            logger.info(f'Finish "{method}" {sequence if sequence is not None else ""}')


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
    sequence = itertools.count()

    def __init__(self, workspace_id=None, user_id=None, scopes=None, auth_id=None, use_async=False, logger=None):
        if not auth_id:
            auth_id = get_auth_id(workspace_id, user_id, scopes)

        def call_rpc(method_path, params, fire_and_forget=False):
            # fire_and_forget does nothing here.
            if use_async:
                return direct_rpc_async(auth_id, method_path, params, logger, next(self.sequence))
            return direct_rpc(auth_id, method_path, params, logger, next(self.sequence))

        super(DirectRPC, self).__init__(call_rpc)
