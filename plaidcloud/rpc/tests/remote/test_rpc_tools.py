#!/usr/bin/env python
# coding=utf-8

import asyncio
import os
from unittest import mock

import pytest

from plaidcloud.rpc.remote.rpc_tools import (
    _create_rpc_args,
    get_auth_id,
    PlainRPCCommon,
    Namespace,
    Object,
    DirectRPC,
    direct_rpc,
    direct_rpc_async,
)


class TestCreateRpcArgs:

    def test_basic_structure(self):
        result = _create_rpc_args('analyze/project/list', {'filter': 'all'})
        assert result['id'] == 0
        assert result['method'] == 'analyze/project/list'
        assert result['params'] == {'filter': 'all'}
        assert result['jsonrpc'] == '2.0'

    def test_empty_params(self):
        result = _create_rpc_args('some/method', {})
        assert result['params'] == {}


class TestGetAuthId:

    def test_basic_structure(self):
        result = get_auth_id('ws123', 'member456', ['read', 'write'])
        assert result['workspace'] == 'ws123'
        assert result['user'] == 'member456'
        assert result['scopes'] == ['read', 'write']


class TestPlainRPCCommon:

    def test_init(self):
        call_fn = lambda method, params, fire_and_forget=False: None
        rpc = PlainRPCCommon(call_fn)
        assert rpc.call_rpc is call_fn

    def test_allow_transmit_default_true(self):
        rpc = PlainRPCCommon(lambda m, p, fire_and_forget=False: None)
        assert rpc.allow_transmit is True

    def test_allow_transmit_with_checker(self):
        rpc = PlainRPCCommon(lambda m, p, fire_and_forget=False: None, check_allow_transmit=lambda: False)
        assert rpc.allow_transmit is False

    def test_getattr_returns_namespace(self):
        rpc = PlainRPCCommon(lambda m, p, fire_and_forget=False: None)
        ns = rpc.analyze
        assert isinstance(ns, Namespace)


class TestNamespace:

    def test_getattr_returns_object(self):
        call_fn = lambda method, params, fire_and_forget=False: None
        rpc = PlainRPCCommon(call_fn)
        ns = rpc.analyze
        obj = ns.project
        assert isinstance(obj, Object)

    def test_rpc_property(self):
        call_fn = lambda method, params, fire_and_forget=False: None
        rpc = PlainRPCCommon(call_fn)
        ns = rpc.analyze
        assert ns.rpc is rpc


class TestObject:

    def test_getattr_returns_callable(self):
        call_fn = lambda method, params, fire_and_forget=False: method
        rpc = PlainRPCCommon(call_fn)
        method = rpc.analyze.project.list
        assert callable(method)

    def test_callable_invokes_call_rpc(self):
        calls = []

        def call_fn(method, params, fire_and_forget=False):
            calls.append((method, params))
            return 'result'

        rpc = PlainRPCCommon(call_fn)
        result = rpc.analyze.project.list(filter='all')
        assert result == 'result'
        assert len(calls) == 1
        assert calls[0][0] == 'analyze/project/list'
        assert calls[0][1] == {'filter': 'all'}

    def test_namespace_property(self):
        call_fn = lambda method, params, fire_and_forget=False: None
        rpc = PlainRPCCommon(call_fn)
        obj = rpc.analyze.project
        assert obj.namespace is not None

    def test_allow_transmit_check(self):
        calls = []

        def call_fn(method, params, fire_and_forget=False):
            calls.append(method)

        rpc = PlainRPCCommon(call_fn, check_allow_transmit=lambda: False)
        result = rpc.analyze.project.list()
        assert result is None
        assert len(calls) == 0


class TestDirectRpc:

    def test_calls_sync_callable(self):
        mock_callable = mock.Mock(return_value='result')
        mock_callable.rpc_method = True
        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(mock_callable, None, None, False, False),
        ):
            result = direct_rpc(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {'x': 1},
            )
        assert result == 'result'
        mock_callable.assert_called_once()

    def test_sync_with_logger(self):
        mock_callable = mock.Mock(return_value='res')
        mock_callable.rpc_method = True
        mock_logger = mock.Mock()
        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(mock_callable, None, None, False, False),
        ):
            result = direct_rpc(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
                logger=mock_logger,
                sequence=1,
            )
        assert result == 'res'
        # logger should be called for start and finish
        assert mock_logger.info.call_count >= 2

    def test_async_callable_is_awaited(self):
        async def async_callable(auth_id, **params):
            return 'async-result'
        async_callable.rpc_method = True

        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(async_callable, None, None, False, False),
        ):
            result = direct_rpc(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
            )
        assert result == 'async-result'


class TestDirectRpcAsync:

    def test_sync_callable(self):
        mock_callable = mock.Mock(return_value='res')
        mock_callable.rpc_method = True
        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(mock_callable, None, None, False, False),
        ):
            result = asyncio.run(direct_rpc_async(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
            ))
        assert result == 'res'

    def test_async_callable(self):
        async def async_callable(auth_id, **params):
            return 'async-res'
        async_callable.rpc_method = True

        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(async_callable, None, None, False, False),
        ):
            result = asyncio.run(direct_rpc_async(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
            ))
        assert result == 'async-res'


class TestDirectRPC:

    def test_init_with_auth_id(self):
        rpc = DirectRPC(auth_id={'workspace': 'w', 'user': 'u', 'scopes': []})
        assert rpc is not None

    def test_init_constructs_auth_id(self):
        rpc = DirectRPC(workspace_id='w', user_id='u', scopes=[])
        assert rpc is not None

    def test_dot_access_routes_via_call_rpc(self):
        mock_callable = mock.Mock(return_value='ok')
        mock_callable.rpc_method = True
        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(mock_callable, None, None, False, False),
        ):
            rpc = DirectRPC(auth_id={'workspace': 'w', 'user': 'u', 'scopes': []})
            result = rpc.analyze.project.list()
        assert result == 'ok'

    def test_use_async(self):
        # Ensure the async path is selected and is coroutine
        rpc = DirectRPC(
            auth_id={'workspace': 'w', 'user': 'u', 'scopes': []},
            use_async=True,
        )
        mock_callable = mock.AsyncMock(return_value='async-res')
        mock_callable.rpc_method = True
        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(mock_callable, None, None, False, False),
        ):
            coro = rpc.analyze.project.list()
            assert asyncio.iscoroutine(coro)
            result = asyncio.run(coro)
        assert result == 'async-res'


class TestDirectRpcGenerator:
    """Test the generator branch of direct_rpc (streaming)."""

    def test_sync_generator_writes_tempfile(self, tmp_path):
        def gen(auth_id, **kwargs):
            yield b'chunk1'
            yield b'chunk2'
        gen.rpc_method = True

        # Ensure the temp download folder exists
        import tempfile as tf
        download_folder = os.path.join(tf.gettempdir(), "plaid/download")
        os.makedirs(download_folder, exist_ok=True)

        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(gen, None, None, False, False),
        ):
            result = direct_rpc(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
            )
        # Result is a temp file path
        assert isinstance(result, str)
        with open(result, 'rb') as fp:
            content = fp.read()
        assert content == b'chunk1chunk2'
        os.remove(result)


class TestDirectRpcAsyncExtra:

    def test_use_thread_branch(self):
        async def async_callable(auth_id, **params):
            return 'thread-res'
        async_callable.rpc_method = True

        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(async_callable, None, None, False, True),
        ):
            result = asyncio.run(direct_rpc_async(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
            ))
        assert result == 'thread-res'

    def test_generator_branch(self, tmp_path):
        def gen(auth_id, **kwargs):
            yield b'async-chunk'
        gen.rpc_method = True

        import tempfile as tf
        download_folder = os.path.join(tf.gettempdir(), "plaid/download")
        os.makedirs(download_folder, exist_ok=True)

        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(gen, None, None, False, False),
        ):
            result = asyncio.run(direct_rpc_async(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
            ))
        assert isinstance(result, str)
        os.remove(result)

    def test_logger_records_start_finish(self):
        async def async_callable(auth_id):
            return 'res'
        async_callable.rpc_method = True
        mock_logger = mock.Mock()

        with mock.patch(
            'plaidcloud.rpc.remote.rpc_tools.get_callable_object',
            return_value=(async_callable, None, None, False, False),
        ):
            asyncio.run(direct_rpc_async(
                {'workspace': 'w', 'user': 'u', 'scopes': []},
                'some/method',
                {},
                logger=mock_logger,
                sequence=42,
            ))
        assert mock_logger.info.call_count >= 2
