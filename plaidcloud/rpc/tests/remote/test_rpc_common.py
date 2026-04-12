#!/usr/bin/env python
# coding=utf-8

import asyncio
import logging
from unittest import mock

import pytest

from plaidcloud.rpc.remote.rpc_common import (
    rpc_error,
    RPCError,
    WARNING_CODE,
    get_filter_matches,
    subcall,
    cosubcall,
    apply_sort,
    get_isolation_from_auth_id,
    rpc_method,
    call_as_coroutine,
)


class TestRpcError:

    def test_default_code(self):
        err = rpc_error('test message')
        assert err['message'] == 'test message'
        assert err['code'] == -32603
        assert err['data'] is None

    def test_custom_code_and_data(self):
        err = rpc_error('msg', data={'key': 'val'}, code=-1000)
        assert err['code'] == -1000
        assert err['data'] == {'key': 'val'}


class TestRPCError:

    def test_message(self):
        exc = RPCError('something broke')
        assert str(exc) == 'something broke'
        assert exc.message == 'something broke'

    def test_default_code(self):
        exc = RPCError('error')
        assert exc.code == -32603

    def test_custom_code(self):
        exc = RPCError('error', code=-1000)
        assert exc.code == -1000

    def test_json_error(self):
        exc = RPCError('msg', data='detail', code=-500)
        result = exc.json_error()
        assert result['message'] == 'msg'
        assert result['data'] == 'detail'
        assert result['code'] == -500

    def test_is_exception(self):
        assert issubclass(RPCError, Exception)


class TestWarningCode:

    def test_warning_code_value(self):
        assert WARNING_CODE == -1000


class TestGetFilterMatches:

    def test_exact_match(self):
        values = ['apple', 'banana', 'cherry']
        result = get_filter_matches(values, 'banana', criteria='exact')
        assert result == ['banana']

    def test_equals_match(self):
        values = ['apple', 'banana']
        result = get_filter_matches(values, 'apple', criteria='equals')
        assert result == ['apple']

    def test_startswith(self):
        values = ['apple', 'apricot', 'banana']
        result = get_filter_matches(values, 'ap', criteria='startswith')
        assert result == ['apple', 'apricot']

    def test_endswith(self):
        values = ['cat', 'bat', 'dog']
        result = get_filter_matches(values, 'at', criteria='endswith')
        assert result == ['cat', 'bat']

    def test_contains(self):
        values = ['foobar', 'bazfoo', 'qux']
        result = get_filter_matches(values, 'foo', criteria='contains')
        assert result == ['foobar', 'bazfoo']

    def test_unsupported_criteria_raises(self):
        with pytest.raises(Exception, match='Unsupported filter criteria'):
            get_filter_matches(['a'], 'a', criteria='regex')

    def test_with_key_parameter(self):
        values = [{'name': 'Alice'}, {'name': 'Bob'}, {'name': 'Alicia'}]
        result = get_filter_matches(values, 'Ali', criteria='startswith', key='name')
        assert len(result) == 2
        assert result[0]['name'] == 'Alice'
        assert result[1]['name'] == 'Alicia'

    def test_no_matches(self):
        values = ['a', 'b', 'c']
        result = get_filter_matches(values, 'z', criteria='exact')
        assert result == []


class TestSubcall:

    def test_passthrough(self):
        result = subcall((True, None))
        assert result == (True, None)

    def test_error_passthrough(self):
        err = {'message': 'Unknown Error', 'code': -32603, 'data': None}
        result = subcall((None, err))
        assert result == (None, err)


class TestApplySort:

    def test_empty_sort_keys(self):
        data = [{'a': 3}, {'a': 1}]
        result = apply_sort(data, [])
        assert result == data

    def test_single_key_ascending(self):
        data = [{'name': 'c'}, {'name': 'a'}, {'name': 'b'}]
        result = apply_sort(data, [('name', False)])
        names = [d['name'] for d in result]
        assert names == ['a', 'b', 'c']

    def test_single_key_descending(self):
        data = [{'name': 'a'}, {'name': 'c'}, {'name': 'b'}]
        result = apply_sort(data, [('name', True)])
        names = [d['name'] for d in result]
        assert names == ['c', 'b', 'a']

    def test_numeric_sort(self):
        data = [{'val': 3}, {'val': 1}, {'val': 2}]
        result = apply_sort(data, [('val', False)])
        vals = [d['val'] for d in result]
        assert vals == [1, 2, 3]

    def test_none_values_handled(self):
        data = [{'val': None}, {'val': 'a'}, {'val': 'b'}]
        result = apply_sort(data, [('val', False)])
        assert len(result) == 3


class TestGetIsolationFromAuthId:

    def test_returns_workspace_and_user(self):
        auth_id = {'workspace': 'ws1', 'user': 'user1'}
        workspace, user = get_isolation_from_auth_id(auth_id)
        assert workspace == 'ws1'
        assert user == 'user1'


class TestCosubcall:

    def test_passthrough_success(self):
        async def make_future():
            return ('result', None)

        result = asyncio.run(cosubcall(make_future()))
        assert result == 'result'

    def test_raises_rpc_error_on_error(self):
        async def make_future():
            return (None, {'message': 'boom', 'code': -1, 'data': None})

        with pytest.raises(RPCError, match='boom'):
            asyncio.run(cosubcall(make_future()))


class TestRpcMethodDecorator:

    def test_sets_rpc_method_flag(self):
        @rpc_method()
        async def my_method(auth_id):
            return 'hi'

        assert my_method.rpc_method is True

    def test_preserves_required_scope(self):
        @rpc_method(required_scope='analyze.read')
        async def my_method(auth_id):
            return 'hi'
        assert my_method.required_scope == 'analyze.read'

    def test_preserves_default_error(self):
        @rpc_method(default_error='Oops')
        async def my_method(auth_id):
            return 'hi'
        assert my_method.default_error == 'Oops'

    def test_preserves_is_streamed(self):
        @rpc_method(is_streamed=True)
        async def my_method(auth_id):
            return 'hi'
        assert my_method.is_streamed is True

    def test_preserves_use_thread(self):
        @rpc_method(use_thread=True)
        async def my_method(auth_id):
            return 'hi'
        assert my_method.use_thread is True

    def test_wrapped_function_invokes(self):
        @rpc_method()
        async def my_method(x):
            return x * 2

        result = asyncio.run(my_method(x=5))
        assert result == 10

    def test_kwarg_transformation(self):
        def transform(kwargs):
            return {k: v.upper() if isinstance(v, str) else v for k, v in kwargs.items()}

        @rpc_method(kwarg_transformation=transform)
        async def my_method(name):
            return name

        assert asyncio.run(my_method(name='foo')) == 'FOO'


class TestCallAsCoroutine:

    def _run(self, *args, **kwargs):
        return asyncio.run(call_as_coroutine(*args, **kwargs))

    def test_sync_function_success(self):
        def fn(x):
            return x + 1

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger, x=5)
        assert result == 6
        assert err is None

    def test_async_function_success(self):
        async def fn(x):
            return x * 3

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger, x=4)
        assert result == 12
        assert err is None

    def test_sync_function_rpc_error(self):
        def fn():
            raise RPCError('explicit', code=-999)

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger)
        assert result is None
        assert err['message'] == 'explicit'
        assert err['code'] == -999

    def test_sync_function_unexpected_error(self):
        def fn():
            raise RuntimeError('something broke')

        logger = logging.getLogger('test')
        result, err = self._run(fn, 'generic error', False, False, False, logger)
        assert result is None
        assert err['message'] == 'generic error'

    def test_async_not_implemented_wrapped(self):
        # Inner try/except wraps all exceptions to -32603 for async fns
        async def fn():
            raise NotImplementedError()

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger)
        assert result is None
        assert err['code'] == -32603

    def test_use_thread_path(self):
        # Covers the use_thread=True branch on async coroutines
        async def fn(x):
            return x * 10

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, True, False, False, logger, x=3)
        assert result == 30
        assert err is None

    def test_timeout_raises(self):
        async def fn():
            raise asyncio.exceptions.TimeoutError()

        logger = logging.getLogger('test')
        with pytest.raises(asyncio.exceptions.TimeoutError):
            self._run(fn, None, False, False, False, logger)

    def test_warning_returns_warning_code(self):
        def fn():
            raise Warning('slow down')

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger)
        assert result is None
        assert err['code'] == WARNING_CODE

    def test_async_rpc_error_passes_through(self):
        async def fn():
            raise RPCError('async bad', code=-111)

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger)
        assert err['code'] == -111

    def test_async_unexpected_error(self):
        async def fn():
            raise RuntimeError('async boom')

        logger = logging.getLogger('test')
        result, err = self._run(fn, 'oops', False, False, False, logger)
        assert err['message'] == 'oops'


class TestApplySortMoreCoverage:

    def test_multiple_keys(self):
        data = [
            {'group': 'b', 'val': 2},
            {'group': 'a', 'val': 1},
            {'group': 'a', 'val': 2},
            {'group': 'b', 'val': 1},
        ]
        result = apply_sort(data, [('group', False), ('val', False)])
        assert result[0] == {'group': 'a', 'val': 1}
        assert result[-1] == {'group': 'b', 'val': 2}

    def test_bool_sort(self):
        data = [{'active': True}, {'active': False}, {'active': True}]
        result = apply_sort(data, [('active', False)])
        # False comes first when ascending
        assert result[0]['active'] is False

    def test_nested_dict_sort(self):
        data = [
            {'group': 'a', 'val': 2},
            {'group': 'a', 'val': 1},
            {'group': 'b', 'val': 1},
        ]
        result = apply_sort(data, [('group', False), ('val', False)])
        assert result[0]['val'] == 1 and result[0]['group'] == 'a'

    def test_all_none_keys_sort(self):
        data = [{'val': None}, {'val': None}]
        result = apply_sort(data, [('val', False)])
        assert len(result) == 2

    def test_sort_unknown_key_type(self):
        """Cover line 306 — unknown (non-str/bool/number) key type uses ident."""
        # Use tuple values as keys
        data = [
            {'k': (1, 2)},
            {'k': (1, 1)},
        ]
        result = apply_sort(data, [('k', False)])
        # Just verify it sorts without error
        assert len(result) == 2


class TestCallAsCoroutineMoreBranches:

    def _run(self, *args, **kwargs):
        return asyncio.run(call_as_coroutine(*args, **kwargs))

    def test_async_warning_logged(self):
        async def fn():
            raise Warning('soft')

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger)
        assert err['code'] == WARNING_CODE

    def test_sync_warning_logged(self):
        def fn():
            raise Warning('soft')

        logger = logging.getLogger('test')
        result, err = self._run(fn, None, False, False, False, logger)
        assert err['code'] == WARNING_CODE

    def test_async_timeout_raises(self):
        async def fn():
            raise asyncio.exceptions.TimeoutError()

        logger = logging.getLogger('test')
        with pytest.raises(asyncio.exceptions.TimeoutError):
            self._run(fn, None, True, False, False, logger)
