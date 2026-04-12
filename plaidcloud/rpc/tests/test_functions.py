#!/usr/bin/env python
# coding=utf-8

import asyncio
import logging
from unittest import mock

import pytest

from plaidcloud.rpc.functions import (
    try_except,
    remove_all,
    regex_map,
    RegexMapKeyError,
    map_across_table,
    getchain,
    deepmerge,
    nd_union,
    any as deprecated_any,
    all as deprecated_all,
    ident,
    compose,
    flatten,
    ForkFailure,
    gather_with_semaphore,
    fork_apply,
)


class TestTryExcept:

    def test_success_returns_result(self):
        assert try_except(lambda: 42, 'fail') == 42

    def test_success_with_zero(self):
        assert try_except(lambda: 0, 'fail') == 0

    def test_failure_returns_value(self):
        assert try_except(lambda: 1/0, 'fallback') == 'fallback'

    def test_failure_calls_callable(self):
        assert try_except(lambda: 1/0, lambda: 'computed') == 'computed'

    def test_failure_returns_none_value(self):
        assert try_except(lambda: 1/0, None) is None

    def test_success_none_result(self):
        assert try_except(lambda: None, 'fail') is None


class TestRemoveAll:

    def test_empty_substrs(self):
        assert remove_all('hello world', []) == 'hello world'

    def test_single_char_removal(self):
        assert remove_all('h!e!l!l!o', ['!']) == 'hello'

    def test_multiple_substrs(self):
        assert remove_all('a?b!c?d!', ['!', '?']) == 'abcd'

    def test_word_removal(self):
        assert remove_all('Four score and seven years ago', ['score and seven ']) == 'Four years ago'

    def test_no_match(self):
        assert remove_all('hello', ['x', 'y']) == 'hello'


class TestRegexMap:

    def test_basic_dict_mapping(self):
        lookup = regex_map({r'^foo$': 'bar', r'^baz$': 'qux'})
        assert lookup('foo') == 'bar'
        assert lookup('baz') == 'qux'

    def test_regex_pattern_matching(self):
        lookup = regex_map({r'^int\d+$': 'integer', r'^str.*': 'string'})
        assert lookup('int32') == 'integer'
        assert lookup('string_value') == 'string'

    def test_no_match_raises_key_error(self):
        lookup = regex_map({r'^foo$': 'bar'})
        with pytest.raises(RegexMapKeyError):
            lookup('nomatch')

    def test_list_of_tuples_mapping(self):
        lookup = regex_map([(r'^a$', 1), (r'^b$', 2)])
        assert lookup('a') == 1
        assert lookup('b') == 2

    def test_regex_map_key_error_is_key_error(self):
        assert issubclass(RegexMapKeyError, KeyError)


class TestMapAcrossTable:

    def test_basic_table(self):
        rows = [[1, 2], [3, 4]]
        result = map_across_table(lambda x: x * 2, rows)
        assert result == [[2, 4], [6, 8]]

    def test_empty_table(self):
        assert map_across_table(lambda x: x, []) == []

    def test_string_transform(self):
        rows = [['a', 'b'], ['c', 'd']]
        result = map_across_table(str.upper, rows)
        assert result == [['A', 'B'], ['C', 'D']]


class TestGetchain:

    def test_first_key_found(self):
        assert getchain({'a': 1, 'b': 2}, ['a', 'b']) == 1

    def test_second_key_found(self):
        assert getchain({'b': 2}, ['a', 'b']) == 2

    def test_no_key_found_returns_default(self):
        assert getchain({'c': 3}, ['a', 'b'], default='none') == 'none'

    def test_no_key_found_default_none(self):
        assert getchain({}, ['a']) is None

    def test_empty_keys(self):
        assert getchain({'a': 1}, [], default='default') == 'default'


class TestDeepmerge:

    def test_simple_merge(self):
        result = deepmerge({'a': 1}, {'b': 2})
        assert result == {'a': 1, 'b': 2}

    def test_override_value(self):
        result = deepmerge({'a': 1}, {'a': 2})
        assert result == {'a': 2}

    def test_nested_merge(self):
        default = {'sort': {'column': '', 'direction': 'desc'}}
        override = {'sort': {'column': 'name'}}
        result = deepmerge(default, override)
        assert result == {'sort': {'column': 'name', 'direction': 'desc'}}

    def test_non_dict_wins(self):
        result = deepmerge({'a': {'b': 1}}, {'a': 'flat'})
        assert result == {'a': 'flat'}

    def test_preserves_defaults(self):
        default = {'a': 1, 'b': 2}
        override = {'b': 3}
        result = deepmerge(default, override)
        assert result == {'a': 1, 'b': 3}


class TestDeprecatedFunctions:

    def test_nd_union_raises(self):
        with pytest.raises(NotImplementedError):
            nd_union({'a': 1}, {'b': 2})

    def test_any_raises(self):
        with pytest.raises(NotImplementedError):
            deprecated_any([True])

    def test_all_raises(self):
        with pytest.raises(NotImplementedError):
            deprecated_all([True])

    def test_ident_raises(self):
        with pytest.raises(NotImplementedError):
            ident(1)

    def test_compose_raises(self):
        with pytest.raises(NotImplementedError):
            compose(lambda x: x)

    def test_flatten_raises(self):
        with pytest.raises(NotImplementedError):
            flatten([[1, 2], [3]])


class TestForkFailure:

    def test_str_representation(self):
        exc = ForkFailure('test error')
        assert 'test error' in str(exc)

    def test_value_attribute(self):
        exc = ForkFailure('test')
        assert exc.value == 'test'


class TestGatherWithSemaphore:

    def test_basic_gather(self):
        async def produce(value):
            return value

        result = asyncio.run(gather_with_semaphore(
            produce(1), produce(2), produce(3),
            concurrent_tasks=2,
        ))
        assert sorted(result) == [1, 2, 3]

    def test_respects_concurrent_limit(self):
        active = {'count': 0, 'max': 0}

        async def task():
            active['count'] += 1
            active['max'] = max(active['max'], active['count'])
            await asyncio.sleep(0.01)
            active['count'] -= 1
            return 'done'

        results = asyncio.run(gather_with_semaphore(
            task(), task(), task(), task(), task(),
            concurrent_tasks=2,
        ))
        assert len(results) == 5
        assert active['max'] <= 2


class TestForkApply:
    """fork_apply uses multiprocessing which can't pickle local closures on
    macOS (spawn default). Tests mock the subprocess layer rather than
    actually forking."""

    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Process')
    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Pipe')
    def test_returns_result_on_success(self, mock_pipe, mock_process_cls):
        parent, child = mock.MagicMock(), mock.MagicMock()
        mock_pipe.return_value = (parent, child)
        parent.recv.return_value = (True, 42)

        def fn(x):
            return x * 2
        assert fork_apply(fn, [21]) == 42
        mock_process_cls.return_value.start.assert_called_once()
        mock_process_cls.return_value.join.assert_called_once()

    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Process')
    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Pipe')
    def test_failure_raises_fork_failure(self, mock_pipe, mock_process_cls):
        parent, child = mock.MagicMock(), mock.MagicMock()
        mock_pipe.return_value = (parent, child)
        parent.recv.return_value = (False, (RuntimeError, 'traceback text'))

        with pytest.raises(ForkFailure):
            fork_apply(lambda: None, [])

    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Process')
    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Pipe')
    def test_failure_logs_traceback(self, mock_pipe, mock_process_cls):
        parent, child = mock.MagicMock(), mock.MagicMock()
        mock_pipe.return_value = (parent, child)
        parent.recv.return_value = (False, (ValueError, 'traceback text'))

        mock_logger = mock.Mock()
        with pytest.raises(ForkFailure):
            fork_apply(lambda: None, [], logger=mock_logger)
        mock_logger.error.assert_called()

    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Process')
    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Pipe')
    def test_failure_no_traceback_still_logs(self, mock_pipe, mock_process_cls):
        parent, child = mock.MagicMock(), mock.MagicMock()
        mock_pipe.return_value = (parent, child)
        parent.recv.return_value = (False, (ValueError, ''))

        mock_logger = mock.Mock()
        with pytest.raises(ForkFailure):
            fork_apply(lambda: None, [], logger=mock_logger)
        mock_logger.error.assert_called()

    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Process')
    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Pipe')
    def test_failure_without_logger_still_raises(self, mock_pipe, mock_process_cls):
        parent, child = mock.MagicMock(), mock.MagicMock()
        mock_pipe.return_value = (parent, child)
        parent.recv.return_value = (False, (ValueError, 'tb text'))

        # No logger provided — should still raise ForkFailure
        with pytest.raises(ForkFailure):
            fork_apply(lambda: None, [])


class TestGatherWithSemaphoreEdgeCases:

    def test_no_futures(self):
        # Empty future list should return empty list
        result = asyncio.run(gather_with_semaphore(concurrent_tasks=3))
        assert result == []


class TestForkApplyShimCoverage:
    """Coverage for the inner shim() function by invoking Process.target."""

    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Pipe')
    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Process')
    def test_shim_success_path(self, mock_process_cls, mock_pipe):
        parent, child = mock.MagicMock(), mock.MagicMock()
        mock_pipe.return_value = (parent, child)

        shim_calls = []

        def fake_process_init(target=None, args=None):
            # Capture and invoke the shim function so lines 394-401 run
            shim_calls.append((target, args))
            proc = mock.MagicMock()

            def fake_start():
                target(*args)
            proc.start = fake_start
            return proc

        mock_process_cls.side_effect = fake_process_init
        # recv returns the parent's perspective after shim runs
        parent.recv.return_value = (True, 6)

        def my_fn(x, y):
            return x + y

        result = fork_apply(my_fn, [2, 4])
        assert result == 6
        # shim should have been invoked with (child, args)
        assert len(shim_calls) == 1
        child_conn_passed = shim_calls[0][1][0]
        # shim should call child_conn.send with success tuple
        child_conn_passed.send.assert_called_with((True, 6))

    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Pipe')
    @mock.patch('plaidcloud.rpc.functions.multiprocessing.Process')
    def test_shim_exception_path(self, mock_process_cls, mock_pipe):
        parent, child = mock.MagicMock(), mock.MagicMock()
        mock_pipe.return_value = (parent, child)

        shim_outputs = []

        def fake_process_init(target=None, args=None):
            proc = mock.MagicMock()

            def fake_start():
                target(*args)  # run shim synchronously
            proc.start = fake_start
            return proc

        mock_process_cls.side_effect = fake_process_init
        # Parent receives the failure tuple that shim would have sent
        parent.recv.return_value = (False, (RuntimeError, 'tb text'))

        def boom():
            raise RuntimeError('real boom')

        with pytest.raises(ForkFailure):
            fork_apply(boom, [])
        # Verify shim called send with False tuple
        send_calls = child.send.call_args_list
        assert any(not call.args[0][0] for call in send_calls)
