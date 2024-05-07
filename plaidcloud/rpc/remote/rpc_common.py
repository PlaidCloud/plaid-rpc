#!/usr/bin/env python
# coding=utf-8

# import sys
import traceback
import operator
import asyncio
# import html
from operator import itemgetter
from functools import wraps as _wraps
from functools import partial
import numbers

from toolz.itertoolz import groupby, concat
from toolz.functoolz import identity
# import bleach


WARNING_CODE = -1000
SCRIPT_REGEX = r'<\s*(\/)?\s*s\s*c\s*r\s*i\s*p\s*t\s*>'  # script tag or closing tag, with any amount of whitespace in between (\s*). Use with re.I flag for case-insensitive.

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2021, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'


def rpc_error(message, data=None, code=-32603):
    return {
        'message': message,
        'code': code,
        'data': data,
    }


def get_isolation_from_auth_id(auth_id):
    # TODO: make this actually work?
    # This will be used heavily to associate the auth_id token with an actual user
    # This should probably be a decorator
    return auth_id['workspace'], auth_id['user']


def get_filter_matches(all_values, text, criteria='exact', key=None):
    criteria_fn_map = {
        'exact': operator.eq,
        'equals': operator.eq,
        'startswith': lambda a, text: a.startswith(text) if a else False,
        'endswith': lambda a, text: a.endswith(text) if a else False,
        'contains': lambda a, text: text in a if a else False,
    }

    if criteria not in criteria_fn_map:
        raise Exception('Unsupported filter criteria specified: {}'.format(criteria))

    criteria_fn = criteria_fn_map[criteria]

    if key is None:
        # Simple list elements to check
        fullcriteria_fn = lambda a: criteria_fn(a, text) if a else None
    else:
        # This is a list of dicts.  Check specific key.
        fullcriteria_fn = lambda a: criteria_fn(a[key], text) if a else None

    return list(filter(fullcriteria_fn, all_values))


class RPCError(Exception):
    """An error to forward to json_rpc.

    Other exceptions that make it to the top will be hidden behind a default
    string, defined by package_error.
    """
    def __init__(self, message, data=None, code=-32603):
        self.message = message
        self.code = code
        self.data = data

    def __str__(self):
        return self.message

    def json_error(self):
        return rpc_error(self.message, self.data, self.code)


def subcall(rpc_output):
    """
    If you want to call another rpc_method from within an rpc_method, and do
    something with it's result, but also forward errors, you can wrap it with
    this.

    Args:
        rpc_output (tuple): a tuple of the kind we get from an rpc_method
    Returns:
        the result half of the rpc_output tuple, but only if there's no error in the error half.
    Raises:
        RPCError - made from the error in the error half of the rpc_output tuple, if there is one.
    Example:
        def outer_rpc_method(auth_id, **kwargs):
            ...  # do some processing on kwargs to get newkwargs
            return subcall(basic_rpc_method(auth_id=auth_id, **newkwargs))

        #Note: Below isn't how you'd actually use subcall, but it might help you
        #      understand what it does.
        >>> subcall((True, None))
        (True, None)
        >>> subcall((None, {'message': 'Unknown Error', 'code': -32603, 'data': None}))
        (None, {'message': 'Unknown Error', 'code': -32603, 'data': None})
    """
    return rpc_output
    # result, err = rpc_output
    # if err is None:
    #     return result
    # else:
    #     raise RPCError(**err)


async def cosubcall(rpc_future):
    """
    Just like subcall, except it works inside a coroutine.
    """

    result, err = await rpc_future
    if err is None:
        return result
    else:
        raise RPCError(**err)


def rpc_method(required_scope=None, default_error=None, is_streamed=False, use_thread=False, skip_clean=None, kwarg_transformation=identity):
    """Decorator for packaging up Exceptions as json_rpc errors.

    Notes:
        If use_thread is set to True and using asyncio, then a event loop policy must be used to create an event
        loop in *every* thread.

    Args:
        required_scope (str): The scope required to execute the RPC method
        default_error (str): The error message to log, in the case of an error.
        is_streamed (bool): Is the result to be streamed
        use_thread (bool): Is the RPC method to be executed in a separate thread, default False
        skip_clean (list, optional): A list of args for which cleaning can be skipped. Used for e.g. UDF code, query filters,
        kwarg_transformation (function): Method with which to transform any kwargs
    Returns:
        A decorator function
    Example:
        @package_error('Exception when retrieving project table information')
        def tables(auth_id, project_id, id_filter):
            ...
            return result
    """
    def real_decorator(function):
        """This is the real decorator, with error_string closed over."""

        # PJM - Adding this decorator so sphinx can read actual method docstring
        # TODO: currently we can't do any preprocessing on non keyword
        #  arguments, nor do they ever get passed in, anyway. We might want to
        #  change that, we might not.
        @_wraps(function)
        async def wrapper(**kwargs):
            """This is the wrapper that takes the place of the decorated function, handling errors."""
            # skip_clean_args = skip_clean or []
            processed_kwargs = kwarg_transformation(kwargs)

            # def clean_arg(arg):
            #     if isinstance(arg, str):
            #         return bleach.clean(arg)
            #     elif isinstance(arg, dict):
            #         for dict_arg in arg:
            #             if dict_arg not in skip_clean_args:
            #                 arg[dict_arg] = clean_arg(arg[dict_arg])
            #         return arg
            #     elif isinstance(arg, list):
            #         return [clean_arg(list_arg) for list_arg in arg]
            #     else:
            #         return arg
            #
            # processed_kwargs = clean_arg(processed_kwargs)
            return await function(**processed_kwargs)

        wrapper.rpc_method = True  # Set a flag that we can check for in the json_rpc handler
        wrapper.required_scope = required_scope
        wrapper.default_error = default_error
        wrapper.is_streamed = is_streamed
        wrapper.use_thread = use_thread
        wrapper.skip_clean = skip_clean
        return wrapper

    return real_decorator


async def call_as_coroutine(function, default_error, use_thread, is_streamed, system_user, logger, **kwargs):
    def _get_error_message(exc):
        # if system_user:
        #     return traceback.format_exc()
        # else:
        #     return f'{"Unexpected error" if not default_error else default_error} - {str(exc)}'
        return f'{"Unexpected error" if not default_error else default_error}'
    try:
        if asyncio.iscoroutinefunction(function):
            try:
                if use_thread and not is_streamed:
                    def run_in_thread():
                        return asyncio.get_event_loop().run_until_complete(function(**kwargs))

                    if getattr(asyncio, 'to_thread', None):
                        result = await asyncio.to_thread(run_in_thread)
                    else:
                        result = await asyncio.get_event_loop().run_in_executor(
                            None, run_in_thread
                        )
                else:
                    result = await function(**kwargs)
            except Warning:
                raise
            except RPCError:
                logger.debug(traceback.format_exc())
                raise
            except asyncio.exceptions.TimeoutError:
                raise
            except Exception as coro_exc:
                logger.exception(f'Unhandled exception in RPC method {function.__name__}')
                raise RPCError(message=_get_error_message(coro_exc), code=-32603)
        else:
            # If it's not a coroutine, we need to make it one with run_in_executor
            async def function_with_error_print(**kw):
                # And it's error tracebacks will be unintelligible, so we try to
                # mitigate it a little here.
                try:
                    if getattr(asyncio, 'to_thread', None):
                        rval = await asyncio.to_thread(function, **kw)
                    else:
                        rval = await asyncio.get_event_loop().run_in_executor(
                            None, partial(function, **kw)
                        )
                except Warning:
                    # We don't print a traceback here because this
                    # happens all the time and we don't want to clutter
                    # the logs.
                    raise
                except RPCError:
                    # If it's an explicit RPCError, we'll assume that
                    # the other end has formatted it the way it should
                    # be shown.
                    logger.debug(traceback.format_exc())
                    raise
                except Exception as sync_exc:
                    # If it's an unexpected error, we encode it as an
                    # RPCError with code -32603 so that it can be sent
                    # forward. It includes an entire traceback because
                    # it is an unexpected error (for system_user only)
                    logger.exception(f'Unhandled exception in RPC method {function.__name__}')
                    raise RPCError(message=_get_error_message(sync_exc), code=-32603)
                else:
                    return rval

            result = await function_with_error_print(**kwargs)
        return result, None
    except RPCError as r_exc:
        return None, r_exc.json_error()
    except NotImplementedError:
        return None, rpc_error('Not implemented', code=-32601)
    except Warning as w:
        logger.warning(str(w))
        return None, rpc_error(str(w), code=WARNING_CODE)
    except asyncio.exceptions.TimeoutError:
        raise
    except Exception as outer_exc:
        logger.exception('Unexpected Error calling an RPC method')
        return None, rpc_error(_get_error_message(outer_exc))


def apply_sort(data, sort_keys):
    # Data is a list to be sorted. Sort_keys is a list of tuples (key, reverse)
    # where key is a dict key in a list item, and reverse says whether to sort
    # in reverse order or not. (i.e. False for ascending, True for descending)
    if not sort_keys:
        return data
    else:
        # Parse the first sort_key
        if isinstance(sort_keys[0], str):
            key = sort_keys
            reverse = False
        else:
            key, reverse = sort_keys[0]

        remaining_sort_keys = sort_keys[1:]

        # Sort into groups by this key
        groups = groupby(itemgetter(key), data)

        try:
            key_sample = next((k for k in groups.keys() if k is not None))
        except StopIteration:
            key_sample = None

        if key_sample is None:
            key_fn = lambda _: True
        elif isinstance(key_sample, str):
            key_fn = lambda s: s.lower() if s is not None else ''
        elif isinstance(key_sample, bool):
            key_fn = bool
        elif isinstance(key_sample, numbers.Number):
            key_fn = lambda n: n if n is not None else 0
        else:
            # Unknown, so we'll just use ident
            key_fn = lambda x: x

        sorted_indices = sorted(list(groups.keys()), key=key_fn, reverse=reverse)

        # Sort each group by remaining keys, and concat them together in an
        # order sorted by this key.
        return list(concat(
            apply_sort(groups[index], remaining_sort_keys)
            for index in sorted_indices
        ))
