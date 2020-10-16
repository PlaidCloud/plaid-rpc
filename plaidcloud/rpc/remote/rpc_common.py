#!/usr/bin/env python
# coding=utf-8

from __future__ import print_function

from __future__ import absolute_import
import sys
import traceback
import operator
from operator import itemgetter
from functools import wraps as _wraps
from six import string_types

from tornado import gen
from tornado.ioloop import IOLoop

import functools
from toolz.itertoolz import groupby, concat
from toolz.functoolz import identity

import logging
from six.moves import filter

logger = logging.getLogger(__name__)

WARNING_CODE = -1000


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
    return (auth_id['workspace'], auth_id['user'])


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


@gen.coroutine
def cosubcall(rpc_future):
    """
    Just like subcall, except it works inside a coroutine.
    """

    result, err = yield rpc_future
    if err is None:
        raise gen.Return(result)
    else:
        raise RPCError(**err)


def rpc_method(required_scope=None, default_error=None, kwarg_transformation=identity):
    """Decorator for packaging up Exceptions as json_rpc errors.

    Args:
        default_error: The error message to log, in the case of an error.
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
        def wrapper(**kwargs):
            """This is the wrapper that takes the place of the decorated function, handling errors."""
            processed_kwargs = kwarg_transformation(kwargs)
            return function(**processed_kwargs)

        wrapper.rpc_method = True  # Set a flag that we can check for in the json_rpc handler
        wrapper.required_scope = required_scope
        wrapper.default_error = default_error
        return wrapper

    return real_decorator


@gen.coroutine
def call_as_coroutine(function, default_error, **kwargs):
    try:
        if gen.is_coroutine_function(function):
            result = yield function(**kwargs)
        else:
            # If it's not a coroutine, we need to make it one with run_in_executor
            def function_with_error_print(**kwargs):
                # And it's error tracebacks will be unintelligible, so we try to
                # mitigate it a little here.
                try:
                    rval = function(**kwargs)
                except Warning:
                    # We don't print a traceback here because this
                    # happens all the time and we don't want to clutter
                    # the logs.
                    raise
                except RPCError:
                    # If it's an explicit RPCError, we'll assume that
                    # the other end has formatted it the way it should
                    # be shown.
                    traceback.print_exc(file=sys.stderr)
                    raise
                except:
                    # If it's an unexpected error, we encode it as an
                    # RPCError with code -32603 so that it can be sent
                    # forward. It includes an entire traceback because
                    # it is an unexpected error.
                    traceback.print_exc(file=sys.stderr)
                    raise RPCError(message=traceback.format_exc(), code=-32603)
                else:
                    return rval

            result = yield IOLoop.current().run_in_executor(
                None, functools.partial(function_with_error_print, **kwargs)
            )

        raise gen.Return((result, None))
    except RPCError as e:
        raise gen.Return((None, e.json_error()))
    except NotImplementedError:
        raise gen.Return((None, rpc_error('Not implemented', code=-32601)))
    except Warning as w:
        logger.warning(str(w))
        raise gen.Return((None, rpc_error(str(w), code=WARNING_CODE)))
    except gen.Return:
        raise
    except:
        traceback.print_exc(file=sys.stderr)
        if default_error is None:
            raise gen.Return((None, rpc_error('Unexpected error')))
        else:
            raise gen.Return((None, rpc_error(default_error)))


def apply_sort(data, sort_keys):
    # Data is a list to be sorted. Sort_keys is a list of tuples (key, reverse)
    # where key is a dict key in a list item, and reverse says whether to sort
    # in reverse order or not. (i.e. False for ascending, True for descending)
    if not sort_keys:
        return data
    else:
        # Parse the first sort_key
        if isinstance(sort_keys[0], string_types):
            key = sort_keys
            reverse = False
        else:
            key, reverse = sort_keys[0]

        remaining_sort_keys = sort_keys[1:]

        # Sort into groups by this key
        groups = groupby(itemgetter(key), data)
        sorted_indices = sorted(list(groups.keys()), key=lambda s: s.lower() if s else '', reverse=reverse)

        # Sort each group by remaining keys, and concat them together in an
        # order sorted by this key.
        return list(concat(
            apply_sort(groups[index], remaining_sort_keys)
            for index in sorted_indices
        ))
