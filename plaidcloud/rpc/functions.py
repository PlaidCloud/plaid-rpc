#!/usr/bin/env python
# coding=utf-8

"""
Utility functions for better functional programming, and other general purpose
functions.

Note: more basic ones can be found in toolz
"""

import multiprocessing
import sys
import traceback
import re
import asyncio
from collections.abc import Iterable, Collection
from typing import TypeVar, Callable, Union, Any
from functools import reduce

from toolz.dicttoolz import merge_with

__author__ = 'Adams Tower'
__credits__ = ['Adams Tower', 'Paul Morel']
__maintainer__ = 'Adams Tower'
__copyright__ = '© Copyright 2011-2021 Tartan Solutions, Inc.'
__license__ = 'Apache 2.0'

T = TypeVar('T')


async def gather_with_semaphore(*futures, concurrent_tasks=3):
    """Works like asyncio.gather, but uses a semaphore to limit th number of concurrent tasks"""
    #TODO: would be great if it could actually avoid starting the tasks - I now realize it
    #      starts up sem_future and then waits, and that might be why I was still seeing warnings
    #      In one place we were once gathering tasks in batches of five, with separate calls to
    #      asyncio.gather() for each batch. I don't love that because it waits for all five to
    #      complete before starting the next batch

    #TODO: maybe we could catch asyncio.exceptions.TimeoutError in here, and reduce concurrent_tasks
    #      or run in serial in response?
    sem = asyncio.Semaphore(concurrent_tasks)
    async def sem_future(future):
        async with sem:
            return await future
    return await asyncio.gather(*[sem_future(future) for future in futures])


def try_except(success: Callable[[], T], failure: Union[T, Callable[[], T]]) -> T:
    """A try except block as a function.

    Note:
        This is very similar to toolz.functoolz.excepts, but this version is
        simpler if you're not actually passing in any arguments. So, I can't
        bring myself to change existing uses of this to use
        toolz.itertoolz.excepts

    Args:
        success (function): 0 argument function to try first
        failure: Either a function to run in the case of failure, or a value to
                 return in the case of failure.

    Returns:
        The result of success, unless success raised an exception. In that
        case, the result or value of failure, depending on whether failure is a
        function or not.

    Examples:
        >>> try_except(lambda: 0/0, 'kasploosh')
        'kasploosh'
        >>> try_except(lambda: 0/1.0, 'kasploosh')
        0.0
        >>> try_except(lambda: 0/0, lambda: 'kasploosh')
        'kasploosh'
    """

    # Using toolz.functoolz.excepts:
    # if not callable(failure):
        # eat_argument_failure = lambda _: failure
    # else:
        # eat_argument_failure = lambda _: failure()
    # return excepts(Exception, lambda _: success(), failure)

    # Original implementation:
    try:
        return success()
    except:
        if callable(failure):
            return failure()
        return failure


def remove_all(string: str, substrs: Iterable[str]) -> str:
    """Removes a whole list of substrings from a string, returning the cleaned
    string.

    Args:
        string (str): A string to be filtered
        substrs (:obj:`list` of :obj:`strs`): The substrings to remove from the string

    Returns:
        str: string with all substrings removed from it.

    Examples:
        >>> remove_all('Four score and seven years ago', [])
        'Four score and seven years ago'
        >>> remove_all('Four !score! an!d s!ev!en yea!rs a!g!o!', ['!'])
        'Four score and seven years ago'
        >>> remove_all('Fo?ur !sc?ore! an!d s!ev!en ye?a!rs a!g!o!', ['!', '?'])
        'Four score and seven years ago'
        >>> remove_all('Four score and seven years ago', ['score and seven '])
        'Four years ago'
        >>> remove_all('Four score and seven years ago', ['score ', 'and ', 'seven '])
        'Four years ago'
    """

    def remove1(string: str, substr: str) -> str:
        return string.replace(substr, '')

    return reduce(remove1, substrs, string)


class RegexMapKeyError(KeyError):
    '''
    Raised when a regex_map can't find a matching regex for a key.
    '''


def regex_map(
    mapping: Union[dict[Union[str, re.Pattern], T], Collection[tuple[Union[str, re.Pattern], T]]]
) -> Callable[[str], T]:
    '''
    Args:
        mapping(dict or list): The mapping can be a dict, or an association list
            of tuples. The keys should be regexes, either compiled or as a
            string. Values can be of any type.

    Returns (function):
        a function that accepts a key, and returns a value the first value for
        which a regex matches its argument.

        The first regex to match will be from left to right in an ordered
        mapping, but arbitrary in an unordered mapping.

    Note:
        When this function is run, it will compile any regexes it's given, so
        you may want to run this function at the import level of your module.
    '''

    #TODO: maybe figure out if some of these are actually hashable, and look
    #those up with a dict first, to speed up lookup? It would probably be
    #premature optimization.

    if isinstance(mapping, dict):
        mapping = list(mapping.items())

    compiled_mapping = [
        (re.compile(regex), val)
        for regex, val in mapping
    ]

    def lookup(key: str) -> T:
        for regex, val in compiled_mapping:
            if re.match(regex, key):
                return val
        raise RegexMapKeyError(key)

    return lookup


def map_across_table(fn, rows):
    """
    Returns:
        (list of lists): A table expressed as a list of lists, having a applied
                         a function to each cell.
    Args:
        fn (function): a single argument function to apply to each cell
        rows (list of lists): A table expressed as a list of lists, each cell of
                              which will be used once as the argument to fn.
    """
    return [
        [
            fn(cell)
            for cell in row
        ]
        for row in rows
    ]


def getchain(dct: dict, keys: Iterable, default: Any = None) -> Any:
    """
    Returns:
        The first value for which a key actually exists in dct, otherwise the
        default value.

    Args:
        dct (dict): The dict to look up keys in
        keys (iterable): A list of keys to try to look up in the dictionary, in
                         order. The first one that is actually in the dictionary
                         is the one that will have its value returned.
        default: The value to return if none of the keys are in dct

    Examples:
        >>> dct = {'bar': 1}
        >>> (
        ...     getchain(dct, ['foo', 'bar', 'baz'], default='default')
        ...     == dct.get('foo', dct.get('bar', dct.get('baz', 'default')))
        ... )
        True
    """

    # Old Implementation (has the disadvantage of doing as many lookups as there
    # are keys, even if the first key is in the dict):

    # if not keys:
    #     return default
    # else:
    #     return dct.get(
    #         keys[0],
    #         getchain(dct, keys[1:], default=default)
    #     )

    # Iterator-oriented version (does not work if dct contains values of None):

    # vals = (dct.get(key) for key in keys)
    # non_none_vals = (val for val in vals if val is not None)
    # return first(chain(non_none_vals, default))

    for key in keys:
        if key in dct:
            return dct[key]
    return default


def deepmerge(*dicts: Union[dict, Any]) -> Union[dict, Any]:
    """
    Merge objects recursively with myDef overwriting defaultDef

    This is useful for setting default object structures but allowing overrides.

    Args:
        dicts (*args): Dicts to merge. For any key, if all values for
                               that key are dicts, they'll be merged with
                               deepmerge. If any are not dicts, the last-nondict
                               will be the value.

    Returns:
        dict: The merged dict.

    Note:
        The top level is treated like a dict value, too, so if you call
        deepmerge on a list containing non-dicts, it will return the last
        non-dict.

        By analogy with toolz.dicttoolz.merge, last-in wins. This is also
        analogous to dict comprehensions with duplicate keys, dict assignment,
        dict literals that for some reason have duplicate keys, etc. So, if your
        goal is to provide a default dict, for keys not appearing in an input
        dict, the default dict should be the first argument and the input dict
        should be the second.

    Examples:
        >>> input = {
        ...     'sort': {
        ...         'column': 'MyRegion',
        ...         'direction': 'ascending'
        ...     },
        ...     'header': {
        ...         'visible': True,
        ...         'columns': [
        ...             'name'
        ...         ]
        ...     },
        ...     'groupby': 'root',
        ...     'BACKGROUND': 'orange'
        ... }
        >>> default = {
        ...     'sort': {
        ...         'column': '',
        ...         'direction': 'descending'
        ...     },
        ...     'header': {
        ...         'visible': True,
        ...         'columns': []
        ...     },
        ...     'groupby': 'MyName',
        ...     'BACKGROUND': 'yellow',
        ...     'footer': {
        ...         'visible': True,
        ...         'columns': []
        ...     }
        ... }
        >>> deepmerge(default, input) == {
        ...     'sort': {
        ...         'column': 'MyRegion',
        ...         'direction': 'ascending'
        ...     },
        ...     'header': {
        ...         'visible': True,
        ...         'columns': ['name']
        ...     },
        ...     'groupby': 'root',
        ...     'BACKGROUND': 'orange',
        ...     'footer': {
        ...         'visible': True,
        ...         'columns': []
        ...     }
        ... }
        True
    """
    def _deepmerge(dicts: tuple[Union[dict, Any], ...]) -> Union[dict, Any]:
        # merge_with expects a non-variadic function

        for maybe_a_dict in reversed(dicts):
            if not isinstance(maybe_a_dict, dict):
                # If we've got any non-dicts, the last non-dict wins.
                return maybe_a_dict
        # Otherwise we want to merge all these dicts, using deepmerge on any
        # collisions
        return merge_with(_deepmerge, *dicts)

    return _deepmerge(dicts)


def nd_union(*dicts):
    """
    Deprecated: User toolz.dicttoolz.merge instead
    """
    raise NotImplementedError


def any(xs):
    "Deprecated: It's a builtin, actually"
    raise NotImplementedError



def all(xs):
    "Deprecated: it's a builtin actually"
    raise NotImplementedError


def ident(x):
    """Deprecated: use toolz.functoolz.identity instead. Since there's an app
    named identity, you might want to import it as ident."""
    raise NotImplementedError


def compose(*fns):
    "Deprecated: use toolz.functoolz.compose instead"
    raise NotImplementedError


def flatten(ioi):
    "Deprecated: use toolz.itertoolz.concat instead"
    raise NotImplementedError


class ForkFailure(Exception):
    def __init__(self, value):
        self.value = value
        super().__init__()
    def __str__(self):
        return repr(self.value)


# Runs a function in another process
# Use when python would otherwise suck up memory
def fork_apply(func, args, logger=None):
    """
    Runs a function in another process
    Use when python would otherwise suck up memory

    Args:
        func (function): The function to run
        args (list): The arguments to send to the function
        logger: A logger object for logging any errors

    Returns:
        The result of func

    Examples:
        >>> def f(x): return 'f({})'.format(x)
        >>> f('x')
        'f(x)'
        >>> fork_apply(f, ['x'])
        'f(x)'
    """
    # Daemons can't have children, so we must pretend we aren't a daemon.
    curr_proc = multiprocessing.current_process()
    daemon_store = curr_proc.daemon
    curr_proc.daemon=False

    # Need a shim to convert a return value to a pipe send
    def shim(conn, args):
        try:
            conn.send((True, func(*args)))
        except:
            exc_type, value, tb = sys.exc_info()
            traceback_ls = traceback.format_exception(exc_type, value, tb)
            traceback_str = ''.join(traceback_ls)
            conn.send((False, (exc_type, traceback_str)))

    # Create a pipe, and a subprocess
    parent_conn, child_conn = multiprocessing.Pipe()
    p = multiprocessing.Process(target=shim, args=(child_conn, args))
    p.start()
    success, result = parent_conn.recv()
    p.join()

    # Return daemon status to it's original value
    curr_proc.daemon=daemon_store

    # Return result if success, or log and raise exception if not.
    if not success:
        exc_type, traceback_str = result
        if logger:
            if traceback_str:
                logger.error("TRACEBACK FROM THE GRANDCHILD PROCESS:\n%s", traceback_str)
            else:
                logger.error("NO TRACEBACK FROM THE GRANDCHILD PROCESS FOR %s", exc_type)
        raise ForkFailure("The grandchild process failed for some reason, see above for traceback.")

    return result


if __name__ == "__main__":
    import doctest
    doctest.testmod()
