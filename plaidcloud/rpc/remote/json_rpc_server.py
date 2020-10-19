#!/usr/bin/env python
# coding=utf-8

from __future__ import print_function

from __future__ import absolute_import
import json
import logging

from toolz.dicttoolz import merge
from tornado import gen
from six.moves import map

from plaidcloud.rpc.remote.rpc_common import call_as_coroutine

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2017-2019, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Proprietary"
__maintainer__ = "Paul Morel"
__email__ = "paul.morel@tartansolutions.com"

BASE_MODULE_PATH = 'plaid/rpc_v{}'

logger = logging.getLogger(__name__)

"""Methods for routing rpc requests."""


def get_callable_object(method, version, base_path=BASE_MODULE_PATH, logger=logger):
    """Obtains a callable RPC object from the method name and version supplied

    Args:
        method (str): RPC method
        version (int): RPC version protocol

    Returns:
        callable method
    """

    if not len(method):
        raise Exception('No method path specified.')

    module_path = "{}/{}".format(base_path.format(version), method)
    parts = module_path.split('/')

    callable_name = parts[-1]
    import_path = '.'.join(parts[:-1])

    mod = __import__(import_path, {}, {}, parts)

    nonexist_error = Exception('RPC method {} does not exist.'.format(method))
    try:
        callable = getattr(mod, callable_name)
    except AttributeError:
        raise nonexist_error

    if hasattr(callable, 'rpc_method'):
        return callable, getattr(callable, 'required_scope', None), getattr(callable, 'default_error', None)
    else:
        raise nonexist_error


def get_args_from_callable(callable_object, base_path=BASE_MODULE_PATH, logger=logger):
    """Obtains the arguments and argument defaults for an RPC method

    Args:
        callable_object (obj): Callable RPC method object

    Returns:
        Dict of arguments and their defaults as key value pairs
    """
    args = callable_object.__code__.co_varnames

    # Drop the auth_id as it is filled in automatically and cannot be provided by user input
    args = args[1:]

    defaults = callable_object.__defaults__

    if not isinstance(args, list):
        args = []

    if not isinstance(defaults, list):
        defaults = []

    # Reverse the order so we can line up the defaults with the args
    # Since python doesn't allow non-defaulted args after defaulted ones
    # we know that the defaults pertain only to the tail of the args
    required_args = list(reversed(args))
    arg_defaults = list(reversed(defaults))

    # Return a dict of the args and their default value or None
    return {k: v for k, v in map(None, required_args, arg_defaults)}


@gen.coroutine
def execute_json_rpc(msg, auth_id, version=1, base_path=BASE_MODULE_PATH, logger=logger, extra_params={}):
    """
    Executes a JSON RPC request and returns response

    Follows the spec from here:  http://www.jsonrpc.org/specification

    Required message attributes include id and method.  The params argument is optional.

    Args:
        msg (str): JSON string containing the properly formatted JSON-RPC request
        auth_id (dict): Authentication Information and Identity
        version (int): API version to use for RPC
        base_path (str): Path to the root of the RPC methods
        logger (logger): A logger to log useful info
        extra_params (dict): Extra parameters to send

    Returns:
        JSON-RPC compliant response dict consisting of:
          - id (int): The requestors unique request id
          - ok (bool): The processing state result
          - result: Result of the request
          - error (dict): Error information including code, message, and data
    """

    try:
        rpc_args = json.loads(msg)
    except:
        logger.exception('Request parse error, probably')
        raise gen.Return({
            'id': None,
            'ok': False,
            'error': {
                'code': -32700,
                'message': 'Request parse error.  Message must be JSON.',
                'data': msg
            }
        })
    else:
        result = yield process_rpc(rpc_args, auth_id, version=version, base_path=base_path, logger=logger, extra_params=extra_params)
        raise gen.Return(result)


@gen.coroutine
def process_rpc(rpc_args, auth_id, version=1, base_path=BASE_MODULE_PATH, logger=logger, extra_params={}):

    if not isinstance(rpc_args, dict):
        raise gen.Return({
            'id': None,
            'ok': False,
            'error': {
                'code': -32600,
                'message': 'Invalid request object format. Must be JSON map. Multiple operations are not supported.',
                'data': None
            }
        })
    else:
        # Args from JSON-RPC standard
        id = rpc_args.get('id')
        jsonrpc = rpc_args.get('jsonrpc')
        method = rpc_args.get('method')
        params = rpc_args.get('params', dict())

        if jsonrpc not in ('2.0', ):
            raise gen.Return({
                'id': id,
                'ok': False,
                'error': {
                    'code': -32600,
                    'message': 'Invalid API version specified. Only JSON-RPC version 2.0 is supported.',
                    'data': None,
                }
            })

        elif method is None:
            raise gen.Return({
                'id': id,
                'ok': False,
                'error': {
                    'code': -32600,
                    'message': 'Invalid method specified',
                    'data': None,
                }
            })

        elif not isinstance(params, dict):
            raise gen.Return({
                'id': id,
                'ok': False,
                'error': {
                    'code': -32600,
                    'message': 'Invalid parameter format',
                    'data': None,
                }
            })

        # Set these default args.
        # AuthID will come from oAuth2 process most likely
        params['auth_id'] = auth_id

        def _get_scopes_to_look_for(scope_required):
            scope_parts = scope_required.split('.')
            return {
                scope_required,  # e.g. analyze.project.read
                # write implies read
                '.'.join(scope_parts[:2] + ['write']) if scope_parts[-1] == 'read' else scope_required
            }

        def check_scopes(method, required_scope, scopes):
            # Returns True if the method is authorized by the scopes. False if it is not.
            if method.startswith('identity/me/'):
                # Special case. identity.me returns info about the caller,
                # and has more relaxed security.
                return True

            return (
                not required_scope
                or any(s in scopes for s in _get_scopes_to_look_for(required_scope))
            )

        if method == 'echo':
            raise gen.Return({
                'id': id,
                'ok': True,
                'result': rpc_args
            })
        elif method == 'help':
            # Special case.  This should return the doc string of the callable item
            describe_method = params.get('method')
            try:
                callable_object, _, _ = get_callable_object(describe_method, version, base_path=base_path, logger=logger)
            except:
                logger.exception("No module")
                # No module
                raise gen.Return({
                    'id': id,
                    'ok': False,
                    'error': {
                        'code': -32601,
                        'message': 'Method to describe not found',
                        'data': None
                    }
                })
            else:
                raise gen.Return({
                    'id': id,
                    'ok': True,
                    'result': {
                        'method': describe_method,
                        'params': get_args_from_callable(callable_object, base_path=base_path, logger=logger),
                        'description': callable_object.__doc__
                    }
                })
        else:
            # Actually execute regular rpc method!
            try:
                try:
                    logger.debug('Finding JSON-RPC module and method')
                    callable_object, required_scope, default_error = get_callable_object(method, version, base_path=base_path, logger=logger)
                    logger.debug('Found JSON-RPC module and method')
                except:
                    logger.exception("No module")
                    # No module
                    raise gen.Return({
                        'id': id,
                        'ok': False,
                        'error': {
                            'code': -32601,
                            'message': 'Method not found',
                            'data': None
                        }
                    })
                else:
                    if not check_scopes(method, required_scope, auth_id.get('scopes', {'public'})):
                        raise gen.Return({
                            'id': id,
                            'ok': False,
                            'error': {
                                'code': -32601,
                                'message': 'Method is not available due to lack of permission scope',
                                'data': 'Possible scopes: {}, Actual scopes {}'.format(
                                    _get_scopes_to_look_for(required_scope),
                                    auth_id.get('scopes', {'public'})
                                )
                            }
                        })
                    try:
                        logger.info('Start "{}" {}'.format(method, id))
                        (result, error) = yield call_as_coroutine(callable_object, default_error, **merge(params, extra_params))
                        logger.info('Complete "{}" {}'.format(method, id))

                    except TypeError:
                        logger.exception("Type Error")
                        # Check the callable object and make sure all the required args are present
                        args = get_args_from_callable(callable_object, base_path=base_path, logger=logger)

                        required_args = [a for a in args if args[a] is None]
                        missing_args = [ra for ra in required_args if ra not in params]

                        # Also check for extra arguments that shouldn't be there
                        extra_args = [p for p in params if p not in args]

                        if missing_args:
                            raise gen.Return({
                                'id': id,
                                'ok': False,
                                'error': {
                                    'code': -32602,
                                    'message': 'Invalid params',
                                    'data': 'Missing required parameter arguments: {}'.format(', '.join(missing_args))
                                }
                            })
                        elif extra_args:
                            raise gen.Return({
                                'id': id,
                                'ok': False,
                                'error': {
                                    'code': -32602,
                                    'message': 'Invalid params',
                                    'data': 'Extra parameters are not allowed: {}'.format(', '.join(extra_args))
                                }
                            })
                        else:
                            # Incorrect arguments passed but not sure what the problem really is
                            raise gen.Return({
                                'id': id,
                                'ok': False,
                                'error': {
                                    'code': -32602,
                                    'message': 'Invalid params',
                                    'data': None
                                }
                            })
                    except gen.Return:
                        raise
                    except:
                        logger.exception('Unspecified error')
                        raise gen.Return({
                            'id': id,
                            'ok': False,
                            'error': {
                                'code': -32603,
                                'message': 'Unspecified error while executing API request',
                                'data': None
                            }
                        })
                    else:
                        if id is None:
                            # This was a one-way message.  No response should be sent.
                            # per JSON-RPC specifications None id does not get response.
                            logger.debug('Json-RPC call without ID - not responding, as per json-RPC spec.')
                            raise gen.Return(None)
                        else:
                            if error is not None:
                                raise gen.Return({
                                    'id': id,
                                    'ok': False,
                                    'error': error
                                })
                            else:
                                raise gen.Return({
                                    'id': id,
                                    'ok': True,
                                    'result': result
                                })
            except gen.Return:
                raise
            except:
                logger.exception('Unspecified error')
                raise gen.Return({
                    'id': id,
                    'ok': False,
                    'error': {
                        'code': -32603,
                        'message': 'Unspecified error while executing API request',
                        'data': None
                    }
                })
