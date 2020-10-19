#!/usr/bin/env python

from __future__ import absolute_import
import types
import logging
import orjson as json

import tornado.web
from tornado import gen

from plaidcloud.rpc.remote.json_rpc_server import execute_json_rpc
from plaidcloud.rpc.orjson import unsupported_object_json_encoder


class JsonRpcHandler(tornado.web.RequestHandler):
    """
    Handles JSON-RPC compliant requests
    """

    def initialize(self, *args, **kwargs):
        self.validations = []
        self.logger = logging.getLogger(__name__)
        self.base_path = None
        self.extra_params = {}

    def prepare(self):
        try:
            self.validate()
        except Exception as e:
            self.logger.exception('Error in JsonRpcHandler.prepare')
            self.set_header('Content-Type', 'application/json')
            self.set_status(401)
            self.finish(json.dumps({'error': {'message': str(e)}}, default=unsupported_object_json_encoder, option=json.OPT_NAIVE_UTC | json.OPT_NON_STR_KEYS))

    def validate(self):
        # Tries to run validations in order, stopping at the first one that
        # works, and raising an error if none work.

        # Validations should return True on success, and False on failure. They
        # should also set workspace_id, scopes, and user_id on success.

        for v in self.validations:
            if v():
                if 'public' not in self.scopes:
                    self.scopes.add('public')
                break
        else:
            self.logger.warning(
                'No validation methods worked, defaulting to "public" scope only!')
            self.workspace_id = None
            self.scopes = {'public'}
            self.user_id = None

    def _validate_pass(self):
        # Just pass through with no validation.
        # Probably not a great idea except while debugging.
        self.workspace_id = None
        self.scopes = {'public'}
        self.user_id = None
        return True

    def _validate_fail(self):
        return False

    @gen.coroutine
    def _process_rpc(self, command, msg):
        auth_id = {
            'workspace': self.workspace_id,
            'user': self.user_id,
            'scopes': self.scopes,
        }

        self.set_header('Content-Type', 'application/json')
        self.logger.debug('About to execute RPC call')

        if not self.base_path:
            result = yield execute_json_rpc(msg, auth_id, logger=self.logger, extra_params=self.extra_params)
        else:
            result = yield execute_json_rpc(msg, auth_id, base_path=self.base_path, logger=self.logger, extra_params=self.extra_params)

        self.logger.debug('RPC call complete - building response')
        if isinstance(result.get('result'), types.GeneratorType):
            # RPC endpoint returned a generator, so we'll use chunked
            # transfer encoding to stream it (via tornado yield magic).
            for chunk in result.get('result'):
                self.write(chunk)
                yield self.flush()
        else:
            self.write(json.dumps(result, default=unsupported_object_json_encoder, option=json.OPT_NAIVE_UTC | json.OPT_NON_STR_KEYS))
            self.flush()
        self.finish()
        self.logger.debug('Response Completed')

    @gen.coroutine
    def post(self, command=None):
        yield self._process_rpc(command, self.request.body.decode('utf-8'))

    @gen.coroutine
    def get(self, command=None):
        yield self._process_rpc(command, self.request.body.decode('utf-8'))
