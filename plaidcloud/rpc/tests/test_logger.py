#!/usr/bin/env python
# coding=utf-8

import sys
import os
import uuid
import logging
import unittest
import pytest
if sys.version_info >= (3, 3):
    from unittest import mock
else:
    import mock

from plaidcloud.rpc import logger
import plaidcloud.rpc.rpc_connect as connect


__author__ = "Pat Buxton"
__copyright__ = "© Copyright 2019, Tartan Solutions, Inc"
__credits__ = ["Pat Buxton"]
__license__ = "Apache 2.0"
__maintainer__ = "Pat Buxton"
__email__ = "patrick.buxton@tartansolutions.com"


def get_mock_rpc():
    mock_rpc = mock.MagicMock()
    mock_rpc.project_id = 'project_id'
    mock_rpc.analyze.project.lookup_by_full_path = mock.Mock(return_value='project_id_from_path')
    mock_rpc.analyze.project.lookup_by_name = mock.Mock(return_value='project_id_from_name')
    mock_rpc.analyze.project.log = mock.Mock()
    mock_rpc.workflow_id = 'workflow_id'
    mock_rpc.analyze.workflow.lookup_by_full_path = mock.Mock(return_value='workflow_id_from_path')
    mock_rpc.analyze.workflow.lookup_by_name = mock.Mock(return_value='workflow_id_from_name')
    mock_rpc.analyze.workflow.log = mock.Mock()
    mock_rpc.step_id = 'step_id'
    mock_rpc.analyze.step.lookup_by_full_path = mock.Mock(return_value='step_id_from_path')
    mock_rpc.analyze.step.lookup_by_name = mock.Mock(return_value='step_id_from_name')
    mock_rpc.analyze.step.log = mock.Mock()
    mock_rpc.analyze.step.step = mock.Mock(return_value=None)

    return mock_rpc


FAKE_ENVIRON = {
    '__PLAID_PROJECT_ID__': 'project_id',
    '__PLAID_WORKFLOW_ID__': 'workflow_id',
    '__PLAID_STEP_ID__': 'step_id'
}


class TestHandler(unittest.TestCase):

    """These tests validate the TestHandler class"""

    def setUp(self):
        self.mock_rpc = get_mock_rpc()

    def test_init_with_rpc(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id is None
                assert self.handler.workflow_id is None
                assert self.handler.step_id is None

            mock_find.assert_called_once()

    def test_init_without_rpc(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'Connect', return_value=self.mock_rpc) as mock_connect:
                with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                    self.handler = logger.LogHandler()
                    assert self.handler.rpc == self.mock_rpc
                    assert self.handler.project_id is None
                    assert self.handler.workflow_id is None
                    assert self.handler.step_id is None

                mock_find.assert_called_once()
                mock_connect.assert_called_once()

    def test_init_with_project_uuid(self):
        project_id = str(uuid.uuid4())
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(project=project_id, rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id == project_id
                assert self.handler.workflow_id is None
                assert self.handler.step_id is None

            mock_find.assert_called_once()

    def test_init_with_project_path(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(project='/', rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id == 'project_id_from_path'
                assert self.handler.workflow_id is None
                assert self.handler.step_id is None

            mock_find.assert_called_once()

    def test_init_with_project_name(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(project='name', rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id == 'project_id_from_name'
                assert self.handler.workflow_id is None
                assert self.handler.step_id is None

            mock_find.assert_called_once()

    def test_init_with_workflow_uuid(self):
        workflow_id = str(uuid.uuid4())
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(workflow=workflow_id, rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id is None
                assert self.handler.workflow_id == workflow_id
                assert self.handler.step_id is None

            mock_find.assert_called_once()

    def test_init_with_workflow_path(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(workflow='/', rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id is None
                assert self.handler.workflow_id == 'workflow_id_from_path'
                assert self.handler.step_id is None

            mock_find.assert_called_once()

    def test_init_with_workflow_name(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(workflow='name', rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id is None
                assert self.handler.workflow_id == 'workflow_id_from_name'
                assert self.handler.step_id is None

            mock_find.assert_called_once()

    def test_init_with_step_id(self):
        step_id = '1234'
        self.mock_rpc.analyze.step.step = mock.Mock(return_value={})
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(step=step_id, rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id is None
                assert self.handler.workflow_id is None
                assert self.handler.step_id == step_id

            mock_find.assert_called_once()

    def test_init_with_step_path(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(step='/', rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id is None
                assert self.handler.workflow_id is None
                assert self.handler.step_id == 'step_id_from_path'

            mock_find.assert_called_once()

    def test_init_with_step_name(self):
        with pytest.raises(Exception):
            with mock.patch.object(connect, 'find_plaid_conf', return_value=None) as mock_find:
                self.handler = logger.LogHandler(step='name', rpc=self.mock_rpc)
                assert self.handler.rpc == self.mock_rpc
                assert self.handler.project_id is None
                assert self.handler.workflow_id is None
                assert self.handler.step_id == 'step_id_from_name'

            mock_find.assert_called_once()

    def test_emit_project(self):
        with mock.patch.dict(os.environ, FAKE_ENVIRON) as mock_environ:
            self.handler = logger.LogHandler(rpc=self.mock_rpc)
            self.handler.workflow_id = None
            self.handler.step_id = None
            self.handler.emit(logging.makeLogRecord({}))

            self.mock_rpc.analyze.step.log.assert_not_called()
            self.mock_rpc.analyze.workflow.log.assert_not_called()
            self.mock_rpc.analyze.project.log.assert_called_once_with(level='level none', message='', project_id='project_id')

    def test_emit_workflow(self):
        with mock.patch.dict(os.environ, FAKE_ENVIRON) as mock_environ:
            self.handler = logger.LogHandler(rpc=self.mock_rpc)
            self.handler.step_id = None
            self.handler.emit(logging.makeLogRecord({}))

            self.mock_rpc.analyze.step.log.assert_not_called()
            self.mock_rpc.analyze.workflow.log.assert_called_once_with(level='level none', message='', project_id='project_id', workflow_id='workflow_id')
            self.mock_rpc.analyze.project.log.assert_not_called()

    def test_emit_step(self):
        with mock.patch.dict(os.environ, FAKE_ENVIRON) as mock_environ:
            self.handler = logger.LogHandler(rpc=self.mock_rpc)
            self.handler.emit(logging.makeLogRecord({}))

            self.mock_rpc.analyze.step.log.assert_called_once_with(level='level none', message='', project_id='project_id', step_id='step_id', workflow_id='workflow_id')
            self.mock_rpc.analyze.workflow.log.assert_not_called()
            self.mock_rpc.analyze.project.log.assert_not_called()

    def tearDown(self):
        pass


class TestLogger(unittest.TestCase):

    """These tests validate the Logger class"""

    def setUp(self):
        self.mock_rpc = get_mock_rpc()

    def test_output(self):
        with mock.patch.dict(os.environ, FAKE_ENVIRON) as mock_environ:
            self.logger = logger.Logger(rpc=self.mock_rpc)
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug('something')
            self.logger.error('error')
            assert self.mock_rpc.analyze.step.log.call_count == 2

    def test_output_level(self):
        with mock.patch.dict(os.environ, FAKE_ENVIRON) as mock_environ:
            self.logger = logger.Logger(rpc=self.mock_rpc)
            self.logger.setLevel(logging.ERROR)
            self.logger.debug('something')
            self.logger.error('error')
            self.mock_rpc.analyze.step.log.assert_called_once()

    def tearDown(self):
        pass


class TestLogHandlerInitBranches(unittest.TestCase):
    """Exercise the project/workflow/step init branches — mocking env dict."""

    def setUp(self):
        self.mock_rpc = get_mock_rpc()

    def _make_handler(self, **kwargs):
        # Make sure env vars are present so fallbacks work without Connect()
        env = dict(FAKE_ENVIRON)
        with mock.patch.dict(os.environ, env, clear=False):
            return logger.LogHandler(rpc=self.mock_rpc, **kwargs)

    def test_init_project_uuid_direct(self):
        project_id = str(uuid.uuid4())
        handler = self._make_handler(project=project_id)
        self.assertEqual(handler.project_id, project_id)

    def test_init_project_by_path(self):
        handler = self._make_handler(project='/path/project')
        self.assertEqual(handler.project_id, 'project_id_from_path')

    def test_init_project_by_name(self):
        handler = self._make_handler(project='my_proj')
        self.assertEqual(handler.project_id, 'project_id_from_name')

    def test_init_workflow_uuid(self):
        workflow_id = str(uuid.uuid4())
        handler = self._make_handler(workflow=workflow_id)
        self.assertEqual(handler.workflow_id, workflow_id)

    def test_init_workflow_by_path(self):
        handler = self._make_handler(workflow='/a/b')
        self.assertEqual(handler.workflow_id, 'workflow_id_from_path')

    def test_init_workflow_by_name(self):
        handler = self._make_handler(workflow='wf_name')
        self.assertEqual(handler.workflow_id, 'workflow_id_from_name')

    def test_init_step_returns_existing(self):
        # When rpc.analyze.step.step returns truthy, it means the step exists
        self.mock_rpc.analyze.step.step = mock.Mock(return_value={'exists': True})
        handler = self._make_handler(step='step_xyz')
        self.assertEqual(handler.step_id, 'step_xyz')

    def test_init_step_by_path(self):
        self.mock_rpc.analyze.step.step = mock.Mock(return_value=None)
        handler = self._make_handler(step='/path')
        self.assertEqual(handler.step_id, 'step_id_from_path')

    def test_init_step_by_name(self):
        self.mock_rpc.analyze.step.step = mock.Mock(return_value=None)
        handler = self._make_handler(step='s_name')
        self.assertEqual(handler.step_id, 'step_id_from_name')


class TestLogHandlerEmitFailure(unittest.TestCase):
    """Test that emit swallows errors rather than raising."""

    def test_emit_exception_is_swallowed(self):
        mock_rpc = get_mock_rpc()
        mock_rpc.analyze.step.log = mock.Mock(side_effect=RuntimeError('rpc boom'))
        with mock.patch.dict(os.environ, FAKE_ENVIRON):
            handler = logger.LogHandler(rpc=mock_rpc)
            # Should not raise
            handler.emit(logging.makeLogRecord({}))


class TestLogHandlerEnvFallback(unittest.TestCase):
    """Cover the env var fallback for workflow/step when none are passed."""

    def test_missing_workflow_env_sets_none(self):
        mock_rpc = get_mock_rpc()
        # Remove env vars so fallback triggers the except: None branch
        env = dict(FAKE_ENVIRON)
        env.pop('__PLAID_WORKFLOW_ID__', None)
        env.pop('__PLAID_STEP_ID__', None)
        with mock.patch.dict(os.environ, env, clear=True):
            os.environ['__PLAID_PROJECT_ID__'] = 'project_id'
            handler = logger.LogHandler(rpc=mock_rpc)
            self.assertIsNone(handler.workflow_id)
            self.assertIsNone(handler.step_id)


class TestLogHandlerDefaultsConnect(unittest.TestCase):
    """Cover the `self.rpc = Connect()` default branch."""

    def test_default_rpc_creates_connect(self):
        # Need to mock Connect at module level to avoid filesystem operations
        with mock.patch('plaidcloud.rpc.logger.Connect') as mock_connect_cls, \
             mock.patch.dict(os.environ, FAKE_ENVIRON):
            mock_instance = get_mock_rpc()
            mock_connect_cls.return_value = mock_instance
            handler = logger.LogHandler()
            mock_connect_cls.assert_called_once()
            self.assertIs(handler.rpc, mock_instance)
