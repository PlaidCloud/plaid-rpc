#!/usr/bin/env python
# coding=utf-8

import logging
import uuid
import os

from plaidcloud.rpc.rpc_connect import Connect

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2019-2023, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Apache 2.0"
__email__ = "paul.morel@tartansolutions.com"

# This sets the basic format of our remote logs.
LOG_FORMAT = logging.Formatter("%(levelname)-8s %(message)s")
LOCAL_LOG_FORMAT = logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")


class LogHandler(logging.Handler):

    def __init__(self, project=None, workflow=None, step=None, rpc=None):
        """

        Args:
            project (str, optional): A project identifier
            workflow (str, optional): A workflow identifier
            step (str, optional): A step identifier
            rpc (Connect, optional): An RPC Connection object
        """
        super(LogHandler, self).__init__()

        if rpc:
            self.rpc = rpc
        else:
            self.rpc = Connect()

        if project:
            try:
                # See if this is a project ID already
                uuid.UUID(project)
                self.project_id = str(project)
            except ValueError:
                if '/' in project:
                    # This is a path lookup
                    self.project_id = self.rpc.analyze.project.lookup_by_full_path(path=project)
                else:
                    # This is a name lookup
                    self.project_id = self.rpc.analyze.project.lookup_by_name(name=project)
        else:
           self.project_id = self.rpc.project_id

        if workflow:
            try:
                # See if this is a workflow ID already
                uuid.UUID(workflow)
                self.workflow_id = str(workflow)
            except ValueError:
                if '/' in workflow:
                    # This is a path lookup
                    self.workflow_id = self.rpc.analyze.workflow.lookup_by_full_path(project_id=self.project_id, path=workflow)
                else:
                    # This is a name lookup
                    self.workflow_id = self.rpc.analyze.workflow.lookup_by_name(project_id=self.project_id, name=workflow)
        else:
            try:
                self.workflow_id = os.environ['__PLAID_WORKFLOW_ID__']
            except:
                self.workflow_id = None

        if step:
            # Since steps are not UUIDs we unfortunately need to make a network call to see if this returns anything
            if self.rpc.analyze.step.step(project_id=self.project_id, step_id=step):
                self.step_id = str(step)
            else:
                if '/' in step:
                    # This is a path lookup
                    self.step_id = self.rpc.analyze.step.lookup_by_full_path(project_id=self.project_id, path=step)
                else:
                    # This is a name lookup
                    self.step_id = self.rpc.analyze.step.lookup_by_name(project_id=self.project_id, name=step)

        else:
            try:
                self.step_id = os.environ['__PLAID_STEP_ID__']
            except:
                self.step_id = None

    def emit(self, record):
        try:
            kwargs = {
                'project_id': self.project_id,
                'message': self.format(record),
                'level': record.levelname.lower(),
            }
            if self.step_id:
                kwargs['step_id'] = self.step_id
                kwargs['workflow_id'] = self.workflow_id
                self.rpc.analyze.step.log(**kwargs)
            elif self.workflow_id:
                kwargs['workflow_id'] = self.workflow_id
                self.rpc.analyze.workflow.log(**kwargs)
            else:
                self.rpc.analyze.project.log(**kwargs)
        except:
            logging.exception('Failed to emit log to PlaidCloud')


class Logger(logging.Logger):
    """Universal logging object for common behavior in local, UDF and Jupyter"""
    def __init__(self, project=None, workflow=None, step=None, rpc=None):
        super(Logger, self).__init__(name=project)
        log_handler = LogHandler(project=project, workflow=workflow, step=step, rpc=rpc)
        log_handler.setFormatter(LOG_FORMAT)
        self.addHandler(log_handler)
        basic_handler = logging.StreamHandler()
        basic_handler.setFormatter(LOCAL_LOG_FORMAT)
        self.addHandler(basic_handler)
