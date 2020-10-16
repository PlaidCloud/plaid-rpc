# coding=utf-8

"""RPC based logger"""

from __future__ import absolute_import
from __future__ import print_function
from six.moves.urllib.parse import urlparse
from six import text_type
from plaidtools.connection.jsonrpc import SimpleRPC

__author__ = "Paul Morel"
__maintainer__ = "Paul Morel <paul.morel@tartansolutions.com>"
__copyright__ = "© Copyright 2017, Tartan Solutions, Inc"
__license__ = "Proprietary"


class RemoteLogger(object):

    """
    A convenient wrapper around the rpc.analyze.step.log method.
    """

    def __init__(self, project_id, model_id, step_id, token, url, verify_ssl):
        """Sets up a SimpleRPC object, and routing info.

        Args:
            project_id (int): The project this logger is being used in
            model_id (int): The model this logger is being used in
            step_id (int): The step this logger is being used in
            token (str): RPC Auth token to use
            url (str): The URL to use to connect
            verify_ssl (bool): Should SSL be required/validated (Should generally be `True`)
        """

        self.project_id = project_id
        self.model_id = model_id
        self.step_id = step_id

        uri = urlparse(url)

        if (uri.path and not uri.netloc):
            # Caller simply passed hostname as url. 
            uri = urlparse('https://{}/json-rpc'.format(uri.path))

        print(("plaidtools.remote_logger, url: {}".format(url)))
        print(("plaidtools.remote_logger, uri: {}".format(uri.geturl())))

        self.rpc_con = SimpleRPC(
            token=token,
            uri=uri.geturl(),
            verify_ssl=verify_ssl,
        )

    def log(self, message, level='info'):
        """Logs a message at the provided level

        Args:
            message (str): The message to log
            level (str): What level to log the message at (debug, info, warn, error)
        """
        kwargs = {
            'project_id': self.project_id,
            'workflow_id': self.model_id,
            'step_id': self.step_id,
            'message': text_type(message),
            'level': level if level in ('debug', 'error', 'warn') else 'info',
        }
        self.rpc_con.analyze.step.log(**kwargs)

    def debug(self, message):
        """Log a message at debug level

        Args:
            message (str): The message to log
        """
        self.log(message, 'debug')

    def info(self, message):
        """Log a message at info level

        Args:
            message (str): The message to log
        """
        self.log(message, 'info')

    def warn(self, message):
        """Log a message at warning level

        Args:
            message (str): The message to log
        """
        self.log(message, 'warn')

    def warning(self, message):
        """Log a message at warning level

        Args:
            message (str): The message to log
        """
        # Alias for warn
        self.warn(message)

    def error(self, message):
        """Log a message at error level

        Args:
            message (str): The message to log
        """
        self.log(message, 'error')

    def exception(self, message):
        """Log a message at error level

        Args:
            message (str): The message to log
        """
        # Alias for error
        self.error(message)
