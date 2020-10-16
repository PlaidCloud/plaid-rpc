# coding=utf-8

from __future__ import absolute_import
from __future__ import print_function
import warnings
from plaidtools.remote.abstract import Abstract

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'

warnings.simplefilter('always', DeprecationWarning)
warnings.warn(
    'plaidtools.remote.log uses a deprecated Websocket connection. Consider using \
            a plaidtools.connection.jsonrpc.SimpleRPC object instead',
    DeprecationWarning
)


class Log(Abstract):

    def __init__(self, auth, uri=None, verify_ssl=None, proxy_url=None,
                 proxy_user=None, proxy_password=None):
        """Initializes Log object settings
            >>> from yaml import load
            >>> from plaidtools.remote.auth import oauth2_auth
            >>> config = load(open('/home/plaid/src/plaidtools/plaidtools/tests/test_config_example.yaml', 'r'))
            >>> log = Log(oauth2_auth(config['auth_token']), 'uri=https://localhost', verify_ssl=False)
            Remote log proxy object created.
        """

        self.open_web_socket(
            auth=auth,
            callback_type=u'log',
            uri=uri,
            verify_ssl=verify_ssl,
            proxy_url=proxy_url,
            proxy_user=proxy_user,
            proxy_password=proxy_password
        )

        self.workspace = None

        print('Remote log proxy object created.')

    def on_open(self, ws):
        ws.send('helo')

    def on_close(self, ws):
        ws.close()

    def on_message(self, ws, message):
        pass

    def on_error(self, ws, error):
        raise Exception(error)

    def set_workspace(self, workspace):
        self.workspace = workspace

    def _log(self, msg, project=None, model=None, step=None, level='info'):
        package = {
            'message': msg,
            'level': level,
            'cloud_id': self.workspace,
            'project_id': project,
            'model_id': model,
            'step_id': step
        }

        print(('Sending Package: {}'.format(repr(package))))

        self.send(package)

    def info(self, msg, project=None, model=None, step=None):
        self._log(msg, project=project, model=model, step=step, level='info')

    def warn(self, msg, project=None, model=None, step=None):
        self._log(msg, project=project, model=model, step=step, level='warn')

    def error(self, msg, project=None, model=None, step=None):
        self._log(msg, project=project, model=model, step=step, level='error')

    def exception(self, msg, project=None, model=None, step=None):
        self._log(msg, project=project, model=model, step=step, level='error')
