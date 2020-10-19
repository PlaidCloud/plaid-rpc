#!/usr/bin/env python
# coding=utf-8
"""
Model Config
Load configuration files and provide access to their settings.
"""
from __future__ import absolute_import
from __future__ import print_function

import os
import logging
import datetime
import shlex
import subprocess
import yaml
import glob
import six
import six.moves

from plaidcloud.rpc.file_helpers import makedirs

# Make a logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Module level CONFIG dict - mutable, and importable from this module.
CONFIG = {}


def get_dict():
    return CONFIG


def load_config_files(paths):
    """(Re)load config file(s) in use by the configuration. Newer config keys replace older config keys.

    Args:
        paths (:type:`list` of :type:`str`): The paths to the config files to load"""

    for path in paths:
        try:
            if path[-5:] == '.yaml' and path[-10:] != '-yaml.json' and not os.path.basename(path).startswith('__'):
                with open(path, 'r') as config_fp:
                    CONFIG.update(yaml.safe_load(config_fp))
            else:
                logger.warning("Skipping: Will not load config from {}".format(path))
        except:
            logger.exception("Could not load config from {}".format(path))


def get_git_short_hash(filepath):
    """Get the short git hash of the code.

    This will work even if the current working directory is not the same as the
    directory which houses the codebase (which is likely to occur in a
    production environment and many other instances).

    Args:
        filepath (str): The file path of the git repo

    Returns:
        str: The short hash of the commit that the repo is currently on"""

    if filepath is None:
        return ''

    py_file_dir = os.path.dirname(os.path.abspath(filepath)).rstrip(os.sep)
    git_wt_dir = py_file_dir.rsplit(os.sep, 1)[0]
    git_dir = "{}{}.git".format(git_wt_dir, os.sep)
    git_cmd = "git rev-parse --short --git-dir={} --work-tree={} HEAD" \
              .format(git_dir, git_wt_dir)
    try:
        git_p = subprocess.Popen(shlex.split(git_cmd), stdout=subprocess.PIPE)
        return git_p.communicate()[0].strip()
    except:
        return ''


def initialize(filepath=None):
    """
    This is old OLD code.  Mike refuses to delete it for now, because it was clever and useful
    for when we were running a series of python files in a manifest, which we no longer do, but
    Mike would like to do again. If I delete it now, we'll have to reinvent the wheel.

    Populate config with run-specific configuration data.

    Wants to know the filepath of it's caller for git hash.

    Args:
        filepath (str, optional): The filepath of the git repo
    """

    # Set run_timestamp to now.
    now = datetime.datetime.utcnow()
    CONFIG['run_timestamp'] = now.strftime('%Y-%m-%dT%H_%M_%SZ')

    # Set build to git hash.
    try:
        CONFIG['build'] = get_git_short_hash(filepath)
    except:
        CONFIG['build'] = 'no build'
        logger.exception("CONFIG['build'] set failed. If you see this message, make sure you have subprocess installed.")
        # ADT2017: Looking at this code, I don't actually think we'll get here
        #          if subprocess isn't installed. Furthermore, it's in the
        #          standard libs. I'm not taking the message out, because there
        #          might be a good reason for it I don't understand.


def create_necessary_directories():
    """Create dirs listed in config."""

    local_storage_path = CONFIG['paths']['LOCAL_STORAGE']
    for d in CONFIG['paths']['create']:
        path = d.format(PROJECT_ROOT=CONFIG['paths']['PROJECT_ROOT']) #TODO, maybe add other path substitute options here.
        makedirs(path)
        logger.debug("Created directory '%s'", d)


def set_runtime_options():
    """Sets pandas chained_assignment option from config."""
    try:
        import pandas as pd

        opts = CONFIG.get('options', {})
        pd.options.mode.chained_assignment = opts.get('pandas_chained_assignment')
    except ImportError:
        logger.info('Not setting pandas related option - pandas is not installed.')


def build_paths(working_user, config_path):
    """
    Formats paths.

    A note, we use a working_user which may or may not be the local user, for purposes of making code
    that can be deployed on a Windows server and then be reliably called.  Enables common usage of shared
    files, regardless of who the local user happens to be.  This was essential for deploying KC reporting.

    Args:
        working_user (str): local user or overridden (typically 'plaidlink') user.
        config_path (str):  client repo's config directory.  It's where to look for client-specific config yaml.
    """

    logger.debug("No files found at the user's home configuration paths. "
                 "This results in using the packaged defaults.")
    paths = glob.glob(config_path+"/*.yaml")
    load_config_files(paths)

    try:
        CONFIG['paths']['PROJECT_ROOT'] = CONFIG['paths']['PROJECT_ROOT'].format(WORKING_USER=working_user)

    except Exception as e:
        print('{}, {}'.format(config_path, e))
        print(config_path)

    CONFIG['paths']['LOCAL_STORAGE'] = CONFIG['paths']['LOCAL_STORAGE'].format(PROJECT_ROOT=CONFIG['paths']['PROJECT_ROOT'])
    CONFIG['paths']['DEBUG'] = CONFIG['paths']['DEBUG'].format(PROJECT_ROOT=CONFIG['paths']['PROJECT_ROOT'])
    CONFIG['paths']['REPORTS'] = CONFIG['paths']['REPORTS'].format(PROJECT_ROOT=CONFIG['paths']['PROJECT_ROOT'])

    #TODO: Deprecate this.
    CONFIG['options']['LOCAL_STORAGE'] = CONFIG['paths']['LOCAL_STORAGE']


def setup_scaffolding(config_path='', file_location=''):
    """
    Setup for creating local file structure and local paths, used when deploying plaidtools in
    a satellite environment or in a local dev environment for local UDF development.

    This gets called from a client-specific repo for local dev or a Windows server.

    Major improvement by eliminating useless redundancy.
    the code in plaidtools.config.build_paths, plaidtools.config.home_conf, and this function
    used to live in each separate client repo.

    Args:
        file_location (str): The file path client-specific config file that calls this, based on __file__
        working_user (str):  local user or overridden (typically 'plaidlink') user.
        config_path (str):   client repo's config directory.  It's where to look for client-specific config yaml.
    """
    working_user = os.path.expanduser('~')  # This will need work in order to again support satellite deployments under a machine user.
    # initialize(file_location) # Don't remove this line.
    build_paths(working_user, config_path)
    create_necessary_directories()
    set_runtime_options()


class PlaidConfig(object):
    _C = {}
    _working_user = os.path.expanduser('~')
    # Members for local and remote
    rpc_uri = ''
    auth_token = ''
    _project_id = ''
    _workflow_id = ''
    _step_id = ''
    branch = ''
    workspace_id = 0
    workspace_uuid = ''
    # Members used for local setup
    cfg_path = ''
    user_id = 0
    client_id = ''
    client_secret = ''
    hostname = ''
    token_uri = ''
    redirect_uri = ''
    name = ''
    auth_code = None
    # Members used inside the UDFs
    is_local = False
    debug = False
    fetch = True
    cache_locally = False
    write_from_local = False

    def __init__(self, config_path, working_user=''):
        """
        Configuration for running UDFs within Plaid

        A note, we use a working_user which may or may not be the local user, for purposes of making code
        that can be deployed on a Windows server and then be reliably called.  Enables common usage of shared
        files, regardless of who the local user happens to be.  This was essential for deploying KC reporting.

        Args:
            config_path (str):  client repo's config directory.  It's where to look for client-specific config yaml.
            working_user (str, optional): local user or overridden (typically 'plaidlink') user. defaults to os.path.expanduser('~')
        """
        def _check_environment_variables():
            try:
                # If this is running in UDF or Jupyter notebook then an RPC connection is already available
                # Grab config values put in environment variables
                self.rpc_uri = os.environ['__PLAID_RPC_URI__']
                self.auth_token = os.environ['__PLAID_RPC_AUTH_TOKEN__']
                self._project_id = os.environ['__PLAID_PROJECT_ID__']
                self.branch = os.environ['__PLAID_BRANCH__']
                self.workspace_id = int(os.environ['__PLAID_WORKSPACE_ID__'])
                self.workspace_uuid = os.environ['__PLAID_WORKSPACE_UUID__']
                self._workflow_id = os.environ['__PLAID_WORKFLOW_ID__']
                self._step_id = os.environ['__PLAID_STEP_ID__']
                self.is_local = False
                try:
                    self.hostname = six.moves.urllib_parse.urlparse(self.rpc_uri).netloc
                except:
                    self.hostname = 'Unknown'
                logger.debug('Environment is configured, running PlaidCloud UDF on {}'.format(self.hostname))
                return True
            except:
                # This must be running from some environment other than UDF or Jupyter
                # Need to use a config file to setup connection
                self.is_local = True
                logger.debug("Environment not configured, running UDF Locally. Checking local configuration.")
                return False

        def _init_plaidcloud_config():
            self.is_local = False
            self.debug = False
            self._C['LOCAL_STORAGE'] = None
            self._C['project_id'] = self.project_id
            self._C['return_df'] = True
            self.fetch = True
            self.cache_locally = False
            self.write_from_local = False

        def _init_local_config():
            _read_plaid_conf()
            _load_config_yaml_files()
            _build_paths()
            _create_necessary_directories()
            _set_runtime_options()

            self.debug = self._C['local'].setdefault('debug', True)
            self.fetch = self._C['local'].setdefault('fetch', True)
            self.cache_locally = self._C['local'].setdefault('cache_locally', False)
            self.write_from_local = self._C['local'].setdefault('write_from_local', False)

        def _read_plaid_conf():
            # This must be running from some environment other than UDF or Jupyter
            # Need to use a config file to setup connection
            # The config information must be read from a plaid.conf file
            if config_path:
                self.cfg_path = os.path.normpath(config_path)
            else:
                # Option A:  See if this is set in an envir
                # Need to perform a directed search upward to find the first plaid.conf file
                self.cfg_path = find_plaid_conf()

            if not isinstance(self.cfg_path, six.string_types):
                raise Exception('ERROR: No RPC connection configuration available.')

            if not os.path.exists(self.cfg_path):
                raise Exception('ERROR: No plaid.conf exists at the specified path: {}'.format(self.cfg_path))

            with open(self.cfg_path, 'r') as cfg_file:
                self._C['config'] = yaml.safe_load(cfg_file)

            self.user_id = self.config['user_id']
            self.client_id = self.config['client_id']
            self.client_secret = self.config['client_secret']
            self.hostname = self.config['hostname']
            self.token_uri = 'https://{}/oauth/token'.format(self.hostname)
            self.rpc_uri = 'https://{}/json-rpc/'.format(self.hostname)

            self.redirect_uri = self.config.get('redirect_uri', '')
            self.auth_token = self.config.get('auth_token')
            self.workspace_id = int(self.config['workspace'])
            self.workspace_uuid = self.config['workspace_uuid']
            self._project_id = self.config.get('project_id', '')
            self._workflow_id = self.config.get('workflow_id', '')
            self._step_id = self.config.get('step_id', '')
            self.branch = self.config.get('branch', 'master')
            self.name = self.config.get('name')

            # No need for an auth code if we already have a token.
            self.auth_code = self.config.get('auth_code') if not self.auth_token else None

            if all([
                isinstance(self._project_id, six.string_types),
                isinstance(self.branch, six.string_types),
                isinstance(self.workspace_id, six.integer_types)
            ]):
                os.environ['__PLAID_PROJECT_ID__'] = self._project_id
                os.environ['__PLAID_BRANCH__'] = self.branch
                os.environ['__PLAID_WORKSPACE_ID__'] = six.text_type(self.workspace_id)
                os.environ['__PLAID_WORKSPACE_UUID__'] = self.workspace_uuid
                os.environ['__PLAID_WORKFLOW_ID__'] = self._workflow_id
                os.environ['__PLAID_STEP_ID__'] = self._step_id

        def _load_config_yaml_files():
            config_files = glob.glob(os.path.dirname(self.cfg_path) + "/*.yaml")
            for file_path in config_files:
                if file_path.endswith('.yaml') and not file_path.startswith('__'):
                    try:
                        with open(file_path, 'r') as config_fp:
                            self._C.update(yaml.safe_load(config_fp))
                    except:
                        logger.exception("Could not load config from {}".format(file_path))
                else:
                    logger.warning("Skipping: Will not load config from {}".format(file_path))

        def _build_paths():
            """
            Formats paths.

            A note, we use a working_user which may or may not be the local user, for purposes of making code
            that can be deployed on a Windows server and then be reliably called.  Enables common usage of shared
            files, regardless of who the local user happens to be.  This was essential for deploying KC reporting.

            """
            if working_user:
                self._working_user = working_user
            paths = self._C['paths']
            try:
                paths['PROJECT_ROOT'] = paths['PROJECT_ROOT'].format(WORKING_USER=self._working_user)
            except Exception:
                logger.exception('Error Reading Paths from {}'.format(config_path))

            project_root = paths['PROJECT_ROOT']
            paths['LOCAL_STORAGE'] = paths['LOCAL_STORAGE'].format(PROJECT_ROOT=project_root)
            paths['DEBUG'] = paths['DEBUG'].format(PROJECT_ROOT=project_root)
            paths['REPORTS'] = paths['REPORTS'].format(PROJECT_ROOT=project_root)

            # TODO: Deprecate this.
            # self._C['options']['LOCAL_STORAGE'] = paths['LOCAL_STORAGE']

        def _create_necessary_directories():
            """Create dirs listed in config."""
            for d in self._C['paths']['create']:
                # TODO - maybe add other path substitution options here.
                path_to_create = d.format(PROJECT_ROOT=self._C['paths']['PROJECT_ROOT'])
                makedirs(path_to_create)
                logger.debug("Created directory '%s'", path_to_create)

        def _set_runtime_options():
            """Sets pandas chained_assignment option from config."""
            try:
                import pandas as pd

                opts = self._C.get('options', {})
                pd.options.mode.chained_assignment = opts.get('pandas_chained_assignment')
            except ImportError:
                logger.info('Not setting pandas related option - pandas is not installed.')

        if _check_environment_variables():
            _init_plaidcloud_config()
        else:
            _init_local_config()

    @property
    def all(self):
        return self._C

    @property
    def config(self):
        return self.all.get('config', {})

    @property
    def paths(self):
        return self.all.get('paths', {})

    def path(self, path_id):
        result = self.paths[path_id]
        if isinstance(result, six.string_types):
            class Default(dict):
                def __missing__(self, key):
                    return '{' + key + '}'

            return os.path.normpath(result.format_map(
                Default({
                    'WORKING_USER': self._working_user,
                    'PROJECT_ROOT': self.paths['PROJECT_ROOT'],
                    'LOCAL_STORAGE': self.paths['LOCAL_STORAGE'],
                    'DEBUG': self.paths['DEBUG']
                })
            ))
        return result

    @property
    def opts(self):
        return self.all.get('options', {})

    @property
    def local(self):
        return self.all.get('local', {})

    @property
    def project_id(self):
        if not self._project_id:
            raise Exception('Project Id has not been set')
        return self._project_id

    @property
    def workflow_id(self):
        if not self._workflow_id:
            raise Exception('Workflow Id has not been set')
        return self._workflow_id

    @property
    def step_id(self):
        if not self._step_id:
            raise Exception('Step Id has not been set')
        return self._step_id


def find_plaid_conf(search_name='plaid.conf', check_sub_folder='.plaid'):
    """Finds plaid.conf by searching upwards for first plaid.conf it finds"""
    current_directory_path = os.getcwd()
    result = walk_up(current_directory_path, search_name, check_sub_folder)
    if not result:
        raise Exception(
            'Could not find the plaid.conf, starting at {}, checking sub_folder {}'.format(
                current_directory_path, check_sub_folder
            )
        )
    return result


def walk_up(path, search_name, check_sub_folder=None):
    real_path = os.path.relpath(path)
    names = os.listdir(real_path)

    if search_name in names:
        return os.path.join(path, search_name)

    if check_sub_folder is not None and check_sub_folder in names:
        sub_folder_path = os.path.realpath(os.path.join(real_path, check_sub_folder))
        sub_names = os.listdir(sub_folder_path)
        if search_name in sub_names:
            return os.path.join(sub_folder_path, search_name)

    if path == '/' or (os.name == 'nt' and os.path.ismount(path)):
        return None

    new_path = os.path.realpath(os.path.join(real_path, '..'))
    return walk_up(new_path, search_name, check_sub_folder)



# TARGET_WORKING_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
# CONFIG_PATH = ('{}/config/'.format(TARGET_WORKING_DIRECTORY))
# WORKING_USER = os.path.expanduser('~')
#
# # Make a logger
# logging.basicConfig(
#     format='%(asctime)s %(levelname)-8s %(message)s',
#     level=logging.INFO,
#     datefmt='%Y-%m-%d %H:%M:%S')
# logger = logging.getLogger(__name__)
# logger.addHandler(logging.NullHandler())
#
# file_location = __file__
#
# file_location = 'c:\\Users\\micha\\py3\\src\\fmc\\c2s\\config.py'
# working_user = 'C:\\Users\\micha'
# config_path = 'c:\\Users\\micha\\py3\\src\\fmc\\c2s/config/'
#
# setup_scaffolding(file_location, working_user, config_path)
#
# 2 + 2
