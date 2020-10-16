# coding=utf-8

from __future__ import absolute_import
from __future__ import print_function
import requests
import zipfile
import os
import gzip
import bz2
import json
import logging
import shutil
from six.moves.urllib.parse import urlparse
# from six.moves import http_client
from plaidtools.remote.abstract import Abstract
import warnings
from functools import reduce

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2017, Tartan Solutions, Inc'
__license__ = 'Proprietary'

# Global Settings
# http_client.HTTPConnection.debuglevel = 1

# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

warnings.simplefilter('always', DeprecationWarning)
warnings.warn(
    'plaidtools.remote.document uses a deprecated Websocket connection. Consider using \
            a plaidtools.connection.jsonrpc.SimpleRPC object instead',
    DeprecationWarning
)


class Document(Abstract):

    def __init__(self, auth, uri=None, verify_ssl=None,
                 proxy_url=None, proxy_user=None, proxy_password=None
                 ):
        """Initializes Document object settings

        Args:
            auth (str): The RPC auth token to use to access Plaid
            uri (str, optional): The URI to use to access plaidcloud. Defaults to Prod.
            verify_ssl (bool, optional): Should SSL be enforced? Defaults to None
            proxy_url (str, optional): The optional URL of a proxy server. Defaults to None
            proxy_user (str, optional): The username to use with the proxy server. Devaults to None
            proxy_pass (str, optional): The password to use with the proxy server. Defaults to None

        Returns:
            `plaidtools.remote.document.Document`: The created Document object.

        Examples:
            # Not creating an actual connection in tests, as committing auth tokens is a bad idea.
            # TODO find a better way to test this.
            >>> from yaml import load
            >>> from plaidtools.remote.auth import oauth2_auth
            >>> config = load(open('/home/plaid/src/plaidtools/plaidtools/tests/test_config_example.yaml', 'r'))
            >>> document = Document(oauth2_auth(config['auth_token']), 'uri=https://localhost', verify_ssl=False)
            Document proxy object created.
        """

        self.open_web_socket(
            auth=auth,
            callback_type=u'document',
            uri=uri,
            verify_ssl=verify_ssl,
            proxy_url=proxy_url,
            proxy_user=proxy_user,
            proxy_password=proxy_password
        )

        # Get the base url
        parsed_uri = urlparse(uri)
        self._download_url = 'https://{uri.netloc}/download'.format(uri=parsed_uri)
        self._upload_url = 'https://{uri.netloc}/upload'.format(uri=parsed_uri)

        self._auth = auth
        self._verify_ssl = verify_ssl
        if proxy_url is not None:
            self._use_proxy = True
            self._proxy_auth = '{}:{}'.format(proxy_user, proxy_password)
            self._proxy_url = proxy_url
        else:
            self._use_proxy = False

        print('Document proxy object created.')

    def on_open(self, ws):
        raise NotImplementedError()

    def on_close(self, ws):
        raise NotImplementedError()

    def on_message(self, ws, message):
        raise NotImplementedError()

    def on_error(self, ws, error):
        raise NotImplementedError()

    # ----------------------------------------------------------
    # ---- Non-Web Socket Operations for upload and download ---
    # ----------------------------------------------------------

    def get(self, document_account, path, local_file_path, uncompress=True, logger=requests_log):
        """Retrieves a file from PlaidCloud and places it on local system"""

        headers = self._auth.get_package()
        headers['PlaidCloud-Document-Account'] = str(document_account)
        headers['PlaidCloud-Remote-Path'] = str(path)

        if self._use_proxy:
            proxy_settings = self._get_proxy_settings()

            r = requests.get(self._download_url, verify=self._verify_ssl, stream=True, headers=headers, proxies=proxy_settings)
        else:
            r = requests.get(self._download_url, verify=self._verify_ssl, stream=True, headers=headers)

        if not r.ok:
            raise Exception(r.raise_for_status())

        dir_path = os.path.dirname(local_file_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

        if uncompress is True:
            _uncompress(local_file_path)

        return r

    def post(self, document_account, path, local_file_path, compress=None, logger=requests_log):
        """Puts a file into Document storage through uploading via PlaidCloud"""

        if not os.path.exists(local_file_path):
            raise Exception('No such file or directory at {0}'.format(local_file_path))

        # Determine if this is a directory or a single file
        if os.path.isdir(local_file_path):
            # Step out of this method and call it recursively
            self._post_directory(document_account, path, local_file_path, compress, logger)
        else:
            self._post_file(document_account, path, local_file_path, compress, logger)

    def _post_directory(self, document_account, path, local_file_path, compress=None, logger=requests_log):
        """Recurse down the directory and grab all the files"""

        if not os.path.exists(local_file_path):
            raise Exception('No such directory at {0}'.format(local_file_path))

        file_list = [
            [os.path.join(dirpath, filename) for filename in filenames]
            for dirpath, dirnames, filenames in os.walk(local_file_path)
        ]

        if file_list is None or len(file_list) == 0:
            files = []
        else:
            files = reduce(
                lambda x, y: x + y, file_list
            )

        for f in files:
            relative_path = os.path.relpath(f, local_file_path)
            upload_path = path.rstrip('/') + '/' + relative_path.replace(os.path.sep, '/')

            self._post_file(document_account, upload_path, f, compress, logger)

    def _post_file(self, document_account, path, local_file_path, compress=None, logger=requests_log):
        """Puts a file into Document storage through uploading via PlaidCloud"""

        if not os.path.exists(local_file_path):
            raise Exception('No such file at {0}'.format(local_file_path))

        delete_archive = True

        if compress == 'zip':
            # archive_name = shutil.make_archive(local_file_path, 'zip')

            if local_file_path.lower().endswith('.zip'):
                # Only zip this file if it isn't already zipped.
                archive_name = local_file_path
            else:
                extension = 'zip'
                archive_name = os.path.realpath(os.path.normpath('{0}.{1}'.format(local_file_path, extension)))
                path = '{0}.{1}'.format(path, extension)

                with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as myzip:
                    myzip.write(local_file_path, os.path.basename(local_file_path))

        elif compress in ('gzip', 'gz'):
            # archive_name = shutil.make_archive(local_file_path, 'gztar')
            if local_file_path.lower().endswith('.gz') or local_file_path.lower().endswith('.gzip'):
                # Only zip this file if it isn't already zipped.
                archive_name = local_file_path
            else:
                extension = 'gz'
                archive_name = os.path.realpath(os.path.normpath('{0}.{1}'.format(local_file_path, extension)))
                path = '{0}.{1}'.format(path, extension)
                with open(local_file_path, 'rb') as f_in:
                    with gzip.open(archive_name, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

        elif compress in ('bz', 'bzip', 'bz2'):
            # archive_name = shutil.make_archive(local_file_path, 'bztar')
            if local_file_path.lower().endswith('.bz') or local_file_path.lower().endswith('.bzip') or local_file_path.lower().endswith('.bz2'):
                # Only zip this file if it isn't already zipped.
                archive_name = local_file_path
            else:
                extension = 'bz2'
                archive_name = os.path.realpath(os.path.normpath('{0}.{1}'.format(local_file_path, extension)))
                path = '{0}.{1}'.format(path, extension)
                with open(local_file_path, 'rb') as f_in:
                    with bz2.BZ2File(archive_name, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
        else:
            archive_name = local_file_path
            delete_archive = False

        headers = self._auth.get_package()
        headers['PlaidCloud-Document-Account'] = str(document_account)
        headers['PlaidCloud-Remote-Path'] = str(path)

        with open(os.path.normpath(archive_name), 'rb') as fpointer:
            files = {'uploaded_file': fpointer}
            handle = {'data': {}, 'action': {}}
            values = {'handle': json.dumps(handle)}

            if self._use_proxy:
                proxy_settings = self._get_proxy_settings()

                r = requests.post(
                    self._upload_url, files=files, verify=self._verify_ssl,
                    proxies=proxy_settings, headers=headers, data=values
                )
            else:
                r = requests.post(
                    self._upload_url, files=files, verify=self._verify_ssl,
                    headers=headers, data=values
                )

        if delete_archive is True:
            try:
                # Delete the temporary archive file to keep the drive clean
                os.unlink(archive_name)
            except:
                pass

        return r


def _uncompress(file_path):
    """Uncompresses stuff

    if it is a single file then it compresses it
    if it is a set of files then all the files will be compressed into
    a single file

    @type   file_path:   str
    @param  file_path:   File path
    """

    valid_extensions = {
        'zip': 'zip',
        'gz': 'gz',
        'gzip': 'gz',
        'bz': 'bz2',
        'bzip': 'bz2',
        'bz2': 'bz2',
    }

    compress = file_path.split('.')[-1]
    extension = valid_extensions.get(compress)

    if extension is None:
        # Skip this file
        return

    # See if the local file name has the extension
    unc_name, file_extension = os.path.splitext(file_path)

    delete_archive = True
    if extension == 'zip':
        dir_path = os.path.dirname(file_path)

        with zipfile.ZipFile(file_path, 'r') as zf:
            zf.extractall(dir_path)

    elif extension == 'gz':
        with gzip.open(file_path, 'rb') as f_in:
            with open(unc_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    elif extension == 'bz2':
        with bz2.BZ2File(file_path, 'rb') as f_in:
            with open(unc_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        delete_archive = False

    if delete_archive is True:
        try:
            # Delete the temporary archive file to keep the drive clean
            os.unlink(file_path)
        except:
            pass
