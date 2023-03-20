#!/usr/bin/env python
# coding=utf-8
# pylint:disable=import-error, no-name-in-module

import time
from os import path
from winreg import ConnectRegistry, OpenKey, QueryValueEx, HKEY_LOCAL_MACHINE
from comtypes import client, COMError, CoInitialize, CoUninitialize
from win32api import GetFileVersionInfo, LOWORD, HIWORD

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2023, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'


def get_pcm_install_directory():
    registry = ConnectRegistry(None, HKEY_LOCAL_MACHINE)
    pcm_key = OpenKey(registry, 'SOFTWARE\\Wow6432Node\\Business Objects\\Profitability')
    return QueryValueEx(pcm_key, 'InstallDir')[0]


def get_version_number(filename):
    """Get the version number of a file

    Args:
        filename: The file to inspect for a version number

    Returns:
        tuple[int, int, int, int]: Major, Minor, Build, Revision

    Examples:
        >>> get_version_number('C:/Program Files (x86)/SAP BusinessObjects/PCM/PCM.dll') # doctest: +SKIP
        (10, 0, 11, 10)
        >>> get_version_number('C:/NotExistentFile') # doctest: +SKIP
        Exc
    """
    info = GetFileVersionInfo(filename, "\\")
    ms = info['FileVersionMS']
    ls = info['FileVersionLS']
    return HIWORD(ms), LOWORD(ms), HIWORD(ls), LOWORD(ls)


def get_pcm_10_service_pack(filename):
    """Gets the Service Pack number from the DLL file version

    Args:
        filename: The DLL file on which to establish a version

    Returns:
        The Service Pack Number of the file

    Examples:
        >>> get_pcm_10_service_pack('C:/Program Files (x86)/SAP BusinessObjects/PCM/PCM.dll') # doctest: +SKIP
        Traceback (most recent call last):
        ...
        WindowsError: [Error 2] The system cannot find the file specified
        >>> get_pcm_10_service_pack('C:/NotExistentFile') # doctest: +SKIP
        Traceback (most recent call last):
        ...
        WindowsError: [Error 2] The system cannot find the file specified

    """
    major, minor, sp, build = get_version_number(filename)
    if not (major == 10 and minor == 0):
        raise Exception('Invalid PCM Version For This Library - {}.{}.{}.{}'.format(major, minor, sp, build))
    return sp


try:
    PCM_INSTALL_DIR = get_pcm_install_directory()
    if not path.exists(PCM_INSTALL_DIR):
        raise Exception('PCM Install is not found')
except:
    PCM_INSTALL_DIR = ''

PCM_DLL_PATH = PCM_INSTALL_DIR+'PCM.dll'
PCM_USERS_DLL_PATH = PCM_INSTALL_DIR+'PCMUsers.dll'
UNICODE_ENCODING = 'utf-8'


class ModelExistsException(Exception):
    def __init__(self, model_name):
        Exception.__init__(self, "Model {0} already exists.".format(model_name))


class ModelDoesNotExistException(Exception):
    def __init__(self, model_name):
        Exception.__init__(self, "Model {0} does not exist.".format(model_name))


class ExportSpecificationNotExistsException(Exception):
    def __init__(self, export_specification_filename):
        Exception.__init__(self, "Export specification file {0} does not exist.".format(export_specification_filename))


class PCMConnection(object):
    def __init__(self, username, password):
        CoInitialize()
        self._pcm_users_dll = client.GetModule(PCM_USERS_DLL_PATH)
        self._pcm_dll = client.GetModule(PCM_DLL_PATH)
        self._pcm_sp_version = get_pcm_10_service_pack(PCM_DLL_PATH)
        self._username = username
        self._password = password
        self._login(self._username, self._password)

    def __enter__(self):
        return self

    def __del__(self, *a):
        pass

    def __exit__(self, *a):
        self._logout()
        CoUninitialize()

    def _call_pcm_method(self, method, *args):
        """Calls a PCM Method in any Service Pack with the correct params

        Notes:
            Prior to SP12, all calls had to be prefixed with a 'passport' parameter - it was removed because it was
            useless and slowed everything down

        Args:
            method: The PCM API Method to call
            *args: The args to call any version of the method with

        Returns:
            mixed: whatever PCM API returns

        """
        if self._pcm_sp_version < 12:
            return method('', *args)
        else:
            return method(*args)

    def _get_pcm_error_message(self, com_exception):
        """Try to get a sensible string from PCM for the error

        Args:
            com_exception (COMError): A COM Exception

        Returns:
            str: The error message that PCM returns

        """
        return self._call_pcm_method(
            self._IPCMUser.ErrorMessage, com_exception.hresult
        ).encode(UNICODE_ENCODING)

    def _login(self, username, password):
        """Connect/Login to an instance of PCM via the COM interfaces

        Note:

        Args:
            username (str): The username of the PCM User in whose context to make the connection to PCM.
            password (str): The password for the provided PCM username

        Returns:
            True if successful, False otherwise.

        """
        self._IPCMUser = client.CreateObject(self._pcm_users_dll.EPOUser._reg_clsid_, interface=self._pcm_users_dll.IEPOUser)
        self._IPCMModel = client.CreateObject(self._pcm_dll.PADModel._reg_clsid_, interface=self._pcm_dll.IPADModel)
        try:
            return_value = self._call_pcm_method(
                self._IPCMUser.Login,
                self._pcm_users_dll.ssoDefault,
                username,
                password,
                '',
                self._pcm_users_dll.pfEPO,
                self._pcm_users_dll.pcPPGUI,
                ''
            )
            self._server = return_value[0]
            self._session_id = return_value[1]

            self._call_pcm_method(
                self._IPCMUser.Attach, self._session_id
            )

            # Connect to PAD DLL
            self._call_pcm_method(
                self._IPCMModel.Attach, self._session_id, self._server
            )

            return True

        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to log into PCM - {0}'.format(error))

    def _logout(self):
        """Logout from an instance of PCM and release any interfaces

        Args:

        Returns:
            None

        """
        self._call_pcm_method(
            self._IPCMModel.Close
        )
        # self._IPCMModel.Release()

        self._call_pcm_method(
            self._IPCMUser.Logout
        )
        # self._IPCMUser.Release()

    def model_exists(self, model_name):
        """Check for existence of a model in PCM by it's name

        Note:

        Args:
            model_name (str): The name of the model to lookup

        Returns:
            True if successful, False otherwise.

        """
        try:
            all_models = self._call_pcm_method(
                self._IPCMUser.ModelList, -2
            )
            # Loop through checking for model_name
            for model in all_models:
                if model[1] == model_name:
                    return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to get model {0} - {1}'.format(model_name, error))

        return False

    def get_model(self, model_name):
        """Lookup a model in PCM by it's name

        Note:

        Args:
            model_name (str): The name of the model to lookup

        Returns:
            tuple: Model record if found {ID, Name, ..} otherwise None

        """
        try:
            all_models = self._call_pcm_method(
                self._IPCMUser.ModelList, -2
            )
            # Loop through checking for model_name
            for model in all_models:
                if model[1] == model_name:
                    return model
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to get model {0} - {1}'.format(model_name, error))

        return None

    def _set_model_server(self, model_name, model_server):
        """Sets the Model Server for a Model

        Args:
            model_name: The model for which to set the server
            model_server: The server on which the model will reside

        Returns:
            None
        """
        self._call_pcm_method(
            self._IPCMModel.ModelServer, model_name
        )
        if self._pcm_sp_version < 12:
            self._IPCMModel.ModelServer['', model_name] = model_server
        else:
            self._IPCMModel.ModelServer[model_name] = model_server

    def create_model(self, new_model_name, model_type, enable_audit=False):
        """Create a new model in PCM

        Note:
            Model Type must be specified, but Audit is disabled by default.
            You really don't want auditing on unless you have to - it makes things very slow.
        Args:
            new_model_name (str): The name of the new model
            model_type (int): The type of the new model. Could be
                1 - Profitability & Costing
                4 - Objectives & Metrics
                8 - Transactional Costing
                16 - Bill of Materials
            enable_audit (bool): Whether or not the auditing should be enabled for the model

        Returns:
            True if successful, False otherwise.

        """
        # check for existence of model name first
        if self.model_exists(new_model_name):
            raise ModelExistsException(new_model_name)
        # now do creation (can the user do it??? need to figure out that check)
        try:
            self._call_pcm_method(
                self._IPCMModel.CreateModel, new_model_name, '', enable_audit, model_type
            )
            default_model_server = self._call_pcm_method(
                self._IPCMUser.DefaultServer, 1
            )
            self._set_model_server(new_model_name, default_model_server)
            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to create model {0} - {1}'.format(new_model_name, error))

    def copy_model(self, src_model_name, target_model_name):
        """Create a copy of a model in PCM

        Note:

        Args:
            src_model_name (str): The name of the original model to copy
            target_model_name (str): The name for the newly created model copy

        Returns:
            True if successful, False otherwise.

        """
        # check for existence of source model first and, if destination name already exists then fail
        if not self.model_exists(src_model_name):
            raise ModelDoesNotExistException(src_model_name)
        if self.model_exists(target_model_name):
            raise ModelExistsException(target_model_name)
        # now do copy
        try:
            self._call_pcm_method(
                self._IPCMModel.CopyModel,
                src_model_name,
                target_model_name,
                ''
            )
            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to copy model "{0}" to "{1}": {2}'.format(src_model_name, target_model_name, error))

    def open_model(self, model_name):
        """Open a model in PCM

        Note:
            This allows methods of IPCMModel that relate to a model to be called
            Checks that model exists first and then, if it exists, attempts to open it and waits
            for the model to be fully open
        Args:
            model_name (str): The name of the model to open

        Returns:
            True if successful, False otherwise.

        """
        # check for existence of model before opening
        if not self.model_exists(model_name):
            raise ModelDoesNotExistException(model_name)
        try:
            self._call_pcm_method(
                self._IPCMModel.Open, model_name
            )
            # wait for model to be fully open
            # TODO : Need to be able to get out of this loop if there's a problem

            def get_model_is_fully_open():
                if self._pcm_sp_version < 12:
                    return self._call_pcm_method(self._IPCMModel.IsModelFullyOpen)
                else:
                    return self._IPCMModel.IsModelFullyOpen

            while get_model_is_fully_open() is not True:
                time.sleep(0.5)
            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to open model {0} - {1}'.format(model_name, error))

    def delete_model(self, model_name, kill=True):
        """Delete a model in PCM

        Note:

        Args:
            model_name (str): The name of the model to delete
            kill (boolean): Kill the model if it is already open

        Returns:
            True if successful, False otherwise.

        """
        # check for existence of model first
        if not self.model_exists(model_name):
            raise ModelDoesNotExistException(model_name)
        remove_audit = True
        remove_views = True
        try:
            model_is_open = self._call_pcm_method(
                self._IPCMModel.IsModelOpen, model_name
            )
            if model_is_open and kill:
                self.open_model(model_name)
                self._call_pcm_method(
                    self._IPCMModel.Close
                )
                self._call_pcm_method(
                    self._IPCMModel.KillModel, model_name
                )
            self._call_pcm_method(
                self._IPCMModel.DeleteModel, model_name, remove_audit, remove_views
            )
            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to delete model {0} - {1}'.format(model_name, error))

    def rename_model(self, old_model_name, new_model_name):
        """Rename a model in PCM

        Note:

        Args:
            old_model_name (str): The name of the model to rename
            new_model_name (str): The new name for the model

        Returns:
            True if successful, False otherwise.

        """
        if not self.model_exists(old_model_name):
            raise ModelDoesNotExistException(old_model_name)
        if self.model_exists(new_model_name):
            raise ModelExistsException(new_model_name)
        try:
            self._call_pcm_method(
                self._IPCMModel.RenameModel,
                old_model_name,
                new_model_name
            )
            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to rename model {0} - {1}'.format(old_model_name, error))

    def model_list(self):
        """Get a list of the models in PCM

        Note:

        Args:

        Returns:
            List of models

        """
        try:
            return self._call_pcm_method(
                self._IPCMUser.ModelList, -2
            )
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to list models - {0}'.format(error))

    def set_calculation_state(self, calculation_state):
        """Wrapper to fix calls to set calculation state because I couldn't figure out how to pass properties

        Args:
            calculation_state (bool): Set the calculation active or not

        Returns:
            None

        """
        if self._pcm_sp_version < 12:
            self._IPCMModel.CalculationActive[''] = calculation_state
            # or maybe self._IPCMModel.CalculationActive[()] = calculation_state
        else:
            self._IPCMModel.CalculationActive = calculation_state

    def stop_calculation(self, model_name):
        """Stops the calculation for a specific model

        Args:
            model_name (str): The name of the model to stop calculating
        Returns:
            True if successful, False otherwise

        """
        if not self.model_exists(model_name):
            raise ModelDoesNotExistException(model_name)
        try:
            self.open_model(model_name)
            self.set_calculation_state(False)
            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to stop calculation on model {0} - {1}'.format(model_name, error))

    def start_calculation(self, model_name, wait_for_calc_complete):
        """Starts the calculation for a specific model

        Args:
            model_name (str): The name of the model to stop calculating
            wait_for_calc_complete (Boolean): Wait around for the calculation to complete
        Returns:
            True if successful, False otherwise

        """
        if not self.model_exists(model_name):
            raise ModelDoesNotExistException(model_name)
        try:
            self.open_model(model_name)
            self.set_calculation_state(False)
            self._call_pcm_method(
                self._IPCMModel.LoadStoredModelValues
            )
            self.set_calculation_state(True)
            if wait_for_calc_complete:
                abort_calc = False  # abort_calc doesn't work anyway (or didn't certainly ~Dec 2017)
                while self._call_pcm_method(self._IPCMModel.CalculationComplete, abort_calc) is not True:
                    time.sleep(1)
            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to start calculation on model {0} - {1}'.format(model_name, error))

    def export_to_database(self, model_name, export_id, export_specification_filename, wait_until_complete=True):
        """Exports the model results to the database as per the export specification file

        Args:
            model_name (str): The name of the model to stop calculating
            export_id (int): The ExportId to use for the results - if None then the default ExportId is used
            export_specification_filename (str): The filename of the PCM export specification
            wait_until_complete (bool): Hang around until the export is complete - this could be a while...
        Returns:
            True if successful, False otherwise

        """
        if not self.model_exists(model_name):
            raise ModelDoesNotExistException(model_name)
        if not path.exists(export_specification_filename):
            raise ExportSpecificationNotExistsException(export_specification_filename)
        try:
            self.open_model(model_name)
            if export_id is None:
                export_id = self._call_pcm_method(
                    self._IPCMModel.GetDefaultExportId
                )
            self._call_pcm_method(
                self._IPCMModel.ExportToDatabase, export_id, export_specification_filename
            )
            if wait_until_complete:
                time.sleep(1.0)  # Wait first to allow threads to start up - perhaps not required??

                def get_export_thread_count():
                    if self._pcm_sp_version < 12:
                        return self._call_pcm_method(self._IPCMModel.ExportThreadCount)
                    else:
                        return self._IPCMModel.ExportThreadCount
                while get_export_thread_count() > 0:
                    time.sleep(1.0)

            self._call_pcm_method(
                self._IPCMModel.Close
            )

            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to export model {0} to database - {1}'.format(model_name, error))

    def export_to_file(self, model_name, export_specification_filename, export_file_name, append,
                       include_rules, export_unicode, delimiter, alias, precision):
        """Exports the model results to the database as per the export specification file

        Args:
            model_name (str): The name of the model to stop calculating
            export_specification_filename (str): The filename of the PCM export specification
            export_file_name (str): The target filename where the results will be exported
            append (bool): Append to the file or not
            include_rules (bool): Include Rules
            export_unicode (bool): Write the file using unicode
            delimiter (str): The delimiter to use in the file
            alias (str): The alias to use for the export
            precision (int): Decimal precision to use for values in the file
        Returns:
            True if successful, False otherwise

        """
        raise NotImplementedError()
        # if not self.model_exists(model_name):
        #     raise ModelDoesNotExistException(model_name)
        # if not path.exists(export_specification_filename):
        #     raise ExportSpecificationNotExistsException(export_specification_filename)
        # try:
        #     self.open_model(model_name)
        #
        #     alias_id = 0
        #     alias_count = self._call_pcm_method(
        #         self._IPCMModel.ModelLanguageCount
        #     )
        #     for i in range(0, alias_count):
        #         if alias == self._call_pcm_method(self._IPCMModel.ModelLanguageName, i):
        #             alias_id = self._call_pcm_method(self._IPCMModel.ModelLanguageID, i)
        #             break
        #
        #     secondary_alias_id = self._call_pcm_method(
        #         self._IPCMModel.SecondaryModelLanguage
        #     )
        #     decimal_separator = '.'
        #
        #     # Need to read ESP at this point to process any further
        #     # Need to figure out anything that is marked undetermined_
        #     # I think it's going to be too difficult to create an ESP reader
        #     undetermined_export_tables = []
        #
        #     for export_table in undetermined_export_tables:
        #         self._call_pcm_method(
        #             self._IPCMModel.ExportFileSetup,
        #             export_file_name,
        #             True,
        #             export_table,
        #             undetermined_Fields,
        #             undetermined_Selections,
        #             undetermined_StructureSelections,
        #             alias_id,
        #             secondary_alias_id,
        #             append,
        #             include_rules,
        #             undetermined_Maps,
        #             delimiter,
        #             decimal_separator,
        #             precision,
        #             export_unicode
        #         )
        #
        #         more_data = True
        #
        #         while more_data:
        #             time.sleep(0.5)
        #             return_value = self._call_pcm_method(
        #                 self._IPCMModel.ExportFile, False
        #             )
        #             # Should return HR, Percent, MoreData True/False as tuple
        #             more_data = return_value[2]
        #
        #     self._call_pcm_method(
        #         self._IPCMModel.Close
        #     )
        #
        #     return True
        # except COMError as e:
        #     error = self._get_pcm_error_message(e)
        #     raise Exception('Failed to export model {0} to database - {1}'.format(model_name, error))

    def copy_version(self, model_name, src_version_name, target_version_name):
        """Copy of a version in another version within a model in PCM

        Note:
            Target version must already exist
        Args:
            model_name (str): The name of the model on which to perform the version copy
            src_version_name (str): The name of the version to use as the source of the copy
            target_version_name (str): The name of the version to use as the destination of the copy

        Returns:
            True if successful, False otherwise.

        """
        # Validation
        if not self.model_exists(model_name):
            raise ModelDoesNotExistException(model_name)
        try:
            self.open_model(model_name)
            src_version_id = self._get_version_id_from_model(model_name, src_version_name)
            target_version_id = self._get_version_id_from_model(model_name, target_version_name)
            self._call_pcm_method(
                self._IPCMModel.CopyVersion, src_version_id, target_version_id
            )
            self._call_pcm_method(
                self._IPCMModel.Close
            )

            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to copy version {0} to {1} of model {2} - {3}'.format(
                src_version_name, target_version_name, model_name, error)
            )

    def copy_period(self, model_name, src_version_name, src_period_name, target_version_name, target_period_name):
        """Create a copy of a period into an existing period within a model in PCM

        Note:
            Destination period must already exist
        Args:
            model_name (str): The name of the model on which to perform the version copy
            src_version_name (str): The name of the version to use as the source of the copy
            src_period_name (str): The name of the period to use as the source of the copy
            target_version_name (str): The name of the version to use as the destination of the copy
            target_period_name (str): The name of the period to use as the destination of the copy

        Returns:
            True if successful, False otherwise.

        """
        # Validation
        if not self.model_exists(model_name):
            raise ModelDoesNotExistException(model_name)
        try:
            self.open_model(model_name)
            src_version_id = self._get_version_id_from_model(model_name, src_version_name)
            src_period_id = self._get_period_id_from_model(model_name, src_period_name)
            target_version_id = self._get_version_id_from_model(model_name, target_version_name)
            target_period_id = self._get_period_id_from_model(model_name, target_period_name)
            self._call_pcm_method(
                self._IPCMModel.CopyPeriodVersion, src_version_id, target_version_id, src_period_id, target_period_id
            )
            self._call_pcm_method(
                self._IPCMModel.Close
            )

            return True
        except COMError as e:
            error = self._get_pcm_error_message(e)
            raise Exception('Failed to copy version/period {0}/{1} of model {2} - {3}'.format(
                src_version_name, src_period_name, model_name, error)
            )

    def _get_item_id_from_model(self, model_name, item_name):
        """Gets the id for an item by name in the named model

        Params:
            model_name (str): The name of the model in which to do the lookup
            item_name (str): The item to lookup
        Returns:
            dimension_id (int): The id of the dimension in which the item was found
            item_id (int): The id of the found item
        """
        model_is_open = self._call_pcm_method(
            self._IPCMModel.IsModelOpen, model_name
        )
        if not model_is_open:
            self.open_model(model_name)
        dimension_id, item_id, success = self._call_pcm_method(
            self._IPCMModel.NameCode, item_name
        )
        if success:
            return dimension_id, item_id
        else:
            raise Exception('Unable to get Item {0} from Model {1}'.format(item_name, model_name))

    def _get_version_id_from_model(self, model_name, version_name):
        """Gets the id for a version dimension item in the named model

        Params:
            model_name (str): The name of the model in which to do the lookup
            version_name (str): The version item to lookup
        Returns:
            item_id (int): The id of the found version item
        """
        dimension_id, item_id = self._get_item_id_from_model(model_name, version_name)
        if dimension_id != self._pcm_dll.Version:
            raise Exception('Member {0} does not exist in Version Dimension'.format(version_name))
        return item_id

    def _get_period_id_from_model(self, model_name, period_name):
        """Gets the id for a period dimension item in the named model

        Params:
            model_name (str): The name of the model in which to do the lookup
            period_name (str): The period item to lookup
        Returns:
            item_id (int): The id of the found period item
        """
        dimension_id, item_id = self._get_item_id_from_model(model_name, period_name)
        if dimension_id != self._pcm_dll.Period:
            raise Exception('Member {0} does not exist in Period Dimension'.format(period_name))
        return item_id

    def _get_model_process_id(self, model_name):
        """Get a ProcessId for the model of name...

        Note:
            Exception may happen if we query this while the process is being restarted for the model

        Args:
            model_name: The name of the model for which to obtain the processId

        Returns:
            int: ProcessId for the model or None
        """
        try:
            model_process_info = self._call_pcm_method(
                self._IPCMModel.ModelProcessInfo, model_name
            )
            if isinstance(model_process_info, tuple):
                return model_process_info[0][2]
            else:
                return None
        except:
            return None

    def execute_data_loader(self, model_name, load_id, replace, delta, target, erase, sum_values, wait_for_refresh=True):
        """Execute the dataloader routines and wait for the model to have been refreshed

        Note:
            Runs all the dataloader procedures, processing those that have data
        Args:
            model_name (str): The name of the model on which to perform the refresh
            load_id (int): The LoadId of the relevant dataload
            replace (bool): Replace data
            delta (bool): Delta data
            target (bool): Target data
            erase (int):  Erase data - values 0=append, 1=erase for same V/P, 2=erase for all V/P
            sum_values (bool): Sum the data
            wait_for_refresh (bool): Wait fro PCM to reload following the dataload
        Returns:
            True if successful, False otherwise.

        """
        self.open_model(model_name)
        # Pick up process id for PCMModel
        model_process_id = self._get_model_process_id(model_name)

        # Calling ExecuteDLRoutines with a zero LoadID sets the model reload flag without loading any data
        self._call_pcm_method(
            self._IPCMModel.ExecuteDLRoutines, load_id, replace, delta, target, erase, sum_values
        )

        if wait_for_refresh:
            while model_process_id == self._get_model_process_id(model_name):
                time.sleep(0.5)

    # def _old_refresh_model(self, model_name):  # doesn't work unless we generate a valid but dummy load id first
    #     """Ask PCM to reload the model, usually after a dataload
    #
    #     Note:
    #         Refreshing is now done by calling the ExecuteDLRoutines with a fake LoadId
    #         This means no load actually runs, but the routine sets the refresh flag at the end regardless.
    #         Then just wait for the processId to change for the model and we know it's been processed so we can proceed
    #     Args:
    #         model_name (str): The name of the model on which to perform the refresh
    #     Returns:
    #         True if successful, False otherwise.
    #
    #     """
    #     self.open_model(model_name)
    #     # Pick up process id for PCMModel
    #     model_process_id = self._get_model_process_id(model_name)
    #
    #     # Calling ExecuteDLRoutines with a zero LoadID sets the model reload flag without loading any data
    #     self._call_pcm_method(
    #         self._IPCMModel.ExecuteDLRoutines, 0, False, False, False, 0, False
    #     )
    #
    #     while model_process_id == self._get_model_process_id(model_name):
    #         time.sleep(0.5)

    def refresh_model(self, model_name):
        """Ask PCM to reload the model, usually after a dataload

        Note:
            Refreshing is now done by killing the open model and reloading
        Args:
            model_name (str): The name of the model on which to perform the refresh
        Returns:
            True if successful, False otherwise.

        """
        self.open_model(model_name)
        self._call_pcm_method(
            self._IPCMModel.Close
        )
        self._call_pcm_method(
            self._IPCMModel.KillModel, model_name
        )
        self.open_model(model_name)
        return True
