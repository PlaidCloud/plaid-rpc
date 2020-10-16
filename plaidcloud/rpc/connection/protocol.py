"""Tools to register a custom URI protocol handler. Designed for use with OAuth"""
import sys

def register_protocol(app_name, command):
    """"Registers a protocol with the system to handle OAuth callbacks

    Args:
        app_name (str): The name of the app to register. Should be lowercase.
        command (str): The command to execute the handler for this protocol.
    
    Returns:
        None
    """
    systems = {
        'linux': _register_linux,
        'win32': _register_windows,
        'darwin': _register_osx,
    }
    systems[sys.platform](app_name, command)


def _register_linux(app_name, command):
   raise NotImplementedError()


def _register_windows(app_name, command):
    """Registers a protocol handler with windows

    Args:
        app_name (str): The name of the app to register. Should be lowercase.
        command (str): The command to execute the handler for this protocol
    
    Returns:
        None
    """
    import ctypes
    try:
        import winreg as wr  # Python 3
    except ImportError:
        import _winreg as wr  # Python 2
    def is_admin():
        try:
            return ctypes.windll.shell32.IsUserAnAdmin()
        except:
            return False
    if not is_admin():
        # Request a re-run as admin
        try:
            # py3
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
        except:
            # py2
            ctypes.windll.shell32.ShellExecuteW(None, u"runas", unicode(sys.executable), unicode(" ".join(sys.argv)), None, 1) # pylint: disable=undefined-variable
        sys.exit(0)
    app_name = app_name.lower()
    # Create the key/values to register
    key_vals = [
        (f'{app_name}', '', wr.REG_SZ, f'URL:{app_name} Protocol'),
        (f'{app_name}', 'URL Protocol', wr.REG_SZ, ''),
        (f'{app_name}\\shell\\open\\command', '', wr.REG_SZ, command)
    ]

    # Edit the registry.
    for key in key_vals:
        key, subkey, key_type, value = key
        try:
            key = wr.OpenKey(wr.HKEY_CLASSES_ROOT, key, 0, wr.KEY_ALL_ACCESS)
        except:
            key = wr.CreateKey(wr.HKEY_CLASSES_ROOT, key)
        wr.SetValueEx(key, subkey, 0, key_type, value)
        wr.CloseKey(key)


def _register_osx(app_name, command):
    raise NotImplementedError()


if __name__ == '__main__':
    handler_script = sys.argv[1]
    if sys.executable:
        # Use the current python executable path if possible
        register_protocol('plaidtest', f'\"{sys.executable}\" \"{handler_script}\" \"%1\"')
    else:
        # otherwise run `python` and hope!
        register_protocol('plaidtest', f'\"python\" \"{handler_script}\" \"%1\"')
