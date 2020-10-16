#!/usr/bin/env python
# coding=utf-8
"""
Useful functions for working with files.
"""

from __future__ import absolute_import
import os
import ctypes
import errno
from six import text_type


def makedirs(path):
    """os.makedirs, but do nothing if the dirs already exist.

    Args:
        path (str): The path to create
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            # Ignore "file already exists" errors
            pass
        else:
            # But not other OSErrors
            raise


def set_windows_hidden(path):
    """Set hidden flag on file using FILE_ATTRIBUTE_HIDDEN=0x2 according to
    http://msdn.microsoft.com/en-us/library/windows/desktop/aa365535.aspx

    If you're tempted to do this in UNIX, you probably just want to write a
    filename starting with a '.'

    Args:
        path (str): The path of the file to set the `hidden` attribute on

    Returns:
        int: The return code sent by the OS
    """

    FILE_ATTRIBUTES_HIDDEN = 0x2

    # API call requirements: Unicode, proper Windows backslashes
    windows_path = text_type(path.replace('/', '\\'))

    return_code = ctypes.windll.kernel32.SetFileAttributesW(
        windows_path, FILE_ATTRIBUTES_HIDDEN)

    return return_code
