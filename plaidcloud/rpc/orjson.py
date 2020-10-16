#!/usr/bin/env python
# coding=utf-8
"""Utility function for pretty display of file sizes"""

import decimal
import datetime

__author__ = 'Pat Buxton'
__maintainer__ = 'Paul Morel'
__copyright__ = '© Copyright 2020 Tartan Solutions, Inc.'
__license__ = 'Proprietary'


def unsupported_object_json_encoder(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    elif isinstance(obj, (decimal.Decimal, datetime.timedelta)):
        return str(obj)
    elif isinstance(obj, set):
        return list(obj)
    else:
        raise TypeError
