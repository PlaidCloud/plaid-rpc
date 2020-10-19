#!/usr/bin/env python

from __future__ import absolute_import
from sqlalchemy import (
    BIGINT, INTEGER, SMALLINT, TEXT, Boolean, NUMERIC, TIMESTAMP, Interval, Date, Time
)

import messytables
import six
import xlrd

from plaidcloud.rpc.functions import regex_map, RegexMapKeyError
from plaidcloud.rpc.database import PlaidUnicode

_ANALYZE_TYPE = regex_map({
        r'^bool$': 'boolean',
        r'^boolean$': 'boolean',
        r'^s\d+': 'text',
        r'^object$': 'text',
        r'^text$': 'text',
        r'^nvarchar(\([0-9]*\))*$': 'text',
        r'^string$': 'text',
        r'^int8$': 'smallint',
        r'^tinyint$': 'smallint',
        r'^int16$': 'smallint',
        r'^smallint$': 'smallint',  # 2 bytes
        r'^int32$': 'integer',  # 4 bytes
        r'^integer$': 'integer',
        r'^int64$': 'bigint',  # 8 bytes
        r'^bigint$': 'bigint',
        r'^float\d*': 'numeric',
        r'^numeric.*': 'numeric',
        r'^decimal.*': 'numeric',
        r'^serial$': 'integer',
        r'^bigserial$': 'bigint',
        r'^datetime.*': 'timestamp',  # This may have to cover all datetimes
        r'^timestamp\b.*': 'timestamp',
        r'^timedelta.*': 'interval',  # This may have to cover all timedeltas
        r'^interval$': 'interval',
        r'^date$': 'date',
        r'^time\b.*': 'time',
})

_PANDAS_DTYPE_FROM_SQL = regex_map({
    r'^boolean$': 'bool',
    r'^text$': 'object',
    r'^nvarchar.*$': 'object' if six.PY3 else 'S1024',
    r'^tinyint$': 'int8',
    r'^smallint$': 'int16',
    r'^integer$': 'Int64',
    r'^bigint$': 'Int64',
    r'^numeric$': 'float64',
    r'^decimal.*': 'float64',
    r'^timestamp\b.*': 'datetime64[s]',
    r'^interval$': 'timedelta64[s]',
    r'^date$': 'datetime64[s]',
    r'^time\b.*': 'datetime64[s]',
    r'^datetime.*': 'datetime64[s]',
})

_PYTHON_DATESTRING_FROM_SQLALCHEMY = {
    'YYYY-MM-DD"T"HH24:MI:SS': '%Y-%m-%dT%H:%M:%S',
    'YYYY-MM-DDTHH24:MI:SS': '%Y-%m-%dT%H:%M:%S',
    'YYYY-MM-DD"T"HH:MI:SS': '%Y-%m-%dT%I:%M:%S',
    'YYYY-MM-DDTHH:MI:SS': '%Y-%m-%dT%I:%M:%S',
    'YYYY-MM-DD HH24:MI:SS': '%Y-%m-%d %H:%M:%S',
    'YYYY-MM-DD HH:MI:SS': '%Y-%m-%d %I:%M:%S %p',
    'MM/DD/YYYY HH24:MI:SS': '%m/%d/%Y %H:%M:%S',
    'MM/DD/YYYY HH:MI:SS': '%m/%d/%Y %I:%M:%S %p',
    'DD/MM/YYYY HH24:MI:SS': '%d/%m/%Y %H:%M:%S',
    'DD/MM/YYYY HH:MI:SS': '%d/%m/%Y %I:%M:%S %p',
    'DD MON YYYY HH24:MI:SS': '%d %b %Y %H:%M:%S',
    'DD MON YYYY HH:MI:SS': '%d %b %Y %I:%M:%S %p',
    'YYYYMMDD HH24:MI:SS': '%Y%m%d %H:%M:%S',
    'YYYYMMDD HH:MI:SS': '%Y%m%d %I:%M:%S',
}


def analyze_type(dtype):
    """
    Args:
        dtype (str): a string containing a dtype from sql, or from pandas.
    Returns:
        (str): An analyze dtype. Should be one of ('text', 'numeric', 'smallint',
        'integer', 'bigint', 'boolean', 'date', 'time', 'timestamp', 'interval')
    Examples:
        >>> analyze_type('time')
        'time'
        >>> analyze_type('time without timezone')
        'time'
        >>> analyze_type('timestamp without timezone')
        'timestamp'
        >>> analyze_type('timestamp with timezone')
        'timestamp'
        >>> analyze_type('float64')
        'numeric'
        >>> analyze_type('nvarchar')
        'text'
        >>> analyze_type('nvarchar(5000)')
        'text'
        >>> analyze_type('smallint')
        'smallint'
    """
    key = str(dtype).lower()

    # No f'in idea why timestamp is getting mangled into some type of time field.  Force it!
    # DO NOT REMOVE THIS HACK until you verify timestamps and times are being handled correctly
    if key == 'timestamp':
        return 'timestamp'
    elif key == 'time':
        return 'time'

    try:
        return _ANALYZE_TYPE(key)
    except KeyError:
        raise Exception((
            "Unrecognized dtype: '{}'. If you think it's valid, "
            "please add it to _ANALYZE_TYPE."
        ).format(dtype))


def python_date_from_sql(datestring):
    """ Translates a SQL date format string to a python
        date format string compatable with strftime

    Args:
        datestring (str): The SQL date format string

    Returns:
        str: The python equivalent of `datestring`
    """
    return _PYTHON_DATESTRING_FROM_SQLALCHEMY[datestring]


def pandas_dtype_from_sql(sql):
    """Translates SQL dtypes to Pandas ones

    Args:
        sql (str): a sql dtype

    Returns (str):
        A dtype suitable for pandas

    Examples:
        >>> pandas_dtype_from_sql('time')
        'datetime64[s]'
        >>> pandas_dtype_from_sql('time with time zone')
        'datetime64[s]'
        >>> pandas_dtype_from_sql('timestamp with time zone')
        'datetime64[s]'
        >>> pandas_dtype_from_sql('timestamp')
        'datetime64[s]'
    """

    key = str(sql).lower()
    try:
        return _PANDAS_DTYPE_FROM_SQL(key)
    except RegexMapKeyError:
        return key


_sqlalchemy_from_dtype = regex_map({
    r'^bool$': Boolean,
    r'^boolean$': Boolean,
    r'^s8$': PlaidUnicode(8),
    r'^s16$': PlaidUnicode(16),
    r'^s32$': PlaidUnicode(32),
    r'^s64$': PlaidUnicode(64),
    r'^s128$': PlaidUnicode(128),
    r'^s256$': PlaidUnicode(256),
    r'^s512$': PlaidUnicode(512),
    r'^s1024$': PlaidUnicode(1024),
    r'^object$': TEXT,
    r'^text$':  PlaidUnicode(5000),
    r'^string$': PlaidUnicode(5000),
    r'^serial$': INTEGER,
    r'^bigserial$': BIGINT,
    r'^int8$': SMALLINT,  # 2 bytes
    r'^int16$': SMALLINT,  # 2 bytes
    r'^smallint$': SMALLINT,
    r'^int32$': INTEGER,  # 4 bytes
    r'^integer$': INTEGER,
    r'^int64$': BIGINT,  # 8 bytes
    r'^bigint$': BIGINT,
    r'^float\d*': NUMERIC,  # variable but ensures precision
    r'^numeric.*': NUMERIC,
    r'^decimal.*': NUMERIC,
    r'^datetime.*': TIMESTAMP,  # This may have to cover all datetimes
    r'^timestamp\b.*': TIMESTAMP,
    r'^timedelta.*': Interval,  # This may have to cover all timedeltas
    r'^interval$': Interval,
    r'^date$': Date,
    r'^time\b.*': Time,
    # MAGIC COLUMN HANDLING
    r'^path': PlaidUnicode(5000),
    r'^file_name$': PlaidUnicode(5000),
    r'^tab_name$': PlaidUnicode(5000),
    r'^last_modified': TIMESTAMP,
})
def sqlalchemy_from_dtype(dtype):
    """
    Returns (sqlalchemy type):
        A sqlalchemy type class
    Args:
        dtype (str): a string that we're going to try to interpret as a dtype
    Examples:
        >>> sqlalchemy_from_dtype('time')
        <class 'sqlalchemy.sql.sqltypes.Time'>
        >>> sqlalchemy_from_dtype('timestamp')
        <class 'sqlalchemy.sql.sqltypes.TIMESTAMP'>
        >>> sqlalchemy_from_dtype('timestamp with time zone')
        <class 'sqlalchemy.sql.sqltypes.TIMESTAMP'>
        >>> sqlalchemy_from_dtype('timestamp without time zone')
        <class 'sqlalchemy.sql.sqltypes.TIMESTAMP'>
        >>> sqlalchemy_from_dtype('time with time zone')
        <class 'sqlalchemy.sql.sqltypes.Time'>
        >>> sqlalchemy_from_dtype('time without time zone')
        <class 'sqlalchemy.sql.sqltypes.Time'>
        >>> sqlalchemy_from_dtype('datetime64')
        <class 'sqlalchemy.sql.sqltypes.TIMESTAMP'>
        >>> sqlalchemy_from_dtype('date')
        <class 'sqlalchemy.sql.sqltypes.Date'>
    """
    key = str(dtype).lower()
    return _sqlalchemy_from_dtype(key)


class BoolType(messytables.BoolType):
    true_values = ('yes', 'true')  # Not '1'
    false_values = ('no', 'false')  # Not '0'


TYPES = [
    messytables.StringType,
    messytables.DecimalType,
    messytables.IntegerType,
    messytables.DateType,
    BoolType,
]


def type_guess(sample, types=TYPES, strict=True):
    """ Replacement for messytables.type_guess that allows for plaid-wide fine-tuning.
        The current difference is that columns made of 0 and 1 are integer
        instead of boolean.
    """
    return messytables.type_guess(sample, types=TYPES, strict=strict)


def dtype_from_excel(excel_type):
    return {
        xlrd.XL_CELL_TEXT: 'text',
        xlrd.XL_CELL_NUMBER: 'numeric',
        xlrd.XL_CELL_DATE: 'timestamp',
        xlrd.XL_CELL_BOOLEAN: 'boolean',
    }.get(excel_type, 'text')
