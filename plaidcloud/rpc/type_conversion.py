#!/usr/bin/env python
from collections.abc import Mapping
from types import MappingProxyType
from typing import NamedTuple

import sqlalchemy
from sqlalchemy import (
    BIGINT,
    FLOAT,
    INTEGER,
    SMALLINT,
    TEXT,
    Boolean,
    Date,
    Interval,
    Time,
)
from sqlalchemy.sql.sqltypes import LargeBinary

# Check SQLAlchemy version
if sqlalchemy.__version__.startswith('2.'):
    from sqlalchemy.types import DOUBLE
else:  # pragma: no cover
    from databend_sqlalchemy.types import DOUBLE
from plaidcloud.rpc.database import (
    GUIDHyphens,
    PlaidCurrency,
    PlaidGeography,
    PlaidGeometry,
    PlaidJSON,
    PlaidNumeric,
    PlaidTimestamp,
    PlaidTinyInt,
    PlaidUnicode,
)
from plaidcloud.rpc.functions import RegexMapKeyError, regex_map
from plaidcloud.rpc.messytables.types import BoolType as _BoolType
from plaidcloud.rpc.messytables.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
)
from plaidcloud.rpc.messytables.types import type_guess as _type_guess

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2023, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

_ANALYZE_TYPE = regex_map({
        r'^any$': 'text',  # qsv stats emits "Any" when a column has no homogeneous type — treat as free text
        r'^array$': 'text',
        r'^bool$': 'boolean',
        r'^boolean$': 'boolean',
        r'^s\d+': 'text',
        r'^object$': 'text',
        r'^[n]?text.*$': 'text',  # text + ntext
        r'^[n]?char': 'text',  # char + nchar
        r'^nvarchar(\([0-9]*\))*': 'text',
        r'^varchar(\([0-9]*\))*': 'text',
        r'^string$': 'text',
        r'^int8$': 'smallint',
        r'^tinyint$': 'smallint',
        r'^int16$': 'smallint',
        r'^smallint$': 'smallint',  # 2 bytes
        r'^int32$': 'integer',  # 4 bytes
        r'^integer$': 'integer',
        r'^integer.*': 'integer',
        r'^int64$': 'bigint',  # 8 bytes
        r'^bigint$': 'bigint',
        r'^bigint.*': 'bigint',
        r'^float\d*': 'numeric',
        r'^numeric.*': 'numeric',
        r'^decimal.*': 'numeric',
        r'^double(?: precision)?$': 'numeric',
        r'^number$': 'numeric',
        r'^serial$': 'integer',
        r'^bigserial$': 'bigint',
        r'^datetime.*': 'timestamp',  # This may have to cover all datetimes
        r'^timestamp.*': 'timestamp',
        r'^timedelta.*': 'interval',  # This may have to cover all timedeltas
        r'^interval$': 'interval',
        r'^date$': 'date',
        r'^date\(.*\)$': 'date', # This covers Date('format') from type guessing
        r'^time\b.*': 'time',
        r'^byte.*': 'largebinary',  # Any byte string goes to large binary
        r'^(?:var|large)?binary$': 'largebinary',  # binary + varbinary + largebinary
        r'^xml$': 'text',
        r'^uuid$': 'uuid',
        r'^(?:small)?money$': 'numeric',  # money + smallmoney
        r'^real$': 'numeric',
        r'^json.*$': 'json',
        r'^cidr$': 'text',
        r'^inet$': 'text',
        r'^macaddr$': 'text',
        r'^cursor$': 'text',  # CRL2022 - No idea if this will work but it is a valid MSSQL data type.
        r'^uniqueidentifier$': 'text',
        r'^bit$': 'numeric',
        r'^int$': 'integer',
        r'^smalldatetime$': 'timestamp',
        r'^image$': 'largebinary',
        r'^rowversion$': 'numeric',
        r'^hierarchyid$': 'text',
        r'^(sql_)?variant$': 'text',
        r'^geometry$': 'text',
        r'^geography$': 'text',
        r'^spatial_(?:geometry|geography)_types$': 'text',  # spatial_geometry_types + spatial_geography_types
        r'^table$': 'text',
        r'^vector$': 'text',
        # parquet types
        r'^map$': 'text',
        r'^list$': 'text',
        r'^enum$': 'text',
        r'^bson.*$': 'json',
        r'^undefined$': 'text',
        r'^none$': 'text',
        # avro types
        r'^long.*$': 'bigint',
        r'^currency$': 'numeric',
        r'^memo.*$': 'text',
        r'^ole.*$': 'largebinary',
})


class UnsupportedDtype(ValueError):
    """Raised when the admission boundary refuses a dtype.

    Deliberately not a `KeyError`: `RegexMapKeyError` is one, so every upstream
    `except KeyError` swallows it and the dtype silently becomes a default again.
    """

    def __init__(self, dtype, context: str, capability: str | None = None, canonical: str | None = None):
        if capability is None and not dtype:
            refusal = 'missing; no dtype was given'
        elif capability is None:
            refusal = 'not a declared analyze dtype, nor a recognized source type spelling'
        elif canonical is None or canonical == dtype:
            refusal = f'not {capability}-capable'
        else:
            refusal = f'not {capability}-capable; a source type spelling for {canonical!r}'
        super().__init__(f'dtype {dtype!r}: {refusal} (asked by {context})')
        self.dtype = dtype
        self.context = context
        self.capability = capability
        self.canonical = canonical


class Dtype(NamedTuple):
    """What an analyze dtype may be used for, so a caller asks instead of guessing.

    The defaults describe an ordinary scalar column: representable everywhere,
    usable as a join key, summed by default, counted but not picklisted when
    profiled. A declaration states only where a dtype differs from that.
    """
    pandas: str | None
    arrow: bool = True
    sqlalchemy: bool = True
    joinable_as_key: bool = True
    aggregatable: bool = False
    default_agg: str | None = 'sum'  # 'group' | 'sum' | None to refuse aggregation
    profilable: str = 'count_only'  # 'none' | 'count_only' | 'values'


# Every dtype a stored column may carry: what analyze_type can infer (13), plus what
# the client offers in its dtype picklist (ANALYZE_DATA_TYPES), plus 'geography',
# which only _sqlalchemy_from_dtype names. It is therefore NOT the range of
# analyze_type, and not the picklist either -- analyze_type can no more return
# 'bitmap' than it can 'geography'. This is the OUTPUT direction; _ANALYZE_TYPE is
# the INPUT direction (source-system type spelling -> canonical dtype), and a key
# here need not appear there. Declarations reproduce today's behaviour, warts
# included: default_agg mirrors table_explorer_common.agg_type(), which groups
# only text/boolean/date/timestamp/time, so interval/json/uuid/largebinary
# default to 'sum'. profilable mirrors table.py's _PROFILE_* sets, and
# joinable_as_key mirrors frame_join_multi_validator._DTYPE_ENUM.
DTYPES: Mapping[str, Dtype] = MappingProxyType({
    'text': Dtype(pandas='object', default_agg='group', profilable='values'),
    'varchar': Dtype(pandas='object', joinable_as_key=False, profilable='values'),
    'boolean': Dtype(pandas='bool', default_agg='group'),
    'tinyint': Dtype(pandas='Int8', aggregatable=True, profilable='values'),
    'smallint': Dtype(pandas='Int16', aggregatable=True, profilable='values'),
    'integer': Dtype(pandas='Int64', aggregatable=True, profilable='values'),
    'bigint': Dtype(pandas='Int64', aggregatable=True, profilable='values'),
    'serial': Dtype(pandas='Int64', profilable='values'),
    'bigserial': Dtype(pandas='Int64', profilable='values'),
    'numeric': Dtype(pandas='float64', aggregatable=True),
    'decimal': Dtype(pandas='float64', aggregatable=True),
    'float': Dtype(pandas='float64', aggregatable=True),
    'double': Dtype(pandas='float64', aggregatable=True),
    'currency': Dtype(pandas='float64', aggregatable=True),
    'date': Dtype(pandas='datetime64[s]', default_agg='group'),
    'time': Dtype(pandas='datetime64[s]', default_agg='group'),
    'timestamp': Dtype(pandas='datetime64[s]', default_agg='group'),
    'interval': Dtype(pandas='timedelta64[s]'),
    'json': Dtype(pandas='object', profilable='none'),
    'uuid': Dtype(pandas=None, arrow=False),
    'largebinary': Dtype(pandas='object', profilable='none'),
    'bitmap': Dtype(
        pandas=None, arrow=False, sqlalchemy=False,
        joinable_as_key=False, profilable='none',
    ),
    'geometry': Dtype(
        pandas=None, arrow=False, joinable_as_key=False, profilable='none',
    ),
    'geography': Dtype(pandas=None, arrow=False, joinable_as_key=False),
})


# What a declaration holds when it cannot do a thing at all. Compared against rather
# than tested for truth: 'none' and 'group' are truthy strings, so a bare
# `if getattr(declared, capability)` admits an unprofilable dtype for profiling.
_UNAVAILABLE = {
    'pandas': None,
    'arrow': False,
    'sqlalchemy': False,
    'joinable_as_key': False,
    'aggregatable': False,
    'default_agg': None,
    'profilable': 'none',
}


def _key(dtype) -> str:
    # A missing dtype must not become str(None) -> 'none', which _ANALYZE_TYPE maps to
    # 'text' -- a half-added column would then admit as text. '' matches no pattern in
    # any table, so it is refused like any other unknown.
    return '' if dtype is None else str(dtype).lower()


def _resolve(dtype, context: str) -> tuple[str, Dtype]:
    """The canonical dtype and its declaration, or a refusal. The one admission path."""
    key = _key(dtype)
    declared = DTYPES.get(key)
    if declared is not None:
        return key, declared
    try:
        canonical = _ANALYZE_TYPE(key)
        return canonical, DTYPES[canonical]
    except (RegexMapKeyError, KeyError):
        # A plain KeyError here is a dtype _ANALYZE_TYPE resolves to that DTYPES does not
        # declare: a half-added dtype. It must refuse, not leak the KeyError this
        # boundary exists to stop.
        raise UnsupportedDtype(key, context) from None


def admit_dtype(dtype, context: str) -> Dtype:
    """Returns the declaration for `dtype`, or refuses it.

    Accepts a canonical analyze dtype, or a source-system type spelling that
    _ANALYZE_TYPE resolves to one. Anything else -- including a missing dtype -- is
    refused by name.

    Args:
        dtype (str): a canonical analyze dtype, or a source type spelling
        context (str): what is asking, named in the refusal

    Raises:
        UnsupportedDtype: if neither vocabulary declares `dtype`

    Examples:
        >>> admit_dtype('numeric', 'docs').default_agg
        'sum'
        >>> admit_dtype('nvarchar(500)', 'docs').pandas
        'object'
    """
    return _resolve(dtype, context)[1]


def require_dtype_capability(dtype, capability: str, context: str) -> Dtype:
    """Returns the declaration for `dtype`, refusing it unless it can do `capability`.

    Args:
        dtype (str): a canonical analyze dtype, or a source type spelling
        capability (str): a field of `Dtype`
        context (str): what is asking, named in the refusal

    Raises:
        UnsupportedDtype: if `dtype` is undeclared, or cannot do `capability`
        ValueError: if `capability` is not a declared capability

    Examples:
        >>> require_dtype_capability('text', 'arrow', 'docs').arrow
        True
    """
    if capability not in _UNAVAILABLE:
        raise ValueError(
            f'unknown dtype capability {capability!r}; declared capabilities are '
            f'{sorted(_UNAVAILABLE)}'
        )
    canonical, declared = _resolve(dtype, context)
    if getattr(declared, capability) == _UNAVAILABLE[capability]:
        raise UnsupportedDtype(_key(dtype), context, capability, canonical)
    return declared


def arrow_type_from_analyze_type(dtype: str, use_decimal_type: bool = False):
    """Returns an arrow/parquet type given an analyze type

    Args:
        dtype (str): The analyze data type

    Raises:
        UnsupportedDtype: If `dtype` is undeclared, or declares arrow=False

    Returns:
        DataType: The arrow/parquet type that matches `dtype`
    """
    try:
        from pyarrow import date64, decimal128, from_numpy_dtype, json_, string
    except ImportError as exc:
        raise ImportError('Use of this method requires full install. Try running `pip install plaid-rpc[full]`') from exc
    key = _key(dtype)
    require_dtype_capability(key, 'arrow', 'arrow_type_from_analyze_type')
    if key == 'date':
        # Special case. Pandas treats all date types as timestamp, arrow needs to specify
        return date64()
    if key.startswith('json'):
        # Numpy treats this like a generic object, specify json
        return json_()
    if key == 'currency':
        # Stored user-declared money type — always an exact decimal in parquet,
        # regardless of use_decimal_type (which callers only set for SF/MSSQL).
        return decimal128(18, 4)
    np_type = pandas_dtype_from_sql(key)
    if np_type == 'object':
        # Fall back to string
        return string()
    if use_decimal_type and np_type == 'float64':
        return decimal128(38, 10)
    return from_numpy_dtype(np_type.lower())

# Mapping of PostgreSQL date format specifiers to Python's datetime format specifiers
_PG_PY_FORMAT_MAPPING = {
    'YYYY-MM-DD"T"HH24:MI:SS': '%Y-%m-%dT%H:%M:%S',  # ISO 8601
    'YYYY-MM-DD"T"HH:MI:SS': '%Y-%m-%dT%I:%M:%S',
    'YYYY-MM-DD': '%Y-%m-%d',
    'HH24:MI:SS': '%H:%M:%S',
    'MM/DD/YYYY': '%m/%d/%Y',
    'DD Mon YYYY': '%d %b %Y',
    'DD MON YYYY': '%d %b %Y',
    'IYYY': '%G', # ISO 8601 Year
    'YYYY': '%Y',  # 4-digit year
    'YY': '%y',    # 2-digit year
    'MM': '%m',    # Month number (01-12)
    'DD': '%d',    # Day of the month (01-31)
    'HH24': '%H',  # Hour (00-23)
    'HH12': '%I',  # Hour (01-12)
    'HH': '%I',    # Hour (01-12)
    'MI': '%M',    # Minute (00-59)
    'SS': '%S',    # Second (00-59)
    'AM': '%p',    # AM or PM
    'PM': '%p',    # AM or PM
    'D': '%u',     # Day of the week (1-7, 1=Monday)
    'Day': '%A',   # Full day name
    'Mon': '%b',   # Abbreviated month name
    'Month': '%B', # Full month name
    'Dy': '%a',    # Abbreviated day name
    'TZ': '%Z',    # Timezone name
    'tz': '%z',    # Timezone offset
    # Add more mappings as needed
    # '': '%w',  # weekday as decimal 0 sunday, 6 saturday
    'US': '%f',  # Microsecond
    'DDD': '%j',  # Day of Year
    # '': '%U',  # Week Number of the year, Sunday as first
    # '': '%W',  # Week Number of the year, Monday as first
    'IW': '%V', # ISO 8601 Week Numer
}

_PY_PG_FORMAT_MAPPING = {v: k for k, v in _PG_PY_FORMAT_MAPPING.items()}

def postgres_to_python_date_format(pg_format: str) -> str:
    """ Translates a Postgres date format string to a python
        date format string compatible with strftime
    Notes:
        Replace PostgreSQL specifiers with Python specifiers, in order of key length (descending)
    Args:
        pg_format (str): The Postgres date format string

    Returns:
        str: The python equivalent of `pg_format`

    Examples:
        >>> postgres_to_python_date_format('YYYY-MM-DD"T"HH24:MI:SS')
        '%Y-%m-%dT%H:%M:%S'
        >>> postgres_to_python_date_format('DD/MM/YYYY HH:MI:SS')
        '%d/%m/%Y %I:%M:%S'
        >>> postgres_to_python_date_format('YYYY-MM-DD')
        '%Y-%m-%d'
    """

    python_format = pg_format
    for pg_spec in sorted(_PG_PY_FORMAT_MAPPING.keys(), key=len, reverse=True):
        python_format = python_format.replace(pg_spec, _PG_PY_FORMAT_MAPPING[pg_spec])

    return python_format


def python_to_postgres_date_format(py_format: str) -> str:
    """ Translates a Python date format string to a Postgres
        date format string
    Notes:
        Replace Python specifiers with PostgreSQL specifiers, in order of key length (descending)
    Args:
        py_format (str): The Python date format string

    Returns:
        str: The Postgres equivalent of `py_format`

    Examples:
        >>> python_to_postgres_date_format('%Y-%m-%dT%H:%M:%S')
        'YYYY-MM-DD"T"HH24:MI:SS'
        >>> python_to_postgres_date_format('%d/%m/%Y %I:%M:%S')
        'DD/MM/YYYY HH:MI:SS'
    """
    postgres_format = py_format
    for py_spec in sorted(_PY_PG_FORMAT_MAPPING.keys(), key=len, reverse=True):
        postgres_format = postgres_format.replace(py_spec, _PY_PG_FORMAT_MAPPING[py_spec])

    return postgres_format


def date_format_from_datetime_format(pg_format: str) -> str:
    """ Return date-only version of predefined date formats, or return original

    Args:
        pg_format (str): The Postgres date format string

    Returns:
        str: The date-only equivalent of `pg_format` or pg_format

    Examples:
        >>> date_format_from_datetime_format('YYYY-MM-DD"T"HH24:MI:SS')
        'YYYY-MM-DD'
        >>> date_format_from_datetime_format('DD/MM/YYYY HH:MI:SS')
        'DD/MM/YYYY'
        >>> date_format_from_datetime_format('YYYY-MM-DDxyz123')
        'YYYY-MM-DDxyz123'
    """

    PRE_DEF_DATE_FORMAT_MAP = {
        'YYYY-MM-DD"T"HH24:MI:SS': 'YYYY-MM-DD',
        'YYYY-MM-DD HH24:MI:SS': 'YYYY-MM-DD',
        'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
        'MM/DD/YYYY HH24:MI:SS': 'MM/DD/YYYY',
        'MM/DD/YYYY HH:MI:SS': 'MM/DD/YYYY',
        'DD/MM/YYYY HH24:MI:SS': 'DD/MM/YYYY',
        'DD/MM/YYYY HH:MI:SS': 'DD/MM/YYYY',
        'DD MON YYYY HH24:MI:SS': 'DD MON YYYY',
        'DD MON YYYY HH:MI:SS': 'DD MON YYYY',
        'YYYYMMDD HH24:MI:SS': 'YYYYMMDD',
        'YYYYMMDD HH:MI:SS': 'YYYYMMDD',
    }
    return PRE_DEF_DATE_FORMAT_MAP.get(pg_format, pg_format)


def analyze_type(dtype):
    """
    Args:
        dtype (str): a string containing a dtype from sql, or from pandas.
    Returns:
        (str): A canonical analyze dtype. Always a key of DTYPES.

    Raises:
        UnsupportedDtype: If `dtype` is not a recognized source type spelling

    Note:
        'currency' is a stored, user-selected analyze dtype that this function
        never returns — the 'currency' *input* spelling here is a source type
        name (e.g. MS Access Currency, a 19,4 decimal) and maps to 'numeric'
        so inference can never auto-narrow money into DECIMAL(18, 4).
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
        >>> analyze_type('jsonb')
        'json'
        >>> analyze_type('uuid')
        'uuid'
    """
    key = _key(dtype)

    # No f'in idea why timestamp is getting mangled into some type of time field.  Force it!
    # DO NOT REMOVE THIS HACK until you verify timestamps and times are being handled correctly 
    if key == 'timestamp':
        return 'timestamp'
    elif key == 'time':
        return 'time'

    try:
        return _ANALYZE_TYPE(key)
    except RegexMapKeyError:
        raise UnsupportedDtype(key, 'analyze_type') from None


def pandas_dtype_from_sql(sql):
    """Translates SQL dtypes to Pandas ones

    Args:
        sql (str): a sql dtype

    Returns (str):
        A dtype suitable for pandas

    Raises:
        UnsupportedDtype: If `sql` is undeclared, or has no pandas representation

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

    return require_dtype_capability(sql, 'pandas', 'pandas_dtype_from_sql').pandas


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
    r'^text$':  PlaidUnicode(4000),
    r'^varchar$': TEXT,
    r'^string$': PlaidUnicode(4000),
    r'^serial$': INTEGER,
    r'^bigserial$': BIGINT,
    r'^int8$': PlaidTinyInt,  # 2 bytes
    r'^int16$': SMALLINT,  # 2 bytes
    r'^smallint$': SMALLINT,
    r'^int32$': INTEGER,  # 4 bytes
    r'^integer$': INTEGER,
    r'^int64$': BIGINT,  # 8 bytes
    r'^bigint$': BIGINT,
    r'^float\d*': FLOAT,
    r'^numeric.*': PlaidNumeric,
    r'^currency$': PlaidCurrency,
    r'^decimal.*': PlaidNumeric,
    r'^datetime.*': PlaidTimestamp,  # This may have to cover all datetimes
    r'^timestamp\b.*': PlaidTimestamp,
    r'^timedelta.*': Interval,  # This may have to cover all timedeltas
    r'^interval$': Interval,
    r'^date$': Date,
    r'^time\b.*': Time,
    # MAGIC COLUMN HANDLING
    r'^path': PlaidUnicode(4000),
    r'^file_name$': PlaidUnicode(4000),
    r'^tab_name$': PlaidUnicode(4000),
    r'^last_modified': PlaidTimestamp,
    r'^source_row_number$': INTEGER,
    r'^source_table_name$': PlaidUnicode(4000),
    ##
    r'^largebinary': LargeBinary,
    r'^byte.*': LargeBinary,
    r'^xml$': PlaidUnicode(4000),
    r'^(?:generated_)?uuid$': GUIDHyphens,
    r'^money$': PlaidNumeric,
    r'^real$': PlaidNumeric,
    r'^json$': PlaidJSON,
    r'^cidr$': PlaidUnicode(100),
    r'^inet$': PlaidUnicode(100),
    r'^macaddr$': PlaidUnicode(100),
    r'^tinyint$': PlaidTinyInt,
    r'^double$': DOUBLE,
    r'^geometry': PlaidGeometry,
    r'^geography': PlaidGeography,
})

def sqlalchemy_from_dtype(dtype):
    """
    Returns (sqlalchemy type):
        A sqlalchemy type class
    Args:
        dtype (str): a string that we're going to try to interpret as a dtype
    Raises:
        UnsupportedDtype: If `dtype` has no sqlalchemy type
    Examples:
        >>> sqlalchemy_from_dtype('time')
        <class 'sqlalchemy.sql.sqltypes.Time'>
        >>> sqlalchemy_from_dtype('timestamp')
        <class 'plaidcloud.rpc.database.PlaidTimestamp'>
        >>> sqlalchemy_from_dtype('timestamp with time zone')
        <class 'plaidcloud.rpc.database.PlaidTimestamp'>
        >>> sqlalchemy_from_dtype('timestamp without time zone')
        <class 'plaidcloud.rpc.database.PlaidTimestamp'>
        >>> sqlalchemy_from_dtype('time with time zone')
        <class 'sqlalchemy.sql.sqltypes.Time'>
        >>> sqlalchemy_from_dtype('time without time zone')
        <class 'sqlalchemy.sql.sqltypes.Time'>
        >>> sqlalchemy_from_dtype('datetime64')
        <class 'plaidcloud.rpc.database.PlaidTimestamp'>
        >>> sqlalchemy_from_dtype('date')
        <class 'sqlalchemy.sql.sqltypes.Date'>
        >>> sqlalchemy_from_dtype('json')
        <class 'plaidcloud.rpc.database.PlaidJSON'>
        >>> sqlalchemy_from_dtype('uuid')
        <class 'plaidcloud.rpc.database.GUIDHyphens'>
    """
    key = _key(dtype)
    try:
        return _sqlalchemy_from_dtype(key)
    except RegexMapKeyError:
        # _resolve refuses an unknown dtype; anything it admits is known but has no
        # sqlalchemy type under this spelling, which is a different refusal.
        canonical, _ = _resolve(key, 'sqlalchemy_from_dtype')
        raise UnsupportedDtype(key, 'sqlalchemy_from_dtype', 'sqlalchemy', canonical) from None


class BoolType(_BoolType):
    true_values = ('yes', 'true')  # Not '1'
    false_values = ('no', 'false')  # Not '0'


TYPES = [
    StringType,
    DecimalType,
    IntegerType,
    DateType,
    BoolType,
]


def type_guess(sample, strict=True):
    """ Replacement for messytables.type_guess that allows for plaid-wide fine-tuning.
        The current difference is that columns made of 0 and 1 are integer
        instead of boolean.
    """
    return _type_guess(sample, types=TYPES, strict=strict)
