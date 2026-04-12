#!/usr/bin/env python
# coding=utf-8

from unittest import mock

import pytest
from sqlalchemy import BIGINT, INTEGER, SMALLINT, Boolean, Date, Time, Interval, FLOAT
from sqlalchemy.sql.sqltypes import LargeBinary

from plaidcloud.rpc.type_conversion import (
    analyze_type,
    pandas_dtype_from_sql,
    sqlalchemy_from_dtype,
    postgres_to_python_date_format,
    python_to_postgres_date_format,
    date_format_from_datetime_format,
    arrow_type_from_analyze_type,
    BoolType,
    TYPES,
    type_guess,
)
from plaidcloud.rpc.messytables.core import Cell
from plaidcloud.rpc.database import (
    PlaidUnicode, PlaidNumeric, PlaidTimestamp, PlaidJSON, GUIDHyphens, PlaidTinyInt,
)


class TestAnalyzeType:

    @pytest.mark.parametrize('input_type,expected', [
        ('boolean', 'boolean'),
        ('bool', 'boolean'),
        ('varchar', 'text'),
        ('varchar(255)', 'text'),
        ('nvarchar', 'text'),
        ('nvarchar(5000)', 'text'),
        ('text', 'text'),
        ('ntext', 'text'),
        ('string', 'text'),
        ('char', 'text'),
        ('nchar', 'text'),
        ('smallint', 'smallint'),
        ('int16', 'smallint'),
        ('int8', 'smallint'),
        ('tinyint', 'smallint'),
        ('integer', 'integer'),
        ('int32', 'integer'),
        ('int', 'integer'),
        ('serial', 'integer'),
        ('bigint', 'bigint'),
        ('int64', 'bigint'),
        ('bigserial', 'bigint'),
        ('float64', 'numeric'),
        ('numeric', 'numeric'),
        ('decimal', 'numeric'),
        ('double', 'numeric'),
        ('double precision', 'numeric'),
        ('money', 'numeric'),
        ('real', 'numeric'),
        ('timestamp', 'timestamp'),
        ('timestamp without timezone', 'timestamp'),
        ('timestamp with timezone', 'timestamp'),
        ('datetime', 'timestamp'),
        ('smalldatetime', 'timestamp'),
        ('time', 'time'),
        ('time without timezone', 'time'),
        ('time with timezone', 'time'),
        ('date', 'date'),
        ('interval', 'interval'),
        ('json', 'json'),
        ('jsonb', 'json'),
        ('uuid', 'uuid'),
        ('largebinary', 'largebinary'),
        ('bytea', 'largebinary'),
        ('binary', 'largebinary'),
        ('varbinary', 'largebinary'),
        ('image', 'largebinary'),
        ('xml', 'text'),
        ('cidr', 'text'),
        ('inet', 'text'),
        ('macaddr', 'text'),
        ('cursor', 'text'),
        ('uniqueidentifier', 'text'),
        ('bit', 'numeric'),
        ('object', 'text'),
        ('array', 'text'),
        ('map', 'text'),
        ('list', 'text'),
        ('enum', 'text'),
        ('vector', 'text'),
        ('long', 'bigint'),
        ('currency', 'numeric'),
    ])
    def test_known_types(self, input_type, expected):
        assert analyze_type(input_type) == expected

    def test_case_insensitive(self):
        assert analyze_type('VARCHAR') == 'text'
        assert analyze_type('BOOLEAN') == 'boolean'
        assert analyze_type('INTEGER') == 'integer'

    def test_unknown_type_raises(self):
        with pytest.raises(Exception, match="Unrecognized dtype"):
            analyze_type('totally_unknown_type_xyz')


class TestPandasDtypeFromSql:

    @pytest.mark.parametrize('sql_type,expected', [
        ('boolean', 'bool'),
        ('text', 'object'),
        ('nvarchar', 'object'),
        ('varchar', 'object'),
        ('tinyint', 'Int8'),
        ('smallint', 'Int16'),
        ('integer', 'Int64'),
        ('bigint', 'Int64'),
        ('numeric', 'float64'),
        ('timestamp', 'datetime64[s]'),
        ('interval', 'timedelta64[s]'),
        ('date', 'datetime64[s]'),
        ('time', 'datetime64[s]'),
        ('time with time zone', 'datetime64[s]'),
        ('timestamp with time zone', 'datetime64[s]'),
        ('largebinary', 'object'),
        ('json', 'object'),
    ])
    def test_known_types(self, sql_type, expected):
        assert pandas_dtype_from_sql(sql_type) == expected

    def test_unknown_returns_input(self):
        assert pandas_dtype_from_sql('unknown_type') == 'unknown_type'


class TestSqlalchemyFromDtype:

    def test_boolean(self):
        assert sqlalchemy_from_dtype('bool') is Boolean

    def test_time(self):
        assert sqlalchemy_from_dtype('time') is Time

    def test_time_with_timezone(self):
        assert sqlalchemy_from_dtype('time with time zone') is Time

    def test_timestamp(self):
        assert sqlalchemy_from_dtype('timestamp') is PlaidTimestamp

    def test_timestamp_with_timezone(self):
        assert sqlalchemy_from_dtype('timestamp with time zone') is PlaidTimestamp

    def test_date(self):
        assert sqlalchemy_from_dtype('date') is Date

    def test_integer(self):
        assert sqlalchemy_from_dtype('integer') is INTEGER

    def test_bigint(self):
        assert sqlalchemy_from_dtype('bigint') is BIGINT

    def test_smallint(self):
        assert sqlalchemy_from_dtype('smallint') is SMALLINT

    def test_json(self):
        assert sqlalchemy_from_dtype('json') is PlaidJSON

    def test_uuid(self):
        assert sqlalchemy_from_dtype('uuid') is GUIDHyphens

    def test_interval(self):
        assert sqlalchemy_from_dtype('interval') is Interval

    def test_largebinary(self):
        assert sqlalchemy_from_dtype('largebinary') is LargeBinary


class TestPostgresToPythonDateFormat:

    def test_iso_format(self):
        assert postgres_to_python_date_format('YYYY-MM-DD"T"HH24:MI:SS') == '%Y-%m-%dT%H:%M:%S'

    def test_date_only(self):
        assert postgres_to_python_date_format('YYYY-MM-DD') == '%Y-%m-%d'

    def test_time_only(self):
        assert postgres_to_python_date_format('HH24:MI:SS') == '%H:%M:%S'

    def test_us_date_format(self):
        assert postgres_to_python_date_format('MM/DD/YYYY') == '%m/%d/%Y'


class TestPythonToPostgresDateFormat:

    def test_iso_format(self):
        assert python_to_postgres_date_format('%Y-%m-%dT%H:%M:%S') == 'YYYY-MM-DD"T"HH24:MI:SS'

    def test_date_only(self):
        result = python_to_postgres_date_format('%Y-%m-%d')
        assert 'YYYY' in result
        assert 'MM' in result
        assert 'DD' in result


class TestDateFormatFromDatetimeFormat:

    @pytest.mark.parametrize('input_fmt,expected', [
        ('YYYY-MM-DD"T"HH24:MI:SS', 'YYYY-MM-DD'),
        ('YYYY-MM-DD HH24:MI:SS', 'YYYY-MM-DD'),
        ('MM/DD/YYYY HH24:MI:SS', 'MM/DD/YYYY'),
        ('MM/DD/YYYY HH:MI:SS', 'MM/DD/YYYY'),
        ('DD/MM/YYYY HH24:MI:SS', 'DD/MM/YYYY'),
        ('DD/MM/YYYY HH:MI:SS', 'DD/MM/YYYY'),
    ])
    def test_known_datetime_formats(self, input_fmt, expected):
        assert date_format_from_datetime_format(input_fmt) == expected

    def test_unknown_returns_original(self):
        assert date_format_from_datetime_format('YYYY-MM-DDxyz123') == 'YYYY-MM-DDxyz123'


class TestBoolType:

    def test_true_values(self):
        assert BoolType.true_values == ('yes', 'true')

    def test_false_values(self):
        assert BoolType.false_values == ('no', 'false')

    def test_cast_yes_is_true(self):
        assert BoolType().cast('yes') is True

    def test_cast_true_is_true(self):
        assert BoolType().cast('true') is True

    def test_cast_no_is_false(self):
        assert BoolType().cast('no') is False

    def test_cast_false_is_false(self):
        assert BoolType().cast('false') is False

    def test_cast_zero_raises(self):
        # This BoolType does NOT accept '0' as a boolean
        with pytest.raises(ValueError):
            BoolType().cast('0')

    def test_cast_one_raises(self):
        with pytest.raises(ValueError):
            BoolType().cast('1')


def _arrow_json_available():
    try:
        from pyarrow import json_  # noqa: F401
        return True
    except ImportError:
        return False


_REQUIRES_JSON = pytest.mark.skipif(
    not _arrow_json_available(),
    reason='pyarrow version does not export json_',
)


class TestArrowTypeFromAnalyzeType:

    @_REQUIRES_JSON
    def test_date_type(self):
        result = arrow_type_from_analyze_type('date')
        assert result is not None

    @_REQUIRES_JSON
    def test_json_type(self):
        result = arrow_type_from_analyze_type('json')
        assert result is not None

    @_REQUIRES_JSON
    def test_text_type(self):
        result = arrow_type_from_analyze_type('text')
        assert result is not None

    @_REQUIRES_JSON
    def test_numeric_no_decimal(self):
        result = arrow_type_from_analyze_type('numeric')
        assert result is not None

    @_REQUIRES_JSON
    def test_numeric_use_decimal(self):
        result = arrow_type_from_analyze_type('numeric', use_decimal_type=True)
        assert result is not None

    @_REQUIRES_JSON
    def test_integer_type(self):
        result = arrow_type_from_analyze_type('integer')
        assert result is not None

    def test_import_error_raises(self):
        # Simulate pyarrow not being installed
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == 'pyarrow':
                raise ImportError('No pyarrow')
            return real_import(name, *args, **kwargs)

        with mock.patch.object(builtins, '__import__', side_effect=fake_import):
            with pytest.raises(ImportError, match='full install'):
                arrow_type_from_analyze_type('text')


class TestArrowTypeMocked:
    """Tests with a mocked pyarrow module that provides all needed symbols."""

    def _fake_pyarrow(self):
        fake = mock.MagicMock()
        fake.from_numpy_dtype = mock.Mock(return_value='numpy-type')
        fake.string = mock.Mock(return_value='string-type')
        fake.date64 = mock.Mock(return_value='date64-type')
        fake.decimal128 = mock.Mock(return_value='decimal128-type')
        fake.json_ = mock.Mock(return_value='json-type')
        return fake

    def test_date_type_mocked(self):
        fake = self._fake_pyarrow()
        with mock.patch.dict('sys.modules', {'pyarrow': fake}):
            result = arrow_type_from_analyze_type('date')
            assert result == 'date64-type'

    def test_json_type_mocked(self):
        fake = self._fake_pyarrow()
        with mock.patch.dict('sys.modules', {'pyarrow': fake}):
            result = arrow_type_from_analyze_type('json')
            assert result == 'json-type'

    def test_object_fallback_to_string(self):
        fake = self._fake_pyarrow()
        with mock.patch.dict('sys.modules', {'pyarrow': fake}):
            # 'text' maps to np_type='object', so falls back to string
            result = arrow_type_from_analyze_type('text')
            assert result == 'string-type'

    def test_decimal_branch(self):
        fake = self._fake_pyarrow()
        with mock.patch.dict('sys.modules', {'pyarrow': fake}):
            # 'numeric' maps to float64, use_decimal_type=True → decimal128
            result = arrow_type_from_analyze_type('numeric', use_decimal_type=True)
            assert result == 'decimal128-type'

    def test_from_numpy_fallback(self):
        fake = self._fake_pyarrow()
        with mock.patch.dict('sys.modules', {'pyarrow': fake}):
            # 'integer' maps to Int64 → goes to from_numpy_dtype
            result = arrow_type_from_analyze_type('integer')
            assert result == 'numpy-type'


class TestTypeGuess:

    def test_basic(self):
        rows = [
            [Cell('1'), Cell('hello')],
            [Cell('2'), Cell('world')],
        ]
        result = type_guess(rows)
        assert len(result) == 2

    def test_types_constant(self):
        # Ensure the TYPES list is populated as expected
        from plaidcloud.rpc.messytables.types import (
            StringType, IntegerType, DecimalType, DateType,
        )
        assert StringType in TYPES
        assert IntegerType in TYPES
        assert DecimalType in TYPES
        assert DateType in TYPES
