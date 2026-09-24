#!/usr/bin/env python
# coding=utf-8

from unittest import mock

import pytest
from sqlalchemy import BIGINT, INTEGER, SMALLINT, TEXT, Boolean, Date, Interval, Time
from sqlalchemy.sql.sqltypes import LargeBinary

from plaidcloud.rpc import type_conversion
from plaidcloud.rpc.type_conversion import (
    DTYPES,
    Dtype,
    UnsupportedDtype,
    admit_dtype,
    require_dtype_capability,
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
    PlaidNumeric, PlaidCurrency, PlaidTimestamp, PlaidJSON, GUIDHyphens,
    PlaidUnicode, PlaidTinyInt, PlaidGeometry, PlaidGeography,
)
from plaidcloud.rpc.functions import RegexMapKeyError


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
        with pytest.raises(UnsupportedDtype) as exc_info:
            analyze_type('totally_unknown_type_xyz')
        assert exc_info.value.dtype == 'totally_unknown_type_xyz'
        assert exc_info.value.context == 'analyze_type'
        assert exc_info.value.capability is None

    def test_unknown_type_refusal_is_not_a_key_error(self):
        # RegexMapKeyError is a KeyError, so an `except KeyError` upstream used to
        # swallow the refusal and default the column silently.
        with pytest.raises(UnsupportedDtype):
            try:
                analyze_type('totally_unknown_type_xyz')
            except KeyError:
                pytest.fail('refusal must not be catchable as KeyError')

    def test_every_inferred_type_is_declared(self):
        assert analyze_type('nvarchar(5000)') in DTYPES


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
        ('currency', 'float64'),
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

    def test_unknown_is_refused(self):
        # Was: returned the input string unchanged, which is not a pandas dtype and
        # crashes later inside pandas with no mention of the column or the dtype.
        with pytest.raises(UnsupportedDtype) as exc_info:
            pandas_dtype_from_sql('unknown_type')
        assert exc_info.value.dtype == 'unknown_type'
        assert exc_info.value.capability is None

    @pytest.mark.parametrize('dtype', ['uuid', 'bitmap', 'geometry', 'geography'])
    def test_unrepresentable_is_refused(self, dtype):
        with pytest.raises(UnsupportedDtype) as exc_info:
            pandas_dtype_from_sql(dtype)
        assert exc_info.value.capability == 'pandas'

    @pytest.mark.parametrize('sql_type,expected', [
        ('varchar(4000)', 'object'),
        ('nvarchar(255)', 'object'),
        ('decimal(18, 4)', 'float64'),
        ('float64', 'float64'),
        ('double precision', 'float64'),
        ('int64', 'Int64'),
        ('bytea', 'object'),
        ('jsonb', 'object'),
    ])
    def test_source_spellings_resolve_through_the_registry(self, sql_type, expected):
        assert pandas_dtype_from_sql(sql_type) == expected

    @pytest.mark.parametrize('dtype', ['float', 'double', 'serial', 'bigserial'])
    def test_picklist_dtypes_absent_from_the_old_table(self, dtype):
        # These reached pandas only via the removed passthrough; 'serial' and
        # 'bigserial' were not pandas dtypes at all.
        assert pandas_dtype_from_sql(dtype) in ('float64', 'Int64')


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

    def test_currency(self):
        assert sqlalchemy_from_dtype('currency') is PlaidCurrency

    def test_varchar(self):
        assert sqlalchemy_from_dtype('varchar') is TEXT

    def test_tinyint(self):
        assert sqlalchemy_from_dtype('tinyint') is PlaidTinyInt

    @pytest.mark.parametrize('dtype', ['serial', 'float', 'double', 'decimal'])
    def test_picklist_dtypes_resolve(self, dtype):
        assert sqlalchemy_from_dtype(dtype) is not None

    def test_geometry(self):
        assert sqlalchemy_from_dtype('geometry') is PlaidGeometry

    def test_geography(self):
        assert sqlalchemy_from_dtype('geography') is PlaidGeography

    def test_text_is_bounded_unicode(self):
        assert isinstance(sqlalchemy_from_dtype('text'), PlaidUnicode)

    def test_unknown_dtype_is_refused(self):
        # Was: a bare RegexMapKeyError carrying only the key and no context.
        with pytest.raises(UnsupportedDtype) as exc_info:
            sqlalchemy_from_dtype('totally_unknown_type_xyz')
        assert exc_info.value.dtype == 'totally_unknown_type_xyz'
        assert exc_info.value.context == 'sqlalchemy_from_dtype'
        assert exc_info.value.capability is None

    def test_declared_dtype_without_a_sqlalchemy_type_is_refused(self):
        with pytest.raises(UnsupportedDtype) as exc_info:
            sqlalchemy_from_dtype('bitmap')
        assert exc_info.value.capability == 'sqlalchemy'

    def test_none_dtype_is_refused(self):
        # _ANALYZE_TYPE does declare 'none' (as text), so a missing dtype has to be
        # refused before str(None).lower() can reach it.
        with pytest.raises(UnsupportedDtype) as exc_info:
            sqlalchemy_from_dtype(None)
        assert exc_info.value.capability is None
        assert 'missing' in str(exc_info.value)

    def test_source_spelling_refusal_names_the_canonical_dtype(self):
        # Was: claimed 'nvarchar(255)' was not a recognized source type spelling, while
        # analyze_type('nvarchar(255)') == 'text'. A refusal that lies is worse than a
        # raw KeyError -- it invites someone to add a row that already exists.
        with pytest.raises(UnsupportedDtype) as exc_info:
            sqlalchemy_from_dtype('nvarchar(255)')
        assert exc_info.value.canonical == 'text'
        assert "source type spelling for 'text'" in str(exc_info.value)

    def test_currency_source_spelling_still_infers_numeric(self):
        # The user-selected dtype and the source-type spelling are distinct:
        # inference keeps collapsing 'currency' source names to numeric, so
        # only an explicitly stored 'currency' dtype reaches PlaidCurrency.
        assert analyze_type('currency') == 'numeric'
        assert sqlalchemy_from_dtype(analyze_type('currency')) is PlaidNumeric


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

    @_REQUIRES_JSON
    def test_currency_is_decimal128_18_4(self):
        import pyarrow
        assert arrow_type_from_analyze_type('currency') == pyarrow.decimal128(18, 4)

    @_REQUIRES_JSON
    def test_currency_ignores_use_decimal_type(self):
        # use_decimal_type is only set by SF/MSSQL callers; currency must not
        # widen to decimal128(38, 10) there.
        import pyarrow
        assert arrow_type_from_analyze_type('currency', use_decimal_type=True) == pyarrow.decimal128(18, 4)

    @_REQUIRES_JSON
    @pytest.mark.parametrize('dtype', ['uuid', 'geometry', 'bitmap'])
    def test_dtype_without_an_arrow_representation_is_refused(self, dtype):
        # Was: numpy's `TypeError: data type 'uuid' not understood`, which names
        # neither the column nor plaid's dtype.
        with pytest.raises(UnsupportedDtype) as exc_info:
            arrow_type_from_analyze_type(dtype)
        assert exc_info.value.capability == 'arrow'
        assert exc_info.value.context == 'arrow_type_from_analyze_type'

    @_REQUIRES_JSON
    def test_undeclared_dtype_is_refused(self):
        with pytest.raises(UnsupportedDtype) as exc_info:
            arrow_type_from_analyze_type('totally_unknown_type_xyz')
        assert exc_info.value.capability is None

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


class TestDtypeRegistry:
    """Every dtype a stored column may carry.

    The sets below are literals copied from `plaid`, `plaid-utilities` and PlaidClient;
    nothing is imported, because no dependency on those repos exists here. So these
    tests fail when a declaration in DTYPES changes and never when the set on the other
    side changes -- they pin this repo's intent, they do not detect drift in the
    consumer. The consuming repos need their own test against DTYPES for that.
    """

    def test_every_inferred_dtype_is_declared(self):
        # _ANALYZE_TYPE (the INPUT direction) may only resolve to declared dtypes.
        inferred = {
            analyze_type(spelling)
            for spelling in (
                'nvarchar(5000)', 'bool', 'int8', 'int16', 'int32', 'int64', 'float64',
                'numeric', 'datetime', 'timedelta64[ns]', 'date', 'time', 'bytea',
                'uuid', 'jsonb', 'text',
            )
        }
        assert inferred <= set(DTYPES)
        assert len(inferred) == 13

    @pytest.mark.parametrize('dtype', sorted(DTYPES))
    def test_sqlalchemy_declaration_matches_the_type_map(self, dtype):
        try:
            sqlalchemy_from_dtype(dtype)
        except UnsupportedDtype:
            assert DTYPES[dtype].sqlalchemy is False
        else:
            assert DTYPES[dtype].sqlalchemy is True

    @pytest.mark.parametrize('dtype', sorted(DTYPES))
    def test_pandas_declaration_is_a_real_pandas_dtype(self, dtype):
        import pandas
        declared = DTYPES[dtype]
        if declared.pandas is not None:
            assert pandas.api.types.pandas_dtype(declared.pandas) is not None

    @pytest.mark.parametrize('dtype', sorted(DTYPES))
    def test_arrow_requires_a_pandas_representation(self, dtype):
        declared = DTYPES[dtype]
        if declared.arrow:
            assert declared.pandas is not None

    @pytest.mark.parametrize('dtype', sorted(DTYPES))
    def test_declared_states_are_in_range(self, dtype):
        declared = DTYPES[dtype]
        assert declared.default_agg in ('group', 'sum', None)
        assert declared.profilable in ('none', 'count_only', 'values')
        if declared.aggregatable:
            assert declared.default_agg == 'sum'

    def test_registry_covers_the_backend_join_enum(self):
        # plaid-utilities frame_join_multi_validator._DTYPE_ENUM, which is hand-kept
        # in at least two other repos. A dtype it admits must be declared joinable.
        join_enum = {
            'text', 'integer', 'bigint', 'smallint', 'tinyint', 'numeric', 'decimal',
            'float', 'double', 'boolean', 'currency', 'date', 'timestamp', 'time',
            'interval', 'json', 'uuid', 'serial', 'bigserial', 'largebinary',
        }
        assert join_enum <= set(DTYPES)
        assert {d for d in DTYPES if DTYPES[d].joinable_as_key} == join_enum

    def test_registry_is_exactly_the_picklist_plus_what_inference_emits(self):
        # PlaidClient Constants.js ANALYZE_DATA_TYPES, and the 13 dtypes analyze_type
        # can return. Asserted both ways: a key in neither is unreachable and should
        # not be here, and a missing key means a column dtype nothing can serve.
        picklist = {
            'text', 'varchar', 'numeric', 'currency', 'tinyint', 'smallint', 'integer',
            'bigint', 'float', 'double', 'decimal', 'boolean', 'serial', 'bigserial',
            'date', 'time', 'timestamp', 'interval', 'largebinary', 'uuid', 'json',
            'bitmap', 'geometry',
        }
        inferred = {
            'text', 'numeric', 'smallint', 'integer', 'bigint', 'boolean', 'date',
            'time', 'timestamp', 'interval', 'largebinary', 'uuid', 'json',
        }
        # 'geography' is in neither, but _sqlalchemy_from_dtype maps it, so a column
        # can carry it even though nothing offers or infers it.
        assert set(DTYPES) == picklist | inferred | {'geography'}

    def test_aggregatable_matches_the_ui_numeric_set(self):
        # PlaidClient Constants.js ANALYZE_NUMERIC_DATA_TYPES.
        numeric = {
            'numeric', 'currency', 'tinyint', 'smallint', 'integer', 'bigint',
            'float', 'double', 'decimal',
        }
        assert {d for d in DTYPES if DTYPES[d].aggregatable} == numeric

    def test_default_agg_matches_todays_agg_type(self):
        # plaid table_explorer_common.DEFAULT_GROUP_BY, preserved warts and all.
        assert {d for d in DTYPES if DTYPES[d].default_agg == 'group'} == {
            'text', 'boolean', 'date', 'timestamp', 'time',
        }

    def test_profilable_matches_todays_profile_sets(self):
        # plaid core/api_utilities/analyze/table.py _PROFILE_* sets.
        assert {d for d in DTYPES if DTYPES[d].profilable == 'none'} == {
            'largebinary', 'bitmap', 'geometry', 'json',
        }
        assert {d for d in DTYPES if DTYPES[d].profilable == 'values'} == {
            'text', 'varchar', 'tinyint', 'smallint', 'integer', 'bigint',
            'serial', 'bigserial',
        }

    def test_declaration_defaults_describe_an_ordinary_scalar(self):
        assert Dtype(pandas='object') == Dtype(
            pandas='object', arrow=True, sqlalchemy=True, joinable_as_key=True,
            aggregatable=False, default_agg='sum', profilable='count_only',
        )


class TestAdmitDtype:

    def test_canonical_dtype(self):
        assert admit_dtype('currency', 'test').pandas == 'float64'

    def test_canonical_declaration_wins_over_the_source_spelling(self):
        # 'currency' and 'tinyint' mean one thing as a stored dtype and another as a
        # source type name, where they infer to 'numeric' and 'smallint'.
        assert analyze_type('currency') == 'numeric'
        assert admit_dtype('tinyint', 'test').pandas == 'Int8'

    def test_source_spelling_resolves(self):
        assert admit_dtype('NVARCHAR(500)', 'test').pandas == 'object'

    def test_undeclared_is_refused_with_context(self):
        with pytest.raises(UnsupportedDtype) as exc_info:
            admit_dtype('totally_unknown_type_xyz', 'my_caller')
        assert exc_info.value.context == 'my_caller'
        assert 'my_caller' in str(exc_info.value)
        assert "'totally_unknown_type_xyz'" in str(exc_info.value)

    def test_refusal_is_a_value_error(self):
        assert issubclass(UnsupportedDtype, ValueError)
        assert not issubclass(UnsupportedDtype, KeyError)
        assert not issubclass(UnsupportedDtype, RegexMapKeyError)


class TestRequireDtypeCapability:

    def test_declared_capability_returns_the_declaration(self):
        assert require_dtype_capability('text', 'arrow', 'test').pandas == 'object'

    def test_refused_capability_names_it(self):
        with pytest.raises(UnsupportedDtype) as exc_info:
            require_dtype_capability('bitmap', 'sqlalchemy', 'test')
        assert exc_info.value.capability == 'sqlalchemy'
        assert 'not sqlalchemy-capable' in str(exc_info.value)

    def test_undeclared_dtype_is_refused_before_the_capability(self):
        with pytest.raises(UnsupportedDtype) as exc_info:
            require_dtype_capability('totally_unknown_type_xyz', 'arrow', 'test')
        assert exc_info.value.capability is None


class TestHalfAddedDtype:
    """The state 30352 passes through: an _ANALYZE_TYPE row landing before a DTYPES row."""

    def test_spelling_resolving_to_an_undeclared_dtype_is_refused(self):
        # Was: DTYPES[_ANALYZE_TYPE(key)] raised a plain KeyError straight through
        # `except RegexMapKeyError`, which does not catch its own parent class -- so the
        # boundary leaked the exact exception type it exists to stop.
        half_added = mock.patch.object(type_conversion, '_ANALYZE_TYPE', lambda key: 'vector')
        with half_added, pytest.raises(UnsupportedDtype) as exc_info:
            admit_dtype('embedding', 'test')
        assert exc_info.value.dtype == 'embedding'

    def test_the_leaked_error_is_not_a_key_error(self):
        with mock.patch.object(type_conversion, '_ANALYZE_TYPE', lambda key: 'vector'):
            try:
                admit_dtype('embedding', 'test')
            except KeyError:
                pytest.fail('a half-added dtype must not surface as KeyError')
            except UnsupportedDtype:
                pass


class TestMissingDtype:
    """A missing dtype is the canonical half-added-column bug, and _ANALYZE_TYPE
    declares 'none' as text, so str(None).lower() would admit it."""

    @pytest.mark.parametrize('converter', [
        analyze_type,
        pandas_dtype_from_sql,
        sqlalchemy_from_dtype,
        lambda dtype: admit_dtype(dtype, 'test'),
        lambda dtype: arrow_type_from_analyze_type(dtype),
    ])
    def test_none_is_refused(self, converter):
        with pytest.raises(UnsupportedDtype) as exc_info:
            converter(None)
        assert exc_info.value.capability is None
        assert 'missing' in str(exc_info.value)

    def test_empty_string_is_refused(self):
        with pytest.raises(UnsupportedDtype):
            analyze_type('')

    def test_the_declared_none_spelling_still_means_text(self):
        # Refusing a missing dtype must not disturb the 'none' *spelling*, which
        # _ANALYZE_TYPE declares and which parquet imports emit.
        assert analyze_type('none') == 'text'


class TestCapabilityStates:
    """Tri-state axes must not be admitted by truthiness: 'none' and 'group' are both
    truthy strings."""

    @pytest.mark.parametrize('dtype,capability', [
        ('text', 'pandas'),
        ('text', 'arrow'),
        ('text', 'sqlalchemy'),
        ('text', 'joinable_as_key'),
        ('numeric', 'aggregatable'),
        ('text', 'default_agg'),
        ('text', 'profilable'),
    ])
    def test_available_capability_passes(self, dtype, capability):
        assert require_dtype_capability(dtype, capability, 'test') is DTYPES[dtype]

    @pytest.mark.parametrize('dtype,capability', [
        ('uuid', 'pandas'),
        ('uuid', 'arrow'),
        ('bitmap', 'sqlalchemy'),
        ('varchar', 'joinable_as_key'),
        ('text', 'aggregatable'),
        ('bitmap', 'profilable'),
    ])
    def test_unavailable_capability_refuses(self, dtype, capability):
        with pytest.raises(UnsupportedDtype) as exc_info:
            require_dtype_capability(dtype, capability, 'test')
        assert exc_info.value.capability == capability

    def test_every_axis_has_an_unavailable_state(self):
        assert set(type_conversion._UNAVAILABLE) == set(Dtype._fields)

    def test_unknown_capability_is_a_programming_error(self):
        with pytest.raises(ValueError, match='unknown dtype capability'):
            require_dtype_capability('text', 'indexable', 'test')


class TestMasterEquivalence:
    """Pins the behaviour this story means to keep, measured against master.

    The DDL round trip is what `plaid-utilities/plaidcloud/utilities/query.py` performs
    on every CSV read: a column's SQLAlchemy type, stringified, back to a pandas dtype.
    """

    @pytest.mark.parametrize('sqlalchemy_str,expected', [
        ('BIGINT', 'Int64'),
        ('BOOLEAN', 'bool'),
        ('DATE', 'datetime64[s]'),
        ('DATETIME', 'datetime64[s]'),
        ('DECIMAL(18, 4)', 'float64'),
        ('INTEGER', 'Int64'),
        ('JSON', 'object'),
        ('NUMERIC', 'float64'),
        ('NVARCHAR(4000)', 'object'),
        ('SMALLINT', 'Int16'),
        ('TEXT', 'object'),
        ('TIME', 'datetime64[s]'),
        ('TIMESTAMP', 'datetime64[s]'),
    ])
    def test_ddl_round_trip_is_unchanged(self, sqlalchemy_str, expected):
        assert pandas_dtype_from_sql(sqlalchemy_str) == expected

    @pytest.mark.parametrize('sqlalchemy_str,expected,master', [
        ('DOUBLE', 'float64', 'double'),  # numpy reads both as float64
        ('FLOAT', 'float64', 'float'),  # likewise
        ('CHAR(36)', 'object', 'char(36)'),  # master's value was not a pandas dtype
    ])
    def test_ddl_round_trip_changed_deliberately(self, sqlalchemy_str, expected, master):
        assert pandas_dtype_from_sql(sqlalchemy_str) == expected != master

    def test_largebinary_ddl_round_trip_now_refuses(self):
        # str(LargeBinary()) is 'BLOB', which no vocabulary declares. Master returned
        # 'blob', which pandas rejects, so this path could never have worked.
        with pytest.raises(UnsupportedDtype):
            pandas_dtype_from_sql('BLOB')

    @pytest.mark.parametrize('dtype,arrow_str', [
        ('bigint', 'int64'),
        ('boolean', 'bool'),
        ('currency', 'decimal128(18, 4)'),
        ('date', 'date64[ms]'),
        ('decimal', 'double'),
        ('double', 'double'),
        ('float', 'double'),
        ('integer', 'int64'),
        ('interval', 'duration[s]'),
        ('json', 'extension<arrow.json>'),
        ('largebinary', 'string'),
        ('numeric', 'double'),
        ('smallint', 'int16'),
        ('text', 'string'),
        ('time', 'timestamp[s]'),
        ('timestamp', 'timestamp[s]'),
        ('tinyint', 'int8'),
        ('varchar', 'string'),
    ])
    @_REQUIRES_JSON
    def test_parquet_write_shape_is_unchanged(self, dtype, arrow_str):
        assert str(arrow_type_from_analyze_type(dtype)) == arrow_str

    @pytest.mark.parametrize('dtype', ['serial', 'bigserial'])
    @_REQUIRES_JSON
    def test_parquet_write_now_works_where_master_crashed(self, dtype):
        # Master reached numpy with 'serial', which is not a dtype it understands.
        assert str(arrow_type_from_analyze_type(dtype)) == 'int64'

    @pytest.mark.parametrize('dtype,arrow_str', [
        ('int8', 'int16'),
        ('int16', 'int16'),
        ('int32', 'int64'),
        ('int64', 'int64'),
        ('float16', 'double'),
        ('float32', 'double'),
        ('timedelta64[ns]', 'duration[s]'),
    ])
    @_REQUIRES_JSON
    def test_numpy_spellings_widen(self, dtype, arrow_str):
        # A numpy dtype spelling is not an analyze dtype, and the four call sites pass
        # analyze dtypes from column meta. Master passed these straight to numpy and got
        # the narrow type; they now resolve through the registry and widen. Pinned so a
        # change is deliberate rather than noticed in a parquet file.
        assert str(arrow_type_from_analyze_type(dtype)) == arrow_str

    @pytest.mark.parametrize('dtype', ['uint8', 'uint16', 'uint32', 'uint64', 'str'])
    @_REQUIRES_JSON
    def test_unsigned_and_str_spellings_now_refuse(self, dtype):
        # Master produced a correct arrow type for these; no analyze dtype is unsigned,
        # so reaching here means a caller passed a numpy dtype rather than a column's.
        with pytest.raises(UnsupportedDtype):
            arrow_type_from_analyze_type(dtype)


class TestRefusalMessages:

    def test_undeclared(self):
        with pytest.raises(UnsupportedDtype, match='not a declared analyze dtype'):
            admit_dtype('zzz', 'test')

    def test_missing(self):
        with pytest.raises(UnsupportedDtype, match='missing; no dtype was given'):
            admit_dtype(None, 'test')

    def test_capability_on_a_canonical_dtype(self):
        with pytest.raises(UnsupportedDtype, match=r"dtype 'uuid': not pandas-capable \(asked by test\)"):
            require_dtype_capability('uuid', 'pandas', 'test')

    def test_capability_on_a_source_spelling(self):
        with pytest.raises(UnsupportedDtype, match="a source type spelling for 'text'"):
            require_dtype_capability('nvarchar(255)', 'aggregatable', 'test')
