#!/usr/bin/env python
# coding=utf-8

import datetime
import decimal
import io
from unittest import mock

import pytest

from plaidcloud.rpc.messytables import (
    Cell,
    RowSet,
    TableSet,
    seekable_stream,
)
from plaidcloud.rpc.messytables.core import (
    BufferedFile,
    CoreProperties,
)
from plaidcloud.rpc.messytables.error import (
    MessytablesError,
    ReadError,
    TableError,
    NoSuchPropertyError,
)
from plaidcloud.rpc.messytables.types import (
    CellType,
    StringType,
    IntegerType,
    DecimalType,
    BoolType,
    TimeType,
    DateType,
    DateUtilType,
    PercentageType,
    CurrencyType,
    FloatType,
    type_guess,
    types_processor,
)
from plaidcloud.rpc.messytables.headers import (
    headers_guess,
    headers_processor,
    headers_make_unique,
    column_count_modal,
)
from plaidcloud.rpc.messytables.util import offset_processor, null_processor
from plaidcloud.rpc.messytables.dateparser import is_date, create_date_formats
from plaidcloud.rpc.messytables.any import clean_ext, guess_ext, guess_mime


class TestCell:

    def test_basic_cell(self):
        cell = Cell('hello')
        assert cell.value == 'hello'
        assert cell.column is None
        assert cell.type is not None

    def test_empty_none_is_empty(self):
        cell = Cell(None)
        assert cell.empty is True

    def test_empty_whitespace_is_empty(self):
        cell = Cell('   ')
        assert cell.empty is True

    def test_non_empty(self):
        assert Cell('hi').empty is False

    def test_int_value_not_empty(self):
        assert Cell(42).empty is False

    def test_repr_without_column(self):
        cell = Cell('x')
        assert 'x' in repr(cell)

    def test_repr_with_column(self):
        cell = Cell('x', column='col1')
        assert 'col1' in repr(cell)

    def test_topleft_default_true(self):
        assert Cell('a').topleft is True

    def test_properties_returns_core_properties(self):
        assert isinstance(Cell('x').properties, CoreProperties)


class TestCoreProperties:

    def test_len_zero(self):
        cp = CoreProperties()
        assert len(cp) == 0

    def test_iter_empty(self):
        assert list(CoreProperties()) == []

    def test_getitem_missing_raises(self):
        with pytest.raises(NoSuchPropertyError):
            CoreProperties()['missing_key']


class TestStringType:

    def test_cast_none(self):
        assert StringType().cast(None) is None

    def test_cast_string_passthrough(self):
        assert StringType().cast('hello') == 'hello'

    def test_cast_int(self):
        assert StringType().cast(42) == '42'

    def test_result_type(self):
        assert StringType.result_type is str


class TestIntegerType:

    def test_cast_empty(self):
        assert IntegerType().cast('') is None

    def test_cast_none(self):
        assert IntegerType().cast(None) is None

    def test_cast_integer_string(self):
        assert IntegerType().cast('42') == 42

    def test_cast_float_string_if_integer(self):
        assert IntegerType().cast('42.0') == 42

    def test_cast_non_integer_raises(self):
        with pytest.raises(ValueError):
            IntegerType().cast('42.5')


class TestDecimalType:

    def test_cast_empty(self):
        assert DecimalType().cast('') is None

    def test_cast_none(self):
        assert DecimalType().cast(None) is None

    def test_cast_decimal(self):
        assert DecimalType().cast('3.14') == decimal.Decimal('3.14')

    def test_cast_integer(self):
        assert DecimalType().cast('42') == decimal.Decimal('42')


class TestPercentageType:

    def test_cast_divides_by_100(self):
        assert PercentageType().cast('50') == decimal.Decimal('0.5')

    def test_cast_none_returns_none(self):
        assert PercentageType().cast(None) is None


class TestCurrencyType:

    def test_cast_strips_suffix(self):
        assert CurrencyType().cast('42.5 USD') == decimal.Decimal('42.5')


class TestBoolType:

    def test_cast_yes(self):
        assert BoolType().cast('yes') is True

    def test_cast_no(self):
        assert BoolType().cast('no') is False

    def test_cast_zero_default_is_true(self):
        # default true_values include '0'
        assert BoolType().cast('0') is True

    def test_cast_invalid_raises(self):
        with pytest.raises(ValueError):
            BoolType().cast('maybe')

    def test_custom_true_false_values(self):
        bt = BoolType(true_values=('y',), false_values=('n',))
        assert bt.cast('y') is True
        assert bt.cast('n') is False


class TestTimeType:

    def test_cast_empty_string(self):
        assert TimeType().cast('') is None

    def test_cast_none(self):
        assert TimeType().cast(None) is None

    def test_cast_already_time(self):
        t = datetime.time(12, 0, 0)
        assert TimeType().cast(t) == t


class TestDateType:

    def test_cast_empty_string(self):
        assert DateType('%Y-%m-%d').cast('') is None

    def test_cast_none(self):
        assert DateType('%Y-%m-%d').cast(None) is None

    def test_cast_valid(self):
        result = DateType('%Y-%m-%d').cast('2024-01-15')
        assert isinstance(result, datetime.datetime)

    def test_equality(self):
        assert DateType('%Y-%m-%d') == DateType('%Y-%m-%d')
        assert DateType('%Y-%m-%d') != DateType('%d/%m/%Y')

    def test_hash(self):
        assert hash(DateType('%Y-%m-%d')) == hash(DateType('%Y-%m-%d'))

    def test_repr(self):
        assert '%Y-%m-%d' in repr(DateType('%Y-%m-%d'))

    def test_test_non_date_string_false(self):
        dt = DateType('%Y-%m-%d')
        assert dt.test('not a date') is False

    def test_instances_returns_list(self):
        instances = DateType.instances()
        assert isinstance(instances, list)
        assert len(instances) > 0


class TestDateUtilType:

    def test_test_rejects_non_datetime_strings(self):
        dut = DateUtilType()
        assert dut.test('not a date') is False

    def test_test_accepts_datetime(self):
        dut = DateUtilType()
        assert dut.test(datetime.datetime(2024, 1, 1)) is True


class TestCellTypeBase:

    def test_repr_strips_type_suffix(self):
        assert repr(StringType()) == 'String'

    def test_equality(self):
        assert StringType() == StringType()
        assert StringType() != IntegerType()

    def test_hash(self):
        assert hash(StringType()) == hash(StringType())

    def test_instances_returns_list(self):
        assert StringType.instances() == [StringType()]

    def test_test_uses_isinstance_fast_path(self):
        st = StringType()
        assert st.test('hello') is True


class TestTypeGuess:

    def test_basic(self):
        rows = [[Cell('1')], [Cell('2')]]
        result = type_guess(rows)
        assert len(result) == 1

    def test_strict(self):
        rows = [[Cell('1')], [Cell('2')], [Cell('3')]]
        result = type_guess(rows, strict=True)
        assert len(result) == 1

    def test_strict_empty_column(self):
        rows = [[Cell('')], [Cell('')]]
        result = type_guess(rows, strict=True)
        # Column with no values defaults to StringType
        assert isinstance(result[0], StringType)


class TestTypesProcessor:

    def test_no_types_passthrough(self):
        fn = types_processor(None)
        row = [Cell('x')]
        assert fn(None, row) == row

    def test_casts_values(self):
        fn = types_processor([IntegerType()])
        row = [Cell('42')]
        result = fn(None, row)
        assert result[0].value == 42


class TestHeaders:

    def test_column_count_modal_empty(self):
        assert column_count_modal([]) == 0

    def test_column_count_modal_basic(self):
        rows = [
            [Cell('a'), Cell('b'), Cell('c')],
            [Cell('1'), Cell('2'), Cell('3')],
            [Cell('x'), Cell('y')],
        ]
        assert column_count_modal(rows) == 3

    def test_headers_guess_basic(self):
        rows = [
            [Cell('a'), Cell('b')],
            [Cell('1'), Cell('2')],
        ]
        offset, headers = headers_guess(rows)
        assert offset == 0
        assert headers == ['a', 'b']

    def test_headers_guess_no_data(self):
        offset, headers = headers_guess([])
        assert offset == 0
        assert headers == []

    def test_headers_processor_adds_names(self):
        fn = headers_processor(['col_a', 'col_b'])
        row = [Cell('1'), Cell('2')]
        result = fn(None, row)
        assert result[0].column == 'col_a'
        assert result[1].column == 'col_b'

    def test_headers_processor_autogenerates_for_missing(self):
        fn = headers_processor([])
        row = [Cell('1'), Cell('2')]
        result = fn(None, row)
        assert result[0].column == 'column_0'
        assert result[0].column_autogenerated is True

    def test_headers_processor_handles_extra_header(self):
        fn = headers_processor(['col_a', 'col_b', 'col_c'])
        row = [Cell('1')]
        result = fn(None, row)
        # Should pad with autogenerated rows for extra headers
        assert len(result) == 3

    def test_headers_make_unique_no_dup(self):
        assert headers_make_unique(['a', 'b', 'c']) == ['a', 'b', 'c']

    def test_headers_make_unique_with_dup(self):
        result = headers_make_unique(['a', 'a', 'b'])
        assert result == ['a_1', 'a_2', 'b']

    def test_headers_make_unique_max_length(self):
        result = headers_make_unique(
            ['abcdef', 'abcdef'], max_length=4,
        )
        # Should be truncated
        assert all(len(h) <= 4 for h in result)

    def test_headers_make_unique_impossible_max_length_raises(self):
        # max_length too small to make them unique
        with pytest.raises(ValueError):
            headers_make_unique(['aaa', 'aaa'], max_length=1)


class TestUtilProcessors:

    def test_offset_processor_skips(self):
        from plaidcloud.rpc.messytables.util import offset_processor
        row_set = RowSet()
        fn = offset_processor(2)
        # First 2 rows return None (skipped)
        assert fn(row_set, [Cell('a')]) is None
        assert fn(row_set, [Cell('b')]) is None
        # 3rd row passes through unchanged (returns the row as-is)
        row = [Cell('c')]
        result = fn(row_set, row)
        assert result is row

    def test_null_processor_replaces(self):
        fn = null_processor(['NULL', 'N/A'])
        row = [Cell('NULL'), Cell('hello'), Cell('N/A')]
        result = fn(None, row)
        assert result[0].value is None
        assert result[1].value == 'hello'
        assert result[2].value is None


class TestDateparser:

    def test_is_date_short_string(self):
        # len=1 returns falsy per source
        assert not is_date('1')

    def test_is_date_valid(self):
        assert is_date('2024-01-15')

    def test_is_date_invalid(self):
        assert not is_date('not a date')

    def test_create_date_formats_day_first(self):
        formats = create_date_formats(day_first=True)
        assert isinstance(formats, list)
        assert len(formats) > 0

    def test_create_date_formats_month_first(self):
        formats = create_date_formats(day_first=False)
        assert isinstance(formats, list)
        assert len(formats) > 0


class TestAnyModule:

    def test_clean_ext_empty(self):
        assert clean_ext('') == ''

    def test_clean_ext_simple(self):
        assert clean_ext('tsv') == 'tsv'

    def test_clean_ext_uppercase(self):
        assert clean_ext('FILE.ZIP') == 'zip'

    def test_clean_ext_url(self):
        assert clean_ext('http://myserver.info/file.xlsx?download=True') == 'xlsx'

    def test_guess_ext_csv(self):
        assert guess_ext('csv') == 'CSV'

    def test_guess_ext_tsv(self):
        assert guess_ext('tsv') == 'TAB'

    def test_guess_ext_zip(self):
        assert guess_ext('zip') == 'ZIP'

    def test_guess_ext_unknown(self):
        assert guess_ext('unknown') is None

    def test_guess_mime_csv(self):
        assert guess_mime('text/csv') == 'CSV'

    def test_guess_mime_zip(self):
        assert guess_mime('application/zip') == 'ZIP'

    def test_guess_mime_fuzzy(self):
        assert guess_mime('Composite Document File V2 Document') == 'XLS'

    def test_guess_mime_unknown(self):
        assert guess_mime('application/unknown') is None


class TestErrors:

    def test_messytables_error_is_exception(self):
        assert issubclass(MessytablesError, Exception)

    def test_read_error(self):
        assert issubclass(ReadError, MessytablesError)

    def test_table_error(self):
        assert issubclass(TableError, MessytablesError)
        assert issubclass(TableError, LookupError)

    def test_no_such_property_error(self):
        assert issubclass(NoSuchPropertyError, MessytablesError)
        assert issubclass(NoSuchPropertyError, KeyError)


class TestBufferedFile:

    def test_read_from_start(self):
        bf = BufferedFile(io.BytesIO(b'hello world'))
        assert bf.read(5) == b'hello'

    def test_tell(self):
        bf = BufferedFile(io.BytesIO(b'hello world'))
        bf.read(5)
        assert bf.tell() == 5

    def test_seek(self):
        bf = BufferedFile(io.BytesIO(b'hello world'))
        bf.read(5)
        bf.seek(0)
        assert bf.tell() == 0

    def test_readline(self):
        bf = BufferedFile(io.BytesIO(b'line1\nline2\n'))
        assert bf.readline() == b'line1\n'
        assert bf.readline() == b'line2\n'

    def test_read_all(self):
        bf = BufferedFile(io.BytesIO(b'short content'))
        assert bf.read(-1) == b'short content'


class TestSeekableStream:

    def test_already_seekable(self):
        stream = io.BytesIO(b'data')
        assert seekable_stream(stream) is stream

    def test_wraps_unseekable(self):
        class NotSeekable:
            def read(self, n):
                return b''

            def seek(self, offset):
                raise IOError('cannot seek')
        ns = NotSeekable()
        result = seekable_stream(ns)
        assert isinstance(result, BufferedFile)


class TestTableSet:

    def test_getitem_no_match_raises(self):
        class DummyTableSet(TableSet):
            def make_tables(self):
                return []
        ts = DummyTableSet(io.BytesIO(b''))
        with pytest.raises(TableError):
            ts['nonexistent']

    def test_getitem_matches(self):
        fake_table = mock.MagicMock()
        fake_table.name = 'foo'

        class DummyTableSet(TableSet):
            def make_tables(self):
                return [fake_table]
        ts = DummyTableSet(io.BytesIO(b''))
        assert ts['foo'] is fake_table

    def test_getitem_multiple_raises(self):
        t1 = mock.MagicMock()
        t1.name = 'dup'
        t2 = mock.MagicMock()
        t2.name = 'dup'

        class DummyTableSet(TableSet):
            def make_tables(self):
                return [t1, t2]
        ts = DummyTableSet(io.BytesIO(b''))
        with pytest.raises(TableError):
            ts['dup']

    def test_make_tables_not_implemented(self):
        class BadTableSet(TableSet):
            pass
        ts = BadTableSet(io.BytesIO(b''))
        with pytest.raises(NotImplementedError):
            ts.make_tables()

    def test_from_fileobj(self):
        class DummyTableSet(TableSet):
            def make_tables(self):
                return []
        ts = DummyTableSet.from_fileobj(io.BytesIO(b''))
        assert isinstance(ts, DummyTableSet)


class TestRowSet:

    def test_set_and_get_types(self):
        rs = RowSet()
        assert rs.typed is False
        rs.set_types([StringType()])
        assert rs.typed is True
        assert rs.get_types() == [StringType()]

    def test_types_property(self):
        rs = RowSet()
        rs.types = [IntegerType()]
        assert rs.types == [IntegerType()]

    def test_register_processor(self):
        rs = RowSet()
        rs.register_processor(lambda rs, row: row)
        assert len(rs._processors) == 1

    def test_raw_not_implemented(self):
        rs = RowSet()
        with pytest.raises(NotImplementedError):
            list(rs.raw())


class TestFloatType:

    def test_deprecated_alias_works(self):
        # FloatType is deprecated, just check it's a subclass of DecimalType
        assert issubclass(FloatType, DecimalType)


class TestAnyTableSet:
    """Tests for any_tableset - the main entry point."""

    def test_by_mimetype(self):
        from plaidcloud.rpc.messytables.any import any_tableset
        stream = io.BytesIO(b'a,b\n1,2\n')
        result = any_tableset(stream, mimetype='text/csv')
        assert result is not None

    def test_by_extension(self):
        from plaidcloud.rpc.messytables.any import any_tableset
        stream = io.BytesIO(b'a,b\n1,2\n')
        result = any_tableset(stream, extension='csv')
        assert result is not None

    def test_by_tsv_extension(self):
        from plaidcloud.rpc.messytables.any import any_tableset
        stream = io.BytesIO(b'a\tb\n1\t2\n')
        result = any_tableset(stream, extension='tsv')
        assert result is not None

    def test_unknown_mimetype_raises(self):
        from plaidcloud.rpc.messytables.any import any_tableset
        stream = io.BytesIO(b'data')
        with pytest.raises(Exception):
            any_tableset(stream, mimetype='application/unknown', auto_detect=False)

    def test_unknown_extension_raises(self):
        from plaidcloud.rpc.messytables.any import any_tableset
        stream = io.BytesIO(b'data')
        with pytest.raises(Exception):
            any_tableset(stream, extension='xyz', auto_detect=False)

    def test_no_detection_raises(self):
        from plaidcloud.rpc.messytables.any import any_tableset
        stream = io.BytesIO(b'data')
        with pytest.raises(Exception):
            any_tableset(stream, auto_detect=False)


class TestAnyTableSetDeprecated:

    def test_from_fileobj_deprecated_passthrough(self):
        from plaidcloud.rpc.messytables.any import AnyTableSet
        stream = io.BytesIO(b'a,b\n1,2\n')
        result = AnyTableSet.from_fileobj(stream, mimetype='text/csv')
        assert result is not None


class TestRowSetDicts:

    def test_dicts_iteration(self):
        class DummyRowSet(RowSet):
            def raw(self, sample=False):
                yield [Cell('a', column='c1'), Cell('b', column='c2')]

        rs = DummyRowSet()
        dicts = list(rs.dicts())
        assert len(dicts) == 1
        assert dicts[0]['c1'] == 'a'
        assert dicts[0]['c2'] == 'b'


class TestRowSetRepr:

    def test_repr(self):
        class DummyRowSet(RowSet):
            name = 'tbl'

            def raw(self, sample=False):
                return iter([])

        rs = DummyRowSet()
        assert 'tbl' in repr(rs)


class TestDateUtilType:

    def test_cast_without_dateutil_raises(self):
        # If dateutil isn't importable, raises ImportError
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == 'dateutil.parser' or (name == 'dateutil' and 'parser' in (args[3] if len(args) > 3 else [])):
                raise ImportError('no dateutil')
            return real_import(name, *args, **kwargs)

        with mock.patch.object(builtins, '__import__', side_effect=fake_import):
            with pytest.raises(ImportError, match='full install'):
                DateUtilType().cast('2024-01-01')

    def test_cast_none_returns_none(self):
        try:
            import dateutil.parser  # noqa: F401
        except ImportError:
            pytest.skip('dateutil not installed')
        assert DateUtilType().cast(None) is None


class TestZipTableSetSkipsInvalid:

    def test_zip_skips_value_error(self):
        import io
        import zipfile
        from plaidcloud.rpc.messytables import zip as zip_mod

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as z:
            z.writestr('data.csv', 'a,b\n1,2\n')
        buf.seek(0)

        # Mock any_tableset to raise ValueError, which zip_mod catches
        real_any = zip_mod.messytables.any.any_tableset

        def fake_any(*args, **kwargs):
            raise ValueError('bad')

        with mock.patch.object(zip_mod.messytables.any, 'any_tableset', side_effect=fake_any):
            with pytest.raises(Exception):
                zip_mod.ZIPTableSet(buf)


class TestCsvRowSetErrorPaths:

    def test_csv_with_null_bytes(self):
        # Test CSV containing a null byte (should be skipped by csv parser)
        import io
        from plaidcloud.rpc.messytables.commas import CSVRowSet
        stream = io.BytesIO(b'a,b\n1,2\n\x00\n')
        rs = CSVRowSet('t', stream)
        # Should iterate without raising
        list(rs)

    def test_csv_with_newline_inside_sample(self):
        import io
        from plaidcloud.rpc.messytables.commas import CSVRowSet
        stream = io.BytesIO(b'a,"b\nb2",c\n1,2,3\n')
        rs = CSVRowSet('t', stream)
        # Should handle the multi-line field in sample mode
        list(rs.sample)


class TestIntegerTypeFromLocale:

    def test_integer_thousands_separator(self):
        import locale
        # Try to use a locale that supports thousands separators
        try:
            locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        except locale.Error:
            pytest.skip('en_US.UTF-8 locale not available')
        try:
            result = IntegerType().cast('1,000')
            assert result == 1000
        finally:
            locale.setlocale(locale.LC_ALL, 'C')


class TestGetMimeMocked:
    """Tests get_mime with mocked python-magic."""

    def test_get_mime_csv(self):
        import io
        from plaidcloud.rpc.messytables.any import get_mime
        mock_magic = mock.MagicMock()
        mock_magic.from_buffer = mock.Mock(return_value='text/csv')
        with mock.patch.dict('sys.modules', {'magic': mock_magic}):
            stream = io.BytesIO(b'a,b\n1,2\n')
            result = get_mime(stream)
            assert result == 'text/csv'

    def test_get_mime_zip_ooxml(self):
        import io
        from plaidcloud.rpc.messytables.any import get_mime
        mock_magic = mock.MagicMock()
        mock_magic.from_buffer = mock.Mock(return_value='application/zip')
        with mock.patch.dict('sys.modules', {'magic': mock_magic}):
            stream = io.BytesIO(b'[Content_Types].xml' + b'\x00' * 10)
            result = get_mime(stream)
            # Returns OOXML mimetype
            assert 'spreadsheetml' in result or result == 'application/zip'

    def test_get_mime_xlsx_with_pk(self):
        import io
        from plaidcloud.rpc.messytables.any import get_mime
        mock_magic = mock.MagicMock()
        mock_magic.from_buffer = mock.Mock(return_value='application/vnd.ms-excel')
        with mock.patch.dict('sys.modules', {'magic': mock_magic}):
            # PK header suggests it's really XLSX
            stream = io.BytesIO(b'PK' + b'\x03\x04' + b'junk' * 100)
            result = get_mime(stream)
            assert 'spreadsheetml' in result

    def test_get_mime_no_magic_raises(self):
        import io
        from plaidcloud.rpc.messytables.any import get_mime
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == 'magic':
                raise ImportError('no magic')
            return real_import(name, *args, **kwargs)

        with mock.patch.object(builtins, '__import__', side_effect=fake_import):
            with pytest.raises(ImportError, match='full install'):
                get_mime(io.BytesIO(b'data'))


class TestAnyTableSetAutoDetect:

    def test_auto_detect_with_mocked_magic(self):
        import io
        from plaidcloud.rpc.messytables.any import any_tableset
        mock_magic = mock.MagicMock()
        mock_magic.from_buffer = mock.Mock(return_value='text/csv')
        with mock.patch.dict('sys.modules', {'magic': mock_magic}):
            stream = io.BytesIO(b'a,b\n1,2\n')
            result = any_tableset(stream)
            assert result is not None

    def test_auto_detect_with_unrecognized_mime(self):
        """Cover lines 163-165 — get_mime returns unrecognized mimetype."""
        import io
        from plaidcloud.rpc.messytables.any import any_tableset
        from plaidcloud.rpc.messytables.error import ReadError
        mock_magic = mock.MagicMock()
        mock_magic.from_buffer = mock.Mock(return_value='application/strange')
        with mock.patch.dict('sys.modules', {'magic': mock_magic}):
            stream = io.BytesIO(b'garbage')
            with pytest.raises(ReadError, match='Did not recognise detected'):
                any_tableset(stream)


class TestStringTypeUnicode:

    def test_non_ascii_preserved(self):
        assert StringType().cast('café') == 'café'


class TestTimeTypeCast:

    def test_cast_time_string(self):
        result = TimeType().cast('0012:30:45')
        assert isinstance(result, datetime.time)
        assert result.hour == 12
        assert result.minute == 30
        assert result.second == 45

    def test_cast_time_over_24_returns_timedelta(self):
        result = TimeType().cast('0026:00:00')
        assert isinstance(result, datetime.timedelta)


class TestBufferedFileEdge:

    def test_read_past_buffer_fallback(self):
        """After filling the buffer, continue reading from the underlying fp."""
        # Make a buffer with tiny buffer_size
        fp = io.BytesIO(b'abcdef' * 100)
        bf = BufferedFile(fp, buffer_size=10)
        # Read more than buffer size to trigger overflow fallback
        data = bf.read(50)
        assert len(data) == 50

    def test_read_all_after_partial(self):
        fp = io.BytesIO(b'123456789012345')
        bf = BufferedFile(fp)
        bf.read(5)
        rest = bf.read(-1)
        assert rest == b'6789012345'

    def test_seek_forward(self):
        fp = io.BytesIO(b'1234567890')
        bf = BufferedFile(fp)
        bf.read(5)
        bf.seek(3)
        assert bf.tell() == 3

    def test_readline_from_iterator_style_fp(self):
        """_next_line falls back to next() if .readline() attribute is missing."""
        class IteratorStream:
            def __init__(self, lines):
                self.lines = iter(lines)

            def read(self, n):
                return b''

            def __next__(self):
                return next(self.lines)

            def seek(self, offset):
                # Make it seekable
                pass
        fp = IteratorStream([b'line1\n', b'line2\n'])
        bf = BufferedFile(fp)
        line = bf.readline()
        assert line == b'line1\n'


class TestTypesEdgeCases:

    def test_date_type_cast_passthrough_datetime(self):
        d = datetime.datetime(2024, 1, 1)
        assert DateType('%Y-%m-%d').cast(d) == d

    def test_date_type_cast_none_format(self):
        dt = DateType(None)
        assert dt.cast('2024-01-01') == '2024-01-01'

    def test_dateutil_type_cast_valid(self):
        try:
            import dateutil.parser  # noqa: F401
        except ImportError:
            pytest.skip('dateutil not installed')
        result = DateUtilType().cast('2024-01-01')
        assert isinstance(result, datetime.datetime)


class TestCellTypeTest:

    def test_cell_type_test_failure_returns_false(self):
        # Test that when cast raises, test returns False
        class BadType(CellType):
            result_type = int

            def cast(self, value):
                raise ValueError('always fail')
        # But test falls through to check isinstance first, then cast
        assert BadType().test('not_int') is False

    def test_cell_type_test_isinstance_fast_path(self):
        # When value matches result_type
        assert IntegerType().test(42) is True

    def test_base_cell_type_cast_is_identity(self):
        """CellType.cast is the default identity function."""
        class TypeNoResult(CellType):
            result_type = object
        assert TypeNoResult().cast('anything') == 'anything'


class TestStringTypeUnicodeEncodeError:
    """Cover the UnicodeEncodeError fallback (lines 63-64)."""

    def test_str_raises_unicode_encode_error(self):
        """Patch str() to raise UnicodeEncodeError once, the except retries."""
        # Create an object whose __str__ raises UnicodeEncodeError
        class Raises:
            def __str__(self):
                # Raise the first time, then succeed
                if not getattr(self, '_called', False):
                    self._called = True
                    raise UnicodeEncodeError('ascii', 'x', 0, 1, 'reason')
                return 'succeeded'

        obj = Raises()
        # StringType.cast will catch UnicodeEncodeError, then call str() again
        result = StringType().cast(obj)
        assert result == 'succeeded'


class TestDecimalTypeLocaleFallback:
    """Cover the locale.atof fallback (lines 103-105)."""

    def test_thousand_separator_via_locale(self):
        import locale
        # Try a locale that supports thousands separators
        try:
            locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        except locale.Error:
            pytest.skip('en_US.UTF-8 locale not available')
        try:
            # DecimalType() falls back to locale.atof when direct Decimal fails
            result = DecimalType().cast('1,000.5')
            assert result == decimal.Decimal('1000.5')
        finally:
            locale.setlocale(locale.LC_ALL, 'C')


class TestBoolTypeEmptyValue:

    def test_cast_empty_returns_none(self):
        """Cover line 152 — value == '' check."""
        assert BoolType().cast('') is None


class TestDateTypeTestIsStringDate:

    def test_test_valid_date_string(self):
        """Cover line 197 — returning via CellType.test."""
        dt = DateType('%Y-%m-%d')
        # is_date returns match object on '2024-01-15'
        assert dt.test('2024-01-15') is True


class TestDateUtilTypeTest:
    """Cover lines 230-233: the negative path of DateUtilType.test."""

    def test_rejects_integer(self):
        assert DateUtilType().test(42) is False

    def test_rejects_non_date_string(self):
        assert DateUtilType().test('not a date') is False

    def test_accepts_date_string(self):
        try:
            import dateutil.parser  # noqa: F401
        except ImportError:
            pytest.skip('dateutil not installed')
        # is_date returns true for '2024-01-15'
        assert DateUtilType().test('2024-01-15') is True


class TestTypeGuessContinueEmpty:
    """Cover line 297 — the `continue` path when cell value is empty."""

    def test_type_guess_with_empty_cells_non_strict(self):
        rows = [
            [Cell(''), Cell('')],
            [Cell('1'), Cell('2')],
        ]
        result = type_guess(rows)
        assert len(result) == 2


class TestTypesProcessorStrict:
    """Cover lines 326-328 — strict mode raising on cast failures."""

    def test_strict_raises(self):
        fn = types_processor([IntegerType()], strict=True)
        row = [Cell('not_an_integer')]
        with pytest.raises(Exception):
            fn(None, row)


class TestBufferedFileMoreBranches:
    """Target specific lines 44, 55-56, 67, 78-79, 87, 98 in core.py"""

    def test_readline_from_buffered_data(self):
        """After reading and seeking back, readline pulls from buffer (55-56)."""
        fp = io.BytesIO(b'line1\nline2\n')
        bf = BufferedFile(fp, buffer_size=100)
        # Read first line into buffer
        bf.readline()
        # Seek back to start
        bf.seek(0)
        # Now readline comes from the buffer
        line = bf.readline()
        assert line == b'line1\n'

    def test_read_from_buffered_data(self):
        """After reading and seeking back, read pulls from buffer (78-79)."""
        fp = io.BytesIO(b'ABCDEFGHIJKLMN')
        bf = BufferedFile(fp, buffer_size=100)
        bf.read(5)
        bf.seek(0)
        # Read from buffer now
        data = bf.read(5)
        assert data == b'ABCDE'

    def test_seek_beyond_buffer_raises(self):
        """Cover line 87 — BufferError when seeking past buffered range."""
        fp = io.BytesIO(b'X' * 200)
        bf = BufferedFile(fp, buffer_size=10)
        # First read fills the buffer past buffer_size
        bf.read(50)  # len=50, _buffer_full now True
        # Second read bypasses the buffer
        bf.read(50)  # len=50, offset=100, fp_offset=100
        # Now seek to a position between len and fp_offset
        with pytest.raises(BufferError):
            bf.seek(70)

    def test_readline_after_buffer_overflow_raises(self):
        """Cover line 44 — BufferError in readline."""
        fp = io.BytesIO(b'X' * 200)
        bf = BufferedFile(fp, buffer_size=10)
        bf.read(50)  # fills buffer, _buffer_full True
        bf.read(50)  # bypasses buffer; len=50, offset=100, fp_offset=100
        # Manually set offset to a point between len and fp_offset
        bf.offset = 70
        with pytest.raises(BufferError):
            bf.readline()

    def test_read_after_buffer_overflow_raises(self):
        """Cover line 67 — BufferError in read."""
        fp = io.BytesIO(b'X' * 200)
        bf = BufferedFile(fp, buffer_size=10)
        bf.read(50)
        bf.read(50)
        bf.offset = 70
        with pytest.raises(BufferError):
            bf.read(5)


class TestCoreProperties:

    def test_getitem_defined_key(self):
        """Cover line 98 — returning via getattr."""
        class MyProps(CoreProperties):
            KEYS = ['foo']

            def get_foo(self):
                return 'bar'
        mp = MyProps()
        assert mp['foo'] == 'bar'

    def test_iter_with_keys(self):
        """Cover line 103 — iteration over KEYS."""
        class MyProps(CoreProperties):
            KEYS = ['a', 'b']
        assert list(MyProps()) == ['a', 'b']

    def test_len_with_keys(self):
        """Cover line 106 — len() returns len of KEYS."""
        class MyProps(CoreProperties):
            KEYS = ['a', 'b', 'c']
        assert len(MyProps()) == 3

    def test_missing_key_raises_with_keys_defined(self):
        """Cover line 100 — raise when key is not in KEYS."""
        class MyProps(CoreProperties):
            KEYS = ['a']
        with pytest.raises(NoSuchPropertyError):
            MyProps()['not_in_keys']


class TestRowSetProcessorNone:
    """Cover lines 240-242 — processor returning None stops iteration of row."""

    def test_processor_returning_none_skips_row(self):
        # Create a RowSet subclass with one row and one processor returning None
        class DummyRowSet(RowSet):
            def raw(self, sample=False):
                yield [Cell('a')]
                yield [Cell('b')]

        rs = DummyRowSet()
        rs.register_processor(lambda rset, row: None)  # drops every row
        # No rows should yield
        rows = list(rs)
        assert rows == []
