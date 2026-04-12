#!/usr/bin/env python
# coding=utf-8
"""Tests for messytables CSV and ZIP parsing — requires chardet and zipfile."""

import io
import zipfile
from unittest import mock

import pytest

pytest.importorskip('chardet')

from plaidcloud.rpc.messytables.commas import (
    CSVTableSet,
    CSVRowSet,
    UTF8Recoder,
    BetterSniffer,
    to_unicode_or_bust,
)
from plaidcloud.rpc.messytables.zip import ZIPTableSet
from plaidcloud.rpc.messytables.error import ReadError


class TestToUnicodeOrBust:

    def test_passes_through_str(self):
        assert to_unicode_or_bust('hello') == 'hello'

    def test_decodes_bytes(self):
        assert to_unicode_or_bust(b'hello') == 'hello'


class TestUTF8Recoder:

    def test_utf8_input(self):
        stream = io.BytesIO(b'line1\nline2\n')
        recoder = UTF8Recoder(stream, encoding='utf-8')
        lines = list(recoder)
        assert len(lines) >= 1
        assert lines[0].startswith(b'line1')

    def test_autodetect_utf8(self):
        stream = io.BytesIO(b'a,b,c\n1,2,3\n')
        recoder = UTF8Recoder(stream, encoding=None)
        lines = list(recoder)
        assert len(lines) >= 1


class TestCSVTableSet:

    def test_basic_csv(self):
        stream = io.BytesIO(b'a,b,c\n1,2,3\n4,5,6\n')
        ts = CSVTableSet(stream)
        tables = ts.make_tables()
        assert len(tables) == 1
        assert tables[0].name == 'table'

    def test_with_delimiter(self):
        stream = io.BytesIO(b'a\tb\tc\n1\t2\t3\n')
        ts = CSVTableSet(stream, delimiter='\t')
        assert ts.delimiter == '\t'


class TestCSVRowSet:

    def test_basic_iteration(self):
        stream = io.BytesIO(b'a,b,c\n1,2,3\n')
        rs = CSVRowSet('table', stream)
        rows = list(rs)
        assert len(rows) == 2
        assert rs.name == 'table'

    def test_sample_property(self):
        stream = io.BytesIO(b'a,b\n1,2\n3,4\n')
        rs = CSVRowSet('table', stream)
        sample = list(rs.sample)
        assert len(sample) >= 1

    def test_overrides_delimiter(self):
        stream = io.BytesIO(b'a,b\n1,2\n')
        rs = CSVRowSet('table', stream, delimiter='|')
        overrides = rs._overrides
        assert overrides['delimiter'] == '|'

    def test_overrides_all(self):
        stream = io.BytesIO(b'a,b\n1,2\n')
        rs = CSVRowSet(
            'table', stream, delimiter=',', quotechar='"',
            doublequote=True, lineterminator='\n', skipinitialspace=True,
        )
        overrides = rs._overrides
        assert overrides.get('delimiter') == ','
        assert overrides.get('quotechar') == '"'

    def test_dialect_returns_dialect(self):
        stream = io.BytesIO(b'a,b,c\n1,2,3\n4,5,6\n')
        rs = CSVRowSet('table', stream)
        d = rs._dialect
        assert d is not None


class TestZIPTableSet:

    def test_zip_with_csv(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as z:
            z.writestr('data.csv', 'a,b\n1,2\n')
        buf.seek(0)
        ts = ZIPTableSet(buf)
        assert len(ts.tables) >= 1

    def test_zip_ignores_macosx(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as z:
            z.writestr('__MACOSX/meta', 'garbage')
            z.writestr('data.csv', 'a,b\n1,2\n')
        buf.seek(0)
        ts = ZIPTableSet(buf)
        assert len(ts.tables) >= 1

    def test_zip_no_recognized_tables_raises(self):
        # Build a ZIP where any_tableset will fail on every entry.
        # auto_detect=False is not honored by ZIPTableSet signature, so mock
        # any_tableset directly to ensure the test is deterministic across
        # environments (python-magic may classify 'content' as text/plain
        # and return a CSV parser on some CI runners).
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as z:
            z.writestr('nothing.unknown', 'content')
        buf.seek(0)

        from plaidcloud.rpc import messytables as mt_pkg
        with mock.patch.object(
            mt_pkg.any, 'any_tableset', side_effect=ValueError('cannot parse'),
        ):
            with pytest.raises(Exception):
                ZIPTableSet(buf)


class TestBetterSniffer:

    def test_guess_comma_delimiter(self):
        sniffer = BetterSniffer()
        sample = '"a","b","c"\n"1","2","3"\n"4","5","6"\n'
        quote, doublequote, delim, skipinitial = sniffer._guess_quote_and_delimiter(
            sample, delimiters=',',
        )
        assert delim == ','

    def test_no_matches_returns_empty(self):
        """Cover line 235-236 — no matches returns default tuple."""
        sniffer = BetterSniffer()
        sample = 'nothing matching any pattern here at all'
        result = sniffer._guess_quote_and_delimiter(sample, delimiters=',')
        # (quotechar, doublequote, delimiter, skipinitialspace)
        assert result == ('', False, None, 0)

    def test_single_column_newline_delim(self):
        """Cover lines 252-256 — single column of quoted data, delim='\n'."""
        sniffer = BetterSniffer()
        # This triggers the 4th regex (just quoted text with no delim)
        sample = '"only one field"\n"and another"\n'
        result = sniffer._guess_quote_and_delimiter(sample, delimiters=None)
        # When no delim seen, delim becomes empty
        assert result[2] == '' or result[2] == '\n'

    def test_space_after_delim_counts(self):
        """Cover line 244 — spaces += 1 when match has a space."""
        sniffer = BetterSniffer()
        # Put space after the comma before quote
        sample = '"a", "b", "c"\n"1", "2", "3"\n'
        result = sniffer._guess_quote_and_delimiter(sample, delimiters=',')
        # skipinitialspace flag depends on all matches having a space
        assert result[0] == '"'  # quote char
        assert result[2] == ','  # delim

    def test_newline_delim_becomes_empty(self):
        """Cover line 254 — delim=='\n' converts to ''."""
        sniffer = BetterSniffer()
        # The second regex `(?:^|\n)"[^\n]*?"(?P<delim>\W)(?P<space> ?)`
        # matches when `\n` is the only char after a quote. Force that regex
        # to pick \n as the delim.
        # Each line starts with a quote and ends with a quote+newline.
        sample = '"first"\n"second"\n"third"\n"fourth"\n'
        result = sniffer._guess_quote_and_delimiter(sample, delimiters=None)
        # delim should be '' (from line 254 after \n is picked)
        assert result[2] == ''

    def test_no_double_quote(self):
        """Cover line 265 — doublequote=False when pattern doesn't match."""
        sniffer = BetterSniffer()
        # Simple data without any doubled quotes
        sample = '"a","b"\n"1","2"\n'
        result = sniffer._guess_quote_and_delimiter(sample, delimiters=',')
        # doublequote should be False
        assert result[1] is False

    def test_doublequote_detected(self):
        """Cover line 267 — doublequote=True when doubled-quote pattern matches."""
        sniffer = BetterSniffer()
        # Sample containing three "s between delims to trigger dq_regexp match
        sample = '"a","b""b","c"\n"1","2""2","3"\n'
        result = sniffer._guess_quote_and_delimiter(sample, delimiters=',')
        assert result[1] is True


class TestCsvRowSetFullIteration:
    """Cover line 178 — full (non-sample) iteration after sample was read."""

    def test_full_iteration_after_sample(self):
        import io
        from plaidcloud.rpc.messytables.commas import CSVRowSet
        # Use content bigger than the default sample window (1000)
        # but a small window here instead
        stream = io.BytesIO(b'a,b\n' + b'1,2\n' * 2000)
        rs = CSVRowSet('t', stream, window=5)
        # Full iteration exercises the self.lines loop (line 177-178)
        rows = list(rs)
        # Should have more rows than the window
        assert len(rows) > 5


class TestUTF8RecoderImportError:

    def test_no_chardet_raises(self):
        """Cover lines 27-28."""
        import io
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == 'chardet':
                raise ImportError('no chardet')
            return real_import(name, *args, **kwargs)

        with mock.patch.object(builtins, '__import__', side_effect=fake_import):
            with pytest.raises(ImportError, match='full install'):
                UTF8Recoder(io.BytesIO(b'data'), encoding=None)


class TestUTF8RecoderChardetFallback:
    """Cover line 36 — encoding still None after chardet, fallback to utf-8."""

    def test_chardet_returns_none_encoding(self):
        import io
        fake_chardet = mock.MagicMock()
        fake_chardet.detect = mock.Mock(return_value={'encoding': None})
        with mock.patch.dict('sys.modules', {'chardet': fake_chardet}):
            stream = io.BytesIO(b'\x00\x00\x00')
            recoder = UTF8Recoder(stream, encoding=None)
            # Should still work, using utf-8 fallback
            assert recoder is not None


class TestCSVRowSetReadError:
    """Cover the raise ReadError branch (line 193)."""

    def test_csv_error_raises_read_error(self):
        """Trigger a csv.Error that doesn't match the known passthroughs."""
        import io
        from plaidcloud.rpc.messytables.commas import CSVRowSet
        from plaidcloud.rpc.messytables.error import ReadError

        stream = io.BytesIO(b'a,b\n1,2\n')
        rs = CSVRowSet('t', stream)

        import csv

        def bad_reader(*args, **kwargs):
            def iterator():
                raise csv.Error('some other random csv error')
                yield  # make it a generator
            return iterator()

        with mock.patch.object(csv, 'reader', side_effect=bad_reader):
            with pytest.raises(ReadError):
                list(rs)

    def test_csv_newline_inside_string_in_sample(self):
        """Cover line 189 — newline inside string with sample=True."""
        import io
        from plaidcloud.rpc.messytables.commas import CSVRowSet

        stream = io.BytesIO(b'a,b\n1,2\n')
        rs = CSVRowSet('t', stream)

        import csv

        def bad_reader(*args, **kwargs):
            def iterator():
                raise csv.Error('newline inside string')
                yield
            return iterator()

        with mock.patch.object(csv, 'reader', side_effect=bad_reader):
            # Using sample=True, the newline error is swallowed
            list(rs.sample)

    def test_csv_null_byte(self):
        """Cover line 191 — NULL byte error is swallowed."""
        import io
        from plaidcloud.rpc.messytables.commas import CSVRowSet

        stream = io.BytesIO(b'a,b\n1,2\n')
        rs = CSVRowSet('t', stream)

        import csv

        def bad_reader(*args, **kwargs):
            def iterator():
                raise csv.Error('line contains NULL byte')
                yield
            return iterator()

        with mock.patch.object(csv, 'reader', side_effect=bad_reader):
            list(rs)  # Should not raise
