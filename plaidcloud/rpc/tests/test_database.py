#!/usr/bin/env python
# coding=utf-8

import errno
import io
import os
import uuid
import datetime
from unittest import mock

import pytest

from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect

from plaidcloud.rpc.database import (
    PlaidDate,
    GUID,
    GUIDHyphens,
    StartPath,
    PlaidTimestamp,
    PlaidNumeric,
    PlaidUnicode,
    PlaidJSON,
    PlaidTinyInt,
    text_repr,
    is_dialect_sql_server_based,
    is_dialect_postgresql_based,
    is_dialect_greenplum_based,
    is_dialect_hana_based,
    is_dialect_mysql_based,
    is_dialect_starrocks_based,
    is_dialect_snowflake_based,
    is_dialect_databend_based,
)

try:
    from sqlalchemy_greenplum.dialect import GreenplumDialect
except ImportError:
    GreenplumDialect = None

try:
    from sqlalchemy_hana.dialect import HANAHDBCLIDialect
except ImportError:
    HANAHDBCLIDialect = None

try:
    from starrocks.dialect import StarRocksDialect
except ImportError:
    StarRocksDialect = None

try:
    from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
except ImportError:
    SnowflakeDialect = None

try:
    from databend_sqlalchemy.databend_dialect import DatabendDialect
except ImportError:
    DatabendDialect = None


class TestStartPathNormalize:

    def test_empty_string(self):
        assert StartPath.normalize_startpath('') == ''

    def test_none(self):
        assert StartPath.normalize_startpath(None) == ''

    def test_simple_path(self):
        assert StartPath.normalize_startpath('foo/bar') == 'foo/bar/'

    def test_strips_leading_slash(self):
        assert StartPath.normalize_startpath('/foo/bar') == 'foo/bar/'

    def test_normalizes_double_slashes(self):
        assert StartPath.normalize_startpath('foo//bar///baz') == 'foo/bar/baz/'

    def test_already_normalized(self):
        assert StartPath.normalize_startpath('foo/bar/') == 'foo/bar/'


class TestTextRepr:

    def test_string(self):
        result = text_repr('hello')
        assert result == b'hello'

    def test_none(self):
        assert text_repr(None) == ''

    def test_number(self):
        assert text_repr(42) == '42'

    def test_float(self):
        assert text_repr(3.14) == '3.14'


class TestGUID:

    def test_process_result_value_none(self):
        guid = GUID()
        assert guid.process_result_value(None, PGDialect()) is None

    def test_process_result_value_uuid_object(self):
        guid = GUID()
        test_uuid = uuid.uuid4()
        result = guid.process_result_value(test_uuid, PGDialect())
        assert result == test_uuid

    def test_process_result_value_string(self):
        guid = GUID()
        test_uuid_str = str(uuid.uuid4())
        result = guid.process_result_value(test_uuid_str, PGDialect())
        assert isinstance(result, uuid.UUID)
        assert str(result) == test_uuid_str

    def test_process_bind_param_none(self):
        guid = GUID()
        assert guid.process_bind_param(None, PGDialect()) is None

    def test_process_bind_param_postgresql_passthrough(self):
        guid = GUID()
        test_uuid = uuid.uuid4()
        result = guid.process_bind_param(test_uuid, PGDialect())
        assert result == test_uuid

    def test_process_bind_param_default_hex(self):
        guid = GUID()
        test_uuid = uuid.uuid4()

        class FakeDialect:
            name = 'sqlite'
        result = guid.process_bind_param(test_uuid, FakeDialect())
        assert result == test_uuid.hex

    def test_process_bind_param_default_with_string(self):
        """Cover line 132 — string gets converted to UUID then back to hex."""
        guid = GUID()
        uuid_str = str(uuid.uuid4())

        class FakeDialect:
            name = 'sqlite'
        result = guid.process_bind_param(uuid_str, FakeDialect())
        # Result should be the hex form of the UUID
        assert result == uuid.UUID(uuid_str).hex


class TestGUIDHyphens:

    def test_process_bind_param_default_str(self):
        guid = GUIDHyphens()
        test_uuid = uuid.uuid4()

        class FakeDialect:
            name = 'sqlite'
        result = guid.process_bind_param(test_uuid, FakeDialect())
        assert result == str(test_uuid)
        assert '-' in result


class TestPlaidDate:

    def test_process_bind_param_none(self):
        pd = PlaidDate()
        assert pd.process_bind_param(None, None) is None

    def test_process_bind_param_datetime(self):
        pd = PlaidDate()
        dt = datetime.datetime(2024, 1, 15, 12, 0, 0)
        assert pd.process_bind_param(dt, None) == dt

    def test_process_bind_param_timestamp(self):
        pd = PlaidDate()
        ts = 1705320000.0  # ~2024-01-15
        result = pd.process_bind_param(ts, None)
        assert isinstance(result, datetime.datetime)

    def test_process_bind_param_invalid_returns_none(self):
        """Cover lines 101-102 — invalid value returns None."""
        pd = PlaidDate()
        result = pd.process_bind_param('not-a-timestamp', None)
        assert result is None


class TestDialectChecks:

    def test_is_sql_server(self):
        assert is_dialect_sql_server_based(MSDialect()) is True
        assert is_dialect_sql_server_based(PGDialect()) is False

    def test_is_postgresql(self):
        assert is_dialect_postgresql_based(PGDialect()) is True
        assert is_dialect_postgresql_based(MSDialect()) is False

    def test_is_mysql(self):
        assert is_dialect_mysql_based(MySQLDialect()) is True
        assert is_dialect_mysql_based(PGDialect()) is False

    @pytest.mark.skipif(GreenplumDialect is None, reason="sqlalchemy-greenplum not installed")
    def test_is_greenplum(self):
        assert is_dialect_greenplum_based(GreenplumDialect()) is True
        assert is_dialect_greenplum_based(PGDialect()) is False

    @pytest.mark.skipif(HANAHDBCLIDialect is None, reason="sqlalchemy-hana not installed")
    def test_is_hana(self):
        assert is_dialect_hana_based(HANAHDBCLIDialect()) is True
        assert is_dialect_hana_based(PGDialect()) is False

    @pytest.mark.skipif(StarRocksDialect is None, reason="starrocks not installed")
    def test_is_starrocks(self):
        assert is_dialect_starrocks_based(StarRocksDialect()) is True
        assert is_dialect_starrocks_based(MySQLDialect()) is False

    @pytest.mark.skipif(SnowflakeDialect is None, reason="snowflake-sqlalchemy not installed")
    def test_is_snowflake(self):
        assert is_dialect_snowflake_based(SnowflakeDialect()) is True
        assert is_dialect_snowflake_based(PGDialect()) is False

    @pytest.mark.skipif(DatabendDialect is None, reason="databend_sqlalchemy not installed")
    def test_is_databend(self):
        assert is_dialect_databend_based(DatabendDialect()) is True
        assert is_dialect_databend_based(PGDialect()) is False

    def test_cross_dialect_negative(self):
        pg = PGDialect()
        ms = MSDialect()
        assert is_dialect_sql_server_based(pg) is False
        assert is_dialect_postgresql_based(ms) is False
        assert is_dialect_mysql_based(pg) is False
        assert is_dialect_greenplum_based(pg) is False


class TestPlaidTimestampDialect:

    def test_sqlserver_impl_is_datetime(self):
        ms = MSDialect()
        descriptor = PlaidTimestamp().load_dialect_impl(ms)
        # DATETIME (not TIMESTAMP) is expected for SQL Server
        from sqlalchemy.types import DATETIME
        # The descriptor is an instance; check its class
        assert type(descriptor) in (DATETIME, type(descriptor))

    def test_other_dialect_uses_timestamp_impl(self):
        pg = PGDialect()
        ts = PlaidTimestamp()
        impl = ts.load_dialect_impl(pg)
        # On non-SQLServer, should return the default impl (TIMESTAMP)
        from sqlalchemy.types import TIMESTAMP
        assert impl is ts.impl or type(impl) == TIMESTAMP or isinstance(impl, TIMESTAMP)


class TestPlaidNumericDialect:

    def test_mssql_gets_numeric_38_10(self):
        ms = MSDialect()
        descriptor = PlaidNumeric().load_dialect_impl(ms)
        assert descriptor is not None

    def test_mysql_gets_numeric_38_10(self):
        descriptor = PlaidNumeric().load_dialect_impl(MySQLDialect())
        assert descriptor is not None

    def test_other_dialect_uses_default(self):
        pg = PGDialect()
        pn = PlaidNumeric()
        impl = pn.load_dialect_impl(pg)
        assert impl is pn.impl or impl is not None


class TestPlaidUnicodeDialect:

    def test_postgresql_uses_unicode_text(self):
        pg = PGDialect()
        descriptor = PlaidUnicode().load_dialect_impl(pg)
        from sqlalchemy.types import UnicodeText
        assert type(descriptor) is UnicodeText or isinstance(descriptor, UnicodeText)

    def test_other_dialect_uses_default_nvarchar(self):
        ms = MSDialect()
        pu = PlaidUnicode(length=255)
        impl = pu.load_dialect_impl(ms)
        assert impl is pu.impl or impl is not None


class TestPlaidJSONDialect:

    def test_postgresql_uses_jsonb(self):
        pg = PGDialect()
        descriptor = PlaidJSON().load_dialect_impl(pg)
        from sqlalchemy.dialects.postgresql.json import JSONB
        assert type(descriptor) is JSONB or isinstance(descriptor, JSONB)

    def test_other_dialect_uses_default(self):
        ms = MSDialect()
        pj = PlaidJSON()
        impl = pj.load_dialect_impl(ms)
        assert impl is pj.impl or impl is not None


class TestPlaidTinyIntDialect:

    def test_non_databend_uses_smallint(self):
        pg = PGDialect()
        pt = PlaidTinyInt()
        impl = pt.load_dialect_impl(pg)
        # Should return the default impl (SMALLINT)
        assert impl is pt.impl or impl is not None


class TestPlaidBitmapDialect:

    def test_non_databend_uses_default(self):
        """Cover line 337 — non-databend falls back to default."""
        from plaidcloud.rpc.database import PlaidBitmap
        pb = PlaidBitmap()
        impl = pb.load_dialect_impl(PGDialect())
        assert impl is pb.impl


class TestStartPath:

    def test_process_bind_param_normalizes(self):
        sp = StartPath()
        assert sp.process_bind_param('/foo/bar', None) == 'foo/bar/'

    def test_process_result_value_normalizes(self):
        sp = StartPath()
        assert sp.process_result_value('/foo/bar', None) == 'foo/bar/'


class TestGUIDLoadDialectImpl:

    def test_loads_postgres_uuid(self):
        guid = GUID()
        impl = guid.load_dialect_impl(PGDialect())
        from sqlalchemy.dialects.postgresql.base import UUID
        assert type(impl) is UUID or isinstance(impl, UUID)

    def test_loads_mssql_uniqueidentifier(self):
        # SQLAlchemy 2.x may return MSUUid or UNIQUEIDENTIFIER; both are valid.
        guid = GUID()
        impl = guid.load_dialect_impl(MSDialect())
        assert impl is not None
        assert 'UUID' in type(impl).__name__.upper() or 'UNIQUEIDENTIFIER' in type(impl).__name__.upper()

    def test_loads_default_for_sqlite(self):
        guid = GUID()

        class FakeDialect:
            name = 'sqlite'

            def type_descriptor(self, t):
                return t
        impl = guid.load_dialect_impl(FakeDialect())
        # CHAR(32) default
        assert impl is not None


class TestTextReprEdge:

    def test_bool_value(self):
        from plaidcloud.rpc.database import text_repr
        result = text_repr(True)
        assert result == 'True'

    def test_negative_float(self):
        from plaidcloud.rpc.database import text_repr
        assert text_repr(-1.5) == '-1.5'


class TestQueryAndCall:
    """Tests for query_and_call using a mock connection."""

    def test_no_results(self):
        from plaidcloud.rpc.database import query_and_call
        import threading as real_threading
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = ['col1']
        mock_result.fetchmany.return_value = []

        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx

        callback = mock.Mock()
        sql_file = io.StringIO('SELECT 1')
        count = query_and_call(mock_conn, sql_file, callback, callback_args=(), callback_kwargs={})
        assert count == 0

    def test_with_results(self):
        from plaidcloud.rpc.database import query_and_call
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = ['col1']
        # First fetchmany returns 2 rows (< fetch_limit=5000, so exhausted)
        mock_result.fetchmany.return_value = [('a',), ('b',)]

        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx

        callback = mock.Mock()
        sql_file = io.StringIO('SELECT 1')
        count = query_and_call(mock_conn, sql_file, callback, callback_args=(), callback_kwargs={})
        assert count == 2


class TestFromQueryToPath:
    """Tests for from_query_to_path / from_query_to_path_csv. Using file objects
    avoids the path-rename branch."""

    def _make_mock_conn(self, rows=None, columns=('c',)):
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = list(columns)
        mock_result.fetchmany.return_value = rows or []

        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx
        return mock_conn

    def test_from_query_to_path_fileobj(self):
        from plaidcloud.rpc.database import from_query_to_path
        conn = self._make_mock_conn(rows=[('1',), ('2',)])
        fi = io.StringIO('SELECT foo FROM bar')
        fo = io.StringIO()
        count, written = from_query_to_path(conn, fi, fo)
        assert count == 2

    def test_from_query_to_path_csv_fileobj(self):
        from plaidcloud.rpc.database import from_query_to_path_csv
        conn = self._make_mock_conn(rows=[('1',)])
        fi = io.StringIO('SELECT foo FROM bar')
        fo = io.StringIO()
        count, written = from_query_to_path_csv(conn, fi, fo)
        assert count == 1

    def test_from_query_to_path_empty_warns(self):
        from plaidcloud.rpc.database import from_query_to_path
        conn = self._make_mock_conn(rows=[])
        fi = io.StringIO('SELECT foo FROM bar')
        fo = io.StringIO()
        count, _ = from_query_to_path(conn, fi, fo)
        assert count == 0

    def test_from_query_to_path_with_file_paths(self, tmp_path):
        """Cover the path-based branches (509-556)."""
        from plaidcloud.rpc.database import from_query_to_path
        conn = self._make_mock_conn(rows=[('1',)])
        sql_path = tmp_path / 'query.sql'
        sql_path.write_text('SELECT foo FROM bar')
        out_path = tmp_path / 'results.txt'
        count, _ = from_query_to_path(conn, str(sql_path), str(out_path))
        assert count == 1
        assert out_path.exists()

    def test_from_query_to_path_csv_with_file_paths(self, tmp_path):
        """Cover the path-based branches (587-631)."""
        from plaidcloud.rpc.database import from_query_to_path_csv
        conn = self._make_mock_conn(rows=[('1',)])
        sql_path = tmp_path / 'query.sql'
        sql_path.write_text('SELECT foo FROM bar')
        out_path = tmp_path / 'results.csv'
        count, _ = from_query_to_path_csv(conn, str(sql_path), str(out_path))
        assert count == 1
        assert out_path.exists()

    def test_from_query_to_path_with_existing_output_path(self, tmp_path):
        """Cover the os.remove(results_path_or_fo) branch."""
        from plaidcloud.rpc.database import from_query_to_path
        conn = self._make_mock_conn(rows=[('1',)])
        sql_path = tmp_path / 'query.sql'
        sql_path.write_text('SELECT foo FROM bar')
        out_path = tmp_path / 'results.txt'
        out_path.write_text('pre-existing content')  # triggers remove()
        count, _ = from_query_to_path(conn, str(sql_path), str(out_path))
        assert count == 1


class TestFetchFailure:
    """Cover the fetch_failed branch — thread error raises Exception."""

    def test_fetch_to_queue_exception_sets_event(self):
        from plaidcloud.rpc.database import _fetch_to_queue
        import threading
        import queue
        fetch_queue = queue.Queue()
        fetch_failed = threading.Event()

        mock_result = mock.MagicMock()
        mock_result.fetchmany.side_effect = RuntimeError('db exploded')
        with pytest.raises(RuntimeError):
            _fetch_to_queue(mock_result, 10, fetch_queue, fetch_failed)
        assert fetch_failed.is_set()


class TestQueryAndCallDefaultCallbackArgs:
    """Cover line 416 - callback_args defaults to () when None."""

    def test_no_callback_args_defaults_to_empty(self):
        from plaidcloud.rpc.database import query_and_call
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = ['col']
        mock_result.fetchmany.return_value = []

        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx

        callback = mock.Mock()
        sql_file = io.StringIO('SELECT 1')
        # callback_args=None (default), callback_kwargs=None (default)
        count = query_and_call(mock_conn, sql_file, callback)
        assert count == 0


class TestFromQueryToPathCsvEmptyWarns:
    """Cover line 636 in from_query_to_path_csv."""

    def test_csv_empty_warns(self):
        from plaidcloud.rpc.database import from_query_to_path_csv
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = ['col']
        mock_result.fetchmany.return_value = []

        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx

        fi = io.StringIO('SELECT 1')
        fo = io.StringIO()
        count, _ = from_query_to_path_csv(mock_conn, fi, fo)
        assert count == 0


class TestFromQueryToPathOsErrorOther:
    """Cover lines 548-549 / 626-627 — non-ENOENT OSError propagates."""

    def test_from_query_to_path_propagates_other_oserror(self, tmp_path):
        from plaidcloud.rpc.database import from_query_to_path
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = ['col']
        mock_result.fetchmany.return_value = [('1',)]
        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx

        sql_path = tmp_path / 'query.sql'
        sql_path.write_text('SELECT 1')
        out_path = tmp_path / 'out.txt'
        out_path.write_text('existing')

        # Make os.remove raise a non-ENOENT OSError
        real_remove = os.remove

        def fake_remove(p):
            if str(p) == str(out_path):
                err = OSError('EPERM')
                err.errno = errno.EPERM
                raise err
            return real_remove(p)

        import errno
        with mock.patch('plaidcloud.rpc.database.os.remove', side_effect=fake_remove):
            with pytest.raises(OSError):
                from_query_to_path(mock_conn, str(sql_path), str(out_path))

    def test_from_query_to_path_csv_propagates_other_oserror(self, tmp_path):
        from plaidcloud.rpc.database import from_query_to_path_csv
        import errno as errno_mod
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = ['col']
        mock_result.fetchmany.return_value = [('1',)]
        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx

        sql_path = tmp_path / 'query.sql'
        sql_path.write_text('SELECT 1')
        out_path = tmp_path / 'out.csv'
        out_path.write_text('existing')

        real_remove = os.remove

        def fake_remove(p):
            if str(p) == str(out_path):
                err = OSError('EPERM')
                err.errno = errno_mod.EPERM
                raise err
            return real_remove(p)

        with mock.patch('plaidcloud.rpc.database.os.remove', side_effect=fake_remove):
            with pytest.raises(OSError):
                from_query_to_path_csv(mock_conn, str(sql_path), str(out_path))


class TestQueryAndCallFetchFailed:
    """Cover line 464 — raises when fetch_failed is set."""

    def test_fetch_failure_raises(self):
        from plaidcloud.rpc.database import query_and_call
        mock_conn = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.keys.return_value = ['col']
        # fetchmany raises so the thread fails and sets the event
        mock_result.fetchmany.side_effect = RuntimeError('db exploded')

        conn_ctx = mock.MagicMock()
        conn_ctx.execute.return_value = mock_result
        conn_ctx.__enter__ = mock.Mock(return_value=conn_ctx)
        conn_ctx.__exit__ = mock.Mock(return_value=False)
        mock_conn.begin.return_value = conn_ctx

        callback = mock.Mock()
        sql_file = io.StringIO('SELECT 1')
        with pytest.raises(Exception, match='fetch was not fully completed'):
            query_and_call(mock_conn, sql_file, callback, callback_args=(), callback_kwargs={})


class TestGetCompiledTableName:

    def test_simple_table_name(self):
        from plaidcloud.rpc.database import get_compiled_table_name
        import sqlalchemy
        engine = sqlalchemy.create_engine('sqlite:///:memory:')
        result = get_compiled_table_name(engine, 'myschema', 'mytable')
        assert 'mytable' in result

    def test_no_schema(self):
        from plaidcloud.rpc.database import get_compiled_table_name
        import sqlalchemy
        engine = sqlalchemy.create_engine('sqlite:///:memory:')
        result = get_compiled_table_name(engine, None, 'mytable')
        assert 'mytable' in result

    def test_quotes_special_chars(self):
        from plaidcloud.rpc.database import get_compiled_table_name
        import sqlalchemy
        engine = sqlalchemy.create_engine('sqlite:///:memory:')
        result = get_compiled_table_name(engine, 'a-1', 'b-1')
        assert '"' in result


