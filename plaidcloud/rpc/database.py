#!/usr/bin/env python
# coding=utf-8
"""
Database Utility
Utility functions for interacting with the database.
"""
from __future__ import absolute_import
import errno
import getpass
import logging
import os
import threading
import time
import datetime
import re
import uuid
import unicodecsv as csv

import six
import six.moves
import sqlalchemy
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.types import TypeDecorator, DateTime, Unicode, CHAR, TEXT, NVARCHAR, UnicodeText
from sqlalchemy_hana.dialect import HANABaseDialect
from sqlalchemy_greenplum.dialect import GreenplumDialect

from plaidcloud.rpc import config

conf = config.get_dict()
logger = logging.getLogger(__name__)


# -------------------------------------------------------------
# -------  END Database Wrapper Methods -----------------------
# -------------------------------------------------------------

class PlaidDate(TypeDecorator):
    """A type that decorates DateTime, converts to unix time on
    the way out and to datetime.datetime objects on the way in."""
    impl = DateTime  # In schema, you want these datetimes to be stored as integers.

    def process_result_value(self, value, _):
        """Assumes a datetime.datetime"""
        if value is None:
            return None  # support nullability
        elif isinstance(value, datetime.datetime):
            try:
                return int(time.mktime(value.timetuple()))
            except ValueError:
                # TODO: this is in here because time.mktime cannot handle years before 1900 or after 9999.
                # We should find a better alternative, but for now dates outside 1900-9999 will be treated as None.
                return None
        raise ValueError("Can operate only on datetime values. "
                         "Offending value type: {0}".format(type(value).__name__))

    def process_bind_param(self, value, _):
        if value is not None:  # support nullability
            if isinstance(value, datetime.datetime):
                return value
            else:
                try:
                    return datetime.datetime.fromtimestamp(float(value))
                except:
                    return None


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses CHAR(32), storing as stringified hex values.

    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(TEXT)

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value)
            else:
                # hexstring
                return "%.32x" % value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return six.text_type(uuid.UUID(value))


class StartPath(TypeDecorator):
    """This custom type assures that StartPaths are stored in a canonical form.

    The "canonical form" is a safeguard for path transformations. A startpath
    must be normalized to have exactly one '/' between each directory level,
    never to have a '/' at the beginning of the string, and to have exactly
    one '/' at the end of the string. In PlaidCloud, empty string StartPaths
    ('') are equivalent to having no StartPath. This convention is useful logic
    for numerous path manipulation functions.
    """

    impl = Unicode

    def process_bind_param(self, value, _):
        return self.normalize_startpath(value)

    def process_result_value(self, value, _):
        return self.normalize_startpath(value)

    @staticmethod
    def normalize_startpath(startpath):
        """Put the StartPath into a form that's safe to use.
        Such a form has no slash at the beginning, only one slash between any
        directory levels, and exactly one slash at the end. This function
        returns the empty string ("") for arguments that evaluate to false
        (like "", None, or False).
        """
        return re.sub(r'/+', '/', (startpath or '') + '/').lstrip('/')


class PlaidUnicode(TypeDecorator):
    """Unicode Type that implements as UnicodeText on Postgresql based environments

    Note:
        Uses Postgresql's non length-checked string type, otherwise uses Unicode for all other implementations.
    """
    impl = NVARCHAR

    def load_dialect_impl(self, dialect):
        """Loads the dialect implementation
        Note:
            Implement as UnicodeText in Greenplum + Postgresql
        Args:
            dialect (Dialect): SQLAlchemy dialect
        Returns:
            str: Type Descriptor
        """
        if is_dialect_postgresql_based(dialect):
            return dialect.type_descriptor(UnicodeText)
        else:
            return self.impl


def text_repr(val):
    """Format values in a way that can be written as text to disk.

    Encode Unicode values as UTF-8 bytestrings (recommended for csv.writer),
    and use the str() representation of numbers.

    Args:
        val (obj): A string-able object, a `unicode` object, or `None`

    Returns:
        str: The string representation of `val`
    """

    if isinstance(val, six.text_type):
        val_text = val.encode('utf8', 'ignore')
    elif val is None:
        val_text = ''
    else:
        val_text = str(val)

    return val_text


def _fetch_to_queue(result_proxy, fetch_limit, fetch_queue, fetch_failed):
    """Use with threading to separate database fetching from the main thread.

    `result_proxy` should be a DB-API-compatible cursor-like object (like a
    SQLAlchemy connection); `fetch_queue` should be a synchronized
    Queue.Queue-like object, and `fetch_failed` should be a
    threading.Event-like object.

    Args:
        result_proxy (DB-API cursor object): The result of a database query to fetch
        fetch_limit (int): The maximum number of results to fetch
        fetch_queue (`Queue.Queue`): The synchronized queue to fetch to
        fetch_failed (`threading.Event`): The event to trigger if the fetch fails
    """

    try:
        fetch_exhausted = False
        while not fetch_exhausted:
            fetch_ls = result_proxy.fetchmany(fetch_limit)
            fetch_exhausted = len(fetch_ls) < fetch_limit
            for record in fetch_ls:
                fetch_queue.put(record)
    except:
        fetch_failed.set()
        raise


def query_and_call(connection, sql_file_obj, callback, callback_args=None,
                   callback_kwargs=None, include_columns=True):
    """Query the database and send the results row-wise to a callback.

    The query is found in `sql_file_obj`, and each row found is provided as a
    list of its fields to `callback`, as the first argument. Additional
    arguments and keyword arguments can be provided. The callback will probably
    be a function that writes the results to disk. When `include_columns` is
    True (the default), the list of columns will be sent to the callback as if
    it were a row.

    Args:
        connection (`sqlalchemy.Connection`): A connection to the database
        sql_file_obj (File): A file containing the query to execute
        callback (method): The method to call with the result of the query
        callback_args (tuple, optional): args to send to the callback method
        callback_kwargs (dict, optional): kwargs to send to the callback method
        include_columns (bool, optional): Include a list of columns as a row

    Returns:
        int: The number of results returned.
    """

    query = sql_file_obj.read().rstrip(' \t\r\n;')
    logger.debug("Using query: %r", query)
    ts = time.time()
    result = connection.execute(query)
    te = time.time()
    logger.debug("ResultProxy (cursor) took %.2f sec to be created", te - ts)
    if include_columns:
        col_names = [key.upper() for key in result.keys()]
        callback(col_names, *callback_args, **callback_kwargs)

    total_records = 0
    fetch_limit = 5000
    fetch_queue = six.moves.queue.Queue(fetch_limit)
    fetch_failed = threading.Event()
    ts = time.time()
    t = threading.Thread(target=_fetch_to_queue,
                         args=(result, fetch_limit,
                               fetch_queue, fetch_failed))
    t.start()
    read_queue = not fetch_failed.is_set()
    while read_queue:
        try:
            row = fetch_queue.get(block=False)
        except six.moves.queue.Empty:
            pass
        else:
            total_records += 1
            if total_records % fetch_limit == 0:
                logger.debug("Fetched %s records, so far",
                             "{:,}".format(total_records))
            if not callback_args:
                callback_args = ()

            if not callback_kwargs:
                callback_kwargs = {}
            callback(row, *callback_args, **callback_kwargs)

        if not t.is_alive():
            if fetch_queue.empty() or fetch_failed.is_set():
                # Done or failed
                read_queue = False
        else:
            # Still waiting for a conclusion
            read_queue = True

    if fetch_failed.is_set():
        # This would happen if the thread were to die.
        raise Exception('The fetch was not fully completed.')

    result.close()
    te = time.time()
    logger.debug("Result fetch took %.2f sec to run", te - ts)

    return total_records


def writerow(row, csv_writer):
    """Callback for query_and_call.

    Called once per row. Row is provided as a list.

    Args:
        row (list): A row returned from a database query
        csv_writer (`csv.writer`): A CSV writer to write the row to
    """

    csv_writer.writerow([text_repr(column) for column in row])


def from_query_to_path(connection, sql_path_or_fo, results_path_or_fo):
    """Write the results of the query found in one location to another.

    The results are written in a pipe-separated value format.
    `sql_path_or_fo` and `results_path_or_fo` are either path name strings or
    file-like objects (like open file pointers or StringIO objects). If a path
    is provided for results, a temporary path will initially be used instead,
    and the results file will be moved to the desired path only after the
    operation has completed successfully. If a file-like object is provided
    for the input SQL, it will *not* be closed by this function.

    Args:
        connection (`sqlalchemy.orm.connection`): The database connection to use
        sql_path_or_fo (str or File): path to or file object containing a query
        results_path_or_fo (str or File) the path or file object to write the results to

    Returns:
        int: The number of records resulting from the query
        int: The number of bytes written out
    """

    # Figure out if paths or file-like objects were provided as arguments.
    if type(sql_path_or_fo) in six.string_types:
        # Assume it's a path.
        fi = open(sql_path_or_fo, 'r')
        using_sql_path = True
    else:
        # Assume it's a file-like object.
        fi = sql_path_or_fo
        using_sql_path = False

    if type(results_path_or_fo) in six.string_types:
        # Assume it's a path.
        temp_results_path = results_path_or_fo + '.tmp'
        fo = open(temp_results_path, 'w')
        using_results_path = True
    else:
        # Assume it's a file-like object.
        temp_results_path = None
        fo = results_path_or_fo
        using_results_path = False

    # Create a CSV writer for use with the writerow function.
    csv_writer = csv.writer(fo, delimiter='|', lineterminator='\n',
                            quoting=csv.QUOTE_ALL)
    record_count = query_and_call(connection, fi, writerow, (csv_writer,))
    written_bytes = fo.tell()

    if using_sql_path:
        # Only close files we opened... not ones given.
        fi.close()
        sql_log_part = " in '{}' ".format(sql_path_or_fo)
    else:
        sql_log_part = ' '

    if using_results_path:
        # Only close files we opened... not ones given.
        fo.close()

        # If a path was given, rename the temporary path to the provided one.
        try:
            os.remove(results_path_or_fo)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        os.rename(temp_results_path, results_path_or_fo)
        logger.debug("Renamed '%s' to '%s'",
                     temp_results_path, results_path_or_fo)
        results_log_part = " to '{}' ".format(results_path_or_fo)
    else:
        results_log_part = ' to the requested destination '

    if record_count == 0:
        logger.warning("Query%sreturned no results", sql_log_part)

    return record_count, written_bytes


def from_query_to_path_csv(connection, sql_path_or_fo, results_path_or_fo):
    """Write the results of the query found in one location to another.

    The results are written in a pipe-separated value format.
    `sql_path_or_fo` and `results_path_or_fo` are either path name strings or
    file-like objects (like open file pointers or StringIO objects). If a path
    is provided for results, a temporary path will initially be used instead,
    and the results file will be moved to the desired path only after the
    operation has completed successfully. If a file-like object is provided
    for the input SQL, it will *not* be closed by this function.

    Args:
        connection (`sqlalchemy.orm.connection`): Connection to a database
        sql_path_or_fo (str or File): Path to or file containing a query
        results_path_or_fo (str or File): Path to or file to write out to

    Returns:
        int: The number of records resulting from the query
        int: The number of bytes written out
    """

    # Figure out if paths or file-like objects were provided as arguments.
    if type(sql_path_or_fo) in six.string_types:
        # Assume it's a path.
        fi = open(sql_path_or_fo, 'r')
        using_sql_path = True
    else:
        # Assume it's a file-like object.
        fi = sql_path_or_fo
        using_sql_path = False

    if type(results_path_or_fo) in six.string_types:
        # Assume it's a path.
        temp_results_path = results_path_or_fo + '.tmp'
        fo = open(temp_results_path, 'w')
        using_results_path = True
    else:
        # Assume it's a file-like object.
        temp_results_path = None
        fo = results_path_or_fo
        using_results_path = False

    # Create a CSV writer for use with the writerow function.
    csv_writer = csv.writer(fo, delimiter=',', lineterminator='\n',
                            quoting=csv.QUOTE_ALL)
    record_count = query_and_call(connection, fi, writerow, (csv_writer,))
    written_bytes = fo.tell()

    if using_sql_path:
        # Only close files we opened... not ones given.
        fi.close()
        sql_log_part = " in '{}' ".format(sql_path_or_fo)
    else:
        sql_log_part = ' '

    if using_results_path:
        # Only close files we opened... not ones given.
        fo.close()

        # If a path was given, rename the temporary path to the provided one.
        try:
            os.remove(results_path_or_fo)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        os.rename(temp_results_path, results_path_or_fo)
        logger.debug("Renamed '%s' to '%s'",
                     temp_results_path, results_path_or_fo)
        results_log_part = " to '{}' ".format(results_path_or_fo)
    else:
        results_log_part = ' to the requested destination '

    if record_count == 0:
        logger.warning("Query%sreturned no results", sql_log_part)

    return record_count, written_bytes


def get_engine(conn_str=None):
    """Return a SQLAlchemy engine object, which can provide a connection.

    By default, the engine object uses a connection string found in the
    config file.

    Args:
        conn_str(str, optional): A connection string to use to get the db connection

    Returns:
        SQLAlchemy.engine: The engine that can connect to the database
    """
    # print("RUNNING get_engine")

    db = 'greenplum_production_cluster'
    if conn_str is None:
        conn_str = conf['database'][db]['connection']
    engine = sqlalchemy.create_engine(conn_str)
    # print("CONN_STRING: "+str(conn_str))
    # print("ENGINE CREATED")
    try:
        with engine.connect() as conn:
            # print("TRY SUCCEEDED")
            pass
    except Exception as e:
        # print("TRY FAILED")
        if hasattr(e, 'message') and 'fe_sendauth' in getattr(e, 'message'):
            # print("HAS ATTR MESSAGE ")
            # This is a psycopg2-raised error.
            logger.debug('No credentials. Asking for username & password...')
            username = six.moves.input("Login name for '{}': ".format(db))
            password = getpass.getpass("Password for {} on '{}': "
                                       .format(username, db))
            remember = six.moves.input(
                    "Remember these credentials in the "
                    "config for this process?\n(They will "
                    "not be saved to disk.) (y/N): "
            )

            credentials = ':'.join((username, password))
            insertion_point = conn_str.find('://') + 3
            new_conn_str = "{}{}@{}".format(conn_str[:insertion_point],
                                            credentials,
                                            conn_str[insertion_point:])
            if remember.upper() in ['Y', 'YE', 'YES']:
                conf['database'][db]['connection'] = new_conn_str
            engine = get_engine(new_conn_str)
        else:
            # print("RAISE IT")
            raise

    return engine


def is_dialect_postgresql_based(dialect):
    """Is a dialect derived from underlying Postgresql dialect

    Args:
        dialect (sqlalchemy.engine.interfaces.Dialect): The dialect to test

    Returns:
        bool: If the dialect is a descendant of the Postgresql dialect

    Examples:
        >>> from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
        >>> is_dialect_postgresql_based(PGDialect())
        True
        >>> is_dialect_postgresql_based(HANABaseDialect())
        False
        >>> is_dialect_postgresql_based(PGDialect_psycopg2())
        True
        >>> is_dialect_postgresql_based(GreenplumDialect())
        True
    """
    return isinstance(dialect, PGDialect)


def is_dialect_greenplum_based(dialect):
    """Is a dialect derived from underlying Greenplum dialect

    Args:
        dialect (sqlalchemy.engine.interfaces.Dialect): The dialect to test

    Returns:
        bool: If the dialect is a descendant of the Greenplum dialect

    Examples:
        >>> from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
        >>> is_dialect_greenplum_based(PGDialect())
        False
        >>> is_dialect_greenplum_based(HANABaseDialect())
        False
        >>> is_dialect_greenplum_based(PGDialect_psycopg2())
        False
        >>> is_dialect_greenplum_based(GreenplumDialect())
        True
    """
    return isinstance(dialect, GreenplumDialect)


def is_dialect_hana_based(dialect):
    """Is a dialect derived from underlying HANA dialect

    Args:
        dialect (sqlalchemy.engine.interfaces.Dialect): The dialect to test

    Returns:
        bool: If the dialect is a descendant of the HANA base dialect

    Examples:
        >>> from sqlalchemy_hana.dialect import HANAHDBCLIDialect, HANAPyHDBDialect
        >>> is_dialect_hana_based(HANAHDBCLIDialect())
        True
        >>> is_dialect_hana_based(PGDialect())
        False
        >>> is_dialect_hana_based(HANAPyHDBDialect())
        True
        >>> is_dialect_hana_based(GreenplumDialect())
        False
    """
    return isinstance(dialect, HANABaseDialect)


def get_compiled_table_name(engine, schema, table_name):
    """Returns a table name quoted in the manner that SQLAlchemy would use to query the table

    Args:
        engine (sqlalchemy.engine.Engine):
        schema (str, optional): The schema name for the table
        table_name (str): The name of the table

    Returns:
        str: The compiled table name

    Examples:
        >>> from sqlalchemy import create_engine
        >>> get_compiled_table_name(create_engine('greenplum://u:p@s'), 'a_schema', 'a_table') == six.text_type('a_schema.a_table')
        True
        >>> get_compiled_table_name(create_engine('greenplum://u:p@s'), 'a_schema-1', 'a_table-1') == six.text_type('"a_schema-1"."a_table-1"')
        True
        >>> get_compiled_table_name(create_engine('greenplum://u:p@s'), None, 'a_table-1') == six.text_type('"a_table-1"')
        True
        >>> get_compiled_table_name(create_engine('greenplum://u:p@s'), '', 'a_table-1') == six.text_type('"a_table-1"')
        True
    """
    target = sqlalchemy.Table(table_name, sqlalchemy.MetaData(), schema=schema)
    return engine.dialect.identifier_preparer.format_table(target)

