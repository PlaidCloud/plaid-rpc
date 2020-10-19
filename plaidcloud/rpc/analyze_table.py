#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import six
import sqlalchemy
from sqlalchemy_greenplum.dialect import GreenplumDialect

from plaidcloud.rpc.rpc_connect import Connect
from plaidcloud.rpc.type_conversion import sqlalchemy_from_dtype

SCHEMA_PREFIX = 'anlz'


def lookup_project(project, rpc=None):
    if not rpc:
        rpc = Connect()

    return rpc.analyze.project.lookup_by_full_path(path=project)


def lookup_table(project_id, table, rpc=None):
    if not rpc:
        rpc = Connect()

    table_id = rpc.analyze.table.lookup_by_full_path(
        project_id=project_id,
        path=table,
    )

    if not table_id.startswith('analyzetable_'):
        return 'analyzetable_' + table_id
    else:
        return table_id


class AnalyzeTable(sqlalchemy.Table):
    def __new__(cls, project, table, branch='master', metadata=None, rpc=None):
        if rpc:
            _rpc = rpc
        else:
            _rpc = Connect()

        if metadata:
            _metadata = metadata
        else:
            _metadata = sqlalchemy.MetaData()


        _project_id = lookup_project(project, _rpc)

        _table_id = lookup_table(_project_id, table, _rpc)

        columns = _rpc.analyze.table.table_meta(
            project_id=_project_id, table_id=_table_id, branch=branch,
        )
        if not columns:
            columns = []  # If the table doesn't actually exist, we assume it's
                          # got no columns

        if _project_id.startswith(SCHEMA_PREFIX):
            _schema = _project_id
        else:
            _schema = '{}{}'.format(SCHEMA_PREFIX, _project_id)

        table_object = super(AnalyzeTable, cls).__new__(
            cls,
            _table_id,
            _metadata,
            *[
                sqlalchemy.Column(
                    c['id'], sqlalchemy_from_dtype(c['dtype']),
                )
                for c in columns
            ],
            schema=_schema,
            extend_existing=True  # If this is the second object representing
                                  # this table, update.
                                  # If you made it with this function, it should
                                  # be no different.
        )

        table_object._metadata = _metadata
        table_object._rpc = _rpc
        table_object._project_id = _project_id
        table_object._table_id = _table_id
        table_object._branch = branch
        table_object._schema = _schema

        return table_object

    def metadata(self):  # pylint: disable=method-hidden
        return self._metadata  # pylint: disable=no-member

    def project_id(self):
        return self._project_id  # pylint: disable=no-member

    def table_id(self):
        return self._table_id  # pylint: disable=no-member

    def branch(self):
        return self._branch  # pylint: disable=no-member

    def schema(self):  # pylint: disable=method-hidden
        return self._schema  # pylint: disable=no-member

    def table_info(self, keys):
        return self._rpc.analyze.table(  # pylint: disable=no-member
            project_id=self.project_id, table_id=self.table_id,
            branch=self.branch, keys=keys,
        )

    def head(self, rows=10):
        if rows is not None:
            query = self.select().limit(rows)
        else:
            query = self.select()
        return send_query(self._rpc, self.project_id, query)  # pylint: disable=no-member


def compiled(sa_query, dialect='greenplum'):
    """Returns SQL query for the supplied dialect, in the form of a string, given a sqlalchemy query.
      Also returns a params dict for use when calling engine.execute(query, params)

    Notes:
        If the requests compilation dialect is not available, an error will be raised

    Args:
        sa_query (sqlalchemy.sql.expression.Executable):
        dialect (str, optional): The sqlalchemy dialect with which to compile the query

    Returns:
        tuple: containing:
            str: compiled query
            dict: query parameters
    """
    dialect = dialect or 'greenplum'  # Just in case someone sends a blank string, or a None by mistake
    eng = sqlalchemy.create_engine(f'{dialect}://127.0.0.1/')
    compiled_query = sa_query.compile(dialect=eng.dialect)
    return str(compiled_query).replace('\n', ''), compiled_query.params


def send_query(project, query, params=None, rpc=None):
    if not rpc:
        rpc = Connect()

    project_id = lookup_project(project, rpc)

    if isinstance(query, six.string_types):
        query_string = query
    else:
        query_string, params = compiled(query)

    return rpc.analyze.query.stream(
        project_id=project_id, query=query_string, params=params,
    )
