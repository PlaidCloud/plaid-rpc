
from unittest import mock

import pytest

from plaidcloud.rpc.connection import jsonrpc
from plaidcloud.rpc.connection.jsonrpc import SimpleRPC
from plaidcloud.rpc.connection.rest_routes import REST_ROUTES


@pytest.fixture
def transport():
    """Fake REST and JSON-RPC transports behind a SimpleRPC, recording what each was sent."""
    rest = mock.Mock(return_value={'rest': True})
    json_rpc = mock.Mock(return_value={'ok': True, 'result': 'rpc'})
    with mock.patch.object(jsonrpc, 'http_rest', rest), mock.patch.object(jsonrpc, 'http_json_rpc', json_rpc):
        client = SimpleRPC('tok', uri='https://example.com/json-rpc', verify_ssl=True)
        yield client, rest, json_rpc


def rest_call(rest):
    """The (verb, path, query, body) the fake REST transport got."""
    (token, uri, verb, path, _verify), kwargs = rest.call_args
    assert (token, uri) == ('tok', 'https://example.com/json-rpc')
    return verb, path, kwargs['params'], kwargs['body']


CASES = [
    ('analyze/table/exists', {'project_id': 'p', 'table_id': 't', 'run_id': 'r'},
     ('GET', 'analyze/table/exists', {'project_id': 'p', 'table_id': 't', 'run_id': 'r'}, None)),
    # None is "not given"; bools are lowercase; lists repeat the key
    ('analyze/table/tables', {'project_id': 'p', 'id_filter': ['a', 'b'], 'keys': None, 'by_role': True},
     ('GET', 'analyze/table/tables', {'project_id': 'p', 'id_filter': ['a', 'b'], 'by_role': 'true'}, None)),
    # RPC sorts by [(key, reverse)], REST by 'key:asc' (A to Z) or 'key:desc'
    ('analyze/dimension/dimensions', {'project_id': 'p', 'sort': [('name', False), ('id', True)]},
     ('GET', 'analyze/dimension/dimensions', {'project_id': 'p', 'sort': ['name:asc', 'id:desc']}, None)),
    ('analyze/table/delete', {'project_id': 'p', 'table_id': 't', 'include_versions': False},
     ('DELETE', 'analyze/table/table', {'project_id': 'p', 'table_id': 't', 'include_versions': 'false'}, None)),
    ('analyze/workflow/stop', {'project_id': 'p', 'workflow_id': 'w', 'state': 'failed', 'run_id': 'r'},
     ('POST', 'analyze/workflow/stop', None, {'project_id': 'p', 'workflow_id': 'w', 'state': 'failed', 'run_id': 'r'})),
    ('analyze/workflow/set_step_state',
     {'project_id': 'p', 'workflow_id': 'w', 'step_id': 's', 'state': 'running', 'dispatch_id': 'd-1_x'},
     ('PUT', 'analyze/workflow/step-state', None,
      {'project_id': 'p', 'workflow_id': 'w', 'step_id': 's', 'state': 'running', 'dispatch_id': 'd-1_x'})),
    ('analyze/workflow/set_variable', {'project_id': 'p', 'workflow_id': 'w', 'variable_id': 'v', 'value': 3},
     ('PUT', 'analyze/workflow/set-variable', None,
      {'project_id': 'p', 'workflow_id': 'w', 'variable_id': 'v', 'value': 3})),
    ('analyze/workflow/queue', {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'variables': {'a': 1}},
     ('POST', 'analyze/workflow/queue', None,
      {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'variables': {'a': 1}})),
    # members: REST takes the filter as a JSON string
    ('identity/member/members', {'filter': {'is_active': True}, 'as_dict': True},
     ('GET', 'identity/member/members', {'filter': '{"is_active": true}', 'as_dict': 'true'}, None)),
    ('identity/me/scopes', {}, ('GET', 'identity/me/scopes', {}, None)),
    ('identity/member/set_security_groups', {'member_id': 'm', 'group_ids': ['g1', 'g2']},
     ('PUT', 'identity/member/security-groups', None, {'member_id': 'm', 'group_ids': ['g1', 'g2']})),
    ('analyze/udf/set_code', {'project_id': 'p', 'udf_id': 'u', 'code': 'x = 1'},
     ('PUT', 'analyze/udf/code', None, {'project_id': 'p', 'udf_id': 'u', 'code': 'x = 1'})),
    ('document/view/download', {'account_id': 'a', 'path': '/x'},
     ('GET', 'document/view/download', {'account_id': 'a', 'path': '/x'}, None)),
    ('analyze/workflow/queue', {'project_id': 'p', 'workflow_id': 'w', 'parent_run_id': 'pr', 'variables': []},
     ('POST', 'analyze/workflow/queue', None, {'project_id': 'p', 'workflow_id': 'w', 'parent_run_id': 'pr'})),
    ('analyze/workflow/acquire_node_lock', {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_id': 'n', 'owner_token': 'o'},
     ('POST', 'analyze/workflow/acquire-node-lock', None,
      {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_id': 'n', 'owner_token': 'o'})),
    ('analyze/workflow/release_node_lock', {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_id': 'n', 'owner_token': 'o'},
     ('POST', 'analyze/workflow/release-node-lock', None,
      {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_id': 'n', 'owner_token': 'o'})),
    ('analyze/workflow/set_node_terminal_state', {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_id': 'n', 'state': 'failed'},
     ('PUT', 'analyze/workflow/node-terminal-state', None,
      {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_id': 'n', 'state': 'failed'})),
    ('analyze/workflow/get_node_terminal_states', {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_ids': ['a', 'b']},
     ('POST', 'analyze/workflow/node-terminal-states/query', None,
      {'project_id': 'p', 'workflow_id': 'w', 'run_id': 'r', 'node_ids': ['a', 'b']})),
    ('analyze/table/create_run_schema', {'project_id': 'p', 'run_id': 'r'},
     ('POST', 'analyze/table/run-schema', None, {'project_id': 'p', 'run_id': 'r'})),
    ('analyze/table/drop_run_schema', {'project_id': 'p', 'run_id': 'r'},
     ('DELETE', 'analyze/table/run-schema', {'project_id': 'p', 'run_id': 'r'}, None)),
    ('analyze/table/copy_macro_port',
     {'project_id': 'p', 'source_table_id': 's', 'dest_table_id': 'd', 'source_run_id': 'a', 'dest_run_id': 'b', 'append': False},
     ('POST', 'analyze/table/copy-macro-port', None,
      {'project_id': 'p', 'source_table_id': 's', 'dest_table_id': 'd', 'source_run_id': 'a', 'dest_run_id': 'b', 'append': False})),
    ('analyze/table/load_parquet', {'project_id': 'p', 'table_id': 't', 'meta': {}, 'parquet_data': 'QQ==', 'append': True},
     ('POST', 'analyze/table/load-parquet', None,
      {'project_id': 'p', 'table_id': 't', 'meta': {}, 'parquet_data': 'QQ==', 'append': True})),
    ('analyze/lakehouse/test_connection', {'spec': {'a': 1}, 'credential': {'b': 2}},
     ('POST', 'analyze/lakehouse/test-connection', None, {'spec': {'a': 1}, 'credential': {'b': 2}})),
    ('analyze/lakehouse/sentinel_bound_project_count', {},
     ('GET', 'analyze/lakehouse/sentinel-bound-project-count', {}, None)),
    ('analyze/onboarding/seed_sample', {'project_name': 'n', 'sample_slug': 's'},
     ('POST', 'analyze/onboarding/seed-sample', None, {'project_name': 'n', 'sample_slug': 's'})),
    ('analyze/onboarding/activation_aggregate', {}, ('GET', 'analyze/onboarding/activation-aggregate', {}, None)),
    ('public/system/endpoints_map', {'version': 'v1'},
     ('GET', 'public/system/endpoints-map', {'version': 'v1'}, None)),
    ('organization/migration/trigger_migration_backups', {},
     ('POST', 'organization/migration/trigger-migration-backups', None, {})),
    ('organization/migration/pause_all_scheduled_events', {'tenant_id': 't'},
     ('POST', 'organization/migration/pause-all-scheduled-events', None, {'tenant_id': 't'})),
    ('organization/migration/remap_completeness_scan', {'tenant_id': 't', 'old_set': ['x']},
     ('POST', 'organization/migration/remap-completeness-scan', None, {'tenant_id': 't', 'old_set': ['x']})),
    ('organization/migration/apply_identity_remap',
     {'tenant_id': 't', 'source_realm': 'r', 'id_map_bucket': 'b', 'id_map_path': 'p', 'timeout_seconds': 60},
     ('POST', 'organization/migration/apply-identity-remap', None,
      {'tenant_id': 't', 'source_realm': 'r', 'id_map_bucket': 'b', 'id_map_path': 'p', 'timeout_seconds': 60})),
    ('organization/migration/remap_data_connection_hosts', {'source_host': 'a', 'target_host': 'b'},
     ('POST', 'organization/migration/remap-data-connection-hosts', None, {'source_host': 'a', 'target_host': 'b'})),
    ('organization/migration/repoint_lakehouse_catalog', {'source_bucket': 'a', 'clone_bucket': 'b', 'verify_only': True},
     ('POST', 'organization/migration/repoint-lakehouse-catalog', None,
      {'source_bucket': 'a', 'clone_bucket': 'b', 'verify_only': True})),
    ('organization/migration/reconcile_rls_after_remap', {'tenant_id': 't'},
     ('POST', 'organization/migration/reconcile-rls-after-remap', None, {'tenant_id': 't'})),
    ('organization/migration/drop_panel_previews', {'tenant_id': 't'},
     ('POST', 'organization/migration/drop-panel-previews', None, {'tenant_id': 't'})),
    ('organization/migration/swap_dashboard_roles_after_remap', {'id_map_bucket': 'b', 'id_map_path': 'p'},
     ('POST', 'organization/migration/swap-dashboard-roles-after-remap', None, {'id_map_bucket': 'b', 'id_map_path': 'p'})),
    ('organization/migration/reprovision_project_database_roles', {'dry_run': True},
     ('POST', 'organization/migration/reprovision-project-database-roles', None, {'dry_run': True})),
    ('organization/migration/restore_store_count', {'store': 's'},
     ('GET', 'organization/migration/restore-store-count', {'store': 's'}, None)),
    ('analyze/query/retrieve_dimension_value',
     {'project_id': 'p', 'table_id': 't', 'value_column': 'v', 'filter_values': {'a': 'ALL'}, 'column_dimension_map': {},
      'strict_filters': True},
     ('POST', 'analyze/query/retrieve-dimension-value', None,
      {'project_id': 'p', 'table_id': 't', 'value_column': 'v', 'filter_values': {'a': 'ALL'}, 'column_dimension_map': {},
       'strict_filters': True})),
    ('analyze/query/retrieve_dimension_values',
     {'project_id': 'p', 'table_id': 't', 'value_columns': ['v', 'w'], 'filter_values': {}, 'column_dimension_map': {},
      'agg_function': 'max'},
     ('POST', 'analyze/query/retrieve-dimension-values', None,
      {'project_id': 'p', 'table_id': 't', 'value_columns': ['v', 'w'], 'filter_values': {}, 'column_dimension_map': {},
       'agg_function': 'max'})),
    ('analyze/query/retrieve_dimension_batch',
     {'project_id': 'p', 'table_id': 't', 'requests': [{'value_column': 'v', 'filter_values': {'a': 'ALL'}}]},
     ('POST', 'analyze/query/retrieve-dimension-batch', None,
      {'project_id': 'p', 'table_id': 't', 'requests': [{'value_column': 'v', 'filter_values': {'a': 'ALL'}}]})),
    ('organization/migration/list_all_project_ids', {}, ('GET', 'organization/migration/project-ids', {}, None)),
    ('organization/migration/restore_partition_slugs', {}, ('GET', 'organization/migration/restore-partition-slugs', {}, None)),
    ('organization/migration/lakehouse_read_probe', {'timeout_s': 5},
     ('GET', 'organization/migration/lakehouse-read-probe', {'timeout_s': 5}, None)),
    ('organization/migration/databend_query_probe', {'timeout_s': 5},
     ('GET', 'organization/migration/databend-query-probe', {'timeout_s': 5}, None)),
    ('organization/migration/connection_decrypt_probe', {'timeout_s': 5},
     ('GET', 'organization/migration/connection-decrypt-probe', {'timeout_s': 5}, None)),
    ('organization/migration/dimension_read_probe', {'sample_limit': 3, 'timeout_s': 5},
     ('GET', 'organization/migration/dimension-read-probe', {'sample_limit': 3, 'timeout_s': 5}, None)),
]


@pytest.mark.parametrize('method_path, params, expected', CASES, ids=[c[0] for c in CASES])
def test_routed_call_reaches_its_rest_twin(transport, method_path, params, expected):
    client, rest, json_rpc = transport
    func = client
    for part in method_path.split('/'):
        func = getattr(func, part)
    assert func(**params) == {'rest': True}
    assert rest_call(rest) == expected
    json_rpc.assert_not_called()


def test_update_shape_sends_the_table_list(transport):
    client, rest, _ = transport
    client.analyze.table.update_shape(project_id='p', table_ids=['t1', 't2'], run_id='r')
    assert rest_call(rest) == ('PUT', 'analyze/table/shape', None,
                               {'project_id': 'p', 'table_ids': ['t1', 't2'], 'run_id': 'r'})


def test_unsetting_a_variable_sends_null_value(transport):
    client, rest, _ = transport
    client.analyze.workflow.set_variable(project_id='p', workflow_id='w', variable_id='v', run_id='r')
    assert rest_call(rest) == ('PUT', 'analyze/workflow/set-variable', None,
                               {'project_id': 'p', 'workflow_id': 'w', 'variable_id': 'v', 'run_id': 'r', 'value': None})


def test_no_content_response_is_none(transport):
    client, rest, _ = transport
    rest.return_value = None
    assert client.analyze.table.clear_data(project_id='p', table_id='t') is None


# Calls whose arguments the REST twin cannot take stay on /json-rpc
@pytest.mark.parametrize('method_path, params', [
    ('analyze/workflow/set_step_state', {'project_id': 'p', 'dispatch_id': 'bad id!'}),
    ('analyze/table/delete', {'project_id': 'p', 'table_id': 't', 'remove_steps': True}),
    ('identity/member/members', {'include_system_users': 'agent'}),
    ('analyze/query/retrieve_dimension_batch', {'project_id': 'p'}),
    ('analyze/query/query', {'project_id': 'p'}),
    ('analyze/table/create', {'project_id': 'p', 'name': 'n', 'freshness_max_age_hours': 4}),
    ('analyze/workflow/queue', {'project_id': 'p', 'workflow_id': 'w', 'variables': [{'id': 'v'}]}),
    ('analyze/query/retrieve_dimension_value', {'project_id': 'p'}),
    ('analyze/query/retrieve_dimension_value',
     {'project_id': 'p', 'table_id': 't', 'value_column': 'v', 'filter_values': None, 'column_dimension_map': {}}),
    ('analyze/query/retrieve_dimension_value',
     {'project_id': 'p', 'table_id': 't', 'value_column': 'v', 'filter_values': {}, 'column_dimension_map': {},
      'agg_function': None}),
    ('analyze/query/retrieve_dimension_values',
     {'project_id': 'p', 'table_id': 't', 'value_columns': ['v'], 'filter_values': {}, 'column_dimension_map': {},
      'max_concurrent': 5}),
    ('analyze/query/retrieve_dimension_batch',
     {'project_id': 'p', 'table_id': 't', 'requests': [{'value_column': 'v', 'column_dimension_map': None}]}),
    ('analyze/query/retrieve_dimension_batch',
     {'project_id': 'p', 'table_id': 't', 'requests': [{'value_column': 'v'}, {'filter_values': {}}]}),
    ('analyze/query/retrieve_dimension_batch',
     {'project_id': 'p', 'table_id': 't', 'requests': [{'value_column': 'v', 'agg': 5}]}),
    ('analyze/query/retrieve_dimension_batch',
     {'project_id': 'p', 'table_id': 't', 'requests': [{'value_column': 'v', 'filter_values': [['a', 'b']]}]}),
    ('analyze/query/retrieve_dimension_batch',
     {'project_id': 'p', 'table_id': 't', 'requests': [{'value_column': 'v'}] * 10_001}),
    ('analyze/query/retrieve_dimension_value',
     {'project_id': 'p', 'table_id': 't', 'value_column': 'v', 'filter_values': {}, 'column_dimension_map': {},
      'strict_filters': 'false'}),
    ('document/view/download_stream', {'account_id': 'a'}),
    ('analyze/table/not_a_routed_method', {'project_id': 'p'}),
])
def test_unroutable_call_falls_back_to_json_rpc(transport, method_path, params):
    client, rest, json_rpc = transport
    func = client
    for part in method_path.split('/'):
        func = getattr(func, part)
    assert func(**params) == 'rpc'
    rest.assert_not_called()
    assert json_rpc.call_args.args[3]['method'] == method_path
    assert json_rpc.call_args.args[3]['params'] == params


def test_bearer_token_and_url_of_a_routed_call():
    session = mock.MagicMock()
    session.request.return_value = mock.Mock(status_code=200, content=b'true', text='true')
    with mock.patch.object(jsonrpc, '_get_session') as get_session:
        get_session.return_value.__enter__.return_value = session
        client = SimpleRPC(lambda: 'fresh', uri='https://example.com/json-rpc', verify_ssl=True)
        assert client.analyze.table.exists(project_id='p', table_id='t') is True
    args, kwargs = session.request.call_args
    assert args == ('GET', 'https://example.com/rest/v1/analyze/table/exists')
    assert kwargs['params'] == {'project_id': 'p', 'table_id': 't'}
    assert kwargs['headers']['Authorization'] == 'Bearer fresh'


def test_every_route_is_a_rest_path():
    for method_path, route in REST_ROUTES.items():
        assert route.verb in ('GET', 'POST', 'PUT', 'DELETE'), method_path
        assert not route.path.startswith('/'), method_path


def test_references_of_a_list_of_one_answers_a_dict(transport):
    client, rest, _ = transport
    rest.return_value = [{'id': 's1'}]
    assert client.analyze.table.references(project_id='p', table_id=['t']) == {'t': [{'id': 's1'}]}
    rest.return_value = [{'id': 's1'}]
    assert client.analyze.table.references(project_id='p', table_id='t') == [{'id': 's1'}]


def test_empty_id_lists_are_answered_without_a_request(transport):
    client, rest, json_rpc = transport
    assert client.analyze.table.references(project_id='p', table_id=[]) == {}
    assert client.analyze.workflow.get_node_terminal_states(
        project_id='p', workflow_id='w', run_id='r', node_ids=[]) == {}
    rest.assert_not_called()
    json_rpc.assert_not_called()


def test_datetime_query_values_are_utc_iso_like_the_json_rpc_encoder(transport):
    import datetime
    client, rest, _ = transport
    client.analyze.table.get_snapshots(project_id='p', table_id='t', start_date=datetime.datetime(2026, 1, 2, 3, 4))
    assert rest_call(rest)[2]['start_date'] == '2026-01-02T03:04:00+00:00'


def test_query_values_for_dicts_dates_and_unsupported_objects():
    import datetime

    from plaidcloud.rpc.connection.rest_routes import _query_value
    assert _query_value({'a': 1}) == '{"a": 1}'
    assert _query_value(datetime.date(2026, 1, 2)) == '2026-01-02'
    assert _query_value(b'ab') == 'ab'
