"""Routes SimpleRPC calls to their REST twins.

`REST_ROUTES` maps an RPC method path ('analyze/table/exists') to the REST endpoint that does the same work. A
method that is not here, or whose arguments its REST twin cannot take, goes over /json-rpc as before.
"""

import datetime
import decimal
import json
import re
from collections.abc import Callable
from typing import Any, NamedTuple

from plaidcloud.rpc.orjson import unsupported_object_json_encoder
from plaidcloud.rpc.remote.rpc_common import WARNING_CODE, RPCError

DISPATCH_ID = re.compile(r'^[A-Za-z0-9_-]{1,64}$')


class Route(NamedTuple):
    verb: str
    path: str  # under /rest/v1
    adapt: Callable[[dict], dict | None] | None = None  # reshapes the params, or returns None to use /json-rpc
    nulls: tuple = ()  # params sent as null when absent, where REST reads omission differently
    empty: tuple | None = None  # (list param, result): an empty list is answered without a request
    finish: Callable[[dict, Any], Any] | None = None  # (params, REST result) -> the result RPC returned
    recover: Callable[[int, dict], Any] | None = None  # (status, error body) -> RPC's result for an error REST raises


def _step_state(params):
    return params if DISPATCH_ID.match(params.get('dispatch_id') or '0') else None


def _table_create(params):
    # REST's table body has no freshness_max_age_hours
    return None if params.get('freshness_max_age_hours') is not None else params


def _queue(params):
    # REST takes run variables as a dict; the list form RPC also accepts stays on /json-rpc
    variables = params.get('variables')
    if isinstance(variables, (list, tuple)):
        return None if variables else {**params, 'variables': None}
    return params


def _members(params):
    if not isinstance(params.get('include_system_users', False), bool):
        return None
    flt = params.get('filter')
    return {**params, 'filter': json.dumps(flt)} if flt is not None and not isinstance(flt, str) else params


def _table_delete(params):
    # REST answers 204; RPC returns the count of steps removed
    return None if params.get('remove_steps') else params


def _references(params, result):
    # A list of ids answers {id: references}; REST answers a bare list for a list of one
    ids = params['table_id']
    return {ids[0]: result} if isinstance(ids, list) and isinstance(result, list) else result


def _dimension_value(params):
    # REST requires both maps and a bool strict_filters, reads a null agg_function as 'sum', and has no max_concurrent
    if params.get('filter_values') is None or params.get('column_dimension_map') is None:
        return None
    if ('agg_function' in params and params['agg_function'] is None) or 'max_concurrent' in params:
        return None
    return params if isinstance(params.get('strict_filters', False), bool) else None


def _dimension_batch_item(item):
    return (isinstance(item, dict) and isinstance(item.get('value_column'), str) and isinstance(item.get('agg', ''), str)
            and isinstance(item.get('filter_values', {}), dict) and isinstance(item.get('column_dimension_map', {}), dict))


def _dimension_batch(params):
    # RPC answers a malformed item with that item's own error status, and too many items with its own message;
    # REST refuses the whole call with a 422
    requests = params.get('requests')
    if not isinstance(requests, list) or len(requests) > 10_000 or not all(map(_dimension_batch_item, requests)):
        return None
    return params


def _publish_sample_status(status, body):
    # REST answers the statuses RPC returned (no_sample, running, not_yet_run) as a 409 or 425
    data = (body or {}).get('data')
    return data if status in (409, 425) and isinstance(data, dict) and 'status' in data else None


REST_ROUTES = {
    'analyze/agent/check_in': Route('POST', 'analyze/agent/check-in'),
    'analyze/agent/config': Route('GET', 'analyze/agent/config'),
    'analyze/agent/get_payload': Route('GET', 'analyze/agent/payload'),
    'analyze/agent/liveness_failed': Route('POST', 'analyze/agent/liveness-failed'),
    'analyze/agent/save_payload': Route('PUT', 'analyze/agent/payload'),
    'analyze/connection/connection_string': Route('GET', 'analyze/connection/connection-string'),
    'analyze/connection/environment_search_by_name': Route('GET', 'analyze/connection/environment-search-by-name'),
    'analyze/connection/get_config': Route('POST', 'analyze/connection/config'),
    'analyze/dimension/dimension': Route('GET', 'analyze/dimension/dimension'),
    'analyze/dimension/dimensions': Route('GET', 'analyze/dimension/dimensions'),
    'analyze/dimension/get_all_aliases': Route('GET', 'analyze/dimension/all-aliases'),
    'analyze/dimension/get_alt_hierarchies': Route('GET', 'analyze/dimension/alt-hierarchies'),
    'analyze/dimension/get_children_flattened': Route('POST', 'analyze/dimension/children-flattened'),
    'analyze/dimension/get_hierarchy': Route('POST', 'analyze/dimension/hierarchy'),
    'analyze/dimension/get_leaves': Route('POST', 'analyze/dimension/leaves'),
    'analyze/dimension/hierarchy_table': Route('POST', 'analyze/dimension/hierarchy-table'),
    'analyze/dimension/is_dimension': Route('GET', 'analyze/dimension/is-dimension'),
    'analyze/dimension/lookup_by_name': Route('GET', 'analyze/dimension/lookup-by-name'),
    'analyze/onboarding/publish_sample': Route('POST', 'analyze/onboarding/publish-sample', recover=_publish_sample_status),
    'analyze/project/get_project_schema': Route('GET', 'analyze/project/project-schema'),
    'analyze/project/log': Route('POST', 'analyze/project/log'),
    'analyze/project/lookup_by_full_path': Route('GET', 'analyze/project/lookup-by-full-path'),
    'analyze/project/lookup_by_name': Route('GET', 'analyze/project/lookup-by-name'),
    'analyze/project/project': Route('GET', 'analyze/project/project'),
    'analyze/project/projects': Route('GET', 'analyze/project/projects'),
    'analyze/project/variable_values': Route('GET', 'analyze/project/variable-values'),
    'analyze/project/variables': Route('GET', 'analyze/project/variables'),
    'analyze/query/dialect': Route('GET', 'analyze/query/dialect'),
    'analyze/query/upsert': Route('POST', 'analyze/query/upsert'),
    'analyze/query/retrieve_dimension_batch': Route('POST', 'analyze/query/retrieve-dimension-batch', adapt=_dimension_batch),
    'analyze/query/retrieve_dimension_value': Route('POST', 'analyze/query/retrieve-dimension-value', adapt=_dimension_value),
    'analyze/query/retrieve_dimension_values': Route('POST', 'analyze/query/retrieve-dimension-values', adapt=_dimension_value),
    'analyze/step/log': Route('POST', 'analyze/step/log'),
    'analyze/step/lookup_by_full_path': Route('GET', 'analyze/step/lookup-by-full-path'),
    'analyze/step/lookup_by_name': Route('GET', 'analyze/step/lookup-by-name'),
    'analyze/step/step': Route('GET', 'analyze/step/step'),
    'analyze/table/clear_data': Route('DELETE', 'analyze/table/clear-data'),
    'analyze/table/create': Route('POST', 'analyze/table/table', adapt=_table_create),
    'analyze/table/create_data_load': Route('POST', 'analyze/table/data-load'),
    'analyze/table/create_directory_paths': Route('POST', 'analyze/table/directory-paths'),
    'analyze/table/create_snapshot': Route('POST', 'analyze/table/snapshot'),
    'analyze/table/delete': Route('DELETE', 'analyze/table/table', adapt=_table_delete),
    'analyze/table/execute_data_load': Route('POST', 'analyze/table-import/execute-data-load'),
    'analyze/table/exists': Route('GET', 'analyze/table/exists'),
    'analyze/table/get_snapshots': Route('GET', 'analyze/table/snapshots'),
    'analyze/table/load_csv': Route('POST', 'analyze/table-import/load-csv'),
    'analyze/table/lookup_by_full_path': Route('GET', 'analyze/table/lookup-by-full-path'),
    'analyze/table/lookup_by_name': Route('GET', 'analyze/table/lookup-by-name'),
    'analyze/table/references': Route('GET', 'analyze/table/references', empty=('table_id', {}), finish=_references),
    'analyze/table/search_by_name': Route('GET', 'analyze/table/search-by-name'),
    'analyze/table/table': Route('GET', 'analyze/table/table'),
    'analyze/table/table_meta': Route('GET', 'analyze/table/table-meta'),
    'analyze/table/tables': Route('GET', 'analyze/table/tables'),
    'analyze/table/touch': Route('POST', 'analyze/table/touch'),
    'analyze/table/update_shape': Route('PUT', 'analyze/table/shape'),
    'analyze/tm1/query': Route('POST', 'analyze/tm1/query'),
    'analyze/udf/create': Route('POST', 'analyze/udf/udf'),
    'analyze/udf/get_code': Route('GET', 'analyze/udf/code'),
    'analyze/udf/set_code': Route('PUT', 'analyze/udf/code'),
    'analyze/udf/udf': Route('GET', 'analyze/udf/udf'),
    'analyze/udf/udfs': Route('GET', 'analyze/udf/udfs'),
    'analyze/workflow/clear_all_steps': Route('POST', 'analyze/workflow/clear-all-steps'),
    'analyze/workflow/clear_run_state': Route('POST', 'analyze/workflow/clear-run-state'),
    'analyze/workflow/clear_steps': Route('POST', 'analyze/workflow/clear-steps'),
    'analyze/workflow/dag': Route('GET', 'analyze/workflow/dag'),
    'analyze/workflow/is_queued': Route('GET', 'analyze/workflow/is-queued'),
    'analyze/workflow/is_running': Route('GET', 'analyze/workflow/is-running'),
    'analyze/workflow/log': Route('POST', 'analyze/workflow/log'),
    'analyze/workflow/lookup_by_full_path': Route('GET', 'analyze/workflow/lookup-by-full-path'),
    'analyze/workflow/lookup_by_name': Route('GET', 'analyze/workflow/lookup-by-name'),
    'analyze/workflow/queue': Route('POST', 'analyze/workflow/queue', adapt=_queue),
    'analyze/workflow/range_start': Route('POST', 'analyze/workflow/range-start'),
    'analyze/workflow/set_running': Route('PUT', 'analyze/workflow/set-running'),
    'analyze/workflow/set_step_loop': Route('PUT', 'analyze/workflow/step-loop'),
    'analyze/workflow/set_step_retry': Route('PUT', 'analyze/workflow/step-retry'),
    'analyze/workflow/set_step_state': Route('PUT', 'analyze/workflow/step-state', adapt=_step_state),
    'analyze/workflow/set_variable': Route('PUT', 'analyze/workflow/set-variable', nulls=('value',)),
    'analyze/workflow/start': Route('POST', 'analyze/workflow/start'),
    'analyze/workflow/state': Route('GET', 'analyze/workflow/state'),
    'analyze/workflow/step_state': Route('GET', 'analyze/workflow/step-state'),
    'analyze/workflow/steps_state_all': Route('GET', 'analyze/workflow/steps-state-all'),
    'analyze/workflow/stop': Route('POST', 'analyze/workflow/stop'),
    'analyze/workflow/structure': Route('GET', 'analyze/workflow/structure'),
    'analyze/workflow/variable_values': Route('GET', 'analyze/workflow/variable-values'),
    'analyze/workflow/workflow': Route('GET', 'analyze/workflow/workflow'),
    'analyze/workflow/workflows': Route('GET', 'analyze/workflow/workflows'),
    'document/view/commit_upload': Route('POST', 'document/upload/commit'),
    'document/view/download': Route('GET', 'document/view/download'),
    'document/view/upload_chunk': Route('POST', 'document/upload/chunk'),
    'identity/group/groups': Route('GET', 'identity/group/groups'),
    'identity/me/available_services': Route('GET', 'identity/me/available-services'),
    'identity/me/get_layout': Route('GET', 'identity/me/layout'),
    'identity/me/scopes': Route('GET', 'identity/me/scopes'),
    'identity/member/create': Route('POST', 'identity/member/member'),
    'identity/member/members': Route('GET', 'identity/member/members', adapt=_members),
    'identity/member/set_security_groups': Route('PUT', 'identity/member/security-groups'),
    'organization/migration/trigger_migration_backups': Route('POST', 'organization/migration/trigger-migration-backups'),
    'organization/migration/pause_all_scheduled_events': Route('POST', 'organization/migration/pause-all-scheduled-events'),
    'organization/migration/remap_completeness_scan': Route('POST', 'organization/migration/remap-completeness-scan'),
    'organization/migration/apply_identity_remap': Route('POST', 'organization/migration/apply-identity-remap'),
    'organization/migration/remap_data_connection_hosts': Route('POST', 'organization/migration/remap-data-connection-hosts'),
    'organization/migration/repoint_lakehouse_catalog': Route('POST', 'organization/migration/repoint-lakehouse-catalog'),
    'organization/migration/reconcile_rls_after_remap': Route('POST', 'organization/migration/reconcile-rls-after-remap'),
    'organization/migration/drop_panel_previews': Route('POST', 'organization/migration/drop-panel-previews'),
    'organization/migration/swap_dashboard_roles_after_remap': Route('POST', 'organization/migration/swap-dashboard-roles-after-remap'),
    'organization/migration/reprovision_project_database_roles': Route('POST', 'organization/migration/reprovision-project-database-roles'),
    'organization/migration/restore_store_count': Route('GET', 'organization/migration/restore-store-count'),
    'organization/migration/list_all_project_ids': Route('GET', 'organization/migration/project-ids'),
    'organization/migration/restore_partition_slugs': Route('GET', 'organization/migration/restore-partition-slugs'),
    'organization/migration/lakehouse_read_probe': Route('GET', 'organization/migration/lakehouse-read-probe'),
    'organization/migration/databend_query_probe': Route('GET', 'organization/migration/databend-query-probe'),
    'organization/migration/connection_decrypt_probe': Route('GET', 'organization/migration/connection-decrypt-probe'),
    'organization/migration/dimension_read_probe': Route('GET', 'organization/migration/dimension-read-probe'),
    'analyze/lakehouse/test_connection': Route('POST', 'analyze/lakehouse/test-connection'),
    'analyze/lakehouse/sentinel_bound_project_count': Route('GET', 'analyze/lakehouse/sentinel-bound-project-count'),
    'analyze/onboarding/seed_sample': Route('POST', 'analyze/onboarding/seed-sample'),
    'analyze/onboarding/activation_aggregate': Route('GET', 'analyze/onboarding/activation-aggregate'),
    'analyze/table/copy_macro_port': Route('POST', 'analyze/table/copy-macro-port'),
    'analyze/table/create_run_schema': Route('POST', 'analyze/table/run-schema'),
    'analyze/table/drop_run_schema': Route('DELETE', 'analyze/table/run-schema'),
    'analyze/table/load_parquet': Route('POST', 'analyze/table/load-parquet'),
    'analyze/workflow/acquire_node_lock': Route('POST', 'analyze/workflow/acquire-node-lock'),
    'analyze/workflow/release_node_lock': Route('POST', 'analyze/workflow/release-node-lock'),
    'analyze/workflow/set_node_terminal_state': Route('PUT', 'analyze/workflow/node-terminal-state'),
    'analyze/workflow/get_node_terminal_states': Route('POST', 'analyze/workflow/node-terminal-states/query', empty=('node_ids', {})),
    'public/system/endpoints_map': Route('GET', 'public/system/endpoints-map'),
}


class Request(NamedTuple):
    verb: str | None  # None: nothing to send, `finish` gives the result
    path: str | None
    query: dict | None
    body: dict | None
    finish: Callable[[Any], Any] | None
    recover: Callable[[int, dict], Any]


class RouteMissing(Exception):
    """The server has no REST route for the call."""


def _query_value(value):
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, datetime.datetime):  # a naive time is UTC, as the json-rpc encoder sends it
        return (value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)).isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (bytes, set, datetime.timedelta, decimal.Decimal)):
        return unsupported_object_json_encoder(value)
    return value


def error_body(error):
    """The JSON body of a REST error, or None when it has none (an HTML 404 from an ingress)."""
    try:
        body = json.loads(error.data)
    except (TypeError, ValueError):
        return None
    return body if isinstance(body, dict) else None


def route_missing(status, body):
    """Whether a REST error says the server has no such route, as opposed to a route answering 404."""
    if status == 404:
        # Tenants before the tag carry a bare FastAPI 404
        return body == {'detail': 'Not Found'} or ((body or {}).get('data') or {}).get('code') == 'route_not_found'
    return status == 405 and (body or {}).get('detail') == 'Method Not Allowed'


def rest_error(status, body, error):
    """The exception json-rpc raised for the failure a REST error reports: a Warning, or an RPCError carrying
    the server's own code (-1001 for a refusal) and, for a typed error, its name in the message."""
    if body is None or 'detail' not in body:
        return error
    detail = body['detail'] if isinstance(body['detail'], str) else json.dumps(body['detail'])
    kind = body.get('type')
    code = body.get('code')
    # Before the server tagged its errors, a Warning was a bare 409
    if kind == 'Warning' or code == WARNING_CODE or (status == 409 and kind is None and code is None and 'data' not in body):
        return Warning(detail)
    if kind and kind.endswith('Error') and kind not in detail and code != -1001:
        detail = f'{kind}: {detail}'
    return RPCError(detail, data=body.get('data'), code=status if code is None else code)


def rest_request(method_path, params):
    """The REST request that does what RPC `method_path` does with `params`, or None when it has to go over
    /json-rpc."""
    route = REST_ROUTES.get(method_path)
    if route is None:
        return None
    if route.adapt and (params := route.adapt(params)) is None:
        return None
    finish = route.finish and (lambda result: route.finish(params, result))
    if route.empty and params.get(route.empty[0]) == []:
        return Request(None, None, None, None, lambda _: route.empty[1], None)
    params = {k: v for k, v in params.items() if v is not None or k in route.nulls}
    params |= {k: None for k in route.nulls if k not in params}
    recover = route.recover or (lambda status, body: None)
    if route.verb in ('GET', 'DELETE'):
        if 'sort' in params:
            # RPC sorts by [(key, reverse)], REST by ['key:asc'] / ['key:desc']
            params['sort'] = [f"{key}:{'desc' if reverse else 'asc'}" for key, reverse in params['sort']]
        query = {k: [_query_value(i) for i in v] if isinstance(v, (list, tuple, set)) else _query_value(v)
                 for k, v in params.items()}
        return Request(route.verb, route.path, query, None, finish, recover)
    return Request(route.verb, route.path, None, params, finish, recover)
