"""Redis connector tests. Everything runs against fakes -- no Redis server."""
import asyncio
import socket
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

redis = pytest.importorskip('redis')
from redis import asyncio as aioredis
from redis.asyncio.sentinel import MasterNotFoundError, SentinelConnectionPool
from redis.asyncio.sentinel import Sentinel as aioSentinel
from redis.sentinel import Sentinel

from plaidcloud.rpc import redis_connector as rc


def rinfo(**kw):
    base = {'cluster': False, 'sentinel': False, 'headless': False, 'hosts': [('h', 6379)], 'database': 2, 'password': 'pw',
                'socket_timeout': 5, 'quorum': None, 'service_name': 'svc'}
    return SimpleNamespace(**{**base, **kw})


def cfg_for(info):
    return SimpleNamespace(redis=SimpleNamespace(get_url=lambda name: info))


# ---- config injection -------------------------------------------------------------------------

def test_redis_info_uses_injected_config():
    info = rinfo()
    assert rc._redis_info('x', cfg_for(info)) is info


def test_redis_info_defaults_to_plaidcloud_config(monkeypatch):
    info = rinfo()
    monkeypatch.setitem(sys.modules, 'plaidcloud.config', SimpleNamespace(config=cfg_for(info)))
    assert rc._redis_info('x') is info


def test_importing_the_package_does_not_import_redis():
    import subprocess
    out = subprocess.run(
        [sys.executable, '-c', 'import plaidcloud.rpc, sys; print("redis" in sys.modules)'],
        capture_output=True, text=True, check=True)
    assert out.stdout.strip() == 'False'


# ---- DNS retry --------------------------------------------------------------------------------

def test_dns_retry_recovers_from_transient_failure():
    lookup = mock.Mock(side_effect=[socket.gaierror('blip'), ('h', [], ['10.0.0.1', '10.0.0.2'])])
    with mock.patch.object(socket, 'gethostbyname_ex', lookup), mock.patch.object(rc.time, 'sleep') as sleep:
        assert rc._resolve_headless_host_with_retry('h') == ['10.0.0.1', '10.0.0.2']
    assert lookup.call_count == 2
    sleep.assert_called_once_with(0.2)


def test_dns_retry_raises_last_error_after_three_attempts():
    lookup = mock.Mock(side_effect=socket.gaierror('down'))
    with mock.patch.object(socket, 'gethostbyname_ex', lookup), mock.patch.object(rc.time, 'sleep') as sleep, \
            pytest.raises(socket.gaierror):
        rc._resolve_headless_host_with_retry('h')
    assert lookup.call_count == 3
    assert [c.args[0] for c in sleep.call_args_list] == [0.2, 0.4]


def test_dns_retry_async_recovers_and_exhausts():
    lookup = mock.Mock(side_effect=[socket.gaierror('blip'), ('h', [], ['10.0.0.1'])])
    with mock.patch.object(socket, 'gethostbyname_ex', lookup), mock.patch.object(rc.asyncio, 'sleep', mock.AsyncMock()):
        assert asyncio.run(rc._resolve_headless_host_with_retry_async('h')) == ['10.0.0.1']
    lookup = mock.Mock(side_effect=socket.gaierror('down'))
    with mock.patch.object(socket, 'gethostbyname_ex', lookup), mock.patch.object(rc.asyncio, 'sleep', mock.AsyncMock()), \
            pytest.raises(socket.gaierror):
        asyncio.run(rc._resolve_headless_host_with_retry_async('h'))
    assert lookup.call_count == 3


# ---- cancel safety ----------------------------------------------------------------------------

class RecordingConn:
    def __init__(self, disconnect_raises=False):
        self.disconnected = False
        self.nowait = None
        self.disconnect_raises = disconnect_raises

    async def send_command(self, *args, **kwargs):
        pass

    async def disconnect(self, nowait=False):
        self.disconnected, self.nowait = True, nowait
        if self.disconnect_raises:
            raise RuntimeError('boom')


async def probe_parse_response(self, connection, command_name, **options):
    if self.parse_raises is not None:
        raise self.parse_raises
    return self.parse_result


class CancelSafeProbe(rc.CancelSafeRedis):
    parse_raises = None
    parse_result = None
    parse_response = probe_parse_response


class StockProbe(aioredis.Redis):
    parse_raises = None
    parse_result = None
    parse_response = probe_parse_response


class PipelineSendProbe(rc.CancelSafePipeline):
    parse_raises = None
    parse_result = None
    parse_response = probe_parse_response


async def raise_cancelled(*_a, **_k):
    raise asyncio.CancelledError()


async def _send_cancelled(cls):
    client = cls()
    client.parse_raises = asyncio.CancelledError()
    conn = RecordingConn()
    with pytest.raises(asyncio.CancelledError):
        await client._send_command_parse_response(conn, 'HGETALL', 'k')
    return conn


def test_cancel_between_send_and_read_drops_the_connection():
    conn = asyncio.run(_send_cancelled(CancelSafeProbe))
    assert conn.disconnected and conn.nowait


def test_stock_client_leaks_the_connection():
    # Control: if this starts disconnecting, redis-py fixed it upstream and the override can go.
    assert not asyncio.run(_send_cancelled(StockProbe)).disconnected


def test_normal_response_passes_through():
    client = CancelSafeProbe()
    client.parse_result = ['f', 'v']
    conn = RecordingConn()
    assert asyncio.run(client._send_command_parse_response(conn, 'HGETALL', 'k')) == ['f', 'v']
    assert not conn.disconnected


def test_failing_disconnect_does_not_mask_the_cancellation():
    conn = RecordingConn(disconnect_raises=True)
    asyncio.run(rc.drop_connection_on_cancel(conn))
    assert conn.disconnected


def test_pipeline_watch_leg_drops_the_connection():
    assert asyncio.run(_send_cancelled(lambda: PipelineSendProbe(
        connection_pool=object(), response_callbacks={}, transaction=True, shard_hint=None))).disconnected


@pytest.mark.parametrize('method', ['_execute_transaction', '_execute_pipeline'])
def test_pipeline_batch_paths_drop_the_connection(method):
    pipe = rc.CancelSafePipeline(connection_pool=object(), response_callbacks={}, transaction=True, shard_hint=None)
    conn = RecordingConn()

    async def run():
        with mock.patch.object(aioredis.client.Pipeline, method, raise_cancelled), \
                pytest.raises(asyncio.CancelledError):
            await getattr(pipe, method)(conn, [], True)

    asyncio.run(run())
    assert conn.disconnected and conn.nowait


def test_cancel_safe_redis_hands_back_cancel_safe_pipeline():
    assert isinstance(rc.CancelSafeRedis(host='127.0.0.1', port=6379).pipeline(), rc.CancelSafePipeline)


def test_aio_connect_defaults_to_cancel_safe_client():
    cls = rc.aio_connect.__defaults__  # (read_only, socket_timeout, redis_class, ...)
    assert rc.CancelSafeRedis in cls


# ---- sentinel plumbing ------------------------------------------------------------------------

def test_plaid_sentinel_aclose_closes_every_sentinel():
    s = rc.PlaidSentinel([('a', 1), ('b', 1)])
    s.sentinels = [mock.AsyncMock(), mock.AsyncMock()]
    asyncio.run(s.aclose())
    assert all(x.aclose.await_count == 1 for x in s.sentinels)


def _pool(is_master=True, master_address=('old', 1)):
    pool = object.__new__(rc.PlaidSentinelConnectionPool)
    pool.service_name = 'svc'
    pool.is_master = is_master
    pool.master_address = master_address
    pool.disconnect = mock.AsyncMock()
    pool.sentinel_manager = SimpleNamespace(discover_master=mock.AsyncMock(return_value=('new', 1)))
    return pool


def test_master_switch_resets_every_connection():
    pool = _pool()
    assert asyncio.run(pool.get_master_address()) == ('new', 1)
    assert pool.master_address == ('new', 1)
    pool.disconnect.assert_awaited_once_with(inuse_connections=True)


def test_unchanged_master_keeps_connections():
    pool = _pool(master_address=('new', 1))
    asyncio.run(pool.get_master_address())
    pool.disconnect.assert_not_awaited()


def test_replica_pool_never_resets():
    pool = _pool(is_master=False)
    asyncio.run(pool.get_master_address())
    pool.disconnect.assert_not_awaited()


@pytest.mark.parametrize('disconnect_fails', [False, True])
def test_pool_acquire_failure_clears_state_and_reraises(disconnect_fails):
    pool = _pool()
    if disconnect_fails:
        pool.disconnect = mock.AsyncMock(side_effect=RuntimeError('x'))
    with mock.patch.object(SentinelConnectionPool, 'get_connection', mock.AsyncMock(side_effect=redis.ConnectionError)), \
            pytest.raises(redis.ConnectionError):
        asyncio.run(pool.get_connection())
    assert pool.master_address is None
    pool.disconnect.assert_awaited_once_with(inuse_connections=True)


def test_pool_acquire_success_passes_through():
    with mock.patch.object(SentinelConnectionPool, 'get_connection', mock.AsyncMock(return_value='conn')):
        assert asyncio.run(_pool().get_connection()) == 'conn'


# ---- headless sentinels (async) ---------------------------------------------------------------

def _async_headless():
    with mock.patch.object(rc, '_resolve_headless_host_with_retry', return_value=['10.0.0.1']):
        return rc.HeadlessSentinel('hl', 26379, sentinel_kwargs={})


def test_headless_sentinel_seeds_from_dns():
    s = _async_headless()
    assert s.headless_host == 'hl' and len(s.sentinels) == 1
    assert s.sentinels_from_headless_host.__func__ is rc.HeadlessSentinel.sentinels_from_headless_host


def test_headless_sentinel_refresh_rebuilds_from_dns():
    s = _async_headless()
    with mock.patch.object(rc, '_resolve_headless_host_with_retry_async', mock.AsyncMock(return_value=['10.0.0.8', '10.0.0.9'])):
        asyncio.run(s.refresh_sentinels())
    assert len(s.sentinels) == 2


def test_headless_discover_master_refreshes_once_then_succeeds():
    s = _async_headless()
    s.refresh_sentinels = mock.AsyncMock()
    with mock.patch.object(aioSentinel, 'discover_master', mock.AsyncMock(side_effect=[redis.ConnectionError, ('m', 1)])):
        assert asyncio.run(s.discover_master('svc')) == ('m', 1)
    s.refresh_sentinels.assert_awaited_once()


def test_headless_discover_master_gives_up_after_one_refresh():
    s = _async_headless()
    s.refresh_sentinels = mock.AsyncMock()
    with mock.patch.object(aioSentinel, 'discover_master', mock.AsyncMock(side_effect=MasterNotFoundError)), \
            pytest.raises(MasterNotFoundError):
        asyncio.run(s.discover_master('svc'))
    s.refresh_sentinels.assert_awaited_once()


def test_headless_discover_slaves_refreshes_once_then_gives_up():
    s = _async_headless()
    s.refresh_sentinels = mock.AsyncMock()
    with mock.patch.object(aioSentinel, 'discover_slaves', mock.AsyncMock(side_effect=[redis.TimeoutError, ['s']])):
        assert asyncio.run(s.discover_slaves('svc')) == ['s']
    with mock.patch.object(aioSentinel, 'discover_slaves', mock.AsyncMock(side_effect=redis.TimeoutError)), \
            pytest.raises(redis.TimeoutError):
        asyncio.run(s.discover_slaves('svc'))


# ---- headless sentinels (sync) ----------------------------------------------------------------

def _sync_headless():
    with mock.patch.object(rc, '_resolve_headless_host_with_retry', return_value=['10.0.0.1']):
        return rc.HeadlessSentinelSync('hl', 26379, sentinel_kwargs={})


def test_sync_headless_refresh_rebuilds_from_dns():
    s = _sync_headless()
    with mock.patch.object(rc, '_resolve_headless_host_with_retry', return_value=['a', 'b']):
        s.refresh_sentinels()
    assert len(s.sentinels) == 2


def test_sync_headless_discover_master_refreshes_once():
    s = _sync_headless()
    s.refresh_sentinels = mock.Mock()
    with mock.patch.object(Sentinel, 'discover_master', mock.Mock(side_effect=[redis.ConnectionError, ('m', 1)])):
        assert s.discover_master('svc') == ('m', 1)
    s.refresh_sentinels.assert_called_once()
    with mock.patch.object(Sentinel, 'discover_master', mock.Mock(side_effect=redis.ConnectionError)), \
            pytest.raises(redis.ConnectionError):
        s.discover_master('svc')


def test_sync_headless_discover_slaves_refreshes_once():
    s = _sync_headless()
    s.refresh_sentinels = mock.Mock()
    with mock.patch.object(Sentinel, 'discover_slaves', mock.Mock(side_effect=[redis.TimeoutError, ['s']])):
        assert s.discover_slaves('svc') == ['s']
    with mock.patch.object(Sentinel, 'discover_slaves', mock.Mock(side_effect=redis.TimeoutError)), \
            pytest.raises(redis.TimeoutError):
        s.discover_slaves('svc')


# ---- connect() --------------------------------------------------------------------------------

def test_connect_cluster_has_no_db():
    with mock.patch.object(rc, 'RedisCluster') as cluster:
        rc.connect('x', config=cfg_for(rinfo(cluster=True)), socket_timeout=9)
    kw = cluster.call_args.kwargs
    assert (kw['host'], kw['port'], kw['password'], kw['socket_timeout']) == ('h', 6379, 'pw', 9)
    assert 'db' not in kw and kw['socket_keepalive'] is True


def test_connect_single_instance_uses_injected_class_and_config_timeout():
    fake = mock.MagicMock()
    result = rc.connect('x', redis_class=fake, decode_responses=True, config=cfg_for(rinfo()))
    kw = fake.call_args.kwargs
    assert (kw['host'], kw['port'], kw['db'], kw['password'], kw['socket_timeout']) == ('h', 6379, 2, 'pw', 5)
    assert kw['decode_responses'] is True and kw['client_name'] == rc.CLIENT_NAME
    assert result is fake.return_value


@pytest.mark.parametrize('read_only', [False, True])
def test_connect_sentinel(read_only):
    info = rinfo(sentinel=True, hosts=[('s1', 26379)], quorum=2)
    with mock.patch.object(rc, 'Sentinel') as sentinel:
        out = rc.connect('x', read_only=read_only, config=cfg_for(info), decode_responses=True)
    method = sentinel.return_value.slave_for if read_only else sentinel.return_value.master_for
    method.assert_called_once()
    assert method.call_args.args == ('svc',) and method.call_args.kwargs['decode_responses'] is True
    assert out is method.return_value
    assert sentinel.call_args.kwargs['min_other_sentinels'] == 2
    # same retry budget and client name as the headless and async paths
    assert sentinel.call_args.kwargs['retry'].get_retries() == 3
    assert sentinel.call_args.kwargs['client_name'] == rc.CLIENT_NAME


@pytest.mark.parametrize('read_only', [False, True])
def test_connect_headless_sentinel(read_only):
    info = rinfo(sentinel=True, headless=True, hosts=[('hl', 26379)])
    with mock.patch.object(rc, 'HeadlessSentinelSync') as sentinel:
        rc.connect('x', read_only=read_only, config=cfg_for(info))
    assert sentinel.call_args.args == ('hl', 26379)
    assert (sentinel.return_value.slave_for if read_only else sentinel.return_value.master_for).called


# ---- aio_connect() ----------------------------------------------------------------------------

def test_aio_connect_cluster_has_no_db_and_retries_on_timeouts():
    with mock.patch.object(rc, 'aioRedisCluster') as cluster:
        rc.aio_connect('x', config=cfg_for(rinfo(cluster=True)))
    kw = cluster.call_args.kwargs
    assert 'db' not in kw
    assert redis.TimeoutError in kw['retry']._supported_errors


def test_aio_connect_single_instance_defaults_to_cancel_safe():
    fake = mock.MagicMock()
    rc.aio_connect('x', redis_class=fake, config=cfg_for(rinfo()))
    kw = fake.call_args.kwargs
    assert (kw['db'], kw['socket_timeout']) == (2, 5)
    client = rc.aio_connect('x', config=cfg_for(rinfo()))
    assert isinstance(client, rc.CancelSafeRedis)


@pytest.mark.parametrize('headless,sentinel_cls', [(False, 'PlaidSentinel'), (True, 'HeadlessSentinel')])
@pytest.mark.parametrize('read_only', [False, True])
def test_aio_connect_sentinel(headless, sentinel_cls, read_only):
    info = rinfo(sentinel=True, headless=headless, hosts=[('s1', 26379)])
    rc.ASYNC_SENTINEL_OBJECTS.clear()
    with mock.patch.object(rc, sentinel_cls) as sentinel:
        out = rc.aio_connect('x', read_only=read_only, config=cfg_for(info))
    mgr = sentinel.return_value
    method = mgr.slave_for if read_only else mgr.master_for
    assert out is method.return_value
    # only the writer path pins the pool that resets connections on master switch
    assert ('connection_pool_class' in method.call_args.kwargs) is (not read_only)
    assert rc.ASYNC_SENTINEL_OBJECTS == [out, mgr]
    rc.ASYNC_SENTINEL_OBJECTS.clear()


def test_aio_connect_sentinel_can_skip_shutdown_registration():
    rc.ASYNC_SENTINEL_OBJECTS.clear()
    with mock.patch.object(rc, 'PlaidSentinel'):
        rc.aio_connect('x', config=cfg_for(rinfo(sentinel=True)), register_for_shutdown=False)
    assert rc.ASYNC_SENTINEL_OBJECTS == []


def test_close_async_sentinel_objects_closes_all_and_survives_failures():
    good, bad = mock.AsyncMock(), mock.AsyncMock()
    bad.aclose.side_effect = RuntimeError('x')
    rc.ASYNC_SENTINEL_OBJECTS[:] = [bad, None, good]
    asyncio.run(rc.close_async_sentinel_objects())
    good.aclose.assert_awaited_once()
    rc.ASYNC_SENTINEL_OBJECTS.clear()
