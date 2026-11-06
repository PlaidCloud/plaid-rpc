#!/usr/bin/env python
"""Redis connectors shared by the platform services (plaid, workflow-runner, cp-rest).

Requires the ``redis`` extra (``pip install plaidcloud-rpc[redis]``); nothing else in
``plaidcloud.rpc`` imports this module.
"""

import asyncio
import logging
import socket
import time
from typing import TypeVar

import redis
from redis import asyncio as aioredis
from redis.asyncio.cluster import RedisCluster as aioRedisCluster
from redis.asyncio.retry import Retry as aioRetry
from redis.asyncio.sentinel import MasterNotFoundError as aioMasterNotFoundError
from redis.asyncio.sentinel import Sentinel as aioSentinel
from redis.asyncio.sentinel import SentinelConnectionPool
from redis.backoff import FullJitterBackoff, NoBackoff
from redis.cluster import RedisCluster
from redis.retry import Retry
from redis.sentinel import Sentinel

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2019-2023, PlaidCloud, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Apache-2.0"
__maintainer__ = "Paul Morel"
__email__ = "paul.morel@tartansolutions.com"


R = TypeVar('R', bound=redis.Redis)
CLIENT_NAME = socket.gethostname().rsplit('-', 2)[0]
DEFAULT_RETRY_ERRORS = (TimeoutError, socket.timeout, redis.TimeoutError, redis.ConnectionError)  # Needed for async version to add socket timeout
DEFAULT_HEALTH_CHECK = 30
# Sentinel master/slave retry count: 3 retries (= 4 total attempts) is
# enough to ride out the typical 5–15 s Redis Sentinel failover window
# with our 1 s socket_timeout default. The retry layer disconnects the
# connection on each TimeoutError/ConnectionError, so the next attempt
# forces sentinel re-discovery via
# SentinelManagedConnection._connect_retry → pool.get_master_address.
# With only 1 retry (2 attempts × 1 s ≈ 2 s), a request landing in the
# no-master window would surface a hard failure to the caller instead of
# transparently recovering. Applies only to the master/slave path —
# cluster, single-instance, and sentinel-to-sentinel kwargs keep their
# existing tighter budgets.
SENTINEL_RETRIES = 3
# TCP keepalive: catch a dead peer (e.g. a Sentinel master pod that vanished
# during failover) in ~90s instead of the Linux default of ~2 hours.
# 60s idle → start probing, 10s between probes, give up after 3 misses.
# getattr() makes this safe to import on non-Linux dev machines.
SOCKET_KEEPALIVE_OPTS = {
    opt: val
    for opt, val in (
        (getattr(socket, 'TCP_KEEPIDLE', None), 60),
        (getattr(socket, 'TCP_KEEPINTVL', None), 10),
        (getattr(socket, 'TCP_KEEPCNT', None), 3),
    )
    if opt is not None
}
KEEPALIVE_KWARGS = {
    'socket_keepalive': True,
    'socket_keepalive_options': SOCKET_KEEPALIVE_OPTS,
}
ASYNC_SENTINEL_OBJECTS = []
LOGGER = logging.getLogger(__name__)


def _redis_info(connection_name, config=None):
    """Connection details for ``connection_name``. ``config`` (anything with ``.redis.get_url``)
    defaults to the process-wide plaidcloud-config; tests inject a fake."""
    if config is None:
        from plaidcloud.config import config
    return config.redis.get_url(connection_name)


def _resolve_headless_host_with_retry(headless_host: str) -> list[str]:
    """Resolve a headless service to its sentinel IPs, retrying briefly on
    transient DNS failures.

    Without retry, a momentary CoreDNS hiccup during a `multiprocessing.spawn`
    re-import (uvicorn reload → fresh worker → import object_tools →
    module-level aio_connect → this lookup) crashes the whole import. The
    pod cycles through reload windows during which every in-flight RPC
    times out client-side. Three tries with short backoff is enough to
    ride out the spawn-time DNS race; a genuinely down DNS still surfaces
    after ~600ms total wait.

    Sync variant — used at construction/import time before the event loop
    is running. For runtime refresh from async sentinel code, use
    ``_resolve_headless_host_with_retry_async`` so the loop is not blocked
    by DNS or the backoff sleep.
    """
    last_error = None
    for attempt in range(3):
        try:
            _host, _aliases, ip_list = socket.gethostbyname_ex(headless_host)
            return ip_list
        except socket.gaierror as e:
            last_error = e
            if attempt < 2:
                LOGGER.warning(
                    "REDIS - DNS lookup for sentinel host %r failed (attempt %d/3): %s",
                    headless_host, attempt + 1, e,
                )
                time.sleep(0.2 * (attempt + 1))
    raise last_error


async def _resolve_headless_host_with_retry_async(headless_host: str) -> list[str]:
    """Async variant of ``_resolve_headless_host_with_retry``: offloads the
    blocking DNS lookup to a thread and uses ``asyncio.sleep`` for backoff
    so the event loop is never blocked during sentinel failover refresh.
    """
    last_error = None
    for attempt in range(3):
        try:
            _host, _aliases, ip_list = await asyncio.to_thread(socket.gethostbyname_ex, headless_host)
            return ip_list
        except socket.gaierror as e:
            last_error = e
            if attempt < 2:
                LOGGER.warning(
                    "REDIS - DNS lookup for sentinel host %r failed (attempt %d/3): %s",
                    headless_host, attempt + 1, e,
                )
                await asyncio.sleep(0.2 * (attempt + 1))
    raise last_error


async def drop_connection_on_cancel(connection):
    """Disconnect a connection whose in-flight command was cancelled, so the
    pool never hands it back out carrying a pending, unread response (sc-23199).

    ``disconnect(nowait=True)`` nulls the reader/writer synchronously, so the
    subsequent ``release`` returns a not-connected connection and the next
    acquire reconnects clean instead of reading the previous command's reply.
    A disconnect failure must never mask the ``CancelledError`` the caller then
    re-raises — same idiom as ``PlaidSentinelConnectionPool.get_connection``.
    """
    try:
        await connection.disconnect(nowait=True)
    except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
        LOGGER.debug('REDIS - disconnect on cancel path raised: %r', e)


class _CancelSafeGuardMixin:
    """Shared cancel-safety for the ``execute_command`` / immediate-command path.

    redis-py's ``_send_command_parse_response`` does ``send_command`` then
    ``read_response`` as two separate awaits. ``send_command`` and
    ``read_response`` each disconnect the connection if a ``BaseException``
    (including ``CancelledError``) is raised *inside* them, but a task cancelled
    at the await boundary *between* the two — command already on the wire,
    response not yet read — unwinds through ``execute_command``'s ``finally``,
    which releases the connection **without** disconnecting it (no error flag is
    set). That connection carries a pending, unread response back into the pool.
    The pool's only guard, ``ensure_connection`` → ``can_read_destructive()``, is
    racy: if the response bytes haven't landed at the instant it polls, the guard
    passes and the next caller reads the *previous* command's response — one reply
    out of phase — until the connection eventually errors or is health-checked.
    Under MCP concurrency (FastMCP/Starlette cancels the request task on client
    disconnect/timeout) this surfaced as ``*_find`` handing back another project's
    objects and count under the requested project's id (sc-23199).

    Mixed into both :class:`CancelSafeRedis` (plain clients) and
    :class:`CancelSafePipeline` — the pipeline's WATCH / pre-``multi()`` immediate
    commands (``pipe.watch``/``pipe.hget`` before a transaction, as in
    ``object_tools._mutate_path_leaves`` on the shared cache) route through this
    same method, so the pipeline needs the guard here as well as on its
    ``_execute_*`` batch path.
    """

    async def _send_command_parse_response(self, conn, command_name, *args, **options):
        try:
            return await super()._send_command_parse_response(conn, command_name, *args, **options)
        except asyncio.CancelledError:
            await drop_connection_on_cancel(conn)
            raise


class CancelSafeRedis(_CancelSafeGuardMixin, aioredis.Redis):
    """Async Redis client that never returns a mid-command connection to the pool
    (sc-23199). Guards the ``execute_command`` path via
    :class:`_CancelSafeGuardMixin` and hands back a :class:`CancelSafePipeline`
    from ``pipeline()`` so the pipeline paths are covered too.
    """

    def pipeline(self, transaction=True, shard_hint=None):
        return CancelSafePipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)


class CancelSafePipeline(_CancelSafeGuardMixin, aioredis.client.Pipeline):
    """Pipeline counterpart of :class:`CancelSafeRedis` (sc-23199).

    A pipeline has two ways to reach the wire, both with the same send/read gap:

    * **Immediate / WATCH commands** (``pipe.watch``, and any command issued while
      ``watching`` before ``pipe.multi()``) go through ``immediate_execute_command``
      → ``_send_command_parse_response`` — covered by :class:`_CancelSafeGuardMixin`.
    * **Batched MULTI/EXEC** runs through ``_execute_transaction`` /
      ``_execute_pipeline``, which call ``send_packed_command`` then
      ``parse_response`` directly; ``execute()``'s ``finally: reset()`` then
      releases the connection **without** disconnecting on the non-watching cancel
      path (the retry layer reacts only to ``ConnectionError``/``TimeoutError``,
      not ``CancelledError``). The overrides below disconnect inside the execute
      methods, before ``reset`` releases the connection.

    ``dimension.AsyncPipeline`` subclasses this to inherit both guards and adds the
    same guard to its custom ``_execute_transaction``.
    """

    async def _execute_transaction(self, connection, commands, raise_on_error):
        try:
            return await super()._execute_transaction(connection, commands, raise_on_error)
        except asyncio.CancelledError:
            await drop_connection_on_cancel(connection)
            raise

    async def _execute_pipeline(self, connection, commands, raise_on_error):
        try:
            return await super()._execute_pipeline(connection, commands, raise_on_error)
        except asyncio.CancelledError:
            await drop_connection_on_cancel(connection)
            raise


class PlaidSentinel(aioSentinel):
    async def aclose(self):
        for s in self.sentinels:
            await s.aclose()


class PlaidSentinelConnectionPool(SentinelConnectionPool):

    async def get_master_address(self):
        """Overridden to apply logging and to close all connections on master switch"""
        LOGGER.debug('REDIS - Get Master Address')
        master_address = await self.sentinel_manager.discover_master(self.service_name)
        if self.is_master:
            LOGGER.debug(f'REDIS - Old Master {self.master_address}, New Master {master_address}')
            if self.master_address != master_address:
                LOGGER.debug('REDIS - Resetting Connections')
                self.master_address = master_address
                # disconnect all connections, the master has changed, make them connect again
                await self.disconnect(inuse_connections=True)
        return master_address

    async def get_connection(self, *args, **options):
        """Defence-in-depth on the rare path where the pool itself (rather
        than a returned connection) raises while acquiring.

        In redis-py 7.x the heavy lifting of sentinel master discovery
        happens lazily inside ``SentinelManagedConnection._connect_retry``,
        not in the pool's ``get_connection``. The parent's
        ``get_connection`` mostly just returns a ``Connection`` from the
        free list or builds one via ``make_connection``; the connection
        then resolves master and reconnects on first use. The bulk of
        sentinel-failover recovery therefore lives at the connection
        layer (``read_response`` disconnect on error → retry → next use
        calls ``_connect_retry`` → ``get_master_address`` → fresh
        sentinel query), not here.

        This override catches the narrow case where the failure surfaces
        at the pool level (pool exhaustion timing out, ``make_connection``
        failing, sentinel quorum lost while we were waiting). On that
        path we forget the cached master_address and drop pool
        connections so the retry layer's next acquire starts from a
        fresh sentinel discovery instead of with stale pool state.

        Pass-through ``*args/**options`` keeps this signature-agnostic
        across redis-py versions whose parent
        ``SentinelConnectionPool.get_connection`` signature has shifted.
        """
        try:
            return await super().get_connection(*args, **options)
        except (TimeoutError, redis.ConnectionError, redis.TimeoutError, asyncio.TimeoutError, aioMasterNotFoundError, OSError):
            LOGGER.warning('REDIS - Connection acquire failed; clearing master_address and pool')
            self.master_address = None
            try:
                await self.disconnect(inuse_connections=True)
            except Exception as e:  # noqa: BLE001
                # Don't mask the original error with a disconnect failure,
                # but also don't swallow it silently.
                LOGGER.debug(f'REDIS - disconnect on error path raised: {e!r}')
            raise


class HeadlessSentinel(PlaidSentinel):
    def __init__(
        self,
        headless_host, port,
        min_other_sentinels=0,
        sentinel_kwargs=None,
        **connection_kwargs,
    ):
        self.headless_host = headless_host
        self.port = port
        super().__init__(
            sentinels=[(ip, port) for ip in self.sentinels_from_headless_host()],
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )

    def sentinels_from_headless_host(self) -> list[str]:
        return _resolve_headless_host_with_retry(self.headless_host)

    async def refresh_sentinels(self):
        LOGGER.debug("REDIS - Refreshing Sentinels")
        ips = await _resolve_headless_host_with_retry_async(self.headless_host)
        self.sentinels = [
            redis.asyncio.Redis(host=ip, port=self.port, **self.sentinel_kwargs)
            for ip in ips
        ]

    async def discover_master(self, service_name, has_refreshed: bool = False):
        LOGGER.debug("REDIS - Discover Master")
        try:
            return await super().discover_master(service_name)
        except DEFAULT_RETRY_ERRORS + (aioMasterNotFoundError,):
            LOGGER.exception("REDIS - Discover Master Exception")
            if not has_refreshed:
                await self.refresh_sentinels()
                return await self.discover_master(service_name, True)
            raise

    async def discover_slaves(self, service_name, has_refreshed: bool = False):
        LOGGER.debug("REDIS - Discover Slaves")
        try:
            return await super().discover_slaves(service_name)
        except DEFAULT_RETRY_ERRORS:
            if not has_refreshed:
                await self.refresh_sentinels()
                return await self.discover_slaves(service_name, True)
            raise


class HeadlessSentinelSync(Sentinel):
    def __init__(
        self,
        headless_host, port,
        min_other_sentinels=0,
        sentinel_kwargs=None,
        **connection_kwargs,
    ):
        self.headless_host = headless_host
        self.port = port
        super().__init__(
            sentinels=[(ip, port) for ip in self.sentinels_from_headless_host()],
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )

    def sentinels_from_headless_host(self) -> list[str]:
        return _resolve_headless_host_with_retry(self.headless_host)

    def refresh_sentinels(self):
        self.sentinels = [
            redis.Redis(host=ip, port=self.port, **self.sentinel_kwargs)
            for ip in self.sentinels_from_headless_host()
        ]

    def discover_master(self, service_name, has_refreshed: bool = False):
        try:
            return super().discover_master(service_name)
        except DEFAULT_RETRY_ERRORS:
            if not has_refreshed:
                self.refresh_sentinels()
                return self.discover_master(service_name, True)
            raise

    def discover_slaves(self, service_name, has_refreshed: bool = False):
        try:
            return super().discover_slaves(service_name)
        except DEFAULT_RETRY_ERRORS:
            if not has_refreshed:
                self.refresh_sentinels()
                return self.discover_slaves(service_name, True)
            raise


def connect(connection_name: str, read_only: bool = False, socket_timeout: float | None = None,
            redis_class: type[R] = redis.Redis, decode_responses: bool = False, config=None) -> R | RedisCluster:
    """Returns a synchronous redis-py connection

    Args:
        connection_name (str): The Redis connection identifier
        read_only (bool): If true, the connection will be made to the master node, else to a slave node
        socket_timeout (float): Socket timeout threshold
        redis_class (Type[redis.Redis]):
        decode_responses (bool): Redis connection kwarg for whether or not to decode_responses
        config: Object with ``.redis.get_url(name)``; defaults to plaidcloud-config

    Returns:
        R: Redis connection
    """
    rinfo = _redis_info(connection_name, config)
    if rinfo.cluster:
        host, port = rinfo.hosts[0]
        return RedisCluster(
            host=host,
            port=port,
            # client params (no db: cluster mode has a single keyspace)
            password=rinfo.password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=DEFAULT_HEALTH_CHECK,
            retry=Retry(FullJitterBackoff(), 1),
            client_name=CLIENT_NAME,
            **KEEPALIVE_KWARGS,
        )

    elif rinfo.sentinel:  # We're connecting to a sentinel cluster.
        if rinfo.headless:
            # establish how many retries would be required to check all IPs of a headless service
            sentinel_connection = HeadlessSentinelSync(
                rinfo.hosts[0][0],
                rinfo.hosts[0][1],
                min_other_sentinels=rinfo.quorum or 0,
                sentinel_kwargs={
                    'retry': redis.retry.Retry(NoBackoff(), 3),
                    'socket_timeout': 0.5,  # short, for fast sentinel failover
                    'health_check_interval': DEFAULT_HEALTH_CHECK,
                    'client_name': CLIENT_NAME,
                    **KEEPALIVE_KWARGS,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=DEFAULT_HEALTH_CHECK,
                retry=redis.retry.Retry(FullJitterBackoff(), SENTINEL_RETRIES),
                client_name=CLIENT_NAME,
                **KEEPALIVE_KWARGS,
            )
        else:
            sentinel_connection = Sentinel(
                rinfo.hosts,
                min_other_sentinels=rinfo.quorum or 0,
                sentinel_kwargs={
                    'retry': Retry(NoBackoff(), 3),
                    'socket_timeout': 0.5,  # short, for fast sentinel failover
                    'health_check_interval': DEFAULT_HEALTH_CHECK,
                    'client_name': CLIENT_NAME,
                    **KEEPALIVE_KWARGS,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=DEFAULT_HEALTH_CHECK,
                retry=Retry(FullJitterBackoff(), SENTINEL_RETRIES),
                client_name=CLIENT_NAME,
                **KEEPALIVE_KWARGS,
            )
        if read_only:
            return sentinel_connection.slave_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
                client_name=CLIENT_NAME,
                **KEEPALIVE_KWARGS,
            )
        else:
            return sentinel_connection.master_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
                client_name=CLIENT_NAME,
                **KEEPALIVE_KWARGS,
            )
    else:  # Single redis instance
        host, port = rinfo.hosts[0]
        return redis_class(
            host=host,
            port=port,
            db=rinfo.database,
            password=rinfo.password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=DEFAULT_HEALTH_CHECK,
            retry=Retry(FullJitterBackoff(), 1),
            client_name=CLIENT_NAME,
            **KEEPALIVE_KWARGS,
        )


def aio_connect(connection_name: str, read_only: bool = False, socket_timeout: float | None = None,
                redis_class: type[aioredis.Redis] = CancelSafeRedis, decode_responses: bool = False,
                register_for_shutdown: bool = True, config=None):
    """Returns an async redis connection

    Args:
        connection_name (str): The Redis connection identifier
        read_only (bool): If true, the connection will be made to the master node, else to a slave node
        socket_timeout (float): Socket timeout threshold
        redis_class (Type[aioredis.Redis]): Defaults to CancelSafeRedis so a
            task cancelled mid-command can't return a desynced connection to
            the pool (sc-23199). Callers passing a custom class should subclass
            CancelSafeRedis to keep that protection.
        decode_responses (bool): Redis connection kwarg for whether or not to decode_responses
        register_for_shutdown (bool): Sentinel path only. If True (default,
            for backward compatibility) the returned client and its
            sentinel manager are appended to `ASYNC_SENTINEL_OBJECTS`
            so `close_async_sentinel_objects()` can clean them up at
            process exit. Long-lived singletons (e.g. events.py
            BasePublisher, layout_tools module-level cache) should
            keep this True — they're created once per process and
            need exit-time cleanup. **Callers that own their own
            connection lifecycle and call `aio_connect` per request
            (e.g. allocation_trace_cache._connection
            asynccontextmanager) MUST pass False** — otherwise every
            call leaks a pair of entries into the global list, which
            grows unbounded over the lifetime of a long-running
            web/MCP worker. The opt-in registration removes the
            unbounded-growth foot-gun without disturbing existing
            callers.

    Returns:
        redis.asyncio.client.Redis: Redis connection
    """
    rinfo = _redis_info(connection_name, config)
    if rinfo.cluster:
        # NOT cancel-safe: this takes no redis_class, so a native-cluster client
        # gets none of CancelSafeRedis's guard and can hand back a connection
        # carrying an unread response. Inert while tenants are sentinel-managed,
        # which is exactly why it would be missed if one ever is not.
        host, port = rinfo.hosts[0]
        cluster_retry = aioRetry(FullJitterBackoff(), 1)
        cluster_retry.update_supported_errors(list(DEFAULT_RETRY_ERRORS))   # can't pass retry_on_timeout to async cluster
        return aioRedisCluster(
            host=host,
            port=port,
            # client params (no db: cluster mode has a single keyspace; async RedisCluster rejects a non-zero db)
            password=rinfo.password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=DEFAULT_HEALTH_CHECK,
            retry=cluster_retry,
            client_name=CLIENT_NAME,
            **KEEPALIVE_KWARGS,
        )
    elif rinfo.sentinel:  # We're connecting to a sentinel cluster.
        if rinfo.headless:
            sentinel_connection = HeadlessSentinel(
                rinfo.hosts[0][0],
                rinfo.hosts[0][1],
                min_other_sentinels=rinfo.quorum or 0,
                sentinel_kwargs={
                    'retry': aioRetry(NoBackoff(), 3),
                    'socket_timeout': 0.5,  # short, for fast sentinel failover
                    'health_check_interval': DEFAULT_HEALTH_CHECK,
                    'client_name': CLIENT_NAME,
                    **KEEPALIVE_KWARGS,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=DEFAULT_HEALTH_CHECK,
                retry=aioRetry(FullJitterBackoff(), SENTINEL_RETRIES),
                client_name=CLIENT_NAME,
                **KEEPALIVE_KWARGS,
            )
        else:
            sentinel_connection = PlaidSentinel(
                rinfo.hosts,
                min_other_sentinels=rinfo.quorum or 0,
                sentinel_kwargs={
                    'retry': aioRetry(NoBackoff(), 3),
                    'socket_timeout': 0.5,  # short, for fast sentinel failover
                    'health_check_interval': DEFAULT_HEALTH_CHECK,
                    'client_name': CLIENT_NAME,
                    **KEEPALIVE_KWARGS,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=DEFAULT_HEALTH_CHECK,
                retry=aioRetry(FullJitterBackoff(), SENTINEL_RETRIES),
                client_name=CLIENT_NAME,
                **KEEPALIVE_KWARGS,
            )
        if read_only:
            sentinel_redis = sentinel_connection.slave_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
                client_name=CLIENT_NAME,
                **KEEPALIVE_KWARGS,
            )
        else:
            sentinel_redis = sentinel_connection.master_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
                client_name=CLIENT_NAME,
                connection_pool_class=PlaidSentinelConnectionPool,
                **KEEPALIVE_KWARGS,
            )
        if register_for_shutdown:
            ASYNC_SENTINEL_OBJECTS.extend([sentinel_redis, sentinel_connection])
        return sentinel_redis
    else:  # Single redis instance
        host, port = rinfo.hosts[0]
        return redis_class(
            host=host,
            port=port,
            db=rinfo.database,
            password=rinfo.password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=DEFAULT_HEALTH_CHECK,
            retry=aioRetry(FullJitterBackoff(), 1),
            client_name=CLIENT_NAME,
            **KEEPALIVE_KWARGS,
        )


async def close_async_sentinel_objects():
    for sentinel_obj in ASYNC_SENTINEL_OBJECTS:
        if sentinel_obj:
            try:
                await sentinel_obj.aclose()
            except Exception:  # noqa: BLE001, S110 - best-effort shutdown
                pass
