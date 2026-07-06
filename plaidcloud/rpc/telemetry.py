# coding=utf-8
"""OpenTelemetry tracing bootstrap for PlaidCloud services.

Single-tenant per process: each RPC pod serves one tenant, so a static
``X-Scope-OrgID`` (the pod's own ``pw-<tenant>``/``ps-<tenant>`` namespace) is set
once at startup and rides every span export. Inert unless ``PLAID_TRACING_ENABLED``
is true — importing this module has no effect until ``init_tracing`` is called.
"""

import os

_DOWNWARD_API_NAMESPACE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
_DEFAULT_SAMPLE_RATIO = 0.05

_initialized = False


def _pod_namespace():
    """The pod's own K8s namespace, from the downward-API file then POD_NAMESPACE env.
    Any read failure falls back to the env var — this runs during startup and must not
    crash the process."""
    try:
        with open(_DOWNWARD_API_NAMESPACE) as ns:
            return ns.read().strip()
    except OSError:
        return os.environ.get("POD_NAMESPACE", "").strip()


def telemetry_org_id():
    """The Tempo/Loki multitenancy org-id for this process.

    Equals the pod namespace (``pw-<tenant>`` / ``ps-<tenant>``) — exactly what the
    per-tenant Grafana datasource reads and what Promtail tags logs with, so traces,
    logs, and reads share one org by construction. Non-tenant (control-plane/infra)
    processes fall back to ``admins``.
    """
    ns = _pod_namespace()
    return ns if ns.startswith(("pw-", "ps-")) else "admins"


def tracing_enabled():
    return os.environ.get("PLAID_TRACING_ENABLED", "false").lower() == "true"


def _sample_ratio():
    """Sampling ratio from PLAID_TRACING_SAMPLE_RATIO, clamped to [0, 1]; a malformed
    value falls back to the default rather than crashing the service at startup."""
    try:
        ratio = float(os.environ.get("PLAID_TRACING_SAMPLE_RATIO", _DEFAULT_SAMPLE_RATIO))
    except ValueError:
        return _DEFAULT_SAMPLE_RATIO
    return min(1.0, max(0.0, ratio))


def init_tracing(service_name, org_id=None):
    """Install a global TracerProvider exporting to Tempo over OTLP gRPC.

    Call once per process at startup. Returns True if tracing is active (this call or a
    prior one installed the provider), False when disabled. Idempotent: a second call is a
    no-op — OTel's provider is process-wide set-once, so re-installing would silently fail.
    ``org_id`` overrides the resolved org-id — pass it when the caller derives the tenant
    differently (e.g. cp-rest), otherwise ``telemetry_org_id()`` is used.
    """
    # Called once at process startup before request handling, so the check-then-set on
    # _initialized is not guarded by a lock; the OTel provider's own set-once guard is the
    # backstop if that assumption is ever violated.
    global _initialized
    if not tracing_enabled():
        return False
    if _initialized:
        return True

    # Imported lazily so the SDK is not a hard import cost for every plaidcloud.rpc user.
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    org = org_id or telemetry_org_id()
    endpoint = os.environ.get(
        "PLAID_TRACING_OTLP_ENDPOINT", "tempo-distributor.cluster-components.svc:4317"
    )
    ratio = _sample_ratio()

    provider = TracerProvider(
        resource=Resource.create({
            "service.name": service_name,
            "service.namespace": org,
            "tenant.id": org,
        }),
        sampler=ParentBased(TraceIdRatioBased(ratio)),
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(
        endpoint=endpoint,
        insecure=True,                          # in-cluster, no TLS
        headers=(("x-scope-orgid", org),),      # gRPC metadata is lowercase
    )))
    trace.set_tracer_provider(provider)
    _initialized = True
    return True


def inject_trace_context(carrier):
    """Inject W3C traceparent/tracestate into a header dict. Zero-cost (returns before
    importing/using OTel) unless init_tracing has installed a provider, so it is safe to
    call unconditionally on every outbound RPC even when tracing is disabled."""
    if not _initialized:
        return
    from opentelemetry import propagate

    propagate.inject(carrier)


def shutdown_tracing():
    """Flush and shut down the tracer provider — required in short-lived processes
    (e.g. workflow-runner K8s Jobs) so the BatchSpanProcessor exports before exit."""
    from opentelemetry import trace

    provider = trace.get_tracer_provider()
    shutdown = getattr(provider, "shutdown", None)
    if shutdown is not None:
        shutdown()
