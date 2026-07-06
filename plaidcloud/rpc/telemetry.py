# coding=utf-8
"""OpenTelemetry tracing bootstrap for PlaidCloud services.

Single-tenant per process: each RPC pod serves one tenant, so a static
``X-Scope-OrgID`` (the pod's own ``pw-<tenant>``/``ps-<tenant>`` namespace) is set
once at startup and rides every span export. Inert unless ``PLAID_TRACING_ENABLED``
is true — importing this module has no effect until ``init_tracing`` is called.
"""

import os

_DOWNWARD_API_NAMESPACE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def _pod_namespace():
    """The pod's own K8s namespace, from the downward-API file then POD_NAMESPACE env."""
    try:
        with open(_DOWNWARD_API_NAMESPACE) as ns:
            return ns.read().strip()
    except (FileNotFoundError, PermissionError):
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


def init_tracing(service_name, org_id=None):
    """Install a global TracerProvider exporting to Tempo over OTLP gRPC.

    Call once per process at startup. No-op (returns False) when tracing is disabled.
    ``org_id`` overrides the resolved org-id — pass it when the caller derives the
    tenant differently (e.g. cp-rest), otherwise ``telemetry_org_id()`` is used.
    """
    if not tracing_enabled():
        return False

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
    ratio = float(os.environ.get("PLAID_TRACING_SAMPLE_RATIO", "0.05"))

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
    return True


def inject_trace_context(carrier):
    """Inject W3C traceparent/tracestate into a header dict. No-op with no active span,
    so it is safe to call unconditionally on every outbound RPC."""
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
