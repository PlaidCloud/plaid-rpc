#!/usr/bin/env python
# coding=utf-8

from unittest import mock

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from plaidcloud.rpc import telemetry


def test_pod_namespace_reads_downward_api_file():
    m = mock.mock_open(read_data=" pw-tartan \n")
    with mock.patch("builtins.open", m):
        assert telemetry._pod_namespace() == "pw-tartan"


def test_pod_namespace_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("POD_NAMESPACE", "ps-tartan")
    with mock.patch("builtins.open", side_effect=FileNotFoundError):
        assert telemetry._pod_namespace() == "ps-tartan"


def test_org_id_production_namespace(monkeypatch):
    monkeypatch.setattr(telemetry, "_pod_namespace", lambda: "pw-tartan")
    assert telemetry.telemetry_org_id() == "pw-tartan"


def test_org_id_sandbox_namespace(monkeypatch):
    monkeypatch.setattr(telemetry, "_pod_namespace", lambda: "ps-tartan")
    assert telemetry.telemetry_org_id() == "ps-tartan"


def test_org_id_non_tenant_is_admins(monkeypatch):
    monkeypatch.setattr(telemetry, "_pod_namespace", lambda: "cluster-components")
    assert telemetry.telemetry_org_id() == "admins"


def test_tracing_enabled_flag(monkeypatch):
    monkeypatch.setenv("PLAID_TRACING_ENABLED", "TRUE")
    assert telemetry.tracing_enabled() is True
    monkeypatch.setenv("PLAID_TRACING_ENABLED", "false")
    assert telemetry.tracing_enabled() is False


def test_init_tracing_noop_when_disabled(monkeypatch):
    monkeypatch.setenv("PLAID_TRACING_ENABLED", "false")
    assert telemetry.init_tracing("svc") is False


def test_init_tracing_installs_provider(monkeypatch):
    monkeypatch.setenv("PLAID_TRACING_ENABLED", "true")
    monkeypatch.setenv("POD_NAMESPACE", "pw-tartan")
    monkeypatch.setenv("PLAID_TRACING_SAMPLE_RATIO", "1.0")
    with mock.patch(
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter"
    ) as exporter, mock.patch(
        "opentelemetry.sdk.trace.export.BatchSpanProcessor"
    ), mock.patch("opentelemetry.trace.set_tracer_provider") as set_provider:
        assert telemetry.init_tracing("plaid-rpc-test") is True
        # org-id flows into the exporter as lowercase gRPC metadata
        _, kwargs = exporter.call_args
        assert kwargs["headers"] == (("x-scope-orgid", "pw-tartan"),)
        assert set_provider.called


def test_inject_is_noop_without_active_span():
    carrier = {}
    telemetry.inject_trace_context(carrier)
    assert "traceparent" not in carrier


def test_inject_writes_traceparent_under_active_span():
    provider = TracerProvider(sampler=ALWAYS_ON)
    tracer = provider.get_tracer("test")
    carrier = {}
    with trace.use_span(tracer.start_span("s"), end_on_exit=True):
        telemetry.inject_trace_context(carrier)
    assert "traceparent" in carrier


def test_shutdown_flushes_provider():
    provider = mock.Mock()
    with mock.patch("opentelemetry.trace.get_tracer_provider", return_value=provider):
        telemetry.shutdown_tracing()
    provider.shutdown.assert_called_once()


def test_shutdown_tolerates_provider_without_shutdown():
    class _NoShutdown:
        pass
    with mock.patch("opentelemetry.trace.get_tracer_provider", return_value=_NoShutdown()):
        telemetry.shutdown_tracing()   # must not raise
