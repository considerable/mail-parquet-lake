import json

import pytest
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from sync import telemetry as module


def test_telemetry_disabled_by_default(monkeypatch):
    monkeypatch.delenv("LAKE_TELEMETRY_ENABLED", raising=False)
    telemetry = module.Telemetry()
    telemetry.configure("test")
    with telemetry.operation("noop"):
        telemetry.count("lake_runs_started", 1)
        telemetry.event("noop")
    assert not telemetry.enabled


def test_error_spans_and_logs_exclude_private_exception_text(monkeypatch):
    logs = InMemoryLogRecordExporter()
    spans = InMemorySpanExporter()
    reader = InMemoryMetricReader()
    monkeypatch.setenv("LAKE_TELEMETRY_ENABLED", "1")
    monkeypatch.setenv("LAKE_METRICS_ENABLED", "1")
    monkeypatch.setenv("LAKE_TRACES_ENABLED", "1")
    monkeypatch.setattr(module, "OTLPLogExporter", lambda **kwargs: logs)
    monkeypatch.setattr(module, "OTLPSpanExporter", lambda **kwargs: spans)
    monkeypatch.setattr(module, "OTLPMetricExporter", lambda **kwargs: object())
    monkeypatch.setattr(
        module, "PeriodicExportingMetricReader", lambda *a, **kw: reader
    )
    monkeypatch.setattr(module.trace, "set_tracer_provider", lambda provider: None)
    telemetry = module.Telemetry()
    telemetry.configure("attachment-backfill")
    secret = "private@example.com / secret-filename.pdf"
    with pytest.raises(ValueError), telemetry.operation("backfill.commit_page"):
        raise ValueError(secret)
    telemetry.count("lake_messages_scanned", 100)
    telemetry.logs.force_flush()
    telemetry.traces.force_flush()
    exported_spans = spans.get_finished_spans()
    assert len(exported_spans) == 1
    assert exported_spans[0].status.status_code.name == "ERROR"
    assert exported_spans[0].attributes["error.type"] == "ValueError"
    assert not exported_spans[0].events
    exported_logs = logs.get_finished_logs()
    assert any(record.log_record.body == "operation_failed" for record in exported_logs)
    assert secret not in repr(exported_logs) + repr(exported_spans)
    data = reader.get_metrics_data()
    names = {
        m.name
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
    }
    assert {
        "lake_messages_scanned",
        "lake_operation_failures",
        "lake_operation_duration_seconds",
    } <= names
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                for point in metric.data.data_points:
                    assert "github_run_id" not in point.attributes
    telemetry.close()


def test_collector_and_iam_are_scoped_to_selected_log_group():
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[1]
    config = yaml.safe_load((root / "observability/collector.yaml").read_text())
    assert set(config["service"]["pipelines"]) == {"logs"}
    assert set(config["exporters"]) == {"otlp_http/logs"}
    policy = json.loads((root / "observability/iam-policy.json").read_text())
    logs = policy["Statement"][0]
    assert logs["Action"] == [
        "logs:CreateLogStream",
        "logs:DescribeLogStreams",
        "logs:PutLogEvents",
    ]
    assert ":log-group:/mail-parquet-lake/otel:" in logs["Resource"]


def test_low_cost_default_exports_logs_but_never_creates_metric_or_trace_exporters(
    monkeypatch,
):
    from unittest.mock import MagicMock

    logs = InMemoryLogRecordExporter()
    monkeypatch.setenv("LAKE_TELEMETRY_ENABLED", "1")
    monkeypatch.delenv("LAKE_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("LAKE_TRACES_ENABLED", raising=False)
    metric_export, trace_export = MagicMock(), MagicMock()
    monkeypatch.setattr(module, "OTLPLogExporter", lambda **kwargs: logs)
    monkeypatch.setattr(module, "OTLPMetricExporter", metric_export)
    monkeypatch.setattr(module, "OTLPSpanExporter", trace_export)
    telemetry = module.Telemetry()
    telemetry.configure("incremental")
    telemetry.count("lake_messages_scanned", 10)
    with telemetry.operation("sync.run"):
        telemetry.event("test_event")
    telemetry.logs.force_flush()
    assert any(r.log_record.body == "test_event" for r in logs.get_finished_logs())
    assert telemetry.metrics is None and telemetry.traces is None
    metric_export.assert_not_called()
    trace_export.assert_not_called()
    telemetry.close()
