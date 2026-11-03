"""Explicit, content-free OpenTelemetry for the lake's batch jobs."""

import logging
import os
import time
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator
from opentelemetry.sdk.metrics import Counter, Histogram, MeterProvider
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import StatusCode


class Telemetry:
    def __init__(self):
        self.enabled = False
        self.mode = "unknown"

    def configure(self, mode):
        if os.getenv("LAKE_TELEMETRY_ENABLED") != "1":
            return
        self.mode = mode
        resource = Resource.create(
            {
                "service.name": "mail-parquet-lake",
                "service.version": os.getenv("GITHUB_SHA", "local"),
                "deployment.environment.name": "github-actions"
                if os.getenv("GITHUB_ACTIONS")
                else "local",
            }
        )
        endpoint = os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"
        ).rstrip("/")
        self.traces = TracerProvider(
            resource=resource, id_generator=AwsXRayIdGenerator()
        )
        self.traces.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=endpoint + "/v1/traces", timeout=5),
                max_queue_size=512,
                max_export_batch_size=64,
            )
        )
        # Set the context provider so the log bridge includes the active trace ID.
        trace.set_tracer_provider(self.traces)
        self.tracer = self.traces.get_tracer("mail-parquet-lake")
        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(
                endpoint=endpoint + "/v1/metrics",
                timeout=5,
                preferred_temporality={
                    Counter: AggregationTemporality.DELTA,
                    Histogram: AggregationTemporality.DELTA,
                },
            ),
            export_interval_millis=30000,
        )
        self.metrics = MeterProvider(resource=resource, metric_readers=[reader])
        self.meter = self.metrics.get_meter("mail-parquet-lake")
        self.counters = {}
        self.duration = self.meter.create_histogram(
            "lake_operation_duration_seconds", unit="s"
        )
        self.logs = LoggerProvider(resource=resource)
        self.logs.add_log_record_processor(
            BatchLogRecordProcessor(
                OTLPLogExporter(endpoint=endpoint + "/v1/logs", timeout=5),
                max_queue_size=512,
                max_export_batch_size=64,
            )
        )
        self.logger = logging.getLogger("lake.telemetry")
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)
        self.handler = LoggingHandler(logger_provider=self.logs)
        self.logger.addHandler(self.handler)
        self.enabled = True
        self.event("run_started")
        self.count("lake_runs_started", 1)

    def count(self, name, value, **dimensions):
        if not self.enabled:
            return
        if name not in self.counters:
            self.counters[name] = self.meter.create_counter(name)
        self.counters[name].add(value, {"mode": self.mode, **dimensions})

    def event(self, name, **fields):
        if self.enabled:
            # Callers supply fixed event names and numeric/enum fields only.
            # Never bridge the root logger, exception text or third-party SDK logs.
            self.logger.info(
                name,
                extra={
                    "mode": self.mode,
                    "github_run_id": os.getenv("GITHUB_RUN_ID", "local"),
                    **fields,
                },
            )

    @contextmanager
    def operation(self, name):
        if not self.enabled:
            yield
            return
        started = time.monotonic()
        # Do not allow automatic exception stack/message recording: API errors
        # can contain addresses, request URLs, bucket keys or message IDs.
        with self.tracer.start_as_current_span(
            name,
            attributes={"mode": self.mode},
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                yield
            except Exception as exc:
                span.set_status(StatusCode.ERROR)
                span.set_attribute("error.type", type(exc).__name__)
                self.count("lake_operation_failures", 1, operation=name)
                self.event(
                    "operation_failed", operation=name, error_type=type(exc).__name__
                )
                raise
            finally:
                self.duration.record(
                    time.monotonic() - started, {"mode": self.mode, "operation": name}
                )

    def close(self):
        if self.enabled:
            for provider in [self.metrics, self.logs, self.traces]:
                provider.force_flush(timeout_millis=10000)
                provider.shutdown()
            self.logger.removeHandler(self.handler)
            self.enabled = False


telemetry = Telemetry()
