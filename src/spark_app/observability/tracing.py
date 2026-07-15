"""OpenTelemetry tracing: one span per pipeline job, exported directly to Tempo.

Uses SimpleSpanProcessor (exports synchronously on span end) rather than the
batching processor — spark-job is a short-lived, few-span process, so there's
no batching benefit and batching risks losing spans if the container exits
before a batch flushes.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import Status, StatusCode

from spark_app.config import settings

logger = logging.getLogger(__name__)

_provider: TracerProvider | None = None


def _get_provider() -> TracerProvider:
    global _provider
    if _provider is None:
        resource = Resource.create({SERVICE_NAME: settings.app_name})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=f"{settings.otel_exporter_endpoint}/v1/traces")
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _provider = provider
    return _provider


@contextmanager
def job_span(job_name: str) -> Iterator[None]:
    """Wrap one job's execution in a trace span named after the job."""
    tracer = _get_provider().get_tracer(__name__)
    with tracer.start_as_current_span(job_name) as span:
        try:
            yield
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        else:
            span.set_status(Status(StatusCode.OK))


def shutdown() -> None:
    """Flush and shut down the tracer provider before process exit."""
    if _provider is not None:
        _provider.shutdown()
