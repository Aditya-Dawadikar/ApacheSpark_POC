"""Pushes shuffle/spill/task-skew/JVM metrics to the Prometheus Pushgateway.

Spark's built-in metrics sink only covers the long-running master/worker
processes. Each job's driver/executors live for a handful of seconds, far too
short for Prometheus to reliably scrape — so instead, right before a job's
SparkSession stops (while its REST API is still up), this queries the Spark
REST API for the job's stage and executor summaries and pushes a snapshot to
the Pushgateway.

Uses a grouping key of just {job_name}, not {job_name, app_id}: `push_to_gateway`
replaces all metrics under a grouping key, so each new run of the same job
overwrites the previous snapshot rather than accumulating unboundedly in the
gateway. app_id is kept as a metric label instead, so Prometheus's own
scrape history still distinguishes runs over time.
"""

from __future__ import annotations

import logging
import time

import requests
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession

from spark_app.config import settings

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 5


def finalize_spark_session(spark: SparkSession, job_name: str) -> None:
    """Collect and push this job's metrics, then stop the SparkSession.

    Never raises: a metrics-collection failure is logged and swallowed so it
    can never break the actual pipeline run.
    """
    try:
        _collect_and_push(spark, job_name)
    except Exception:
        logger.warning("Failed to collect/push observability metrics for %s", job_name, exc_info=True)
    finally:
        spark.stop()


def _collect_and_push(spark: SparkSession, job_name: str) -> None:
    sc = spark.sparkContext
    app_id = sc.applicationId
    base_url = f"{settings.spark_driver_ui_url}/api/v1/applications/{app_id}"

    registry = CollectorRegistry()

    duration_seconds = time.time() - sc.startTime / 1000.0
    Gauge(
        "spark_job_duration_seconds", "Wall-clock duration of the job's SparkSession", ["app_id"], registry=registry
    ).labels(app_id=app_id).set(duration_seconds)

    stages = requests.get(f"{base_url}/stages?status=complete", timeout=_REQUEST_TIMEOUT_SECONDS).json()
    _push_stage_metrics(registry, base_url, app_id, stages)
    _push_executor_metrics(registry, base_url, app_id)

    push_to_gateway(settings.pushgateway_url, job="spark_app", grouping_key={"job_name": job_name}, registry=registry)
    logger.info("Pushed observability metrics for %s (app_id=%s) to %s", job_name, app_id, settings.pushgateway_url)


def _push_stage_metrics(registry: CollectorRegistry, base_url: str, app_id: str, stages: list[dict]) -> None:
    labelnames = ["stage_id", "stage_name", "app_id"]
    shuffle_read = Gauge("spark_stage_shuffle_read_bytes", "Shuffle bytes read", labelnames, registry=registry)
    shuffle_write = Gauge("spark_stage_shuffle_write_bytes", "Shuffle bytes written", labelnames, registry=registry)
    memory_spill = Gauge("spark_stage_memory_spill_bytes", "Bytes spilled to memory", labelnames, registry=registry)
    disk_spill = Gauge("spark_stage_disk_spill_bytes", "Bytes spilled to disk", labelnames, registry=registry)
    skew_ratio = Gauge(
        "spark_stage_task_duration_skew_ratio",
        "Max task duration / median task duration within the stage",
        labelnames,
        registry=registry,
    )

    for stage in stages:
        stage_id = str(stage.get("stageId"))
        stage_name = stage.get("name", "")
        labels = {"stage_id": stage_id, "stage_name": stage_name, "app_id": app_id}

        shuffle_read.labels(**labels).set(stage.get("shuffleReadBytes", 0))
        shuffle_write.labels(**labels).set(stage.get("shuffleWriteBytes", 0))
        memory_spill.labels(**labels).set(stage.get("memoryBytesSpilled", 0))
        disk_spill.labels(**labels).set(stage.get("diskBytesSpilled", 0))

        ratio = _stage_task_skew_ratio(base_url, stage.get("stageId"), stage.get("attemptId", 0))
        if ratio is not None:
            skew_ratio.labels(**labels).set(ratio)


def _stage_task_skew_ratio(base_url: str, stage_id: int, attempt_id: int) -> float | None:
    """Max/median task duration for a stage, as an indicator of data skew."""
    url = f"{base_url}/stages/{stage_id}/{attempt_id}/taskSummary"
    resp = requests.get(url, params={"quantiles": "0,0.25,0.5,0.75,1"}, timeout=_REQUEST_TIMEOUT_SECONDS)
    if resp.status_code != 200:
        return None

    run_times = resp.json().get("executorRunTime", [])
    if len(run_times) != 5:
        return None

    median, maximum = run_times[2], run_times[4]
    return maximum / median if median > 0 else None


def _push_executor_metrics(registry: CollectorRegistry, base_url: str, app_id: str) -> None:
    labelnames = ["executor_id", "app_id"]
    heap_used = Gauge("spark_executor_jvm_heap_used_bytes", "JVM heap used", labelnames, registry=registry)
    heap_max = Gauge("spark_executor_jvm_heap_max_bytes", "JVM heap max", labelnames, registry=registry)
    gc_time = Gauge("spark_executor_jvm_gc_time_ms", "Cumulative JVM GC time", labelnames, registry=registry)

    executors = requests.get(f"{base_url}/executors", timeout=_REQUEST_TIMEOUT_SECONDS).json()
    for executor in executors:
        labels = {"executor_id": str(executor.get("id")), "app_id": app_id}
        heap_used.labels(**labels).set(executor.get("memoryUsed", 0))
        heap_max.labels(**labels).set(executor.get("maxMemory", 0))
        gc_time.labels(**labels).set(executor.get("totalGCTime", 0))
