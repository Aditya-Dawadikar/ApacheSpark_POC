"""Central place for run configuration, sourced from environment variables with sane defaults."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.environ.get("SPARK_APP_NAME", "spark-app")
    master: str = os.environ.get("SPARK_MASTER", "local[*]")
    input_path: str = os.environ.get("INPUT_PATH", "data/input")
    output_path: str = os.environ.get("OUTPUT_PATH", "data/output")
    output_format: str = os.environ.get("OUTPUT_FORMAT", "parquet")
    shuffle_partitions: str = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")
    driver_host: str = os.environ.get("SPARK_DRIVER_HOST", "")

    # Executor sizing (standalone mode). Executor count is derived by Spark as
    # cores_max / executor_cores, not set directly - defaults below give 4
    # executors of 1 core / 1g each, matching the spark-worker capacity set in
    # docker-compose.yml.
    executor_cores: str = os.environ.get("SPARK_EXECUTOR_CORES", "1")
    executor_memory: str = os.environ.get("SPARK_EXECUTOR_MEMORY", "1g")
    cores_max: str = os.environ.get("SPARK_CORES_MAX", "4")

    # Observability (see spark_app/observability/)
    pushgateway_url: str = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
    otel_exporter_endpoint: str = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo:4318")
    spark_driver_ui_url: str = os.environ.get("SPARK_DRIVER_UI_URL", "http://localhost:4040")


settings = Settings()
