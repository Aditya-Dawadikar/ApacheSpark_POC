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


settings = Settings()
