"""Runs the full claims-review DAG (Job 1 through Job 6) in one invocation.

Each job still creates and tears down its own SparkSession (see
spark_app.session.get_spark_session), so this just calls them in the
dependency order fixed by SPEC.md: Job 1 -> 2 -> 3 -> 4 -> 5 -> 6.
"""

from __future__ import annotations

import logging

from spark_app.jobs import job_01, job_02, job_03, job_04, job_05, job_06

logger = logging.getLogger(__name__)

PIPELINE = [
    ("job_01", job_01.run),
    ("job_02", job_02.run),
    ("job_03", job_03.run),
    ("job_04", job_04.run),
    ("job_05", job_05.run),
    ("job_06", job_06.run),
]


def run_all() -> None:
    for name, job_run in PIPELINE:
        logger.info("=== Starting %s ===", name)
        job_run()
        logger.info("=== Finished %s ===", name)
