"""CLI entry point. Add new jobs under spark_app/jobs, then register them in JOBS below."""

import argparse

from spark_app.jobs.job_01 import run as job_01_run
from spark_app.jobs.job_02 import run as job_02_run
from spark_app.jobs.job_03 import run as job_03_run
from spark_app.jobs.job_04 import run as job_04_run
from spark_app.jobs.job_05 import run as job_05_run
from spark_app.jobs.job_06 import run as job_06_run
from spark_app.observability import logging_setup, tracing
from spark_app.pipeline import run_all

JOBS = {
    "job_01": job_01_run,
    "job_02": job_02_run,
    "job_03": job_03_run,
    "job_04": job_04_run,
    "job_05": job_05_run,
    "job_06": job_06_run,
    "all": run_all,
}


def main() -> None:
    logging_setup.configure()

    parser = argparse.ArgumentParser(description="Run a Spark data processing job")
    parser.add_argument(
        "--job",
        choices=sorted(JOBS.keys()),
        required=True,
        help="Name of the job to run ('all' runs the full pipeline in sequence)",
    )
    args = parser.parse_args()

    try:
        if args.job == "all":
            # run_all() opens its own per-job spans (see pipeline.py).
            JOBS[args.job]()
        else:
            with tracing.job_span(args.job):
                JOBS[args.job]()
    finally:
        tracing.shutdown()


if __name__ == "__main__":
    main()
