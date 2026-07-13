"""CLI entry point. Add new jobs under spark_app/jobs, then register them in JOBS below."""

import argparse

from spark_app.jobs.job_01 import run as job_01_run

JOBS = {
    "job_01": job_01_run,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Spark data processing job")
    parser.add_argument(
        "--job",
        choices=sorted(JOBS.keys()),
        required=True,
        help="Name of the job to run",
    )
    args = parser.parse_args()

    JOBS[args.job]()


if __name__ == "__main__":
    main()
