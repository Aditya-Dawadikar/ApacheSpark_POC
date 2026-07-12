"""CLI entry point. Add new jobs under spark_app/jobs, then register them in JOBS below."""

import argparse

JOBS = {
    # "my_job": my_job.run,
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
