# SparkDAG — Claims Review PySpark Pipeline

A batch PySpark pipeline that turns noisy OCR claim documents into one
evidence-rich case record per claim for human review, per `SPEC.md`. The
pipeline never auto-approves or auto-rejects a claim — every claim ends up
`ETL_COMPLETE` or `MANUAL_REVIEW_REQUIRED`.

## The pipeline

```
ocr_claims.csv
      |
      v
Job 1  OCR Validation and Canonicalization      -> canonical_claims
      |
      v
Job 2  Reference Enrichment (policy/member/provider) -> reference_enriched_claims
      |
      v
Job 3  Procedure Coverage                        -> coverage_enriched_claims
      |
      v
Job 4  Historical Analysis                       -> historical_enriched_claims
      |
      v
Job 5  Authorization Validation                  -> authorization_enriched_claims
      |
      v
Job 6  Evidence Assembly and Persist              -> claim_case
```

Every job reads the previous job's output plus whatever reference CSV(s) it
needs from `data/input/`, and writes its own output under `data/output/`.
Every stage preserves one row per claim (180 rows for the bundled sample
data) — no claim is ever silently dropped, per `SPEC.md`'s invariants.

## Project layout

```
SparkDAG/
├── src/
│   └── spark_app/
│       ├── main.py           # CLI entry point: --job job_01 .. job_06, or --job all
│       ├── pipeline.py        # run_all(): runs job_01..job_06 in sequence
│       ├── config.py          # Settings, read from env vars with defaults
│       ├── session.py         # SparkSession construction
│       ├── io_utils.py        # Read/write helpers, incl. CSV array/type round-tripping
│       └── jobs/
│           ├── job_01.py      # OCR validation and canonicalization
│           ├── job_02.py      # Policy/member/provider reference enrichment
│           ├── job_03.py      # Procedure coverage evaluation
│           ├── job_04.py      # Historical claims analysis / duplicate + risk scoring
│           ├── job_05.py      # Pre-authorization validation
│           └── job_06.py      # Evidence assembly, workflow status, final case record
├── data/
│   ├── input/                  # ocr_claims.csv, dim_*.csv, fact_*.csv
│   └── output/                  # One folder per job's output (see pipeline diagram)
├── SPEC.md                      # Full pipeline specification this code implements
├── requirements.txt              # Runtime deps (pyspark)
├── requirements-dev.txt          # + pytest, for local dev
├── Dockerfile
├── docker-compose.yml
└── README.md
```

**Why this shape:**
- `io_utils.py` and `session.py` isolate Spark boilerplate so job files stay focused on
  business logic.
- Every job's output is written to CSV (`OUTPUT_FORMAT=csv`, set in `docker-compose.yml`).
  CSV can't hold array or struct columns, so `write_output` flattens `array<string>`
  columns to a `|`-joined string on write, and `read_job_output` (in `io_utils.py`)
  reverses that on read — splitting them back into arrays for whichever downstream job
  needs to actually operate on them (e.g. Job 3 exploding `procedure_codes`). Nested
  evidence (per-procedure coverage/authorization detail, bounded historical matches) is
  built as JSON-serialized strings from the moment it's created in Jobs 3-5 rather than
  as native Spark struct/array<struct> columns, since CSV has no way to represent those
  either — see the docstring at the top of `job_06.py` for the full reasoning.
- `pipeline.py` exists so the whole DAG can run with one command instead of invoking
  each job by hand in order.

## Running the pipeline

### Persistent cluster (recommended — lets you watch progress in the Spark UI)

`docker-compose.yml` defines three services: a long-running `spark-master` and
`spark-worker` you start once, and a short-lived `spark-job` you use to submit runs.

```bash
docker compose up -d spark-master spark-worker
```

Open http://localhost:8080 — you should see 1 worker registered and `ALIVE`. Then run
the full pipeline in one command:

```bash
docker compose run --rm spark-job --job all
```

This runs Job 1 through Job 6 in sequence, each writing its own CSV output under
`data/output/<dataset_name>/`. While a job is running, open http://localhost:4040 for
live stage/task progress. Bring the cluster down when you're done:

```bash
docker compose down
```

To run a single job against a previous job's already-written output (e.g. after tweaking
Job 4's logic, without re-running Jobs 1-3):

```bash
docker compose run --rm spark-job --job job_04
```

`spark-worker` and `spark-job` both mount `./data:/app/data`. This isn't optional: in
standalone cluster mode, executors run inside the `spark-worker` container, not the
driver — so `spark-worker` needs the same live host mount as `spark-job`, or executors
silently read/write against the stale copy of `data/` baked into the image at build time
instead of your actual host files (the job still reports success; the output just never
reaches your host).

### One-shot container (no cluster, no UI)

```bash
docker build -t spark-dag:latest .
docker run --rm -v "$(pwd)/data:/app/data" spark-dag:latest --job all
```

On Windows, prefer Docker Compose or a native Windows-style path for the volume mount
(`-v "D:\path\to\SparkDAG\data:/app/data"`) — running `docker run -v` from Git Bash with
a `/d/...`-style path can get mistranslated and silently mount the wrong location.

### Running locally (no Docker)

Requires Python 3.11+ and a JDK (Java 17+) installed, since PySpark needs a JVM.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements-dev.txt

PYTHONPATH=src python -m spark_app.main --job all
```

On Windows PowerShell, set `PYTHONPATH` like this instead:

```powershell
$env:PYTHONPATH = "src"
python -m spark_app.main --job all
```

#### Note for local (non-Docker) runs on Windows

Writing output on Windows local mode requires `winutils.exe` (Spark shells out to it via
Hadoop's `RawLocalFileSystem` for directory permissions). Without it you'll see:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
```

Reading and `.show()` still work fine — only the final write step fails. To fix it,
download a matching `winutils.exe` for your Hadoop version, put it under
`C:\hadoop\bin\winutils.exe`, and set `HADOOP_HOME=C:\hadoop` before running. Easiest
way to sidestep this entirely: run the job via Docker instead, since Linux containers
don't need `winutils`.

## Configuration

Copy `.env.example` to `.env` and adjust values as needed — `docker-compose.yml` loads it
automatically for the `spark-worker` and `spark-job` services (`env_file: .env`). `.env`
is gitignored so local tweaks don't get committed; `.env.example` is the tracked template.
Settings that reference other compose services by hostname (`SPARK_MASTER`,
`SPARK_DRIVER_HOST`, `INPUT_PATH`, `OUTPUT_PATH`) stay hardcoded directly in
`docker-compose.yml` instead, since those aren't meant to be casually edited.

All settings are environment variables with defaults (see `src/spark_app/config.py`):

| Variable                  | Default        | Meaning                          |
|----------------------------|-----------------|-----------------------------------|
| `SPARK_APP_NAME`            | `spark-app`     | Spark application name           |
| `SPARK_MASTER`              | `local[*]`      | Spark master URL                 |
| `SPARK_DRIVER_HOST`         | (unset)          | Hostname workers use to reach this driver (set to the compose service name in cluster mode) |
| `INPUT_PATH`                | `data/input`    | Input path                       |
| `OUTPUT_PATH`               | `data/output`   | Output path                      |
| `OUTPUT_FORMAT`             | `parquet`        | Output format (`csv` in `docker-compose.yml`) |
| `SPARK_SHUFFLE_PARTITIONS`  | `4`               | `spark.sql.shuffle.partitions`   |
| `SPARK_EXECUTOR_CORES`      | `1`               | Cores per executor (`spark.executor.cores`) |
| `SPARK_EXECUTOR_MEMORY`     | `1g`              | Memory per executor (`spark.executor.memory`) |
| `SPARK_CORES_MAX`           | `4`               | Total cores this app requests cluster-wide (`spark.cores.max`) |
| `SPARK_WORKER_CORES`        | `4`               | Total cores the `spark-worker` container offers to the cluster |
| `SPARK_WORKER_MEMORY`       | `4g`              | Total memory the `spark-worker` container offers to the cluster |

### Executor sizing

In standalone mode there's no direct "number of executors" setting — Spark derives it as
`SPARK_CORES_MAX / SPARK_EXECUTOR_CORES`. The defaults above give **up to 4 executors of
1 core / 1g each**, and `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY` size `spark-worker` to
exactly fit all 4 on the one worker (the `Worker` process picks these up itself as a
fallback when no `--cores`/`--memory` flag is given). To change the split (e.g. 2
executors of 2 cores each instead of 4 of 1), keep `SPARK_CORES_MAX` fixed and change
`SPARK_EXECUTOR_CORES` — just make sure `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY` can
actually supply what you're requesting, and add more `spark-worker` replicas if you want
executors spread across multiple machines instead of packed onto one.

### Running tests

```bash
pytest
```

## Adding a new job

1. Add pure `DataFrame -> DataFrame` transform functions inside the job module (see any
   existing `jobs/job_0N.py` for the pattern: small `_stage_name(df)` functions composed
   in `run()`), keeping I/O and `SparkSession` construction out of them so they're easy
   to unit test.
2. Read the previous job's output via `io_utils.read_job_output`, listing which columns
   need restoring as arrays/dates/bools/doubles/ints when reading CSV.
3. Register it in `main.py`'s `JOBS` dict and add it to the `PIPELINE` list in
   `pipeline.py` in the right dependency position.
4. Add tests under `tests/`.
