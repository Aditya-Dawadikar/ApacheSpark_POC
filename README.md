# SparkDAG — PySpark Data Processing Scaffold

Baseline project structure for batch-style PySpark jobs. It has no pipelines yet —
just the layout and plumbing (config, session, I/O helpers, Docker setup) to build
jobs on top of.

## Project layout

```
SparkDAG/
├── src/
│   └── spark_app/
│       ├── main.py          # CLI entry point (--job <name>); register new jobs in JOBS
│       ├── config.py        # Settings, read from env vars with defaults
│       ├── session.py        # SparkSession construction
│       ├── io_utils.py       # Read/write helpers (CSV/parquet)
│       └── jobs/             # One module per pipeline; empty for now
├── data/
│   ├── input/                 # Place input data here
│   └── output/                 # Job output lands here
├── tests/                      # pytest tests go here
├── requirements.txt            # Runtime deps (pyspark)
├── requirements-dev.txt        # + pytest, for local dev
├── Dockerfile
├── docker-compose.yml
└── README.md
```

**Why this shape:**
- `io_utils.py` and `session.py` isolate Spark boilerplate (readers/writers, session
  config) so job files stay short and readable.
- `jobs/` holds one file per pipeline. Each job should expose a `run()` function that
  composes session + io + transforms, and pure `DataFrame -> DataFrame` transform
  logic should live in its own module (not inside the job) so it's unit-testable
  without spinning up a job end-to-end.
- `config.py` centralizes environment-driven settings (input/output paths, Spark
  master, app name) so behavior can change between local/dev/prod without code edits.

## Adding your first job

1. Add a module with pure `DataFrame -> DataFrame` transform functions (e.g.
   `src/spark_app/transforms.py`) — keep it free of I/O and `SparkSession` so it's
   easy to unit test.
2. Add a new file under `jobs/`, e.g. `jobs/my_job.py`, with a `run()` function that
   builds the Spark session (`session.get_spark_session`), reads input
   (`io_utils.read_csv`), applies your transforms, and writes output
   (`io_utils.write_output`).
3. Register it in `main.py`'s `JOBS` dict: `"my_job": my_job.run`.
4. Add tests under `tests/`.

## Running locally (no Docker)

Requires Python 3.11+ and a JDK (Java 17+) installed, since PySpark needs a JVM.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements-dev.txt

PYTHONPATH=src python -m spark_app.main --job <name>
```

On Windows PowerShell, set `PYTHONPATH` like this instead:

```powershell
$env:PYTHONPATH = "src"
python -m spark_app.main --job <name>
```

### Configuration

All settings are environment variables with defaults (see `src/spark_app/config.py`):

| Variable                  | Default        | Meaning                          |
|----------------------------|-----------------|-----------------------------------|
| `SPARK_APP_NAME`            | `spark-app`     | Spark application name           |
| `SPARK_MASTER`              | `local[*]`      | Spark master URL                 |
| `INPUT_PATH`                | `data/input`    | Input path                       |
| `OUTPUT_PATH`               | `data/output`   | Output path                      |
| `OUTPUT_FORMAT`             | `parquet`        | Output format (parquet/csv/json) |
| `SPARK_SHUFFLE_PARTITIONS`  | `4`               | `spark.sql.shuffle.partitions`   |

### Running tests

```bash
pytest
```

### Note for local (non-Docker) runs on Windows

Writing output on Windows local mode requires `winutils.exe` (Spark shells out to it via
Hadoop's `RawLocalFileSystem` for directory permissions). Without it you'll see:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
```

Reading and `.show()` still work fine — only the final write step fails. To fix it,
download a matching `winutils.exe` for your Hadoop version, put it under
`C:\hadoop\bin\winutils.exe`, and set `HADOOP_HOME=C:\hadoop` before running. Easiest
way to sidestep this entirely: run the job via Docker instead (see below), since Linux
containers don't need `winutils`.

## Running with Docker

There are two ways to run jobs in Docker: a one-shot container (spins up, runs, exits),
or a persistent Spark standalone cluster you can watch jobs run against in the Spark UI.

### One-shot (no cluster, no UI to watch progress in)

```bash
docker build -t spark-dag:latest .
docker run --rm -v "$(pwd)/data:/app/data" spark-dag:latest --job <name>
```

On Windows, prefer Docker Compose or a native Windows-style path for the volume mount
(`-v "D:\path\to\SparkDAG\data:/app/data"`) — running `docker run -v` from Git Bash with
a `/d/...`-style path can get mistranslated and silently mount the wrong location.

### Persistent cluster (watch job progress in the Spark UI)

`docker-compose.yml` defines three services:

| Service        | Role                                              | UI                              |
|----------------|----------------------------------------------------|----------------------------------|
| `spark-master` | Long-running standalone master                    | http://localhost:8080 (cluster status, workers, running/completed apps) |
| `spark-worker` | Long-running worker, registers with the master     | http://localhost:8081 (this worker's executors and logs) |
| `spark-job`    | Short-lived — submits one job run, then exits      | http://localhost:4040 (live stage/task progress, only while a job is running) |

`spark-worker` and `spark-job` both mount `./data:/app/data`. This isn't optional: in
standalone cluster mode, executors run inside the `spark-worker` container, not the
driver — so `spark-worker` needs the same live host mount as `spark-job`, or executors
silently read/write against the stale copy of `data/` baked into the image at build time
instead of your actual host files (the job still reports success; the output just never
reaches your host).

Start the cluster once and leave it running:

```bash
docker compose up -d spark-master spark-worker
```

Open http://localhost:8080 — you should see 1 worker registered and `ALIVE`. Then submit
job runs against it as many times as you like; the master/worker stay up between runs:

```bash
docker compose run --rm spark-job --job <name>
```

While a run is in progress, open http://localhost:4040 for the live Spark application UI
(jobs, stages, tasks). After it finishes, the master's UI at :8080 keeps a record of it
under "Completed Applications" (state `FINISHED`), and output lands under `data/output/`
on your host as usual.

Bring the cluster down when you're done:

```bash
docker compose down
```
