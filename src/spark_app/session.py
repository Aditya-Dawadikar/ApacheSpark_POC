"""SparkSession construction, kept separate so jobs and tests can share one code path."""

from pyspark.sql import SparkSession

from spark_app.config import settings


def get_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.master)
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.executor.cores", settings.executor_cores)
        .config("spark.executor.memory", settings.executor_memory)
        .config("spark.cores.max", settings.cores_max)
    )

    if settings.driver_host:
        # Lets workers on a standalone cluster connect back to this driver by
        # a stable hostname (e.g. the docker-compose service name) instead of
        # whatever address Spark would otherwise auto-detect for the container.
        builder = builder.config("spark.driver.host", settings.driver_host)

    return builder.getOrCreate()
