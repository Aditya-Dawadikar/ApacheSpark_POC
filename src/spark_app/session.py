"""SparkSession construction, kept separate so jobs and tests can share one code path."""

from pyspark.sql import SparkSession

from spark_app.config import settings


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.master)
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .getOrCreate()
    )
