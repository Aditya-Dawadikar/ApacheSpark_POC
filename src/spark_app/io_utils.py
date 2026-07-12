"""Reusable read/write helpers so jobs don't repeat Spark I/O boilerplate."""

from pyspark.sql import DataFrame, SparkSession


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def write_output(df: DataFrame, path: str, fmt: str = "parquet") -> None:
    df.write.mode("overwrite").format(fmt).save(path)
