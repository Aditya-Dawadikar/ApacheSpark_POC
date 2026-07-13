"""Reusable read/write helpers so jobs don't repeat Spark I/O boilerplate."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def write_output(df: DataFrame, path: str, fmt: str = "parquet") -> None:
    writer = df.write.mode("overwrite")
    if fmt == "csv":
        # CSV has no array type; join array columns into a delimited string
        # instead so the output stays inspectable (and doesn't just error out).
        df = _stringify_array_columns(df)
        writer = df.write.mode("overwrite").option("header", True)
    writer.format(fmt).save(path)


def _stringify_array_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, F.array_join(field.name, "|"))
    return df
