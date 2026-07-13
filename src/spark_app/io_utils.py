"""Reusable read/write helpers so jobs don't repeat Spark I/O boilerplate."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
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


def read_job_output(
    spark: SparkSession,
    path: str,
    fmt: str,
    array_columns=(),
    date_columns=(),
    bool_columns=(),
    double_columns=(),
    int_columns=(),
) -> DataFrame:
    """Read a previous job's persisted output, regardless of format.

    Parquet already round-trips types and arrays natively. CSV flattens
    everything to strings on write (see write_output/_stringify_array_columns),
    so on read we split array columns back out and re-cast the columns this
    job actually needs to compute with.
    """
    if fmt != "csv":
        return spark.read.format(fmt).load(path)

    df = read_csv(spark, path)
    df = _restore_array_columns(df, array_columns)
    df = _cast_columns(df, date_columns, bool_columns, double_columns, int_columns)
    return df


def _restore_array_columns(df: DataFrame, columns) -> DataFrame:
    for name in columns:
        if name in df.columns:
            df = df.withColumn(
                name,
                F.when(
                    F.col(name).isNull() | (F.col(name) == ""),
                    F.array().cast("array<string>"),
                ).otherwise(F.split(F.col(name), r"\|")),
            )
    return df


def _cast_columns(df: DataFrame, date_columns, bool_columns, double_columns, int_columns) -> DataFrame:
    for name in date_columns:
        if name in df.columns:
            df = df.withColumn(name, F.to_date(F.col(name)))
    for name in bool_columns:
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast("boolean"))
    for name in double_columns:
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast("double"))
    for name in int_columns:
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast("int"))
    return df


def _stringify_array_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, F.array_join(field.name, "|"))
    return df
