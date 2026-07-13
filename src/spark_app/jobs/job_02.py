from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_app.config import settings
from spark_app.io_utils import read_csv, read_job_output, write_output
from spark_app.session import get_spark_session

logger = logging.getLogger(__name__)

JOB1_COLUMNS = [
   "claim_id",
    "source_document_ids",
    "document_count",
    "policy_number_raw",
    "policy_number_normalized",
    "member_first_name_raw",
    "member_first_name_normalized",
    "member_last_name_raw",
    "member_last_name_normalized",
    "member_dob_raw",
    "member_dob_parsed",
    "provider_npi_raw",
    "provider_npi_normalized",
    "service_date_raw",
    "service_date_parsed",
    "admission_date_parsed",
    "discharge_date_parsed",
    "diagnosis_codes",
    "procedure_codes",
    "billed_amount_raw",
    "billed_amount_parsed",
    "currency_normalized",
    "selected_document_id",
    "selected_ocr_confidence",
    "max_ocr_confidence",
    "avg_ocr_confidence",
    "pii_detected",
    "pii_types",
    "notes_redacted",
    "validation_errors",
    "normalization_warnings",
    "missing_fields",
    "duplicate_document_flag",
    "cross_document_conflict",
    "cross_document_conflict_fields",
    "job1_processed_at", 
]

def _norm_text(col_name: str):
    return F.upper(F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " "))

def _read_claims(spark: SparkSession) -> DataFrame:
    path = f"{settings.output_path}/canonical_claims"
    return read_job_output(
        spark,
        path,
        settings.output_format.lower(),
        date_columns=[
            "member_dob_parsed",
            "service_date_parsed",
            "admission_date_parsed",
            "discharge_date_parsed",
            "job1_processed_at",
        ],
        bool_columns=[
            "pii_detected",
            "duplicate_document_flag",
            "cross_document_conflict",
        ],
        double_columns=[
            "billed_amount_parsed",
            "selected_ocr_confidence",
            "max_ocr_confidence",
            "avg_ocr_confidence",
        ],
    )

def _prepare_policy(dim_policy: DataFrame) -> DataFrame:
    policy = (
        dim_policy.withColumn("policy_number_key", _norm_text("policy_number"))
        .withColumn("policy_status", F.upper(F.trim(F.col("policy_status"))))
        .withColumn("coverage_start_date", F.to_date(F.col("coverage_start_date")))
        .withColumn("coverage_end_date", F.to_date(F.col("coverage_end_date")))
    )

    count_window = Window.partitionBy("policy_number_key")
    rank_window = Window.partitionBy("policy_number_key").orderBy(
        (F.col("policy_status") == "ACTIVE").cast("int").desc(),
        F.col("coverage_end_date").desc_nulls_last(),
        F.col("coverage_start_date").desc_nulls_last(),
        F.col("policy_id").asc(),
    )

    return (
        policy.withColumn("policy_match_count", F.count("*").over(count_window))
        .withColumn("rn", F.row_number().over(rank_window))
        .filter(F.col("rn") == 1)
        .select(
            "policy_number_key",
            "policy_match_count",
            "policy_id",
            "member_id",
            "plan_id",
            "policy_status",
            "coverage_start_date",
            "coverage_end_date",
        )
    )

def _prepare_member(dim_member: DataFrame) -> DataFrame:
    return (
        dim_member.withColumn("reference_first_name", F.col("first_name"))
        .withColumn("reference_last_name", F.col("last_name"))
        .withColumn("reference_dob", F.to_date(F.col("dob")))
        .withColumn("reference_first_name_normalized", _norm_text("first_name"))
        .withColumn("reference_last_name_normalized", _norm_text("last_name"))
        .select(
            "member_id",
            "reference_first_name",
            "reference_last_name",
            "reference_dob",
            "reference_first_name_normalized",
            "reference_last_name_normalized",
        )
    )

def _prepare_provider(dim_provider: DataFrame) -> DataFrame:
    provider = (
        dim_provider.withColumn("provider_npi_key", F.regexp_replace(F.trim(F.col("npi")), r"[^0-9]", ""))
        .withColumn("provider_active", F.col("active_flag").cast("boolean"))
        .withColumn("provider_network_status", F.upper(F.trim(F.col("network_status"))))
    )

    count_window = Window.partitionBy("provider_npi_key")
    rank_window = Window.partitionBy("provider_npi_key").orderBy(
        F.col("provider_active").cast("int").desc(),
        F.col("provider_id").asc(),
    )

    return (
        provider.withColumn("provider_match_count", F.count("*").over(count_window))
        .withColumn("rn", F.row_number().over(rank_window))
        .filter(F.col("rn") == 1)
        .select(
            "provider_npi_key",
            "provider_match_count",
            "provider_id",
            "provider_name",
            "provider_type",
            "provider_active",
            "provider_network_status",
        )
    )

def _enrich_policy(claims: DataFrame, policy: DataFrame) -> DataFrame:
    return (
        claims.alias("c")
        .join(
            policy.alias("p"),
            F.col("c.policy_number_normalized") == F.col("p.policy_number_key"),
            "left"
        )
        .drop("policy_number_key")
        .withColumn("policy_match_count", F.coalesce(F.col("policy_match_count"), F.lit(0)))
        .withColumn("policy_exists", F.col("policy_match_count") > 0)
        .withColumn("policy_ambiguous", F.col("policy_match_count") > 1)
        .withColumn(
            "policy_active_on_service_date",
            F.when(
                F.col("policy_exists") & F.col("service_date_parsed").isNotNull(),
                F.coalesce(
                    (F.col("policy_status") == "ACTIVE")
                    & F.col("service_date_parsed").between(
                        F.col("coverage_start_date"),
                        F.col("coverage_end_date"),
                    ),
                    F.lit(False),
                ),
            ).otherwise(F.lit(False)),
        )
    )

def _enrich_member(claims: DataFrame, member: DataFrame) -> DataFrame:
    enriched = (
        claims.alias("c")
        .join(member.alias("m"), on="member_id", how="left")
        .withColumn("member_exists", F.col("reference_first_name").isNotNull())
        .withColumn(
            "first_name_match",
            F.when(
                F.col("member_exists"),
                F.coalesce(
                    F.col("member_first_name_normalized") == F.col("reference_first_name_normalized"),
                    F.lit(False),
                ),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "last_name_match",
            F.when(
                F.col("member_exists"),
                F.coalesce(
                    F.col("member_last_name_normalized") == F.col("reference_last_name_normalized"),
                    F.lit(False),
                ),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "dob_match",
            F.when(
                F.col("member_exists"),
                F.coalesce(F.col("member_dob_parsed") == F.col("reference_dob"), F.lit(False)),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "entity_match_confidence",
            (
                F.col("first_name_match").cast("double") * F.lit(0.25)
                + F.col("last_name_match").cast("double") * F.lit(0.35)
                + F.col("dob_match").cast("double") * F.lit(0.40)
            ),
        )
        .withColumn(
            "member_eligible",
            F.col("member_exists")
            & F.col("policy_active_on_service_date")
            & (F.col("entity_match_confidence") == F.lit(1.0)),
        )
        .drop("reference_first_name_normalized", "reference_last_name_normalized")
    )

    return enriched


def _enrich_provider(claims: DataFrame, provider: DataFrame) -> DataFrame:
    return (
        claims.alias("c")
        .join(
            provider.alias("p"),
            F.col("c.provider_npi_normalized") == F.col("p.provider_npi_key"),
            "left",
        )
        .drop("provider_npi_key")
        .withColumn("provider_match_count", F.coalesce(F.col("provider_match_count"), F.lit(0)))
        .withColumn("provider_exists", F.col("provider_match_count") > 0)
        .withColumn("provider_ambiguous", F.col("provider_match_count") > 1)
        .withColumn("provider_active", F.coalesce(F.col("provider_active"), F.lit(False)))
    )

def _add_reference_errors(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "reference_enrichment_errors",
        F.filter(
            F.array(
                F.when(~F.col("policy_exists"), F.lit("POLICY_NOT_FOUND")),
                F.when(F.col("policy_ambiguous"), F.lit("POLICY_AMBIGUOUS")),
                F.when(
                    F.col("policy_exists") & (~F.col("policy_active_on_service_date")),
                    F.lit("POLICY_INACTIVE_ON_SERVICE_DATE"),
                ),
                F.when(~F.col("member_exists"), F.lit("MEMBER_NOT_FOUND")),
                F.when(
                    F.col("member_exists") & (F.col("entity_match_confidence") < F.lit(1.0)),
                    F.lit("MEMBER_IDENTITY_MISMATCH"),
                ),
                F.when(~F.col("provider_exists"), F.lit("PROVIDER_NOT_FOUND")),
                F.when(F.col("provider_ambiguous"), F.lit("PROVIDER_AMBIGUOUS")),
                F.when(
                    F.col("provider_exists") & (~F.col("provider_active")),
                    F.lit("PROVIDER_INACTIVE"),
                ),
            ),
            lambda value: value.isNotNull(),
        ),
    ).withColumn("job2_processed_at", F.current_timestamp())

def _select_job2_schema(df: DataFrame) -> DataFrame:
    return df.select(
        *JOB1_COLUMNS,
        "policy_id",
        "member_id",
        "plan_id",
        "policy_status",
        "coverage_start_date",
        "coverage_end_date",
        "policy_match_count",
        "policy_exists",
        "policy_ambiguous",
        "policy_active_on_service_date",
        "member_exists",
        "reference_first_name",
        "reference_last_name",
        "reference_dob",
        "first_name_match",
        "last_name_match",
        "dob_match",
        "entity_match_confidence",
        "member_eligible",
        "provider_id",
        "provider_name",
        "provider_type",
        "provider_match_count",
        "provider_exists",
        "provider_ambiguous",
        "provider_active",
        "provider_network_status",
        "reference_enrichment_errors",
        "job2_processed_at",
    )

def run():
    spark = get_spark_session()
    try:
        logger.info("Job 2 Starting")

        claims = _read_claims(spark)
        input_count = claims.count()
        
        logger.info("Read %d canonical claims", input_count)

        dim_policy = read_csv(spark, f"{settings.input_path}/dim_policy.csv")
        dim_member = read_csv(spark, f"{settings.input_path}/dim_member.csv")
        dim_provider = read_csv(spark, f"{settings.input_path}/dim_provider.csv")

        policy = _prepare_policy(dim_policy)
        member = _prepare_member(dim_member)
        provider = _prepare_provider(dim_provider)

        enriched = _enrich_policy(claims, policy)
        enriched = _enrich_member(enriched, member)
        enriched = _enrich_provider(enriched, provider)
        enriched = _add_reference_errors(enriched)

        final_df = _select_job2_schema(enriched)
        output_count = final_df.count()

        logger.info("Reference enrichment produced %d rows", output_count)

        output_path = f"{settings.output_path}/reference_enriched_claims"
        write_output(final_df, output_path, settings.output_format)

        logger.info("Job 2 complete: write %d rows to %s", output_count, output_path)
    finally:
        spark.stop()
