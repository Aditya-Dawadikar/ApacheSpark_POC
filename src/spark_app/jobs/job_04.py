"""Job 4: Historical Analysis."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_app.config import settings
from spark_app.io_utils import read_csv, read_job_output, write_output
from spark_app.session import get_spark_session

logger = logging.getLogger(__name__)

JOB3_COLUMNS = [
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
    "covered_procedure_codes",
    "uncovered_procedure_codes",
    "pre_auth_required_procedure_codes",
    "coverage_evidence",
    "all_procedures_have_coverage",
    "covered_procedure_count",
    "uncovered_procedure_count",
    "estimated_eligible_amount",
    "coverage_errors",
    "job3_processed_at",
]

HISTORY_WINDOW_DAYS = 365
DUPLICATE_WINDOW_DAYS = 30
DUPLICATE_AMOUNT_TOLERANCE = 0.05


def _read_claims(spark: SparkSession) -> DataFrame:
    path = f"{settings.output_path}/coverage_enriched_claims"
    return read_job_output(
        spark,
        path,
        settings.output_format.lower(),
        array_columns=[
            "source_document_ids",
            "diagnosis_codes",
            "procedure_codes",
            "pii_types",
            "validation_errors",
            "normalization_warnings",
            "missing_fields",
            "cross_document_conflict_fields",
            "reference_enrichment_errors",
            "covered_procedure_codes",
            "uncovered_procedure_codes",
            "pre_auth_required_procedure_codes",
            "coverage_errors",
        ],
        date_columns=[
            "member_dob_parsed",
            "service_date_parsed",
            "admission_date_parsed",
            "discharge_date_parsed",
            "job1_processed_at",
            "coverage_start_date",
            "coverage_end_date",
            "job2_processed_at",
            "job3_processed_at",
        ],
        bool_columns=[
            "pii_detected",
            "duplicate_document_flag",
            "cross_document_conflict",
            "policy_exists",
            "policy_ambiguous",
            "policy_active_on_service_date",
            "member_exists",
            "first_name_match",
            "last_name_match",
            "dob_match",
            "member_eligible",
            "provider_exists",
            "provider_ambiguous",
            "provider_active",
            "all_procedures_have_coverage",
        ],
        double_columns=[
            "billed_amount_parsed",
            "selected_ocr_confidence",
            "max_ocr_confidence",
            "avg_ocr_confidence",
            "entity_match_confidence",
            "estimated_eligible_amount",
        ],
        int_columns=[
            "document_count",
            "policy_match_count",
            "provider_match_count",
            "covered_procedure_count",
            "uncovered_procedure_count",
        ],
    )


def _read_historical_claims(spark: SparkSession) -> DataFrame:
    path = f"{settings.input_path}/fact_claims.csv"
    return (
        read_csv(spark, path)
        .withColumn("service_date", F.to_date(F.col("service_date")))
        .withColumn("claim_amount", F.col("claim_amount").cast("double"))
        .withColumn("approved_amount", F.col("approved_amount").cast("double"))
    )


def _explode_current_procedures(claims: DataFrame) -> DataFrame:
    """One row per claim-procedure code, used for procedure-level duplicate/history checks."""
    return (
        claims.withColumn("procedure_count", F.size(F.coalesce(F.col("procedure_codes"), F.array())))
        .select(
            "claim_id",
            "member_id",
            "provider_id",
            "service_date_parsed",
            "billed_amount_parsed",
            "procedure_count",
            F.explode_outer("procedure_codes").alias("procedure_code"),
        )
        .withColumn(
            "current_procedure_billed_amount",
            F.when(F.col("procedure_count") > 0, F.col("billed_amount_parsed") / F.col("procedure_count")),
        )
    )


def _historical_window_join(claims: DataFrame, historical: DataFrame) -> DataFrame:
    """Historical claims for the same member, strictly before the current service date
    and within the trailing HISTORY_WINDOW_DAYS window."""
    base = claims.select("claim_id", "member_id", "service_date_parsed")
    return (
        base.alias("cur")
        .join(historical.alias("h"), F.col("cur.member_id") == F.col("h.member_id"), "inner")
        .filter(
            F.col("cur.service_date_parsed").isNotNull()
            & F.col("h.service_date").isNotNull()
            & (F.col("h.service_date") < F.col("cur.service_date_parsed"))
            & (F.col("h.service_date") >= F.date_sub(F.col("cur.service_date_parsed"), HISTORY_WINDOW_DAYS))
        )
        .select(
            F.col("cur.claim_id").alias("claim_id"),
            F.col("h.historical_claim_id").alias("historical_claim_id"),
            F.col("h.service_date").alias("historical_service_date"),
            F.col("h.provider_id").alias("historical_provider_id"),
            F.col("h.procedure_code").alias("historical_procedure_code"),
            F.col("h.claim_amount").alias("historical_claim_amount"),
            F.col("h.claim_status").alias("historical_claim_status"),
        )
    )


def _historical_metrics(window_joined: DataFrame) -> DataFrame:
    return window_joined.groupBy("claim_id").agg(
        F.count("*").alias("prior_claim_count_365d"),
        F.sum("historical_claim_amount").alias("prior_claim_amount_365d"),
        F.max("historical_service_date").alias("most_recent_prior_service_date"),
        F.avg("historical_claim_amount").alias("historical_average_claim_amount"),
    )


def _historical_evidence(window_joined: DataFrame) -> DataFrame:
    """Bounded evidence: the 5 most recent prior claims per current claim, not every historical row."""
    ranked = window_joined.withColumn(
        "rn",
        F.row_number().over(Window.partitionBy("claim_id").orderBy(F.col("historical_service_date").desc())),
    ).filter(F.col("rn") <= 5)

    evidence_struct = F.struct(
        F.col("historical_claim_id"),
        F.col("historical_service_date").alias("service_date"),
        F.col("historical_provider_id").alias("provider_id"),
        F.col("historical_procedure_code").alias("procedure_code"),
        F.col("historical_claim_amount").alias("claim_amount"),
        F.col("historical_claim_status").alias("claim_status"),
    )

    return ranked.groupBy("claim_id").agg(
        F.to_json(F.collect_list(evidence_struct)).alias("historical_evidence")
    )


def _same_procedure_counts(exploded_current: DataFrame, historical: DataFrame) -> DataFrame:
    joined = (
        exploded_current.alias("cur")
        .join(
            historical.alias("h"),
            (F.col("cur.member_id") == F.col("h.member_id"))
            & (F.col("cur.procedure_code") == F.col("h.procedure_code")),
            "inner",
        )
        .filter(
            F.col("cur.service_date_parsed").isNotNull()
            & F.col("h.service_date").isNotNull()
            & (F.col("h.service_date") < F.col("cur.service_date_parsed"))
            & (F.col("h.service_date") >= F.date_sub(F.col("cur.service_date_parsed"), HISTORY_WINDOW_DAYS))
        )
        .select(F.col("cur.claim_id").alias("claim_id"), F.col("h.historical_claim_id").alias("historical_claim_id"))
    )
    return joined.groupBy("claim_id").agg(
        F.countDistinct("historical_claim_id").alias("same_procedure_claim_count_365d")
    )


def _duplicate_candidates(exploded_current: DataFrame, historical: DataFrame) -> DataFrame:
    """Same provider, same procedure, service date within 30 days, amount within 5%."""
    joined = (
        exploded_current.alias("cur")
        .join(
            historical.alias("h"),
            (F.col("cur.member_id") == F.col("h.member_id"))
            & (F.col("cur.provider_id") == F.col("h.provider_id"))
            & (F.col("cur.procedure_code") == F.col("h.procedure_code")),
            "inner",
        )
        .filter(
            F.col("cur.current_procedure_billed_amount").isNotNull()
            & F.col("cur.service_date_parsed").isNotNull()
            & F.col("h.service_date").isNotNull()
            & (F.abs(F.datediff(F.col("h.service_date"), F.col("cur.service_date_parsed"))) <= DUPLICATE_WINDOW_DAYS)
            & (
                F.abs(F.col("h.claim_amount") - F.col("cur.current_procedure_billed_amount"))
                <= (F.col("cur.current_procedure_billed_amount") * DUPLICATE_AMOUNT_TOLERANCE)
            )
        )
        .select(F.col("cur.claim_id").alias("claim_id"), F.col("h.historical_claim_id").alias("historical_claim_id"))
    )
    return joined.groupBy("claim_id").agg(F.countDistinct("historical_claim_id").alias("candidate_duplicate_count"))


def _add_risk_score(df: DataFrame) -> DataFrame:
    score = (
        F.when(F.col("duplicate_claim_flag"), F.lit(50)).otherwise(F.lit(0))
        + F.when(F.col("prior_claim_count_365d") > 10, F.lit(20)).otherwise(F.lit(0))
        + F.when(F.col("same_procedure_claim_count_365d") > 3, F.lit(15)).otherwise(F.lit(0))
        + F.when(
            F.col("historical_average_claim_amount").isNotNull()
            & F.col("billed_amount_parsed").isNotNull()
            & (F.col("billed_amount_parsed") > (F.col("historical_average_claim_amount") * 3)),
            F.lit(15),
        ).otherwise(F.lit(0))
    )
    return df.withColumn("fraud_risk_score", F.least(F.lit(100), score))


def _select_job4_schema(df: DataFrame) -> DataFrame:
    return df.select(
        *JOB3_COLUMNS,
        "prior_claim_count_365d",
        "prior_claim_amount_365d",
        "same_procedure_claim_count_365d",
        "most_recent_prior_service_date",
        "historical_average_claim_amount",
        "candidate_duplicate_count",
        "duplicate_claim_flag",
        "fraud_risk_score",
        "historical_evidence",
        "job4_processed_at",
    )


def run() -> None:
    spark = get_spark_session()
    try:
        logger.info("Job 4 starting")

        claims = _read_claims(spark)
        input_count = claims.count()
        logger.info("Read %d coverage-enriched claims", input_count)

        historical = _read_historical_claims(spark)
        exploded_current = _explode_current_procedures(claims)

        window_joined = _historical_window_join(claims, historical)
        metrics = _historical_metrics(window_joined)
        evidence = _historical_evidence(window_joined)
        same_procedure = _same_procedure_counts(exploded_current, historical)
        duplicates = _duplicate_candidates(exploded_current, historical)

        enriched = (
            claims.join(metrics, on="claim_id", how="left")
            .join(evidence, on="claim_id", how="left")
            .join(same_procedure, on="claim_id", how="left")
            .join(duplicates, on="claim_id", how="left")
            .withColumn("prior_claim_count_365d", F.coalesce(F.col("prior_claim_count_365d"), F.lit(0)))
            .withColumn("prior_claim_amount_365d", F.coalesce(F.col("prior_claim_amount_365d"), F.lit(0.0)))
            .withColumn(
                "same_procedure_claim_count_365d",
                F.coalesce(F.col("same_procedure_claim_count_365d"), F.lit(0)),
            )
            .withColumn("candidate_duplicate_count", F.coalesce(F.col("candidate_duplicate_count"), F.lit(0)))
            .withColumn("duplicate_claim_flag", F.col("candidate_duplicate_count") > 0)
            .withColumn("historical_evidence", F.coalesce(F.col("historical_evidence"), F.lit("[]")))
        )

        enriched = _add_risk_score(enriched)
        enriched = enriched.withColumn("job4_processed_at", F.current_timestamp())

        final_df = _select_job4_schema(enriched)
        output_count = final_df.count()
        logger.info("Historical analysis produced %d rows", output_count)

        output_path = f"{settings.output_path}/historical_enriched_claims"
        write_output(final_df, output_path, settings.output_format)
        logger.info("Job 4 complete: %d rows written to %s", output_count, output_path)
    finally:
        spark.stop()
