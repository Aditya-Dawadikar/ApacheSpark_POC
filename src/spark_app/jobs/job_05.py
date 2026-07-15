"""Job 5: Authorization Validation."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_app.config import settings
from spark_app.io_utils import read_csv, read_job_output, write_output
from spark_app.observability.metrics import finalize_spark_session
from spark_app.session import get_spark_session

logger = logging.getLogger(__name__)

JOB4_COLUMNS = [
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
]

# Claim-level rollup priority (lower number wins): AMBIGUOUS > DENIED > PENDING >
# EXPIRED > MISSING > VALID > NOT_REQUIRED, per SPEC.md Stage 5.3.
RESULT_PRIORITY = {
    "AMBIGUOUS": 1,
    "DENIED": 2,
    "PENDING": 3,
    "EXPIRED": 4,
    "MISSING": 5,
    "VALID": 6,
    "NOT_REQUIRED": 7,
}
PRIORITY_TO_RESULT = {v: k for k, v in RESULT_PRIORITY.items()}


def _read_claims(spark: SparkSession) -> DataFrame:
    path = f"{settings.output_path}/historical_enriched_claims"
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
            "most_recent_prior_service_date",
            "job4_processed_at",
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
            "duplicate_claim_flag",
        ],
        double_columns=[
            "billed_amount_parsed",
            "selected_ocr_confidence",
            "max_ocr_confidence",
            "avg_ocr_confidence",
            "entity_match_confidence",
            "estimated_eligible_amount",
            "prior_claim_amount_365d",
            "historical_average_claim_amount",
        ],
        int_columns=[
            "document_count",
            "policy_match_count",
            "provider_match_count",
            "covered_procedure_count",
            "uncovered_procedure_count",
            "prior_claim_count_365d",
            "same_procedure_claim_count_365d",
            "candidate_duplicate_count",
            "fraud_risk_score",
        ],
    )


def _read_authorizations(spark: SparkSession) -> DataFrame:
    path = f"{settings.input_path}/fact_pre_authorization.csv"
    return (
        read_csv(spark, path)
        .withColumn("valid_from", F.to_date(F.col("valid_from")))
        .withColumn("valid_to", F.to_date(F.col("valid_to")))
    )


def _explode_required_procedures(claims: DataFrame) -> DataFrame:
    """One row per claim and pre-auth-required procedure; explode_outer keeps
    claims that require no pre-authorization present with a null placeholder."""
    return claims.select(
        "claim_id",
        "member_id",
        "provider_id",
        "service_date_parsed",
        F.explode_outer("pre_auth_required_procedure_codes").alias("procedure_code"),
    )


def _join_authorizations(required: DataFrame, authorizations: DataFrame) -> DataFrame:
    joined = required.alias("r").join(
        authorizations.alias("a"),
        (F.col("r.member_id") == F.col("a.member_id"))
        & (F.col("r.provider_id") == F.col("a.provider_id"))
        & (F.col("r.procedure_code") == F.col("a.procedure_code")),
        "left",
    )

    is_valid = (
        (F.col("a.authorization_status") == "APPROVED")
        & F.col("r.service_date_parsed").isNotNull()
        & F.col("r.service_date_parsed").between(F.col("a.valid_from"), F.col("a.valid_to"))
    )

    scored = joined.select(
        F.col("r.claim_id").alias("claim_id"),
        F.col("r.procedure_code").alias("procedure_code"),
        F.col("a.authorization_id").alias("authorization_id"),
        F.col("a.authorization_status").alias("authorization_status"),
        F.col("a.valid_from").alias("valid_from"),
        F.col("a.valid_to").alias("valid_to"),
        F.coalesce(is_valid, F.lit(False)).alias("is_valid"),
    )

    # Reference data has at most one authorization per (member, provider, procedure)
    # today, but we rank/dedupe defensively per the duplicate-handling invariant:
    # prefer a valid match, then the one with the latest expiry, as the
    # procedure's representative authorization record.
    count_window = Window.partitionBy("claim_id", "procedure_code")
    rank_window = Window.partitionBy("claim_id", "procedure_code").orderBy(
        F.col("is_valid").cast("int").desc(),
        F.col("valid_to").desc_nulls_last(),
    )

    return (
        scored.withColumn("authorization_match_count", F.count(F.col("authorization_id")).over(count_window))
        .withColumn("valid_authorization_count", F.sum(F.col("is_valid").cast("int")).over(count_window))
        .withColumn("rn", F.row_number().over(rank_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def _classify_result(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "authorization_result",
        F.when(F.col("procedure_code").isNull(), F.lit("NOT_REQUIRED"))
        .when(F.col("authorization_match_count") == 0, F.lit("MISSING"))
        .when(F.col("authorization_match_count") > 1, F.lit("AMBIGUOUS"))
        .when(F.col("is_valid"), F.lit("VALID"))
        .when(F.col("authorization_status") == "DENIED", F.lit("DENIED"))
        .when(F.col("authorization_status") == "PENDING", F.lit("PENDING"))
        .when(F.col("authorization_status") == "CANCELLED", F.lit("CANCELLED"))
        .when(F.col("authorization_status") == "APPROVED", F.lit("EXPIRED"))
        .otherwise(F.lit("MISSING")),
    )


def _aggregate_by_claim(classified: DataFrame) -> DataFrame:
    priority_map = F.create_map(*[F.lit(x) for kv in RESULT_PRIORITY.items() for x in kv])
    reverse_map = F.create_map(*[F.lit(x) for kv in PRIORITY_TO_RESULT.items() for x in kv])

    with_priority = classified.withColumn("result_priority", priority_map[F.col("authorization_result")])

    evidence_struct = F.struct(
        F.col("procedure_code"),
        F.col("authorization_result"),
        F.col("authorization_id"),
        F.col("authorization_status"),
        F.col("valid_from"),
        F.col("valid_to"),
    )

    per_claim = with_priority.groupBy("claim_id").agg(
        F.array_distinct(
            F.collect_list(F.when(F.col("authorization_result") == "VALID", F.col("procedure_code")))
        ).alias("authorized_procedure_codes"),
        F.array_distinct(
            F.collect_list(
                F.when(
                    F.col("procedure_code").isNotNull()
                    & (~F.col("authorization_result").isin("VALID", "NOT_REQUIRED")),
                    F.col("procedure_code"),
                )
            )
        ).alias("missing_authorization_procedure_codes"),
        F.to_json(F.collect_list(F.when(F.col("procedure_code").isNotNull(), evidence_struct))).alias(
            "authorization_evidence"
        ),
        F.min("result_priority").alias("claim_result_priority"),
    )

    return per_claim.withColumn(
        "pre_authorization_status", reverse_map[F.col("claim_result_priority")]
    ).drop("claim_result_priority")


def _select_job5_schema(df: DataFrame) -> DataFrame:
    return df.select(
        *JOB4_COLUMNS,
        "authorized_procedure_codes",
        "missing_authorization_procedure_codes",
        "authorization_evidence",
        "pre_authorization_status",
    )


def run() -> None:
    spark = get_spark_session()
    try:
        logger.info("Job 5 starting")

        claims = _read_claims(spark)
        input_count = claims.count()
        logger.info("Read %d historical-enriched claims", input_count)

        authorizations = _read_authorizations(spark)

        required = _explode_required_procedures(claims)
        joined = _join_authorizations(required, authorizations)
        classified = _classify_result(joined)
        per_claim = _aggregate_by_claim(classified)

        enriched = claims.join(per_claim, on="claim_id", how="left")
        final_df = _select_job5_schema(enriched)
        output_count = final_df.count()
        logger.info("Authorization validation produced %d rows", output_count)

        output_path = f"{settings.output_path}/authorization_enriched_claims"
        write_output(final_df, output_path, settings.output_format)
        logger.info("Job 5 complete: %d rows written to %s", output_count, output_path)
    finally:
        finalize_spark_session(spark, "job_05")
