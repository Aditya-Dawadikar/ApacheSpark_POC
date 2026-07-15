"""Job 6: Evidence Assembly and Persist.

Nested evidence (per-procedure coverage/authorization detail, historical
matches) is built as JSON strings from the moment it's created in Jobs 3-5,
rather than as native Spark struct/array<struct> columns. CSV has no way to
represent those types, and this project writes every job's output to CSV, so
representing evidence as pre-serialized JSON text avoids a lossy round trip:
evidence_package here is a JSON object whose evidence fields are themselves
JSON text, which is a deliberate simplification, not a bug.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_app.config import settings
from spark_app.io_utils import read_job_output, write_output
from spark_app.observability.metrics import finalize_spark_session
from spark_app.session import get_spark_session

logger = logging.getLogger(__name__)

RULE_ENGINE_VERSION = "1.0.0"


def _read_claims(spark: SparkSession) -> DataFrame:
    path = f"{settings.output_path}/authorization_enriched_claims"
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
            "authorized_procedure_codes",
            "missing_authorization_procedure_codes",
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


def _build_review_reasons(df: DataFrame) -> DataFrame:
    derived = F.filter(
        F.array(
            F.when(F.size(F.col("missing_fields")) > 0, F.lit("OCR_REQUIRED_FIELD_MISSING")),
            F.when(
                F.array_contains(F.col("validation_errors"), "LOW_OCR_CONFIDENCE"),
                F.lit("OCR_LOW_CONFIDENCE"),
            ),
            F.when(
                F.array_contains(F.col("validation_errors"), "UNREADABLE_DOCUMENT"),
                F.lit("OCR_UNREADABLE_DOCUMENT"),
            ),
            F.when(F.col("cross_document_conflict"), F.lit("OCR_CROSS_DOCUMENT_CONFLICT")),
            F.when(
                F.array_contains(F.col("validation_errors"), "INVALID_BILLED_AMOUNT")
                | F.array_contains(F.col("validation_errors"), "NEGATIVE_BILLED_AMOUNT"),
                F.lit("INVALID_BILLED_AMOUNT"),
            ),
            F.when(
                F.array_contains(F.col("validation_errors"), "INVALID_SERVICE_DATE")
                | F.array_contains(F.col("validation_errors"), "SERVICE_DATE_OUTSIDE_STAY")
                | F.array_contains(F.col("validation_errors"), "ADMISSION_AFTER_DISCHARGE"),
                F.lit("INVALID_SERVICE_DATE"),
            ),
            F.when(F.col("pre_authorization_status") == "MISSING", F.lit("PRE_AUTHORIZATION_MISSING")),
            F.when(F.col("pre_authorization_status") == "EXPIRED", F.lit("PRE_AUTHORIZATION_EXPIRED")),
            F.when(F.col("pre_authorization_status") == "PENDING", F.lit("PRE_AUTHORIZATION_PENDING")),
            F.when(F.col("pre_authorization_status") == "DENIED", F.lit("PRE_AUTHORIZATION_DENIED")),
            F.when(F.col("duplicate_claim_flag"), F.lit("LIKELY_DUPLICATE_CLAIM")),
        ),
        lambda x: x.isNotNull(),
    )

    # reference_enrichment_errors and coverage_errors already use the exact
    # literal codes this catalog expects (POLICY_NOT_FOUND, PROCEDURE_NOT_COVERED,
    # ...), so they're unioned in directly rather than re-derived.
    combined = F.array_distinct(
        F.flatten(
            F.array(
                derived,
                F.coalesce(F.col("reference_enrichment_errors"), F.array()),
                F.coalesce(F.col("coverage_errors"), F.array()),
            )
        )
    )

    return df.withColumn("review_reasons", combined)


def _build_evidence_package(df: DataFrame) -> DataFrame:
    data_quality_evidence = F.to_json(
        F.struct(
            F.col("validation_errors"),
            F.col("normalization_warnings"),
            F.col("missing_fields"),
            F.col("pii_detected"),
            F.col("pii_types"),
            F.col("duplicate_document_flag"),
            F.col("cross_document_conflict"),
            F.col("cross_document_conflict_fields"),
            F.col("document_count"),
            F.col("selected_ocr_confidence"),
            F.col("max_ocr_confidence"),
            F.col("avg_ocr_confidence"),
        )
    )

    policy_evidence = F.to_json(
        F.struct(
            F.col("policy_exists"),
            F.col("policy_ambiguous"),
            F.col("policy_match_count"),
            F.col("policy_status"),
            F.col("coverage_start_date"),
            F.col("coverage_end_date"),
            F.col("policy_active_on_service_date"),
        )
    )

    member_evidence = F.to_json(
        F.struct(
            F.col("member_exists"),
            F.col("first_name_match"),
            F.col("last_name_match"),
            F.col("dob_match"),
            F.col("entity_match_confidence"),
            F.col("member_eligible"),
        )
    )

    provider_evidence = F.to_json(
        F.struct(
            F.col("provider_exists"),
            F.col("provider_ambiguous"),
            F.col("provider_active"),
            F.col("provider_network_status"),
            F.col("provider_match_count"),
        )
    )

    evidence_package = F.to_json(
        F.struct(
            data_quality_evidence.alias("data_quality_evidence"),
            policy_evidence.alias("policy_evidence"),
            member_evidence.alias("member_evidence"),
            provider_evidence.alias("provider_evidence"),
            F.col("coverage_evidence").alias("coverage_evidence"),
            F.col("historical_evidence").alias("historical_evidence"),
            F.col("authorization_evidence").alias("authorization_evidence"),
        )
    )

    return df.withColumn("evidence_package", evidence_package)


def _derive_workflow(df: DataFrame) -> DataFrame:
    review_required = F.size(F.col("review_reasons")) > 0
    workflow_status = F.when(review_required, F.lit("MANUAL_REVIEW_REQUIRED")).otherwise(F.lit("ETL_COMPLETE"))
    decision_reason = F.when(
        review_required,
        F.concat(F.lit("Manual review required: "), F.array_join(F.col("review_reasons"), "; ")),
    ).otherwise(F.lit("Automated processing complete; no review reasons identified"))

    return (
        df.withColumn("review_required", review_required)
        .withColumn("workflow_status", workflow_status)
        .withColumn("decision_reason", decision_reason)
    )


def _select_final_schema(df: DataFrame) -> DataFrame:
    return df.select(
        F.concat(F.lit("CASE-"), F.col("claim_id")).alias("case_id"),
        F.col("claim_id"),
        F.col("source_document_ids"),
        F.col("document_count"),
        F.col("member_id"),
        F.col("policy_id"),
        F.col("provider_id"),
        F.col("plan_id"),
        F.col("service_date_parsed").alias("service_date"),
        F.col("admission_date_parsed").alias("admission_date"),
        F.col("discharge_date_parsed").alias("discharge_date"),
        F.col("diagnosis_codes"),
        F.col("procedure_codes"),
        F.col("billed_amount_parsed").alias("billed_amount"),
        F.col("estimated_eligible_amount"),
        F.lit(None).cast("double").alias("approved_amount"),
        F.col("currency_normalized").alias("currency"),
        F.col("policy_status"),
        F.col("policy_active_on_service_date"),
        F.col("member_eligible"),
        F.col("entity_match_confidence"),
        F.col("provider_active"),
        F.col("provider_network_status"),
        F.col("pre_authorization_status"),
        F.col("duplicate_claim_flag"),
        F.col("fraud_risk_score"),
        F.col("workflow_status"),
        F.col("review_required"),
        F.col("review_reasons"),
        F.col("decision_reason"),
        F.col("selected_ocr_confidence"),
        F.col("max_ocr_confidence"),
        F.col("avg_ocr_confidence"),
        F.col("evidence_package"),
        F.lit(RULE_ENGINE_VERSION).alias("rule_engine_version"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )


def run() -> None:
    spark = get_spark_session()
    try:
        logger.info("Job 6 starting")

        claims = _read_claims(spark)
        input_count = claims.count()
        logger.info("Read %d authorization-enriched claims", input_count)

        enriched = _build_review_reasons(claims)
        enriched = _build_evidence_package(enriched)
        enriched = _derive_workflow(enriched)

        final_df = _select_final_schema(enriched)
        output_count = final_df.count()
        review_count = final_df.filter(F.col("review_required")).count()
        logger.info(
            "Case assembly produced %d claim cases (%d flagged for manual review)",
            output_count,
            review_count,
        )

        output_path = f"{settings.output_path}/claim_case"
        write_output(final_df, output_path, settings.output_format)
        logger.info("Job 6 complete: %d rows written to %s", output_count, output_path)
    finally:
        finalize_spark_session(spark, "job_06")
