"""Job 3: Procedure Coverage."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_app.config import settings
from spark_app.io_utils import read_csv, read_job_output, write_output
from spark_app.session import get_spark_session

logger = logging.getLogger(__name__)

JOB2_COLUMNS = [
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
]


def _read_claims(spark: SparkSession) -> DataFrame:
    path = f"{settings.output_path}/reference_enriched_claims"
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
        ],
        double_columns=[
            "billed_amount_parsed",
            "selected_ocr_confidence",
            "max_ocr_confidence",
            "avg_ocr_confidence",
            "entity_match_confidence",
        ],
        int_columns=["document_count", "policy_match_count", "provider_match_count"],
    )


def _prepare_coverage(dim_plan_coverage: DataFrame) -> DataFrame:
    coverage = (
        dim_plan_coverage.withColumn("coverage_percentage", F.col("coverage_percentage").cast("double"))
        .withColumn("deductible_amount", F.col("deductible_amount").cast("double"))
        .withColumn("copay_amount", F.col("copay_amount").cast("double"))
        .withColumn("requires_pre_authorization", F.col("requires_pre_authorization").cast("boolean"))
    )

    # dim_plan_coverage is documented as one row per plan-procedure pair, but we
    # still rank/dedupe defensively per the "every many-to-one join handles
    # duplicates" invariant, rather than assuming the reference data stays clean.
    rank_window = Window.partitionBy("plan_id", "procedure_code").orderBy(F.col("coverage_type").asc())
    return (
        coverage.withColumn("rn", F.row_number().over(rank_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "plan_id",
            "procedure_code",
            "coverage_type",
            "coverage_percentage",
            "deductible_amount",
            "copay_amount",
            "requires_pre_authorization",
        )
    )


def _explode_procedures(claims: DataFrame) -> DataFrame:
    """One row per claim-procedure code; explode_outer keeps claims with no procedures."""
    return (
        claims.withColumn("procedure_count", F.size(F.coalesce(F.col("procedure_codes"), F.array())))
        .select(
            "claim_id",
            "plan_id",
            "billed_amount_parsed",
            "procedure_count",
            F.posexplode_outer("procedure_codes").alias("procedure_index", "procedure_code"),
        )
    )


def _join_coverage(claim_procedures: DataFrame, coverage: DataFrame) -> DataFrame:
    joined = claim_procedures.join(
        coverage,
        on=["plan_id", "procedure_code"],
        how="left",
    )

    return (
        joined.withColumn("coverage_found", F.col("coverage_type").isNotNull())
        .withColumn(
            "procedure_billed_amount",
            F.when(F.col("procedure_count") > 0, F.col("billed_amount_parsed") / F.col("procedure_count")),
        )
        .withColumn(
            "estimated_eligible_amount",
            F.when(
                F.col("coverage_found"),
                F.greatest(
                    F.lit(0.0),
                    F.col("procedure_billed_amount") * F.col("coverage_percentage") - F.col("copay_amount"),
                ),
            ),
        )
    )


def _aggregate_by_claim(joined: DataFrame) -> DataFrame:
    has_procedure = F.col("procedure_code").isNotNull()

    evidence_struct = F.struct(
        F.col("procedure_code"),
        F.col("coverage_found"),
        F.col("coverage_type"),
        F.col("coverage_percentage"),
        F.col("deductible_amount"),
        F.col("copay_amount"),
        F.col("requires_pre_authorization"),
        F.col("procedure_billed_amount"),
        F.col("estimated_eligible_amount"),
    )

    per_claim = joined.groupBy("claim_id").agg(
        F.array_distinct(
            F.collect_list(F.when(has_procedure & F.col("coverage_found"), F.col("procedure_code")))
        ).alias("covered_procedure_codes"),
        F.array_distinct(
            F.collect_list(F.when(has_procedure & (~F.col("coverage_found")), F.col("procedure_code")))
        ).alias("uncovered_procedure_codes"),
        F.array_distinct(
            F.collect_list(
                F.when(has_procedure & F.col("requires_pre_authorization"), F.col("procedure_code"))
            )
        ).alias("pre_auth_required_procedure_codes"),
        F.to_json(F.collect_list(F.when(has_procedure, evidence_struct))).alias("coverage_evidence"),
        F.sum("estimated_eligible_amount").alias("estimated_eligible_amount"),
    )

    return (
        per_claim.withColumn("covered_procedure_count", F.size("covered_procedure_codes"))
        .withColumn("uncovered_procedure_count", F.size("uncovered_procedure_codes"))
        .withColumn(
            "all_procedures_have_coverage",
            (F.col("uncovered_procedure_count") == 0) & (F.col("covered_procedure_count") > 0),
        )
        .withColumn(
            "coverage_errors",
            F.filter(
                F.array(F.when(F.col("uncovered_procedure_count") > 0, F.lit("PROCEDURE_NOT_COVERED"))),
                lambda x: x.isNotNull(),
            ),
        )
        .withColumn("job3_processed_at", F.current_timestamp())
    )


def _select_job3_schema(df: DataFrame) -> DataFrame:
    return df.select(
        *JOB2_COLUMNS,
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
    )


def run() -> None:
    spark = get_spark_session()
    try:
        logger.info("Job 3 starting")

        claims = _read_claims(spark)
        input_count = claims.count()
        logger.info("Read %d reference-enriched claims", input_count)

        dim_plan_coverage = read_csv(spark, f"{settings.input_path}/dim_plan_coverage.csv")
        coverage = _prepare_coverage(dim_plan_coverage)

        claim_procedures = _explode_procedures(claims)
        joined = _join_coverage(claim_procedures, coverage)
        per_claim = _aggregate_by_claim(joined)

        enriched = claims.join(per_claim, on="claim_id", how="left")
        final_df = _select_job3_schema(enriched)
        output_count = final_df.count()
        logger.info("Procedure coverage evaluated for %d claims", output_count)

        output_path = f"{settings.output_path}/coverage_enriched_claims"
        write_output(final_df, output_path, settings.output_format)
        logger.info("Job 3 complete: %d rows written to %s", output_count, output_path)
    finally:
        spark.stop()
