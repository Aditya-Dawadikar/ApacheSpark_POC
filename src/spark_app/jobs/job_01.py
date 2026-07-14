"""Job 1: OCR validation and canonicalization."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_app.config import settings
from spark_app.io_utils import write_output
from spark_app.session import get_spark_session

logger = logging.getLogger(__name__)

OCR_COLUMNS = [
    "source_document_id",
    "claim_id",
    "policy_number",
    "member_first_name",
    "member_last_name",
    "member_dob",
    "provider_npi",
    "service_date",
    "admission_date",
    "discharge_date",
    "diagnosis_codes",
    "procedure_codes",
    "billed_amount",
    "currency",
    "ocr_confidence",
    "extracted_ssn",
    "extracted_notes",
    "expected_quality",
    "noise_type",
]

CONFIDENCE_THRESHOLD = 0.75


def _read_raw_ocr(spark: SparkSession, input_root: str) -> DataFrame:
    """Read OCR input as all-string columns and add ingestion metadata."""
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in OCR_COLUMNS])
    path = f"{input_root}/ocr_claims.csv"

    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .schema(schema)
        .csv(path)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("raw_record_hash", F.sha2(F.concat_ws("||", *[F.col(c) for c in OCR_COLUMNS]), 256))
    )


def _norm_text(col_name: str, upper: bool = False):
    """Normalize whitespace and optionally uppercase text values."""
    c = F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " ")
    return F.upper(c) if upper else c


def _parse_date_multi(col_name: str):
    """Parse a date using supported OCR formats."""
    c = F.trim(F.col(col_name))
    return F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),
        F.to_date(c, "MM/dd/yyyy"),
        F.to_date(c, "yyyy/MM/dd"),
        F.to_date(c, "dd-MM-yyyy"),
    )


_NULL_LITERAL_CODES = ("N/A", "NA", "NULL", "NONE")


def _parse_codes(col_name: str):
    """Split and normalize delimited diagnosis/procedure code strings."""
    raw = F.upper(F.trim(F.coalesce(F.col(col_name), F.lit(""))))
    raw = F.when(raw.isin(*_NULL_LITERAL_CODES), F.lit("")).otherwise(raw)
    cleaned = F.regexp_replace(raw, r"[;,\r\n]+", "|")
    parts = F.split(cleaned, r"\|")
    normalized = F.transform(parts, lambda x: F.trim(x))
    non_blank = F.filter(normalized, lambda x: x.isNotNull() & (x != ""))
    return F.array_distinct(non_blank)


def _parse_amount(col_name: str):
    """Parse billed amounts while handling common OCR formatting variants."""
    raw = F.trim(F.col(col_name))
    no_currency = F.regexp_replace(raw, r"[$€£\s]", "")
    comma_decimal = F.when(no_currency.rlike(r"^-?\d+,\d{2}$"), F.regexp_replace(no_currency, ",", "."))
    plain = F.when(no_currency.rlike(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$"), F.regexp_replace(no_currency, ",", ""))
    fallback = F.when(no_currency.rlike(r"^-?\d+(\.\d+)?$"), no_currency)
    parsed = F.coalesce(comma_decimal, plain, fallback)
    return parsed.cast("double")


def _normalize(df: DataFrame) -> DataFrame:
    """Add normalized columns while preserving original raw columns."""
    return (
        df.withColumn("policy_number_normalized", _norm_text("policy_number", upper=True))
        .withColumn("member_first_name_normalized", _norm_text("member_first_name", upper=True))
        .withColumn("member_last_name_normalized", _norm_text("member_last_name", upper=True))
        .withColumn("member_dob_parsed", _parse_date_multi("member_dob"))
        .withColumn("provider_npi_normalized", F.regexp_replace(_norm_text("provider_npi"), r"[^0-9]", ""))
        .withColumn("service_date_parsed", _parse_date_multi("service_date"))
        .withColumn("admission_date_parsed", _parse_date_multi("admission_date"))
        .withColumn("discharge_date_parsed", _parse_date_multi("discharge_date"))
        .withColumn("diagnosis_code_array", _parse_codes("diagnosis_codes"))
        .withColumn("procedure_code_array", _parse_codes("procedure_codes"))
        .withColumn("billed_amount_parsed", _parse_amount("billed_amount"))
        .withColumn("currency_normalized", F.upper(F.trim(F.col("currency"))))
        .withColumn("ocr_confidence_parsed", F.trim(F.col("ocr_confidence")).cast("double"))
    )


def _validate(df: DataFrame) -> DataFrame:
    """Generate missing fields, validation errors, and normalization warnings."""
    missing_fields = F.filter(
        F.array(
            F.when(F.col("claim_id").isNull() | (F.trim("claim_id") == ""), F.lit("claim_id")),
            F.when(F.col("policy_number_normalized").isNull() | (F.col("policy_number_normalized") == ""), F.lit("policy_number")),
            F.when(
                (F.col("member_first_name_normalized").isNull() | (F.col("member_first_name_normalized") == ""))
                | (F.col("member_last_name_normalized").isNull() | (F.col("member_last_name_normalized") == "")),
                F.lit("member_name"),
            ),
            F.when(F.col("provider_npi_normalized").isNull() | (F.col("provider_npi_normalized") == ""), F.lit("provider_npi")),
            F.when(F.size(F.col("procedure_code_array")) == 0, F.lit("procedure_codes")),
        ),
        lambda x: x.isNotNull(),
    )

    header_label_values = [
        "POLICY_NUMBER",
        "PATIENT LAST NAME",
        "PATIENT FIRST NAME",
        "DATE OF SERVICE",
    ]

    validation_errors = F.filter(
        F.array(
            F.when(F.col("claim_id").isNull() | (F.trim("claim_id") == ""), F.lit("MISSING_CLAIM_ID")),
            F.when(F.col("policy_number_normalized").isNull() | (F.col("policy_number_normalized") == ""), F.lit("MISSING_POLICY_NUMBER")),
            F.when(F.col("policy_number_normalized").isNotNull() & (~F.col("policy_number_normalized").rlike(r"^POL-\d+$")), F.lit("INVALID_POLICY_NUMBER")),
            F.when(
                (F.col("member_first_name_normalized").isNull() | (F.col("member_first_name_normalized") == ""))
                | (F.col("member_last_name_normalized").isNull() | (F.col("member_last_name_normalized") == "")),
                F.lit("MISSING_MEMBER_NAME"),
            ),
            F.when(F.col("member_dob").isNotNull() & F.col("member_dob_parsed").isNull(), F.lit("INVALID_MEMBER_DOB")),
            F.when(F.col("provider_npi_normalized").isNull() | (F.col("provider_npi_normalized") == ""), F.lit("MISSING_PROVIDER_NPI")),
            F.when(F.col("provider_npi_normalized").isNotNull() & (~F.col("provider_npi_normalized").rlike(r"^\d{10}$")), F.lit("INVALID_PROVIDER_NPI")),
            F.when(F.col("service_date").isNotNull() & F.col("service_date_parsed").isNull(), F.lit("INVALID_SERVICE_DATE")),
            F.when(
                F.col("admission_date_parsed").isNotNull()
                & F.col("discharge_date_parsed").isNotNull()
                & (F.col("admission_date_parsed") > F.col("discharge_date_parsed")),
                F.lit("ADMISSION_AFTER_DISCHARGE"),
            ),
            F.when(
                F.col("service_date_parsed").isNotNull()
                & F.col("admission_date_parsed").isNotNull()
                & F.col("discharge_date_parsed").isNotNull()
                & (
                    (F.col("service_date_parsed") < F.col("admission_date_parsed"))
                    | (F.col("service_date_parsed") > F.col("discharge_date_parsed"))
                ),
                F.lit("SERVICE_DATE_OUTSIDE_STAY"),
            ),
            F.when(F.size(F.col("procedure_code_array")) == 0, F.lit("MISSING_PROCEDURE_CODES")),
            F.when(F.col("billed_amount").isNotNull() & F.col("billed_amount_parsed").isNull(), F.lit("INVALID_BILLED_AMOUNT")),
            F.when(F.col("billed_amount_parsed") < 0, F.lit("NEGATIVE_BILLED_AMOUNT")),
            F.when(F.col("currency_normalized").isNotNull() & (~F.col("currency_normalized").isin("USD")), F.lit("UNSUPPORTED_CURRENCY")),
            F.when(F.col("ocr_confidence_parsed").isNull(), F.lit("INVALID_OCR_CONFIDENCE")),
            F.when(F.col("ocr_confidence_parsed") < F.lit(CONFIDENCE_THRESHOLD), F.lit("LOW_OCR_CONFIDENCE")),
            F.when(F.lower(F.coalesce(F.col("noise_type"), F.lit(""))).contains("unreadable"), F.lit("UNREADABLE_DOCUMENT")),
            F.when(
                F.col("policy_number_normalized").isin(*header_label_values)
                | F.upper(F.trim(F.coalesce(F.col("member_last_name"), F.lit("")))).isin(*header_label_values)
                | F.upper(F.trim(F.coalesce(F.col("service_date"), F.lit("")))).isin(*header_label_values),
                F.lit("HEADER_TEXT_EXTRACTED_AS_VALUE"),
            ),
            F.when(
                F.col("policy_number").rlike(r"[|;,/]") | F.col("provider_npi").rlike(r"[|;,/]"),
                F.lit("MULTIPLE_VALUES_EXTRACTED_IN_SINGLE_FIELD"),
            ),
        ),
        lambda x: x.isNotNull(),
    )

    normalization_warnings = F.filter(
        F.array(
            F.when(F.col("provider_npi").isNotNull() & (F.col("provider_npi") != F.col("provider_npi_normalized")), F.lit("NPI_PUNCTUATION_NORMALIZED")),
            F.when(F.col("diagnosis_codes").rlike(r"[;,]"), F.lit("DIAGNOSIS_DELIMITER_NORMALIZED")),
            F.when(F.col("procedure_codes").rlike(r"[;,]"), F.lit("PROCEDURE_DELIMITER_NORMALIZED")),
            F.when(F.col("billed_amount").rlike(r","), F.lit("BILLED_AMOUNT_COMMA_INTERPRETED")),
        ),
        lambda x: x.isNotNull(),
    )

    return (
        df.withColumn("missing_fields", missing_fields)
        .withColumn("validation_errors", validation_errors)
        .withColumn("normalization_warnings", normalization_warnings)
        .withColumn("is_valid_record", F.size(F.col("validation_errors")) == 0)
    )


def _pii(df: DataFrame) -> DataFrame:
    """Detect PII indicators and redact sensitive tokens in notes."""
    ssn_pattern = r"\b\d{3}-?\d{2}-?\d{4}\b"
    phone_pattern = r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"

    notes = F.coalesce(F.col("extracted_notes"), F.lit(""))
    pii_types = F.filter(
        F.array(
            F.when(F.col("extracted_ssn").isNotNull() & (F.trim(F.col("extracted_ssn")) != ""), F.lit("SSN_FIELD")),
            F.when(notes.rlike(ssn_pattern), F.lit("SSN_IN_NOTES")),
            F.when(notes.rlike(phone_pattern), F.lit("PHONE_IN_NOTES")),
        ),
        lambda x: x.isNotNull(),
    )

    notes_redacted = F.regexp_replace(F.regexp_replace(notes, ssn_pattern, "[REDACTED_SSN]"), phone_pattern, "[REDACTED_PHONE]")
    pii_hash = F.when(
        F.size(pii_types) > 0,
        F.sha2(F.concat_ws("|", F.coalesce(F.col("claim_id"), F.lit("")), F.coalesce(F.col("extracted_ssn"), F.lit(""))), 256),
    )

    return (
        df.withColumn("pii_types", pii_types)
        .withColumn("pii_detected", F.size(F.col("pii_types")) > 0)
        .withColumn("notes_redacted", notes_redacted)
        .withColumn("pii_hash", pii_hash)
    )


def _canonicalize(df: DataFrame) -> DataFrame:
    """Select one canonical OCR row per claim and aggregate duplicate evidence."""
    conflict_fields = [
        "policy_number_normalized",
        "member_first_name_normalized",
        "member_last_name_normalized",
        "member_dob_parsed",
        "provider_npi_normalized",
        "service_date_parsed",
        "billed_amount_parsed",
        "currency_normalized",
    ]

    per_claim_agg = (
        df.groupBy("claim_id")
        .agg(
            F.count("*").alias("document_count"),
            F.collect_set("source_document_id").alias("source_document_ids"),
            F.max("ocr_confidence_parsed").alias("max_ocr_confidence"),
            F.avg("ocr_confidence_parsed").alias("avg_ocr_confidence"),
            F.array_distinct(F.flatten(F.collect_list("diagnosis_code_array"))).alias("diagnosis_codes_union"),
            F.array_distinct(F.flatten(F.collect_list("procedure_code_array"))).alias("procedure_codes_union"),
            *[F.countDistinct(F.col(c)).alias(f"{c}_distinct_count") for c in conflict_fields],
        )
        .withColumn("duplicate_document_flag", F.col("document_count") > 1)
    )

    cross_conflict_exprs = [F.when(F.col(f"{c}_distinct_count") > 1, F.lit(c)) for c in conflict_fields]
    per_claim_agg = (
        per_claim_agg.withColumn("cross_document_conflict_fields", F.filter(F.array(*cross_conflict_exprs), lambda x: x.isNotNull()))
        .withColumn("cross_document_conflict", F.size(F.col("cross_document_conflict_fields")) > 0)
    )

    ranked = (
        df.withColumn("validation_error_count", F.size(F.col("validation_errors")))
        # Null confidences are ranked last while keeping deterministic sorting.
        .withColumn("ocr_conf_sort", F.coalesce(F.col("ocr_confidence_parsed"), F.lit(-1.0)))
        .withColumn("source_doc_numeric", F.regexp_extract(F.coalesce(F.col("source_document_id"), F.lit("")), r"(\d+)$", 1).cast("int"))
    )

    w = Window.partitionBy("claim_id").orderBy(
        F.col("is_valid_record").cast("int").desc(),
        F.col("ocr_conf_sort").desc(),
        F.col("validation_error_count").asc(),
        F.col("source_doc_numeric").desc(),
    )

    chosen = (
        ranked.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "validation_error_count", "ocr_conf_sort", "source_doc_numeric")
    )

    out = (
        chosen.alias("c")
        .join(per_claim_agg.alias("a"), on="claim_id", how="left")
        .withColumn("job1_processed_at", F.current_timestamp())
        .withColumn("selected_document_id", F.col("c.source_document_id"))
        .withColumn("selected_ocr_confidence", F.col("c.ocr_confidence_parsed"))
        .withColumn("diagnosis_codes", F.col("a.diagnosis_codes_union"))
        .withColumn("procedure_codes", F.col("a.procedure_codes_union"))
        .drop("diagnosis_codes_union", "procedure_codes_union")
    )

    for c in conflict_fields:
        out = out.drop(f"{c}_distinct_count")

    return out


def _select_job1_schema(df: DataFrame) -> DataFrame:
    """Project the canonical dataframe into the Job 1 output schema."""
    return df.select(
        "claim_id",
        "source_document_ids",
        "document_count",
        F.col("policy_number").alias("policy_number_raw"),
        "policy_number_normalized",
        F.col("member_first_name").alias("member_first_name_raw"),
        "member_first_name_normalized",
        F.col("member_last_name").alias("member_last_name_raw"),
        "member_last_name_normalized",
        F.col("member_dob").alias("member_dob_raw"),
        "member_dob_parsed",
        F.col("provider_npi").alias("provider_npi_raw"),
        "provider_npi_normalized",
        F.col("service_date").alias("service_date_raw"),
        "service_date_parsed",
        "admission_date_parsed",
        "discharge_date_parsed",
        "diagnosis_codes",
        "procedure_codes",
        F.col("billed_amount").alias("billed_amount_raw"),
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
    )


def run() -> None:
    """Run Job 1 end-to-end and persist canonical claims."""
    spark = get_spark_session()
    try:
        logger.info("Job 1 starting: reading OCR input from %s", settings.input_path)
        raw = _read_raw_ocr(spark, settings.input_path)
        raw_count = raw.count()
        logger.info("Read %d raw OCR documents", raw_count)

        norm = _normalize(raw)
        valid = _validate(norm)
        invalid_count = valid.filter(~F.col("is_valid_record")).count()
        logger.info(
            "Validated documents: %d/%d failed at least one check",
            invalid_count,
            raw_count,
        )

        with_pii = _pii(valid)
        pii_count = with_pii.filter(F.col("pii_detected")).count()
        logger.info("PII scan complete: %d documents flagged", pii_count)

        canonical = _canonicalize(with_pii)
        final_df = _select_job1_schema(canonical)
        final_count = final_df.count()
        logger.info("Canonicalized %d documents into %d unique claims", raw_count, final_count)

        output_path = f"{settings.output_path}/canonical_claims"
        logger.info("Writing output to %s (format=%s)", output_path, settings.output_format)
        write_output(final_df, output_path, settings.output_format)
        logger.info("Job 1 complete: %d canonical claims written to %s", final_count, output_path)
    finally:
        spark.stop()
