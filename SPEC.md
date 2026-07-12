# Spark Claims Review DAG Specification

## 1. Pipeline Goal

Build a batch PySpark pipeline that converts noisy OCR claim documents into one evidence-rich case record per claim for human review.

The pipeline must never auto-approve or auto-reject a claim.

Allowed workflow outcomes:

- `ETL_COMPLETE`
- `MANUAL_REVIEW_REQUIRED`

---

# 2. Source Datasets

| Dataset | Rows | Grain |
|---|---:|---|
| `ocr_claims.csv` | 200 | One OCR document per row |
| `dim_policy.csv` | 250 | One policy record per row |
| `dim_member.csv` | 240 | One member record per row |
| `dim_provider.csv` | 120 | One provider record per row |
| `dim_plan_coverage.csv` | 500 | One plan-procedure combination per row |
| `fact_claims.csv` | 1,000 | One historical claim line per row |
| `fact_pre_authorization.csv` | 300 | One authorization record per row |

OCR distribution:

- 200 OCR documents
- 180 unique claims
- 60 clean documents
- 140 noisy documents
- 20 rescanned or duplicate documents

---

# 3. High-Level DAG

```text
ocr_claims.csv
      |
      v
Job 1: OCR Validation and Canonicalization
      |
      v
canonical_claims
      |
      +---------------------+
      |                     |
      v                     v
Job 2: Reference       Job 4: Historical
Enrichment             Analysis
      |                     |
      v                     |
enriched_claims             |
      |                     |
      v                     |
Job 3: Procedure Coverage   |
      |                     |
      v                     |
coverage_enriched_claims <--+
      |
      v
Job 5: Authorization Validation
      |
      v
authorization_enriched_claims
      |
      v
Job 6: Evidence Assembly and Persist
      |
      v
claim_case
```

Primary dependency order:

```text
Job 1
  -> Job 2
  -> Job 3
  -> Job 4
  -> Job 5
  -> Job 6
```

Job 4 can technically run after Job 2 in parallel with Job 3, but the first implementation should keep the pipeline sequential for clarity.

---

# 4. Job 1 — OCR Validation and Canonicalization

## Purpose

Read raw OCR output, preserve all source documents, normalize safe formatting issues, detect extraction failures, redact PII, compare rescans, and produce one canonical row per claim.

## Inputs

### `ocr_claims.csv`

Input grain:

```text
one row per OCR document
```

Expected input rows:

```text
200 documents
```

Important source columns:

```text
source_document_id
claim_id
policy_number
member_first_name
member_last_name
member_dob
provider_npi
service_date
admission_date
discharge_date
diagnosis_codes
procedure_codes
billed_amount
currency
ocr_confidence
extracted_ssn
extracted_notes
expected_quality
noise_type
```

## Stages

### Stage 1.1 — Enforce Raw Schema

Read every OCR-sensitive field as a string first.

Do not directly parse dates, decimals, or arrays during ingestion because malformed OCR values must remain available for validation.

Output:

```text
raw_ocr_documents
```

Grain:

```text
one row per OCR document
```

Expected rows:

```text
200
```

Add:

```text
ingested_at
source_file
raw_record_hash
```

### Stage 1.2 — Normalize Fields

Create normalized columns without destroying raw columns.

Examples:

```text
policy_number_normalized
member_first_name_normalized
member_last_name_normalized
member_dob_parsed
provider_npi_normalized
service_date_parsed
admission_date_parsed
discharge_date_parsed
diagnosis_code_array
procedure_code_array
billed_amount_parsed
currency_normalized
ocr_confidence_parsed
notes_redacted
```

Normalization rules:

- trim leading and trailing whitespace
- collapse repeated whitespace
- uppercase identifiers and codes
- normalize safe punctuation in NPI
- standardize supported date formats
- standardize code delimiters
- remove blank codes
- deduplicate repeated codes
- remove currency symbols from amounts
- interpret comma-decimal only when unambiguous
- preserve uncertain transformations as warnings

Do not automatically guess values when multiple interpretations are possible.

### Stage 1.3 — Validate OCR Extraction

Generate:

```text
validation_errors: array<string>
normalization_warnings: array<string>
missing_fields: array<string>
```

Validation examples:

```text
MISSING_CLAIM_ID
MISSING_POLICY_NUMBER
INVALID_POLICY_NUMBER
MISSING_MEMBER_NAME
INVALID_MEMBER_DOB
MISSING_PROVIDER_NPI
INVALID_PROVIDER_NPI
INVALID_SERVICE_DATE
ADMISSION_AFTER_DISCHARGE
SERVICE_DATE_OUTSIDE_STAY
MISSING_PROCEDURE_CODES
INVALID_PROCEDURE_CODE
INVALID_DIAGNOSIS_CODE
INVALID_BILLED_AMOUNT
NEGATIVE_BILLED_AMOUNT
UNSUPPORTED_CURRENCY
INVALID_OCR_CONFIDENCE
LOW_OCR_CONFIDENCE
UNREADABLE_DOCUMENT
HEADER_TEXT_EXTRACTED_AS_VALUE
MULTIPLE_VALUES_EXTRACTED_IN_SINGLE_FIELD
```

### Stage 1.4 — Detect and Protect PII

Generate:

```text
pii_detected
pii_types
notes_redacted
pii_hash
```

Rules:

- remove raw SSN from downstream rows
- redact SSN-like patterns in notes
- optionally redact phone numbers
- never persist `extracted_ssn`
- PII detection is evidence, not a claim decision

### Stage 1.5 — Detect Duplicate OCR Documents

Group by `claim_id`.

For each claim calculate:

```text
document_count
source_document_ids
max_ocr_confidence
avg_ocr_confidence
duplicate_document_flag
cross_document_conflict
cross_document_conflict_fields
```

Conflict fields:

```text
policy_number
member_first_name
member_last_name
member_dob
provider_npi
service_date
billed_amount
currency
```

Canonical-selection rule:

1. valid records before invalid records
2. higher OCR confidence
3. fewer validation errors
4. newest `source_document_id` only as deterministic tie-breaker

Union diagnosis and procedure codes across non-empty documents.

### Stage 1.6 — Canonicalize Claims

Output dataset:

```text
canonical_claims
```

Output grain:

```text
one row per unique claim
```

Expected rows:

```text
180
```

## Job 1 Output Schema

```text
claim_id
source_document_ids
document_count

policy_number_raw
policy_number_normalized

member_first_name_raw
member_first_name_normalized
member_last_name_raw
member_last_name_normalized
member_dob_raw
member_dob_parsed

provider_npi_raw
provider_npi_normalized

service_date_raw
service_date_parsed
admission_date_parsed
discharge_date_parsed

diagnosis_codes
procedure_codes

billed_amount_raw
billed_amount_parsed
currency_normalized

selected_document_id
selected_ocr_confidence
max_ocr_confidence
avg_ocr_confidence

pii_detected
pii_types
notes_redacted

validation_errors
normalization_warnings
missing_fields

duplicate_document_flag
cross_document_conflict
cross_document_conflict_fields

job1_processed_at
```

---

# 5. Job 2 — Reference Enrichment

## Purpose

Resolve policy, member, and provider records while preserving missing and ambiguous matches.

## Inputs

### Primary input

```text
canonical_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

### Reference inputs

```text
dim_policy.csv          250 rows
dim_member.csv          240 rows
dim_provider.csv        120 rows
```

## Stages

### Stage 2.1 — Policy Join

Join:

```text
canonical_claims.policy_number_normalized
=
dim_policy.policy_number
```

Join type:

```text
left join
```

Before joining, group or rank policies by policy number to expose ambiguity.

Generate:

```text
policy_match_count
policy_exists
policy_ambiguous
policy_id
member_id
plan_id
policy_status
coverage_start_date
coverage_end_date
policy_active_on_service_date
```

Policy active rule:

```text
policy_status = ACTIVE
and service_date between coverage_start_date and coverage_end_date
```

### Stage 2.2 — Member Join

Join:

```text
policy.member_id
=
dim_member.member_id
```

Join type:

```text
left join
```

Generate:

```text
member_exists
first_name_match
last_name_match
dob_match
entity_match_confidence
member_eligible
```

Suggested score:

```text
first name = 0.25
last name  = 0.35
DOB        = 0.40
```

### Stage 2.3 — Provider Join

Join:

```text
canonical_claims.provider_npi_normalized
=
dim_provider.npi
```

Join type:

```text
left join
```

Generate:

```text
provider_match_count
provider_exists
provider_ambiguous
provider_id
provider_active
provider_network_status
provider_name
provider_type
```

Do not reject out-of-network providers.

## Job 2 Output

Dataset:

```text
reference_enriched_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

## Job 2 Output Schema

All Job 1 columns plus:

```text
policy_id
member_id
plan_id
policy_status
coverage_start_date
coverage_end_date
policy_match_count
policy_exists
policy_ambiguous
policy_active_on_service_date

member_exists
reference_first_name
reference_last_name
reference_dob
first_name_match
last_name_match
dob_match
entity_match_confidence
member_eligible

provider_id
provider_name
provider_type
provider_match_count
provider_exists
provider_ambiguous
provider_active
provider_network_status

reference_enrichment_errors
job2_processed_at
```

---

# 6. Job 3 — Procedure Coverage

## Purpose

Evaluate every procedure code against the matched insurance plan and aggregate procedure-level evidence back to claim level.

## Inputs

### Primary input

```text
reference_enriched_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

### Reference input

```text
dim_plan_coverage.csv
```

Grain:

```text
one row per plan-procedure pair
```

Expected rows:

```text
500
```

## Stages

### Stage 3.1 — Explode Procedures

Create:

```text
claim_procedures
```

Grain:

```text
one row per claim-procedure code
```

Expected row count:

```text
variable
approximately 300-360 rows
```

Claims with missing procedure arrays must be preserved using `explode_outer`.

Columns:

```text
claim_id
plan_id
procedure_code
procedure_index
procedure_count
billed_amount_parsed
```

### Stage 3.2 — Coverage Join

Join:

```text
claim_procedures.plan_id
=
dim_plan_coverage.plan_id

and

claim_procedures.procedure_code
=
dim_plan_coverage.procedure_code
```

Join type:

```text
left join
```

Generate per procedure:

```text
coverage_found
coverage_type
coverage_percentage
deductible_amount
copay_amount
requires_pre_authorization
procedure_billed_amount
estimated_eligible_amount
```

Dummy allocation rule:

```text
procedure_billed_amount =
billed_amount_parsed / procedure_count
```

Estimated eligible amount:

```text
max(
  0,
  procedure_billed_amount * coverage_percentage - copay_amount
)
```

This is not an approved amount.

### Stage 3.3 — Aggregate by Claim

Output:

```text
coverage_enriched_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

## Job 3 Output Schema

All Job 2 columns plus:

```text
covered_procedure_codes
uncovered_procedure_codes
pre_auth_required_procedure_codes
coverage_evidence
all_procedures_have_coverage
covered_procedure_count
uncovered_procedure_count
estimated_eligible_amount
coverage_errors
job3_processed_at
```

`coverage_evidence` should be:

```text
array<
  struct<
    procedure_code:string,
    coverage_found:boolean,
    coverage_type:string,
    coverage_percentage:double,
    deductible_amount:decimal,
    copay_amount:decimal,
    requires_pre_authorization:boolean,
    procedure_billed_amount:decimal,
    estimated_eligible_amount:decimal
  >
>
```

---

# 7. Job 4 — Historical Analysis

## Purpose

Generate review evidence from previous claims and identify likely duplicate or unusual claim patterns.

## Inputs

### Primary input

```text
coverage_enriched_claims
```

Grain:

```text
one row per current claim
```

Expected rows:

```text
180
```

### Historical input

```text
fact_claims.csv
```

Grain:

```text
one historical claim line per row
```

Expected rows:

```text
1,000
```

## Stages

### Stage 4.1 — Join Historical Claims

Join primarily by:

```text
current.member_id = historical.member_id
```

Further duplicate comparison:

```text
same provider_id
same procedure code
historical service date within 30 days
historical amount within 5 percent
```

Use exploded current procedure codes where required.

Intermediate output:

```text
claim_history_matches
```

Grain:

```text
one row per current claim and matching historical claim
```

### Stage 4.2 — Window Metrics

For each current claim derive:

```text
prior_claim_count_365d
prior_claim_amount_365d
same_procedure_claim_count_365d
most_recent_prior_service_date
historical_average_claim_amount
candidate_duplicate_count
duplicate_claim_flag
```

### Stage 4.3 — Risk Score

Suggested score:

```text
+50 likely duplicate
+20 more than 10 prior claims in 365 days
+15 more than 3 same-procedure claims in 365 days
+15 current billed amount > 3x historical average
```

Cap score at 100.

## Job 4 Output

Dataset:

```text
historical_enriched_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

## Job 4 Output Schema

All Job 3 columns plus:

```text
prior_claim_count_365d
prior_claim_amount_365d
same_procedure_claim_count_365d
most_recent_prior_service_date
historical_average_claim_amount
candidate_duplicate_count
duplicate_claim_flag
fraud_risk_score
historical_evidence
job4_processed_at
```

`historical_evidence` should include a bounded array of candidate historical matches rather than every historical row.

---

# 8. Job 5 — Authorization Validation

## Purpose

Validate required pre-authorizations at procedure level.

## Inputs

### Primary input

```text
historical_enriched_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

### Authorization input

```text
fact_pre_authorization.csv
```

Grain:

```text
one authorization record per row
```

Expected rows:

```text
300
```

## Stages

### Stage 5.1 — Explode Required Procedures

Explode:

```text
pre_auth_required_procedure_codes
```

Use `explode_outer` so claims without required procedures remain present.

Intermediate dataset:

```text
required_authorization_procedures
```

Grain:

```text
one row per claim and pre-auth-required procedure
```

### Stage 5.2 — Authorization Join

Join on:

```text
member_id
provider_id
procedure_code
```

Join type:

```text
left join
```

A valid authorization requires:

```text
authorization_status = APPROVED
and service_date between valid_from and valid_to
```

Generate per procedure:

```text
authorization_match_count
valid_authorization_count
authorization_result
authorization_id
authorization_status
valid_from
valid_to
```

Procedure-level result values:

```text
NOT_REQUIRED
VALID
MISSING
EXPIRED
PENDING
DENIED
CANCELLED
AMBIGUOUS
```

### Stage 5.3 — Aggregate by Claim

Generate:

```text
authorized_procedure_codes
missing_authorization_procedure_codes
authorization_evidence
pre_authorization_status
```

Claim-level priority:

```text
AMBIGUOUS
DENIED
PENDING
EXPIRED
MISSING
VALID
NOT_REQUIRED
```

## Job 5 Output

Dataset:

```text
authorization_enriched_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

---

# 9. Job 6 — Evidence Assembly and Persist

## Purpose

Collect all data-quality, reference, coverage, historical, and authorization evidence into one final case record.

## Inputs

```text
authorization_enriched_claims
```

Grain:

```text
one row per claim
```

Expected rows:

```text
180
```

## Stages

### Stage 6.1 — Build Review Reasons

Create:

```text
review_reasons: array<string>
```

Include applicable reasons from every previous job.

Examples:

```text
OCR_REQUIRED_FIELD_MISSING
OCR_LOW_CONFIDENCE
OCR_UNREADABLE_DOCUMENT
OCR_CROSS_DOCUMENT_CONFLICT
POLICY_NOT_FOUND
POLICY_AMBIGUOUS
POLICY_INACTIVE_ON_SERVICE_DATE
MEMBER_NOT_FOUND
MEMBER_IDENTITY_MISMATCH
PROVIDER_NOT_FOUND
PROVIDER_INACTIVE
PROVIDER_AMBIGUOUS
PROCEDURE_NOT_COVERED
PRE_AUTHORIZATION_MISSING
PRE_AUTHORIZATION_EXPIRED
PRE_AUTHORIZATION_PENDING
PRE_AUTHORIZATION_DENIED
LIKELY_DUPLICATE_CLAIM
INVALID_BILLED_AMOUNT
INVALID_SERVICE_DATE
```

Do not include out-of-network status as an automatic blocking reason unless the business rules explicitly require it.

### Stage 6.2 — Build Evidence Package

Create:

```text
evidence_package
```

Suggested schema:

```text
struct<
  data_quality_evidence:struct,
  policy_evidence:struct,
  member_evidence:struct,
  provider_evidence:struct,
  coverage_evidence:array<struct>,
  historical_evidence:struct,
  authorization_evidence:array<struct>
>
```

### Stage 6.3 — Derive Workflow Status

```text
review_required = size(review_reasons) > 0
```

```text
workflow_status =
  MANUAL_REVIEW_REQUIRED when review_required
  ETL_COMPLETE otherwise
```

This is not a claim adjudication decision.

### Stage 6.4 — Persist

Final dataset:

```text
claim_case
```

Output grain:

```text
one row per claim
```

Expected rows:

```text
180
```

Recommended output:

```text
output/claim_case/
```

Format:

```text
Parquet
```

Optional debug output:

```text
output/claim_case_json/
```

## Final Output Schema

```text
case_id
claim_id
source_document_ids
document_count

member_id
policy_id
provider_id
plan_id

service_date
admission_date
discharge_date

diagnosis_codes
procedure_codes

billed_amount
estimated_eligible_amount
approved_amount

currency

policy_status
policy_active_on_service_date
member_eligible
entity_match_confidence

provider_active
provider_network_status

pre_authorization_status

duplicate_claim_flag
fraud_risk_score

workflow_status
review_required
review_reasons
decision_reason

selected_ocr_confidence
max_ocr_confidence
avg_ocr_confidence

evidence_package

rule_engine_version
created_at
updated_at
```

Required semantics:

```text
approved_amount = null
```

Allowed workflow values:

```text
ETL_COMPLETE
MANUAL_REVIEW_REQUIRED
```

Forbidden values:

```text
APPROVED
REJECTED
DENIED
```

---

# 10. Job Input/Output Summary

| Job | Input | Input Grain | Output | Output Grain | Expected Rows |
|---|---|---|---|---|---:|
| Job 1 | `ocr_claims.csv` | OCR document | `canonical_claims` | claim | 180 |
| Job 2 | `canonical_claims`, policy, member, provider | claim + reference rows | `reference_enriched_claims` | claim | 180 |
| Job 3 | `reference_enriched_claims`, coverage | claim and plan-procedure | `coverage_enriched_claims` | claim | 180 |
| Job 4 | `coverage_enriched_claims`, historical claims | current claim and historical claim | `historical_enriched_claims` | claim | 180 |
| Job 5 | `historical_enriched_claims`, authorizations | claim and authorization | `authorization_enriched_claims` | claim | 180 |
| Job 6 | `authorization_enriched_claims` | claim | `claim_case` | claim | 180 |

---

# 11. Critical Pipeline Invariants

Every job must preserve these invariants:

```text
No claim is silently dropped.
```

```text
All reference joins are left joins from the current claim dataset.
```

```text
Explode operations use explode_outer when missing arrays must remain visible.
```

```text
Every many-to-one join explicitly handles duplicate reference records.
```

```text
Each job returns to one row per claim before handing data to the next job.
```

```text
Raw SSNs are never persisted after Job 1.
```

```text
approved_amount is always null.
```

```text
The pipeline never produces APPROVED or REJECTED.
```

```text
Final output contains exactly 180 rows for this dataset.
```