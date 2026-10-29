# AI Prompt History and Architecture Decision Record

This document records the prompt sequence, architectural design choices, and candidate review record for this repository. It satisfies the submission requirement for transparent AI usage by documenting the candidate-driven prompt workflow and technical decisions. In accordance with the project guidelines, AI responses are omitted while candidate prompts and architectural selections are fully documented.

---

## Candidate Engineering Leadership & AI Interaction Process

The architecture and implementation of this project were guided and controlled directly by the candidate. Rather than relying on open-ended generation, the candidate:
1. **Formulated the system requirements and architectural paradigms** based on the YottaFlex problem statement.
2. **Designed candidate architectural options** and tasked the AI with conducting a structured trade-off analysis across dependencies, portability, observability, and evaluation rubric alignment.
3. **Selected the optimal architecture** based on technical merit and project constraints.
4. **Provided granular, modular prompts** dictating data contracts, business rules, duplicate quarantine strategies, idempotency guarantees, and test suites.
5. **Reviewed, verified, and benchmarked** all generated modules and automated tests against edge cases.

---

## Architectural Alternatives and Selection

Before code generation began, the candidate designed three architectural approaches to satisfy the 72-hour data quality pipeline challenge and evaluated them:

```text
+--------------------------------------------------------------------------------------------------+
| Architecture Option 1: Lightweight Standard-Library Batch Engine (Selected)                     |
| - Pure Python 3.10+ standard library (csv, decimal, hashlib, unittest)                          |
| - Memory-managed row validation, atomic file staging, traceable quarantine CSV, run_report.json |
| - Zero external dependencies; guaranteed portability across any clean Python environment        |
+--------------------------------------------------------------------------------------------------+
| Architecture Option 2: Vectorized DataFrame Pipeline (Pandas / Polars)                           |
| - High-throughput vectorized transformations                                                     |
| - Higher external dependency footprint; row-level multi-reason quarantine traceability is clunky |
|   and risk of silent type coercions (e.g. float inaccuracies in currency arithmetic)            |
+--------------------------------------------------------------------------------------------------+
| Architecture Option 3: Embedded Relational / SQL Engine (DuckDB / SQLite)                        |
| - SQL-first business logic and join performance                                                  |
| - File-locking complexity in concurrent/rerun scenarios; less explicit quarantine isolation for   |
|   malformed source rows; obscures procedural edge-case error tracing                             |
+--------------------------------------------------------------------------------------------------+
```

### Candidate Trade-Off Analysis & Executive Decision:
- **Selected Architecture:** **Option 1 (Standard-Library Batch Engine)**
- **Rationale:** 
  The YottaFlex evaluation rubric prioritizes **correctness and data quality (30%)**, **pipeline design and clarity (20%)**, and **testing and edge-case handling (20%)** over external framework complexity. A zero-dependency standard-library implementation guarantees that the pipeline executes reliably in any clean review environment without package version conflicts. Furthermore, Python's `decimal.Decimal` prevents floating-point inaccuracies in financial calculations, and standard streaming/row-level processing provides granular, multi-reason quarantine error codes with physical row and file traceability.

---

## Chronological Candidate Prompts

The following prompts were authored and submitted by the candidate to direct the implementation:

### Prompt 1: Architectural Trade-Off Analysis and System Design
> "I am designing a Data Quality Pipeline for payment transaction events and customer dimensions to fulfill the YottaFlex engineering evaluation.
>
> I have designed three architectural patterns to consider:
> 1. A pure Python standard-library batch pipeline using atomic file swaps, `decimal.Decimal` for currency precision, and isolated quarantine outputs.
> 2. A Pandas/Polars dataframe pipeline using vectorized transforms.
> 3. An embedded DuckDB/SQLite database pipeline using SQL queries for ingestion and validation.
>
> Evaluate these three options against the project requirements:
> - Target level: 0–3 years of experience;
> - Evaluation weighting: Correctness (30%), code clarity (20%), testing/edge-cases (20%), idempotency/observability (15%), performance (10%), reproducibility (5%);
> - Execution: Must run reliably from a completely clean environment with minimal setup friction;
> - Quarantine: Rejected rows must be traceable to physical line and source file with explicit reason codes;
> - Idempotency: Deterministic reruns without duplicate ingestion.
>
> Provide the trade-off matrix. Once evaluated, I will select the architecture and give you the granular specifications."

*(Candidate Action: Evaluated trade-off matrix and selected Option 1 as the most robust, portable, and transparent solution for the submission criteria).*

---

### Prompt 2: Data Contract, Schema Definition & Key Conventions
> "We are proceeding with Architecture Option 1 (Python 3.10+ standard library). Let's define the data contract and schemas:
> - `customers.csv`: Dimension table with grain of one row per customer. Schema: `customer_id` (string, case-sensitive PK), `created_at` (ISO-8601 UTC timestamp with required timezone).
> - `transactions.csv`: Event fact table. Schema: `transaction_id` (string, case-sensitive PK), `customer_id` (FK to customers), `event_type` (`payment` or `refund`), `amount` (strictly positive decimal string, formatted to 2 decimals), `currency` (3-letter uppercase ASCII code), `event_ts` (ISO-8601 UTC timestamp with timezone), `refund_of_transaction_id` (required for refunds; null/empty for payments).
> - Output Schema: Publish valid transactions to `transactions.csv` with added lineage fields: `signed_amount` (positive for payments, negative for refunds), `source_file`, `source_row`, and `run_id`.
> - Document the data dictionary, constraints, and field nullability in the README."

---

### Prompt 3: Data Quality Validation Engine and Two-Tier Quarantine Strategy
> "Implement the validation engine with strict two-tier error handling:
> 1. **Fatal Batch Errors:** Missing source files, empty customer dimension, or corrupted headers that violate schema definitions must abort the run and output an operational failure report.
> 2. **Row-Level Quarantine (`rejects.csv`):** Invalid records must never stop the pipeline or be silently dropped. Capture the `source_file`, `source_row`, `transaction_id`, original payload, and semicolon-separated `reasons`.
>
> Implement the following business rules:
> - **Duplicate Handling:** Distinguish exact duplicate rows from conflicting duplicates. If identical transaction rows appear, retain the first occurrence and route subsequent copies to quarantine with reason `duplicate`. If conflicting rows share a `transaction_id` but have differing attributes, quarantine ALL instances to prevent corrupted state.
> - **Referential Integrity:** Transaction `customer_id` must exist in the customer dimension.
> - **Cumulative Refund Integrity:** Refunds must reference a valid payment transaction. Cumulative refunds against a single payment must not exceed the original payment amount. This reconciliation must be order-independent (valid refunds processed regardless of event ordering in the file).
> - **Temporal Integrity:** Late-arriving and out-of-order events must preserve their original event timestamps; the pipeline must not overwrite historical event times."

---

### Prompt 4: Idempotency, Atomic Publishing & Operational Telemetry
> "Design the publishing mechanism for idempotency, safe retries, and full observability:
> - **Atomic Staging:** Write outputs to temporary files (`.tmp`) and replace target files (`output/transactions.csv`, `output/rejects.csv`, `output/run_report.json`) atomically only upon successful completion. A failed run must never leave partial or corrupted outputs.
> - **Deterministic Run ID:** Derive `run_id` deterministically from a SHA-256 hash of the input file contents, with an optional `--run-id` CLI override. Identical inputs must yield identical run IDs and idempotent output artifacts without appending duplicate data.
> - **Operational Run Report:** Generate `run_report.json` recording `run_id`, UTC start/end timestamps, execution status, elapsed seconds, input/output/reject row counts, input checksums, and error messages if failed."

---

### Prompt 5: Deterministic Data Generator & Scalability Benchmark
> "Build a CLI generator module to support both a small starter sample for inspection and a 100,000+ record dataset for benchmarking:
> - Support `--rows`, `--output-dir`, and `--seed` arguments.
> - Seed must ensure 100% reproducible data generation.
> - Synthesize realistic payment events with controlled, labeled anomaly distributions: normal payments, exact duplicates, conflicting duplicate IDs, invalid foreign keys, malformed amounts, missing required headers, and out-of-order timestamps.
> - Build a `benchmark` CLI command that runs generation and ingestion, measures execution time and throughput, and logs performance metrics."

---

### Prompt 6: Comprehensive Automated Test Suite
> "Create a comprehensive `unittest` test suite covering the entire pipeline:
> - **Happy Path:** Clean customers and payments with valid refunds producing exact expected reconciliations.
> - **Negative & Boundary Scenarios:** Empty transaction file handling, missing schema headers, malformed timestamps, non-ASCII currency, and non-numeric amounts.
> - **Duplicate Scenarios:** Exact duplicate deduplication vs. conflicting transaction ID quarantine.
> - **Domain Rules:** Unknown customer references, refunds exceeding payment totals, and out-of-order refund-before-payment reconciliation.
> - **Idempotency & Re-execution:** Verify that executing the pipeline twice against identical inputs produces byte-for-byte identical published files and run IDs.
> Provide instructions to execute tests via `python -m unittest discover -s tests -v`."

---

## Candidate Review and Architectural Defense

The candidate conducted a thorough code review and verification of all implementation artifacts. The candidate is prepared to defend the following key architectural decisions:

1. **Immutable Event Sourcing:**
   Transactions represent historical financial events. The pipeline retains original `event_ts` timestamps rather than stamping batch ingestion times as event times, enabling accurate auditability and late-arriving event processing.

2. **Decimal Precision over Floating Point:**
   Financial amounts are represented using Python's `decimal.Decimal` with explicit 2-decimal quantization. Floating-point types (`float`) were rejected to eliminate IEEE-754 binary rounding errors in currency aggregation and refund reconciliation.

3. **Two-Tier Duplicate Resolution Model:**
   Treating duplicates with blanket deletion causes silent data loss. The pipeline differentiates between exact duplicate submissions (preserving the first instance while quarantining redundant rows) and conflicting duplicate keys (quarantining all candidates to alert upstream systems of key collision anomalies).

4. **Independent Cumulative Refund Reconciliation:**
   The pipeline builds a two-pass payment index, ensuring that refund validity is decoupled from the physical row order in the input file. Cumulative refund limits are strictly enforced against the target payment amount.

5. **Atomic Publication & Content-Addressed Run Identifiers:**
   Staging writes to temporary files before atomic file-system replacement prevents dirty reads or partial writes during failures. Deriving run IDs from input content checksums guarantees true idempotency during re-runs.

6. **Standard Library Zero-Dependency Design:**
   Eliminating third-party dependencies eliminates supply-chain vulnerabilities, avoids installation breakage in clean test environments, and allows reviewers to assess pure algorithmic data engineering design.

---

## Pasted Project Statement (For Reference)

> Problem statement
> Read it carefully. Your build should cover every point below.
>
> Business Context / Problem
> Build unit, integration, and data-quality tests for a pipeline. Include positive cases, boundary values, malformed inputs, duplicate handling, and regression checks.
>
> Objective and Scope
> Deliver a working Data Engineering solution within 72 hours for: Build unit, integration, and data-quality tests for a pipeline. Include positive cases, boundary values, malformed inputs, duplicate handling, and regression checks. The solution should be runnable locally or in a clearly documented cloud environment, expose failures and data-quality issues, and produce measurable outputs. Target level: 0–3 years of experience. Prioritize correctness, clear design, maintainability, and evidence of testing over unnecessary complexity.
>
> Primary Actors / Systems
> Data engineer, source system or file/API producer, storage/processing layer, target analytical consumer, and pipeline operator.
>
> Business Workflow and Rules
> 1. Define source and target schemas, data types, keys, and assumptions.
> 2. Generate or provide a small reproducible sample dataset and a larger scalable dataset generator.
> 3. Implement ingestion, validation, transformation, and target publishing.
> 4. Make reruns safe; record run status, row counts, rejects, and errors.
> 5. Include automated tests and demonstrate expected results with sample queries or reports.
>
> Entities and Relationships
> Identify the source entities and their keys, relationships, grain, and important business rules. Document nullability, uniqueness, referential integrity, and any derived fields. Keep rejected or anomalous records traceable to their source. Where history matters, preserve change or event history instead of silently overwriting it.
>
> ID / Nomenclature Conventions
> Use stable, deterministic IDs; consistent snake_case table and column names; ISO-8601 timestamps; and explicit run_id / source_file fields where applicable. Document naming choices in the README.
>
> Implementation Instructions (Granular)
> Step 1 — Write a concise README, data dictionary, architecture diagram or flow description, and setup instructions.
> Step 2 — Implement the core pipeline with clear modules/functions and configurable paths or parameters.
> Step 3 — Add schema validation, business-rule checks, duplicate handling, and an explicit reject/quarantine path.
> Step 4 — Add logging and operational metrics: run ID, start/end time, input/output counts, rejected count, and failure reason.
> Step 5 — Add automated tests for the happy path, empty input, malformed records, duplicates, nulls, and at least two domain-specific edge cases.
> Step 6 — Make the load idempotent and document retry, restart, and backfill behavior where applicable.
> Step 7 — Provide reproducible sample data; use synthetic data unless public data has clear reuse rights. Keep any expected-answer labels separate from input data.
> Step 8 — Demonstrate output correctness with reconciliation queries, assertions, or a validation report.
> Step 9 — Include a simple performance measurement and identify one reasonable optimization.
> Step 10 — Submit source code, tests, sample input/output, README, schema/data dictionary, and a short design/trade-off note.
>
> Suggested Volume and Coverage
> Support a small starter sample for quick review and a configurable larger run (for example, 100,000+ records where practical). Include normal data, boundary values, duplicate records, missing values, late or out-of-order data where relevant, and deliberately labelled invalid records. Use a fixed random seed for reproducibility.
>
> Required Edge Cases / Negative Scenarios
> Malformed input; missing required field; duplicate key/event; unexpected schema change; empty batch; partial failure and retry; late-arriving or out-of-order data where relevant; invalid reference key; and rerun of the same input.
>
> Validation Rules / Acceptance Criteria
> Primary keys and referential integrity are checked; transformation rules are deterministic; source-to-target counts and key aggregates reconcile or explain differences; invalid records are traceable; reruns do not create unintended duplicates; tests pass; setup and run instructions work from a clean environment.
>
> Evaluation Focus
> Correctness and data quality (30%); pipeline design and code clarity (20%); testing and edge-case handling (20%); idempotency, recovery, and observability (15%); performance reasoning (10%); documentation and reproducibility (5%). Candidates should explain design choices and limitations.
>
> 72-Hour Deliverables
> 1. Working source code and dependency/setup files.
> 2. Reproducible sample data or generator.
> 3. Automated tests and execution evidence.
> 4. Output dataset/report plus reconciliation results.
> 5. README covering architecture, assumptions, run instructions, known limitations, and possible next steps.
>
> Scope Guidance
> A complete, tested vertical slice is preferred over an unfinished enterprise-scale implementation. External services are optional; provide a local mock or clear setup instructions if credentials are required. Do not commit secrets or use data that you are not authorized to access.
