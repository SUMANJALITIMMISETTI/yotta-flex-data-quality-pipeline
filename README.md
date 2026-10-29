# YottaFlex Data Quality Pipeline

A small, reproducible payments-event pipeline that ingests customers and transaction events, validates and transforms them, quarantines invalid records, and publishes clean CSV output plus an operational run report. It uses only the Python standard library.

## Quick start

Requires Python 3.10 or newer. Install the local package once, then from this directory:

```powershell
python -m pip install -e .
python -m unittest discover -s tests -v
python -m yotta_pipeline run --customers data/sample/customers.csv --transactions data/sample/transactions.csv --output-dir output
```

The sample run writes `output/transactions.csv`, `output/rejects.csv`, and `output/run_report.json`. For a larger deterministic dataset:

```powershell
python -m yotta_pipeline generate --output-dir data/generated --rows 100000 --seed 42
python -m yotta_pipeline run --customers data/generated/customers.csv --transactions data/generated/transactions.csv --output-dir output
```

Run `python -m yotta_pipeline --help` for options. Input paths and output directory are configurable. `--run-id` may be supplied for a known run identifier; otherwise a stable ID is derived from input file contents.

## Architecture

```text
customers.csv + transactions.csv
             |
   schema and business validation
             |
  duplicate / reference checks
     /                  \
clean event rows       rejects.csv
     |                  (reason + source row)
transactions.csv       |
             \         /
      run_report.json (counts, timings, checksum)
```

The pipeline reads both inputs before publishing. Outputs are written to temporary files and replaced only after validation and serialization complete. Re-running identical inputs replaces the same output artifacts with the same content and stable run ID; it does not append duplicates. A validation failure writes a failed run report when possible and leaves prior data outputs intact. Correct a source and rerun to retry. Backfills use the same command with the chosen historical input files and a separate output directory.

## Data dictionary and rules

### `customers.csv` (one row per customer)

| Column | Type | Required | Rule |
|---|---|---:|---|
| `customer_id` | string | yes | Stable primary key; unique |
| `created_at` | ISO-8601 UTC timestamp | yes | Must include timezone |

### `transactions.csv` input (one row per transaction event)

| Column | Type | Required | Rule |
|---|---|---:|---|
| `transaction_id` | string | yes | Stable event key; unique |
| `customer_id` | string | yes | Must exist in customer dimension |
| `event_type` | string | yes | `payment` or `refund` |
| `amount` | decimal string | yes | Finite and greater than zero |
| `currency` | string | yes | Three uppercase ASCII letters |
| `event_ts` | ISO-8601 UTC timestamp | yes | Must include timezone; event time is retained, so late/out-of-order events are supported |
| `refund_of_transaction_id` | string | refund only | Must identify an existing payment; refund total cannot exceed its payment total |

The output retains source fields and adds `signed_amount` (positive payment, negative refund), `source_file`, `source_row`, and `run_id`. Amounts are normalized to two decimal places. Exact duplicate transaction rows are collapsed with a `duplicate` reject for each extra occurrence; conflicting rows sharing a transaction ID are all quarantined. Invalid rows have one or more semicolon-separated reason codes. Customer IDs and transaction IDs are case-sensitive. No event is discarded silently. Expected-answer labels are not embedded in the input: sample expectations are in tests and `docs/validation.md`.

## Validation and operational behavior

Checks cover required headers/fields, unexpected schema changes, malformed CSV and data types, timestamp format, valid event/currency, primary key uniqueness, customer references, refund references and refund amounts, exact/conflicting duplicates, and empty batches. A valid empty transaction file is allowed and produces zero output rows. A missing or malformed file fails the run. Rejects contain source filename, physical row number, transaction ID when available, original row values, and reason codes.

The report records run ID, UTC start/end, status, elapsed time, input/output/reject counts, input checksum, and failure reason. The deterministic generator creates reproducible normal, boundary, duplicate, missing-value, invalid-reference, and late/out-of-order records. Partial failures are atomic at the publication step; the command exits nonzero and reports the failure.

## Verify results and performance

The test suite exercises happy path, empty input, malformed schema and rows, missing values, exact and conflicting duplicates, invalid customer/refund references, refund amount boundaries, out-of-order events, deterministic transformations, and idempotent reruns. Expected sample reconciliation and commands are in [`docs/validation.md`](docs/validation.md). Run a local timing measurement with:

```powershell
python -m yotta_pipeline benchmark --rows 100000 --seed 42
```

This reports generation and pipeline elapsed time and records counts; timings depend on machine and disk. A sensible next optimization for larger-than-memory inputs is streaming validation and chunked output while preserving transaction-level refund reconciliation, for example by maintaining a compact payment index.

## Assumptions, trade-offs, and limitations

- This is a batch CSV vertical slice; CSV is the local storage/processing layer and published CSV is the analytical target.
- Refunds must reference an included payment transaction and aggregate refunds cannot exceed that payment. Refund reconciliation is independent of file order.
- Timestamps must carry an explicit timezone. The generator uses UTC.
- The implementation is single-process and reads the batch into memory; 100,000 rows is a practical demonstration, not a production throughput claim.
- Atomic replacement is per output file, not a multi-file transaction. The run report is written last as the publication marker.
- A deterministic run ID identifies content, not an execution attempt. Start/end timings naturally vary between executions.
- No external API, cloud credentials, or real customer data are used.

## AI use and submission notes

AI assistance was utilized as an interactive engineering tool under strict candidate direction to evaluate architectural alternatives, implement modules, generate synthetic data, and author tests. The candidate formulated the architecture options, selected the standard-library batch engine after trade-off analysis, steered the technical requirements with structured prompts, and verified all outputs. The complete prompt sequence and architecture decision record are documented in [`docs/ai_prompt_history.md`](docs/ai_prompt_history.md); AI responses are omitted. Review the project and extend that record with prompts from any other AI tools before public submission. Create a dedicated public repository and submit its link separately as required by the supplied guidelines; this workspace does not publish or submit the repository.

## Project files

- `src/yotta_pipeline/`: pipeline, generator, and command-line interface
- `tests/`: automated unit and integration tests
- `data/sample/`: small synthetic reproducible input
- `docs/`: design note, AI prompt history, and validation evidence
- `output/`: generated reports and transformed/rejected sample rows after running
