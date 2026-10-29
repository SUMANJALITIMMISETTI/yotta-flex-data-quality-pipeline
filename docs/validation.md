# Validation evidence

## Sample batch

Run:

```powershell
python -m unittest discover -s tests -v
python -m yotta_pipeline run --customers data/sample/customers.csv --transactions data/sample/transactions.csv --output-dir output
```

Expected reconciliation for the checked-in sample: 8 input transaction rows = 4 published rows + 4 quarantined rows; difference is 0. The published signed amounts are `25.00`, `10.50`, `-5.00`, and `7.25`. The quarantines cover an exact duplicate, an unknown customer, a refund exceeding the payment, and a malformed amount. The late event timestamp is preserved. The test suite checks the report and output assertions rather than relying on manual inspection alone.

## Reconciliation query

Using any CSV reader, verify `input_transaction_rows = output_rows + rejected_rows` and `difference = 0` in `run_report.json`. To inspect the net signed amount, sum the `signed_amount` column in `transactions.csv`; the checked-in sample totals `37.75`.

## Larger run

`python -m yotta_pipeline benchmark --rows 100000 --seed 42` prints the elapsed generation and pipeline time, accepted count, and reject count. The benchmark is a local measurement, not a service-level guarantee; record the machine and result when presenting it.

Recorded verification on 2026-10-29: all 10 automated tests passed. The fixed-seed 100,000-row run generated in 3.47 seconds and processed in 9.37 seconds, publishing 99,968 rows and quarantining 32. These timings are specific to the execution environment; rerun locally before citing them as a performance target.
