from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from yotta_pipeline.generate import generate_dataset
from yotta_pipeline.pipeline import PipelineError, run_pipeline


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CUSTOMERS = ROOT / "data" / "sample" / "customers.csv"
SAMPLE_TRANSACTIONS = ROOT / "data" / "sample" / "transactions.csv"


def read_rejects(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


class PipelineTests(unittest.TestCase):
    def test_sample_reconciles_and_transforms(self):
        with tempfile.TemporaryDirectory() as temp:
            report = run_pipeline(SAMPLE_CUSTOMERS, SAMPLE_TRANSACTIONS, temp)
            self.assertEqual(report["status"], "success")
            self.assertEqual(report["output_rows"], 4)
            self.assertEqual(report["rejected_rows"], 4)
            self.assertEqual(report["reconciliation"]["difference"], 0)
            with (Path(temp) / "transactions.csv").open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(next(r for r in rows if r["transaction_id"] == "txn_003")["signed_amount"], "-5.00")
            self.assertEqual(next(r for r in rows if r["transaction_id"] == "txn_004")["event_ts"], "2024-12-31T23:59:59Z")

    def test_rerun_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            first = run_pipeline(SAMPLE_CUSTOMERS, SAMPLE_TRANSACTIONS, temp)
            target = Path(temp) / "transactions.csv"
            before = target.read_text(encoding="utf-8")
            second = run_pipeline(SAMPLE_CUSTOMERS, SAMPLE_TRANSACTIONS, temp)
            self.assertEqual(first["run_id"], second["run_id"])
            self.assertEqual(before, target.read_text(encoding="utf-8"))

    def test_empty_batch_is_valid(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _, tx = generate_dataset(root / "input", 0)
            report = run_pipeline(root / "input" / "customers.csv", tx, root / "output")
            self.assertEqual(report["output_rows"], 0)
            self.assertEqual(report["rejected_rows"], 0)

    def test_missing_required_field_and_unknown_customer_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _, tx = generate_dataset(root / "input", 0)
            with tx.open("a", newline="", encoding="utf-8") as stream:
                stream.write("txn_x,,payment,1.00,USD,2025-01-01T00:00:00Z,\n")
                stream.write("txn_y,cust_unknown,payment,1.00,USD,2025-01-01T00:00:00Z,\n")
            report = run_pipeline(root / "input" / "customers.csv", tx, root / "output")
            self.assertEqual(report["rejected_rows"], 2)
            reasons = [row["reasons"] for row in read_rejects(root / "output" / "rejects.csv")]
            self.assertIn("missing_customer_id", reasons[0])
            self.assertIn("unknown_customer", reasons[1])

    def test_conflicting_duplicate_quarantines_both_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _, tx = generate_dataset(root / "input", 0)
            with tx.open("a", encoding="utf-8") as stream:
                stream.write("txn_dup,cust_000001,payment,1.00,USD,2025-01-01T00:00:00Z,\n")
                stream.write("txn_dup,cust_000001,payment,2.00,USD,2025-01-01T00:00:00Z,\n")
            report = run_pipeline(root / "input" / "customers.csv", tx, root / "output")
            self.assertEqual(report["rejected_rows"], 2)
            self.assertTrue(all("duplicate_conflict" in row["reasons"] for row in read_rejects(root / "output" / "rejects.csv")))

    def test_unexpected_schema_change_fails_and_reports(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            customers, tx = generate_dataset(root / "input", 0)
            tx.write_text("transaction_id,customer_id,unexpected\n", encoding="utf-8")
            with self.assertRaises(PipelineError):
                run_pipeline(customers, tx, root / "output")
            report = json.loads((root / "output" / "run_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "failed")
            self.assertIn("schema mismatch", report["failure_reason"])

    def test_failed_batch_preserves_published_data_and_can_be_retried(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            customers, tx = generate_dataset(root / "input", 0)
            out = root / "output"
            run_pipeline(customers, tx, out)
            published_before = (out / "transactions.csv").read_bytes()
            tx.write_text("transaction_id,customer_id,unexpected\n", encoding="utf-8")
            with self.assertRaises(PipelineError):
                run_pipeline(customers, tx, out)
            self.assertEqual((out / "transactions.csv").read_bytes(), published_before)
            generate_dataset(root / "input", 3)
            report = run_pipeline(customers, tx, out)
            self.assertEqual(report["status"], "success")
            self.assertEqual(report["output_rows"], 3)

    def test_refund_boundary_allows_exact_full_refund(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            customers, tx = generate_dataset(root / "input", 0)
            with tx.open("a", encoding="utf-8") as stream:
                stream.write("refund,cust_000001,refund,10.00,USD,2025-01-02T00:00:00Z,pay\n")
                stream.write("pay,cust_000001,payment,10.00,USD,2025-01-01T00:00:00Z,\n")
            report = run_pipeline(customers, tx, root / "output")
            self.assertEqual(report["output_rows"], 2)
            self.assertEqual(report["rejected_rows"], 0)

    def test_refund_overpayment_is_rejected_even_when_event_is_out_of_order(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            customers, tx = generate_dataset(root / "input", 0)
            with tx.open("a", encoding="utf-8") as stream:
                stream.write("refund,cust_000001,refund,10.01,USD,2025-01-02T00:00:00Z,pay\n")
                stream.write("pay,cust_000001,payment,10.00,USD,2025-01-01T00:00:00Z,\n")
            report = run_pipeline(customers, tx, root / "output")
            self.assertEqual(report["output_rows"], 1)
            reject = read_rejects(root / "output" / "rejects.csv")[0]
            self.assertIn("refund_exceeds_payment", reject["reasons"])

    def test_missing_reference_and_malformed_amount_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            customers, tx = generate_dataset(root / "input", 0)
            with tx.open("a", encoding="utf-8") as stream:
                stream.write("refund,cust_000001,refund,1.00,USD,2025-01-02T00:00:00Z,no_such_payment\n")
                stream.write("bad,cust_000001,payment,NaN,USD,2025-01-02T00:00:00Z,\n")
            report = run_pipeline(customers, tx, root / "output")
            self.assertEqual(report["rejected_rows"], 2)
            reasons = [r["reasons"] for r in read_rejects(root / "output" / "rejects.csv")]
            self.assertIn("unknown_payment_reference", reasons[0])
            self.assertIn("invalid_amount", reasons[1])


if __name__ == "__main__":
    unittest.main()
