"""Validation, transformation, quarantine, and idempotent CSV publishing."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

CUSTOMER_COLUMNS = ("customer_id", "created_at")
TRANSACTION_COLUMNS = (
    "transaction_id", "customer_id", "event_type", "amount", "currency",
    "event_ts", "refund_of_transaction_id",
)
OUTPUT_COLUMNS = (*TRANSACTION_COLUMNS, "signed_amount", "source_file", "source_row", "run_id")
REJECT_COLUMNS = ("source_file", "source_row", "transaction_id", "reasons", "record_json")


class PipelineError(Exception):
    """A source or publication error that should be represented in the run report."""


def _read_csv(path: Path, expected: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.is_file():
        raise PipelineError(f"input file not found: {path}")
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames is None:
                raise PipelineError(f"empty file or missing header: {path.name}")
            names = tuple(reader.fieldnames)
            missing = sorted(set(expected) - set(names))
            extra = sorted(set(names) - set(expected))
            if missing or extra or len(names) != len(set(names)):
                raise PipelineError(
                    f"schema mismatch in {path.name}: missing={missing}, unexpected={extra}, duplicate_headers={len(names) != len(set(names))}"
                )
            rows: list[dict[str, str]] = []
            for row in reader:
                if None in row:
                    row["__extra_fields__"] = json.dumps(row.pop(None), ensure_ascii=False)
                rows.append({str(k): (v if v is not None else "") for k, v in row.items()})
            return rows
    except (OSError, UnicodeError, csv.Error) as exc:
        raise PipelineError(f"cannot read {path.name}: {exc}") from exc


def _valid_timestamp(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.tzinfo is not None and parsed.utcoffset() is not None
    except (ValueError, OverflowError):
        return False


def _digest(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode("utf-8"))
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _atomic_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as stream:
            temp_name = stream.name
            writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except OSError as exc:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)
        raise PipelineError(f"cannot publish {path.name}: {exc}") from exc


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as stream:
            temp_name = stream.name
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except OSError as exc:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)
        raise PipelineError(f"cannot publish {path.name}: {exc}") from exc


def run_pipeline(customers_path: str | Path, transactions_path: str | Path,
                 output_dir: str | Path, run_id: str | None = None) -> dict[str, Any]:
    """Run a full batch. Invalid event rows are quarantined; malformed batches fail."""
    started = datetime.now(timezone.utc)
    clock = time.perf_counter()
    customers_file, transactions_file = Path(customers_path), Path(transactions_path)
    out = Path(output_dir)
    report_path = out / "run_report.json"
    try:
        customers = _read_csv(customers_file, CUSTOMER_COLUMNS)
        transactions = _read_csv(transactions_file, TRANSACTION_COLUMNS)
        checksum = _digest([customers_file, transactions_file])
        batch_id = run_id or f"run_{checksum[:16]}"
        customer_keys: set[str] = set()
        for index, row in enumerate(customers, start=2):
            key = row.get("customer_id", "").strip()
            if not key or not row.get("created_at", "").strip() or not _valid_timestamp(row["created_at"].strip()):
                raise PipelineError(f"invalid customer dimension row {index}: customer_id and timezone-aware created_at are required")
            if key in customer_keys:
                raise PipelineError(f"duplicate customer_id in customer dimension at row {index}: {key}")
            customer_keys.add(key)

        counts = Counter()
        by_key: dict[str, list[tuple[int, dict[str, str], tuple[tuple[str, str], ...]]]] = defaultdict(list)
        for source_row, raw in enumerate(transactions, start=2):
            counts["input_rows"] += 1
            reasons: list[str] = []
            row = {key: value.strip() for key, value in raw.items()}
            if "__extra_fields__" in row:
                reasons.append("extra_fields")
            for name in TRANSACTION_COLUMNS:
                if name != "refund_of_transaction_id" and not row.get(name, ""):
                    reasons.append(f"missing_{name}")
            if row.get("transaction_id"):
                txid = row["transaction_id"]
                by_key[txid].append((source_row, row, tuple(sorted(row.items()))))
            else:
                txid = ""
                by_key[f"__missing__{source_row}"].append((source_row, row, tuple(sorted(row.items()))))
            if row.get("customer_id") and row["customer_id"] not in customer_keys:
                reasons.append("unknown_customer")
            if row.get("event_type") not in {"payment", "refund"}:
                reasons.append("invalid_event_type")
            try:
                amount = Decimal(row.get("amount", ""))
                if not amount.is_finite() or amount <= 0:
                    reasons.append("invalid_amount")
            except InvalidOperation:
                amount = Decimal(0)
                reasons.append("invalid_amount")
            if not (len(row.get("currency", "")) == 3 and row["currency"].isascii() and row["currency"].isalpha() and row["currency"].isupper()):
                reasons.append("invalid_currency")
            if not _valid_timestamp(row.get("event_ts", "")):
                reasons.append("invalid_event_ts")
            if row.get("event_type") == "refund" and not row.get("refund_of_transaction_id"):
                reasons.append("missing_refund_reference")
            if row.get("event_type") == "payment" and row.get("refund_of_transaction_id"):
                reasons.append("payment_has_refund_reference")
            if reasons:
                counts["invalid_rows"] += 1
                raw["__validation_reasons__"] = ";".join(sorted(set(reasons)))

        # Duplicate identity checks happen before domain reference checks so every conflicting copy is quarantined.
        duplicate_rows: set[int] = set()
        duplicate_reasons: dict[int, str] = {}
        for key, copies in by_key.items():
            if key.startswith("__missing__") or len(copies) < 2:
                continue
            normalized = {item[2] for item in copies}
            if len(normalized) == 1:
                for source_row, _, _ in copies[1:]:
                    duplicate_rows.add(source_row)
                    duplicate_reasons[source_row] = "duplicate_exact"
            else:
                for source_row, _, _ in copies:
                    duplicate_rows.add(source_row)
                    duplicate_reasons[source_row] = "duplicate_conflict"

        # Build reason map independently so refund checks work regardless of input order.
        valid_candidates: dict[int, dict[str, str]] = {}
        for source_row, raw in enumerate(transactions, start=2):
            row = {key: value.strip() for key, value in raw.items()}
            reasons = set(raw.get("__validation_reasons__", "").split(";")) - {""}
            if source_row in duplicate_rows:
                reasons.add(duplicate_reasons[source_row])
            if not reasons:
                valid_candidates[source_row] = row
        payments: dict[str, Decimal] = {}
        for row in valid_candidates.values():
            if row["event_type"] == "payment":
                payments[row["transaction_id"]] = Decimal(row["amount"])
        refunds: dict[str, Decimal] = defaultdict(Decimal)
        for row in valid_candidates.values():
            if row["event_type"] == "refund" and row["refund_of_transaction_id"] in payments:
                refunds[row["refund_of_transaction_id"]] += Decimal(row["amount"])

        output_rows: list[dict[str, Any]] = []
        rejects: list[dict[str, str]] = []
        seen: set[str] = set()
        for source_row, raw in enumerate(transactions, start=2):
            row = {key: value.strip() for key, value in raw.items()}
            reasons = set(raw.get("__validation_reasons__", "").split(";")) - {""}
            if source_row in duplicate_rows:
                reasons.add(duplicate_reasons[source_row])
            if not reasons and row["event_type"] == "refund":
                reference = row["refund_of_transaction_id"]
                if reference not in payments:
                    reasons.add("unknown_payment_reference")
                elif refunds[reference] > payments[reference]:
                    reasons.add("refund_exceeds_payment")
            if reasons:
                rejects.append({
                    "source_file": transactions_file.name,
                    "source_row": str(source_row),
                    "transaction_id": row.get("transaction_id", ""),
                    "reasons": ";".join(sorted(reasons)),
                    "record_json": json.dumps({k: v for k, v in row.items() if k != "__validation_reasons__"}, sort_keys=True),
                })
                continue
            txid = row["transaction_id"]
            if txid in seen:
                continue
            seen.add(txid)
            amount = Decimal(row["amount"]).quantize(Decimal("0.01"))
            signed = amount if row["event_type"] == "payment" else -amount
            output_rows.append({**{k: row.get(k, "") for k in TRANSACTION_COLUMNS},
                                "amount": f"{Decimal(row['amount']).quantize(Decimal('0.01')):.2f}",
                                "signed_amount": f"{signed:.2f}", "source_file": transactions_file.name,
                                "source_row": source_row, "run_id": batch_id})

        counts.update({"customer_rows": len(customers), "output_rows": len(output_rows), "rejected_rows": len(rejects)})
        ended = datetime.now(timezone.utc)
        report = {
            "run_id": batch_id, "status": "success", "started_at": started.isoformat().replace("+00:00", "Z"),
            "ended_at": ended.isoformat().replace("+00:00", "Z"), "elapsed_seconds": round(time.perf_counter() - clock, 6),
            "input_checksum_sha256": checksum, "inputs": {"customers_file": customers_file.name,
                "transactions_file": transactions_file.name, "customer_rows": len(customers), "transaction_rows": counts["input_rows"]},
            "output_rows": counts["output_rows"], "rejected_rows": counts["rejected_rows"],
            "reconciliation": {"input_transaction_rows": counts["input_rows"], "output_rows": counts["output_rows"],
                "rejected_rows": counts["rejected_rows"], "difference": counts["input_rows"] - counts["output_rows"] - counts["rejected_rows"]},
        }
        _atomic_csv(out / "transactions.csv", OUTPUT_COLUMNS, output_rows)
        _atomic_csv(out / "rejects.csv", REJECT_COLUMNS, rejects)
        _atomic_json(report_path, report)
        return report
    except Exception as exc:
        out.mkdir(parents=True, exist_ok=True)
        failure = {"run_id": run_id, "status": "failed", "started_at": started.isoformat().replace("+00:00", "Z"),
                   "ended_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                   "elapsed_seconds": round(time.perf_counter() - clock, 6), "failure_reason": str(exc)}
        try:
            _atomic_json(report_path, failure)
        except Exception:
            pass
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError(str(exc)) from exc
