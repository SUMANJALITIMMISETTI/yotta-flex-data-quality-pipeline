"""Fixed-seed synthetic data generator for pipeline demonstrations."""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .pipeline import CUSTOMER_COLUMNS, TRANSACTION_COLUMNS


def generate_dataset(output_dir: str | Path, rows: int = 100, seed: int = 42) -> tuple[Path, Path]:
    if rows < 0:
        raise ValueError("rows must be zero or greater")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customer_count = max(1, min(max(rows // 5, 5), 10000))
    customers = [{"customer_id": f"cust_{i:06d}", "created_at": (base - timedelta(days=365 - i % 300)).isoformat().replace("+00:00", "Z")}
                 for i in range(1, customer_count + 1)]
    with (out / "customers.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=CUSTOMER_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(customers)
    events: list[dict[str, str]] = []
    for i in range(rows):
        event_type = "payment"
        payment_ref = ""
        if i >= 2 and i % 10 == 7:
            event_type = "refund"
            payment_ref = f"txn_{i - 2:09d}"
            amount = "1.00"
        else:
            amount = f"{rng.randint(1, 50000) / 100:.2f}"
        event_day = (i * 7) % 365  # Deliberately non-monotonic event time.
        record = {"transaction_id": f"txn_{i:09d}", "customer_id": customers[i % len(customers)]["customer_id"],
                  "event_type": event_type, "amount": amount, "currency": "USD",
                  "event_ts": (base + timedelta(days=event_day, minutes=i % 1440)).isoformat().replace("+00:00", "Z"),
                  "refund_of_transaction_id": payment_ref}
        events.append(record)
    if rows >= 8:
        events[1] = dict(events[0])  # Exact duplicate.
        events[2]["amount"] = "not-a-number"  # Malformed amount.
        events[3]["customer_id"] = "cust_missing"  # Invalid reference.
        events[4]["currency"] = "usd"  # Invalid normalized currency.
        events[5]["transaction_id"] = ""  # Missing key.
        events[6]["amount"] = "0"  # Boundary violation.
    with (out / "transactions.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=TRANSACTION_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(events)
    return out / "customers.csv", out / "transactions.csv"
