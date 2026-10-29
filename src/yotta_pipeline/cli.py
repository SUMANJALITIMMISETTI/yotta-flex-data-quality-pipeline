from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

from .generate import generate_dataset
from .pipeline import PipelineError, run_pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="yotta-pipeline", description="Validate and publish synthetic payment events.")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="validate input CSV files and publish outputs")
    run.add_argument("--customers", required=True)
    run.add_argument("--transactions", required=True)
    run.add_argument("--output-dir", required=True)
    run.add_argument("--run-id")
    gen = sub.add_parser("generate", help="create deterministic synthetic input CSV files")
    gen.add_argument("--output-dir", required=True)
    gen.add_argument("--rows", type=int, default=100)
    gen.add_argument("--seed", type=int, default=42)
    bench = sub.add_parser("benchmark", help="generate and process a reproducible batch, printing elapsed time")
    bench.add_argument("--rows", type=int, default=100000)
    bench.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            customers, transactions = generate_dataset(args.output_dir, args.rows, args.seed)
            print(json.dumps({"customers": str(customers), "transactions": str(transactions), "rows": args.rows, "seed": args.seed}, indent=2))
        elif args.command == "run":
            report = run_pipeline(args.customers, args.transactions, args.output_dir, args.run_id)
            print(json.dumps(report, indent=2))
        else:
            with tempfile.TemporaryDirectory(prefix="yotta-flex-benchmark-") as temp:
                root = Path(temp)
                generation_started = time.perf_counter()
                customers, transactions = generate_dataset(root / "input", args.rows, args.seed)
                generation_seconds = time.perf_counter() - generation_started
                pipeline_started = time.perf_counter()
                report = run_pipeline(customers, transactions, root / "output")
                print(json.dumps({"rows_requested": args.rows, "seed": args.seed, "generation_seconds": round(generation_seconds, 6),
                                  "pipeline_seconds": round(time.perf_counter() - pipeline_started, 6), "output_rows": report["output_rows"],
                                  "rejected_rows": report["rejected_rows"]}, indent=2))
        return 0
    except (PipelineError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
