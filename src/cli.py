import argparse
from pathlib import Path
import sys
import time

import pandas as pd

from src.common.audit import new_run_id
from src.common.validation import run_validations
from src.config import DB, PROJECT_ROOT, SETTINGS, path_for
from src.extract.files import extract_sources
from src.load.postgres import load_partition, upsert_curated
from src.transform.curated import curate_all
from src.transform.staging import stage_all


def run_extract(run_id: str) -> Path:
    print(f"[Extract] Snapshotting raw sources (run_id={run_id})...")
    raw_path = extract_sources(run_id)
    print(f"-> Raw data saved at: {raw_path}")
    return raw_path


def run_transform(run_id: str) -> Path:
    print(f"[Transform] Staging raw datasets (run_id={run_id})...")
    staging_path = stage_all(run_id)
    print(f"-> Staged Parquet files saved at: {staging_path}")

    print(f"[Transform] Curating staged datasets (run_id={run_id})...")
    curated_path = curate_all(run_id)
    print(f"-> Curated Parquet files saved at: {curated_path}")
    return curated_path


def run_load(run_id: str) -> int:
    print(f"[Load] Upserting curated data into PostgreSQL (run_id={run_id})...")
    
    # Check for active run path or fall back to the latest generated run
    curated_path = path_for("curated_dir") / f"run_id={run_id}" / "curated_sales.parquet"
    if not curated_path.exists():
        curated_dirs = sorted(path_for("curated_dir").glob("run_id=*"))
        if curated_dirs:
            curated_path = curated_dirs[-1] / "curated_sales.parquet"

    if not curated_path.exists():
        print("[Load Error] No curated data found. Run 'python -m src.cli transform' first.")
        return 0

    df = pd.read_parquet(curated_path)
    return upsert_curated(df, run_id)


def run_pipeline(run_id: str):
    print(f"=== Starting Modular Pipeline Execution [run_id: {run_id}] ===")
    start_time = time.time()

    run_extract(run_id)
    run_transform(run_id)
    run_load(run_id)

    elapsed = time.time() - start_time
    print(f"=== Pipeline completed successfully in {elapsed:.2f}s ===")


def main():
    parser = argparse.ArgumentParser(description="DSS150P modular pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("validate-env")
    sub.add_parser("extract")
    sub.add_parser("transform")
    sub.add_parser("load")
    sub.add_parser("validate")

    b = sub.add_parser("benchmark")
    b.add_argument("--repeats", type=int, default=5)

    p = sub.add_parser("load-partition")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)

    sub.add_parser("run-all")
    args = parser.parse_args()

    # Environment inspection
    if args.command == "validate-env":
        print("PROJECT_ROOT=", PROJECT_ROOT)
        print("DB host/database=", DB["host"], DB["dbname"])
        print("Configured source=", SETTINGS["pipeline"]["source_dir"])
        return

    # Generate execution run_id for tracking
    run_id = new_run_id()

    if args.command == "extract":
        run_extract(run_id)

    elif args.command == "transform":
        run_transform(run_id)

    elif args.command == "load":
        run_load(run_id)

    elif args.command == "load-partition":
        print(f"[Load-Partition] Loading partition {args.year}-{args.month:02d} (run_id={run_id})...")
        curated_dirs = sorted(path_for("curated_dir").glob("run_id=*"))
        if curated_dirs:
            curated_path = curated_dirs[-1] / "curated_sales.parquet"
            df = pd.read_parquet(curated_path)
            load_partition(df, args.year, args.month, run_id)
        else:
            print("[Load Error] No curated data found.")

    elif args.command == "validate":
        run_validations(run_id)

    elif args.command == "run-all":
        run_pipeline(run_id)

    elif args.command == "benchmark":
        print(f"[Benchmark] Running pipeline benchmark ({args.repeats} repeats)...")
        times = []
        for i in range(args.repeats):
            b_run_id = new_run_id()
            t0 = time.time()
            run_pipeline(b_run_id)
            times.append(time.time() - t0)
        avg_time = sum(times) / len(times)
        print(f"\n[Benchmark Finished] Avg Execution Time: {avg_time:.3f}s over {args.repeats} runs.")

    else:
        raise NotImplementedError(f"Wire command: {args.command}")


if __name__ == "__main__":
    main()