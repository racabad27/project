import argparse
import sys
import time

from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import stage_all
from src.transform.curated import curate_all


def run_extract(run_id: str):
    print(f"[Extract] Snapshotting raw sources (run_id={run_id})...")
    raw_path = extract_sources(run_id)
    print(f"-> Raw data saved at: {raw_path}")


def run_transform(run_id: str):
    print(f"[Transform] Staging raw datasets (run_id={run_id})...")
    staging_path = stage_all(run_id)
    print(f"-> Staged Parquet files saved at: {staging_path}")

    print(f"[Transform] Curating staged datasets (run_id={run_id})...")
    curated_path = curate_all(run_id)
    print(f"-> Curated Parquet files saved at: {curated_path}")


def run_pipeline(run_id: str):
    print(f"=== Starting Modular Pipeline Execution [run_id: {run_id}] ===")
    start_time = time.time()

    run_extract(run_id)
    run_transform(run_id)

    elapsed = time.time() - start_time
    print(f"=== Pipeline completed successfully in {elapsed:.2f}s ===")


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    
    b = sub.add_parser('benchmark')
    b.add_argument('--repeats', type=int, default=5)
    
    p = sub.add_parser('load-partition')
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--month', type=int, required=True)
    
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    # Generate a run identifier for tracking
    run_id = new_run_id()

    if args.command == 'extract':
        run_extract(run_id)
    elif args.command == 'transform':
        run_transform(run_id)
    elif args.command == 'run-all':
        run_pipeline(run_id)
    elif args.command in ['load', 'validate', 'benchmark', 'load-partition']:
        print(f"Command '{args.command}' is not yet implemented.")
    else:
        raise NotImplementedError(f'Wire command: {args.command}')


if __name__ == '__main__':
    main()