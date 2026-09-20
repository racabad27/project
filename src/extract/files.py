import os 
from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    # Resolve paths using path_for helper from config
    source_dir = path_for('source_dir')
    raw_base_dir = path_for('raw_dir')

    # Directory for the current run
    run_raw_dir = raw_base_dir / f"run_id={run_id}"
    run_raw_dir.mkdir(parents=True, exist_ok=True)

    # Snapshotting necessary files for the current run
    files_to_snapshot = ["customers.csv", "orders.csv", "products.json"]

    for file_name in files_to_snapshot:
        src_file = source_dir / file_name
        dest_file = run_raw_dir / file_name

        if src_file.exists():
            shutil.copy2(src_file, dest_file)
        else:
            raise FileNotFoundError(f"Source file {src_file} not found.")

    return run_raw_dir
