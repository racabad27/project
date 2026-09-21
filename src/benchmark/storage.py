import os
import time
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from src.config import DB


def get_file_size(file_path):
    return os.path.getsize(file_path) if os.path.exists(file_path) else 0


def run_benchmark(curated_path, output_dir="data/benchmarks", repeats: int = 5, filter_status: str = "DELIVERED"):
    """Compare CSV, JSON Lines, Parquet, and PostgreSQL storage footprint and read latency."""
    os.makedirs(output_dir, exist_ok=True)
    # Construct connection URL from DB dictionary in src.config
    db_url = f"postgresql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['dbname']}"
    engine = create_engine(db_url)

    # Read base curated data
    if curated_path.endswith(".parquet"):
        df_base = pd.read_parquet(curated_path)
    else:
        df_base = pd.read_csv(curated_path)

    csv_path = os.path.join(output_dir, "benchmark_curated.csv")
    jsonl_path = os.path.join(output_dir, "benchmark_curated.jsonl")
    parquet_path = os.path.join(output_dir, "benchmark_curated.parquet")

    # 1. Materialize file formats and measure sizes/write times
    t0 = time.perf_counter()
    df_base.to_csv(csv_path, index=False)
    csv_write_time = time.perf_counter() - t0
    csv_size = get_file_size(csv_path)

    t0 = time.perf_counter()
    df_base.to_json(jsonl_path, orient="records", lines=True, date_format="iso")
    jsonl_write_time = time.perf_counter() - t0
    jsonl_size = get_file_size(jsonl_path)

    t0 = time.perf_counter()
    df_base.to_parquet(parquet_path, index=False, compression="snappy")
    parquet_write_time = time.perf_counter() - t0
    parquet_size = get_file_size(parquet_path)

    # Query Postgres server for actual table size
    with engine.connect() as conn:
        res = conn.execute(text("SELECT pg_total_relation_size('curated.sales_order_lines');"))
        pg_size = res.scalar()

    # 2. Benchmark read performance across 5 iterations
    csv_full_times, csv_filt_times = [], []
    for _ in range(repeats):
        t0 = time.perf_counter()
        df = pd.read_csv(csv_path)
        csv_full_times.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        _ = df[df["status"] == filter_status]
        csv_filt_times.append(time.perf_counter() - t0)

    jsonl_full_times, jsonl_filt_times = [], []
    for _ in range(repeats):
        t0 = time.perf_counter()
        df = pd.read_json(jsonl_path, lines=True)
        jsonl_full_times.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        _ = df[df["status"] == filter_status]
        jsonl_filt_times.append(time.perf_counter() - t0)

    parquet_full_times, parquet_filt_times = [], []
    for _ in range(repeats):
        t0 = time.perf_counter()
        _ = pd.read_parquet(parquet_path)
        parquet_full_times.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        _ = pd.read_parquet(parquet_path, filters=[("status", "==", filter_status)])
        parquet_filt_times.append(time.perf_counter() - t0)

    pg_full_times, pg_filt_times = [], []
    for _ in range(repeats):
        t0 = time.perf_counter()
        _ = pd.read_sql("SELECT * FROM curated.sales_order_lines;", engine)
        pg_full_times.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        _ = pd.read_sql(f"SELECT * FROM curated.sales_order_lines WHERE status = '{filter_status}';", engine)
        pg_filt_times.append(time.perf_counter() - t0)

    benchmark_matrix = {
        "Format": ["CSV", "JSON Lines", "Parquet", "PostgreSQL"],
        "Size_Bytes": [csv_size, jsonl_size, parquet_size, pg_size],
        "Write_Time_Sec": [csv_write_time, jsonl_write_time, parquet_write_time, "N/A (Loaded)"],
        "Full_Read_Median_Sec": [
            np.median(csv_full_times),
            np.median(jsonl_full_times),
            np.median(parquet_full_times),
            np.median(pg_full_times),
        ],
        "Filtered_Read_Median_Sec": [
            np.median(csv_filt_times),
            np.median(jsonl_filt_times),
            np.median(parquet_filt_times),
            np.median(pg_filt_times),
        ],
        "Row_Count": [len(df_base)] * 4,
    }

    df_results = pd.DataFrame(benchmark_matrix)
    print("\n=== Goal 3 Benchmark Matrix ===")
    print(df_results.to_string(index=False))
    return df_results


def write_partitioned_parquet(df, partition_dir="data/partitioned"):
    """Write Hive-style partitioned Parquet by order_year and order_month."""
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["order_year"] = df["order_timestamp"].dt.year
    df["order_month"] = df["order_timestamp"].dt.month

    df.to_parquet(
        partition_dir,
        index=False,
        partition_cols=["order_year", "order_month"],
        compression="snappy",
    )
    print(f"\nWrote partitioned Parquet to {partition_dir}")