from pathlib import Path
import pandas as pd

from src.config import path_for


def validate_staged_layer(run_id: str) -> bool:
    """Validates schema integrity and null constraints in staging outputs."""
    staging_dirs = sorted(path_for("staging_dir").glob("run_id=*"))
    if not staging_dirs:
        print("[Validation Error] No staging output directories found.")
        return False
    
    staging_dir = path_for("staging_dir") / f"run_id={run_id}"
    if not staging_dir.exists():
        staging_dir = staging_dirs[-1]

    cust_df = pd.read_parquet(staging_dir / "customers.parquet")
    prod_df = pd.read_parquet(staging_dir / "products.parquet")
    orders_df = pd.read_parquet(staging_dir / "orders.parquet")

    assert cust_df["customer_id"].notna().all(), "Staged customers contain null IDs"
    assert prod_df["product_id"].notna().all(), "Staged products contain null IDs"
    assert orders_df["order_id"].notna().all(), "Staged orders contain null IDs"

    assert (prod_df["unit_price"] > 0).all(), "Staged products contain non-positive prices"
    assert (orders_df["quantity"] > 0).all(), "Staged orders contain non-positive quantities"

    print("[Validation] Staging layer passed all integrity checks.")
    return True


def validate_curated_layer(run_id: str) -> bool:
    """Validates financial math, lineage, and record hashes in curated outputs."""
    curated_dirs = sorted(path_for("curated_dir").glob("run_id=*"))
    if not curated_dirs:
        print("[Validation Error] No curated output directories found.")
        return False

    curated_dir = path_for("curated_dir") / f"run_id={run_id}"
    if not curated_dir.exists():
        curated_dir = curated_dirs[-1]

    curated_file = curated_dir / "curated_sales.parquet"
    if not curated_file.exists():
        print("[Validation] No curated sales file found.")
        return False

    df = pd.read_parquet(curated_file)
    if df.empty:
        print("[Validation] Curated dataset is empty.")
        return True

    # Validate financial calculations: net_amount == gross_amount - discount_amount
    calc_net = df["gross_amount"] - df["discount_amount"]
    assert (df["net_amount"] - calc_net).abs().max() < 1e-4, "Financial math mismatch in net_amount"

    # Validate lineage
    assert df["record_hash"].notna().all(), "Curated records missing record_hash"
    assert df["processed_at_utc"].notna().all(), "Curated records missing processed_at_utc"

    print("[Validation] Curated layer passed all financial and lineage checks.")
    return True


def run_validations(run_id: str) -> bool:
    """Executes all data quality and layer assertions."""
    print(f"\n--- Running Quality & Validation Checks [run_id: {run_id}] ---")
    staged_ok = validate_staged_layer(run_id)
    curated_ok = validate_curated_layer(run_id)
    return staged_ok and curated_ok