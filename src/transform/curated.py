from datetime import datetime, timezone
import hashlib
from pathlib import Path
import pandas as pd

from src.config import path_for


def _generate_record_hash(row: pd.Series) -> str:
    """Creates a deterministic hash of core order values for lineage."""
    raw_str = f"{row['order_id']}_{row['customer_id']}_{row['product_id']}_{row['order_timestamp']}"
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()


def quarantine_records(df_invalid: pd.DataFrame, entity_name: str, run_id: str, reason: str, quarantine_dir: Path):
    """Writes orphan or invalid records to data/quarantine/."""
    if df_invalid.empty:
        return

    quarantine_run_dir = quarantine_dir / f"run_id={run_id}"
    quarantine_run_dir.mkdir(parents=True, exist_ok=True)

    df_quarantine = df_invalid.copy()
    df_quarantine["quarantine_reason"] = reason
    df_quarantine["quarantined_at_utc"] = datetime.now(timezone.utc)

    out_file = quarantine_run_dir / f"invalid_{entity_name}.parquet"
    if out_file.exists():
        existing_df = pd.read_parquet(out_file)
        df_quarantine = pd.concat([existing_df, df_quarantine], ignore_index=True)

    df_quarantine.to_parquet(out_file, index=False)


def build_curated(staging: dict, run_id: str) -> pd.DataFrame:
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Required columns include gross_amount, discount_amount, net_amount,
    processed_at_utc, pipeline_run_id, and record_hash.

    Orphan customer/product references must be quarantined, not silently dropped.
    """
    df_orders = staging.get("orders", pd.DataFrame()).copy()
    df_customers = staging.get("customers", pd.DataFrame()).copy()
    df_products = staging.get("products", pd.DataFrame()).copy()

    quarantine_dir = path_for("quarantine_dir")

    # 1. Check for orphan customer and product references
    valid_customer_ids = set(df_customers["customer_id"].dropna().unique())
    valid_product_ids = set(df_products["product_id"].dropna().unique())

    orphan_cust_mask = ~df_orders["customer_id"].isin(valid_customer_ids)
    orphan_prod_mask = ~df_orders["product_id"].isin(valid_product_ids)

    # Quarantine orphans
    df_orphan_cust = df_orders[orphan_cust_mask]
    df_orphan_prod = df_orders[orphan_prod_mask & ~orphan_cust_mask]

    quarantine_records(df_orphan_cust, "orders_orphan_customer", run_id, "Orphan customer_id reference", quarantine_dir)
    quarantine_records(df_orphan_prod, "orders_orphan_product", run_id, "Orphan product_id reference", quarantine_dir)

    # Filter out all orphans from the valid fact set
    valid_orders_mask = ~orphan_cust_mask & ~orphan_prod_mask
    df_valid_orders = df_orders[valid_orders_mask].copy()

    if df_valid_orders.empty:
        return pd.DataFrame()

    # 2. Joins
    cust_cols = [c for c in ["customer_id", "name", "city", "customer_tier"] if c in df_customers.columns]
    prod_cols = [c for c in ["product_id", "name", "category", "brand"] if c in df_products.columns]

    fact = df_valid_orders.merge(
        df_customers[cust_cols], on="customer_id", how="inner", suffixes=("", "_cust")
    )
    fact = fact.merge(
        df_products[prod_cols], on="product_id", how="inner", suffixes=("", "_prod")
    )

    if "name_cust" in fact.columns:
        fact.rename(columns={"name_cust": "customer_name"}, inplace=True)
    if "name_prod" in fact.columns:
        fact.rename(columns={"name_prod": "product_name"}, inplace=True)

    # 3. Required financial calculations
    fact["gross_amount"] = fact["quantity"] * fact["unit_price"]
    fact["discount_amount"] = fact["gross_amount"] * fact["discount_pct"]
    fact["net_amount"] = fact["gross_amount"] - fact["discount_amount"]

    # 4. Audit lineage & record hash
    fact["processed_at_utc"] = datetime.now(timezone.utc)
    fact["pipeline_run_id"] = run_id
    fact["record_hash"] = fact.apply(_generate_record_hash, axis=1)

    return fact


def curate_all(run_id: str) -> Path:
    """Wrapper function to read staged parquet files and save curated output."""
    staging_base_dir = path_for("staging_dir")
    curated_base_dir = path_for("curated_dir")

    staging_run_dir = staging_base_dir / f"run_id={run_id}"
    curated_run_dir = curated_base_dir / f"run_id={run_id}"

    if not staging_run_dir.exists():
        raise FileNotFoundError(f"Staging directory for run_id={run_id} does not exist at {staging_run_dir}")

    curated_run_dir.mkdir(parents=True, exist_ok=True)

    # Load staged datasets into dictionary
    staging = {
        "customers": pd.read_parquet(staging_run_dir / "customers.parquet"),
        "products": pd.read_parquet(staging_run_dir / "products.parquet"),
        "orders": pd.read_parquet(staging_run_dir / "orders.parquet"),
    }

    # Build curated sales dataframe
    df_curated_sales = build_curated(staging, run_id)

    # Write output
    df_curated_sales.to_parquet(curated_run_dir / "curated_sales.parquet", index=False)

    return curated_run_dir