from datetime import datetime, timezone
import json
from pathlib import Path
import pandas as pd

from src.config import path_for


def quarantine_records(df_invalid: pd.DataFrame, entity_name: str, run_id: str, reason: str, quarantine_dir: Path):
    """Writes invalid records to data/quarantine/ with a reason."""
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


def stage_customers(raw_run_dir: Path, run_id: str, quarantine_dir: Path) -> pd.DataFrame:
    """Cleans, normalizes, deduplicates, and validates customers data."""
    csv_path = raw_run_dir / "customers.csv"
    df = pd.read_csv(csv_path)

    # 1. Clean IDs, normalize strings, and combine names
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["first_name"] = df["first_name"].astype(str).str.strip()
    df["last_name"] = df["last_name"].astype(str).str.strip()
    df["name"] = (df["first_name"] + " " + df["last_name"]).str.strip()
    
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["city"] = df["city"].astype(str).str.strip().str.title()
    df["customer_tier"] = df["customer_tier"].astype(str).str.strip().str.lower()

    # Parse timestamps as UTC
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")

    # 2. Validation: quarantine missing essential fields
    invalid_mask = df["customer_id"].isna() | (df["customer_id"] == "") | df["email"].isna()
    df_invalid = df[invalid_mask]
    df_valid = df[~invalid_mask].copy()

    quarantine_records(df_invalid, "customers", run_id, "Missing customer_id or email", quarantine_dir)

    # 3. Deduplicate by business key (customer_id), keeping greatest updated_at
    df_valid = df_valid.sort_values("updated_at").groupby("customer_id").last().reset_index()

    # 4. Add audit metadata
    df_valid["pipeline_run_id"] = run_id
    df_valid["staged_at_utc"] = datetime.now(timezone.utc)

    return df_valid


def stage_products(raw_run_dir: Path, run_id: str, quarantine_dir: Path) -> pd.DataFrame:
    """Cleans, flattens, validates, and deduplicates products data."""
    json_path = raw_run_dir / "products.json"
    
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # 1. Normalize strings and flatten category
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["brand"] = df["brand"].astype(str).str.strip()
    
    df["category"] = df["category"].apply(
        lambda x: x.get("name") if isinstance(x, dict) else str(x)
    ).str.strip().str.lower()

    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")

    # 2. Validate product unit_price (must be strictly positive)
    invalid_mask = df["product_id"].isna() | df["unit_price"].isna() | (df["unit_price"] <= 0)
    df_invalid = df[invalid_mask]
    df_valid = df[~invalid_mask].copy()

    quarantine_records(df_invalid, "products", run_id, "Invalid product unit_price (<= 0 or NaN)", quarantine_dir)

    # 3. Deduplicate by business key (product_id), keeping greatest updated_at
    df_valid = df_valid.sort_values("updated_at").groupby("product_id").last().reset_index()

    # 4. Add audit metadata
    df_valid["pipeline_run_id"] = run_id
    df_valid["staged_at_utc"] = datetime.now(timezone.utc)

    return df_valid


def stage_orders(raw_run_dir: Path, run_id: str, quarantine_dir: Path) -> pd.DataFrame:
    """Cleans, validates order quantity/status, deduplicates, and adds audit columns."""
    csv_path = raw_run_dir / "orders.csv"
    df = pd.read_csv(csv_path)

    # 1. Parse timestamps as UTC & Clean fields
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.lower()

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0.0)

    # Calculate total_amount
    df["total_amount"] = df["quantity"] * df["unit_price"] * (1 - df["discount_pct"])

    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], utc=True, errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")

    # 2. Validate order quantity (>0) and status
    valid_statuses = ["completed", "pending", "cancelled", "shipped", "delivered"]
    invalid_mask = (
        df["order_id"].isna()
        | df["quantity"].isna()
        | (df["quantity"] <= 0)
        | (~df["status"].isin(valid_statuses))
    )

    df_invalid = df[invalid_mask]
    df_valid = df[~invalid_mask].copy()

    quarantine_records(df_invalid, "orders", run_id, "Invalid order quantity or status", quarantine_dir)

    # 3. Deduplicate by business key (order_id), keeping greatest updated_at
    df_valid = df_valid.sort_values("updated_at").groupby("order_id").last().reset_index()

    # 4. Add audit metadata
    df_valid["pipeline_run_id"] = run_id
    df_valid["staged_at_utc"] = datetime.now(timezone.utc)

    return df_valid


def stage_all(run_id: str) -> Path:
    """Main staging entrypoint."""
    raw_base_dir = path_for("raw_dir")
    staging_base_dir = path_for("staging_dir")
    quarantine_dir = path_for("quarantine_dir")

    raw_run_dir = raw_base_dir / f"run_id={run_id}"
    staging_run_dir = staging_base_dir / f"run_id={run_id}"

    if not raw_run_dir.exists():
        raise FileNotFoundError(f"Raw directory for run_id={run_id} does not exist at {raw_run_dir}")

    staging_run_dir.mkdir(parents=True, exist_ok=True)

    # Process entities
    df_customers = stage_customers(raw_run_dir, run_id, quarantine_dir)
    df_products = stage_products(raw_run_dir, run_id, quarantine_dir)
    df_orders = stage_orders(raw_run_dir, run_id, quarantine_dir)

    # Write staged Parquet outputs
    df_customers.to_parquet(staging_run_dir / "customers.parquet", index=False)
    df_products.to_parquet(staging_run_dir / "products.parquet", index=False)
    df_orders.to_parquet(staging_run_dir / "orders.parquet", index=False)

    return staging_run_dir