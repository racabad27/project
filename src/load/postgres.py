from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text

from src.config import DB


def get_db_engine():
    """Creates a SQLAlchemy engine using application database parameters."""
    url = f"postgresql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['dbname']}"
    return create_engine(url)


def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics."""
    if df.empty:
        return 0

    df_load = df.copy()

    # Ensure optional/schema-required columns exist
    if "status" not in df_load.columns:
        df_load["status"] = "COMPLETED"

    if "source_updated_at" not in df_load.columns:
        df_load["source_updated_at"] = df_load["order_timestamp"]

    engine = get_db_engine()

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS curated;"))

    upsert_sql = text("""
        INSERT INTO curated.sales_order_lines (
            order_id, customer_id, product_id, order_timestamp, status, source_updated_at,
            quantity, unit_price, discount_pct, gross_amount, discount_amount, net_amount,
            pipeline_run_id, processed_at_utc, record_hash
        ) VALUES (
            :order_id, :customer_id, :product_id, :order_timestamp, :status, :source_updated_at,
            :quantity, :unit_price, :discount_pct, :gross_amount, :discount_amount, :net_amount,
            :pipeline_run_id, :processed_at_utc, :record_hash
        )
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            order_timestamp = EXCLUDED.order_timestamp,
            status = EXCLUDED.status,
            source_updated_at = EXCLUDED.source_updated_at,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            discount_pct = EXCLUDED.discount_pct,
            gross_amount = EXCLUDED.gross_amount,
            discount_amount = EXCLUDED.discount_amount,
            net_amount = EXCLUDED.net_amount,
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            processed_at_utc = EXCLUDED.processed_at_utc,
            record_hash = EXCLUDED.record_hash
        WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash;
    """)

    records = df_load.to_dict(orient="records")

    with engine.begin() as conn:
        result = conn.execute(upsert_sql, records)
        rows_affected = result.rowcount

    print(f"[UPSERT] Processed {len(records)} records ({rows_affected} updated/inserted).")
    return rows_affected


def load_partition(df: pd.DataFrame, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    if df.empty:
        return 0

    df_temp = df.copy()
    df_temp["order_timestamp"] = pd.to_datetime(df_temp["order_timestamp"], utc=True)

    partition_mask = (
        (df_temp["order_timestamp"].dt.year == year) & 
        (df_temp["order_timestamp"].dt.month == month)
    )
    partition_df = df_temp[partition_mask].copy()

    if partition_df.empty:
        print(f"[Partition Load] No records found for year={year}, month={month}.")
        return 0

    rows_loaded = upsert_curated(partition_df, run_id)

    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit.partition_loads (
                partition_key TEXT PRIMARY KEY,
                loaded_at_utc TIMESTAMPTZ NOT NULL,
                row_count INTEGER NOT NULL,
                pipeline_run_id TEXT NOT NULL
            );
        """))

        audit_sql = text("""
            INSERT INTO audit.partition_loads (
                partition_key, loaded_at_utc, row_count, pipeline_run_id
            ) VALUES (
                :partition_key, :loaded_at_utc, :row_count, :run_id
            )
            ON CONFLICT (partition_key) DO UPDATE SET
                loaded_at_utc = EXCLUDED.loaded_at_utc,
                row_count = EXCLUDED.row_count,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
        """)

        conn.execute(audit_sql, {
            "partition_key": f"{year}-{month:02d}",
            "loaded_at_utc": datetime.now(timezone.utc),
            "row_count": rows_loaded,
            "run_id": run_id
        })

    print(f"[Partition Load] Audited load of {rows_loaded} rows for partition {year}-{month:02d}.")
    return rows_loaded