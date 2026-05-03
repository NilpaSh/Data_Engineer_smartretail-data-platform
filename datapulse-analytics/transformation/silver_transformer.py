"""
DataPulse — Silver Layer Transformer
======================================
Transforms Bronze (raw) → Silver (cleaned, typed, deduplicated):
  - Type casting (strings → dates, decimals, integers)
  - Null handling and standardization
  - Email & phone normalization
  - Duplicate removal using last-write-wins
  - Data quality flagging
"""

import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'datapulse')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'datapulse123')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'retail_dw')}"
)


@contextmanager
def get_engine():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


# ── Cleaning helpers ────────────────────────────────────────────────────────

def clean_email(email: str) -> str | None:
    if not email or pd.isna(email):
        return None
    email = str(email).strip().lower()
    return email if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email) else None


def clean_numeric(val, default=0.0) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


def clean_date(val: str) -> datetime | None:
    if not val or str(val).strip() == "":
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except ValueError:
            continue
    return None


def standardize_status(status: str) -> str:
    mapping = {
        "complete": "completed",
        "done": "completed",
        "shipped": "shipped",
        "in_transit": "shipped",
        "processing": "processing",
        "pending": "processing",
        "cancel": "cancelled",
        "canceled": "cancelled",
        "return": "returned",
    }
    return mapping.get(str(status).strip().lower(), str(status).strip().lower())


# ── Transformation functions ─────────────────────────────────────────────────

def transform_customers(engine) -> int:
    log.info("[customers] Reading from bronze...")
    df = pd.read_sql("SELECT * FROM bronze.raw_customers", engine)
    log.info("[customers] %d raw rows loaded", len(df))

    # Deduplicate: keep the most recently ingested record per customer_id
    df = df.sort_values("_ingested_at").drop_duplicates(subset=["customer_id"], keep="last")

    # Clean fields
    df["email"] = df["email"].apply(clean_email)
    df["registration_date"] = df["registration_date"].apply(clean_date)
    df["customer_segment"] = df["customer_segment"].str.upper().str.strip()
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()
    df["state"] = df["state"].str.upper().str.strip()
    df["country"] = df["country"].str.upper().str.strip()

    # Filter out invalid records
    before = len(df)
    df = df.dropna(subset=["customer_id", "first_name", "last_name"])
    df = df[df["customer_id"].str.startswith("CUST-")]
    after = len(df)
    if before != after:
        log.warning("[customers] Dropped %d invalid records", before - after)

    # Select Silver columns
    silver_cols = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "city", "state", "country", "zip_code", "customer_segment", "registration_date",
    ]
    df = df[silver_cols]

    _upsert_to_silver(engine, df, "silver.customers", "customer_id")
    log.info("[customers] %d rows written to silver", len(df))
    return len(df)


def transform_products(engine) -> int:
    log.info("[products] Reading from bronze...")
    df = pd.read_sql("SELECT * FROM bronze.raw_products", engine)

    df = df.sort_values("_ingested_at").drop_duplicates(subset=["product_id"], keep="last")

    # Type casting
    df["cost_price"] = df["cost_price"].apply(lambda x: round(clean_numeric(x), 2))
    df["selling_price"] = df["selling_price"].apply(lambda x: round(clean_numeric(x), 2))
    df["stock_quantity"] = df["stock_quantity"].apply(lambda x: int(clean_numeric(x, 0)))

    # Validate pricing
    before = len(df)
    df = df[(df["selling_price"] > 0) & (df["cost_price"] >= 0)]
    df = df[df["cost_price"] <= df["selling_price"]]  # Cost shouldn't exceed price
    after = len(df)
    if before != after:
        log.warning("[products] Dropped %d rows with invalid pricing", before - after)

    # Standardize strings
    df["category"] = df["category"].str.strip().str.title()
    df["brand"] = df["brand"].str.strip()
    df["product_name"] = df["product_name"].str.strip()

    silver_cols = [
        "product_id", "product_name", "category", "subcategory", "brand",
        "cost_price", "selling_price", "stock_quantity", "sku",
    ]
    df = df[silver_cols]

    _upsert_to_silver(engine, df, "silver.products", "product_id")
    log.info("[products] %d rows written to silver", len(df))
    return len(df)


def transform_orders(engine) -> int:
    log.info("[orders] Reading from bronze...")
    df = pd.read_sql("SELECT * FROM bronze.raw_orders", engine)

    df = df.sort_values("_ingested_at").drop_duplicates(subset=["order_id"], keep="last")

    # Type casting
    df["order_date"] = df["order_date"].apply(clean_date)
    df["ship_date"] = df["ship_date"].apply(clean_date)
    df["total_amount"] = df["total_amount"].apply(lambda x: round(clean_numeric(x), 2))
    df["order_status"] = df["order_status"].apply(standardize_status)

    # Validate
    before = len(df)
    df = df.dropna(subset=["order_id", "customer_id", "order_date"])
    df = df[df["total_amount"] >= 0]
    # Only keep orders from customers that exist in silver
    valid_customers = pd.read_sql("SELECT customer_id FROM silver.customers", engine)["customer_id"].tolist()
    df = df[df["customer_id"].isin(valid_customers)]
    after = len(df)
    if before != after:
        log.warning("[orders] Dropped %d invalid/orphan orders", before - after)

    silver_cols = [
        "order_id", "customer_id", "order_date", "ship_date",
        "shipping_method", "order_status", "total_amount", "discount_code",
    ]
    df = df[silver_cols]

    _upsert_to_silver(engine, df, "silver.orders", "order_id")
    log.info("[orders] %d rows written to silver", len(df))
    return len(df)


def transform_order_items(engine) -> int:
    log.info("[order_items] Reading from bronze...")
    df = pd.read_sql("SELECT * FROM bronze.raw_order_items", engine)

    df = df.sort_values("_ingested_at").drop_duplicates(subset=["item_id"], keep="last")

    # Type casting
    df["quantity"] = df["quantity"].apply(lambda x: max(1, int(clean_numeric(x, 1))))
    df["unit_price"] = df["unit_price"].apply(lambda x: round(clean_numeric(x), 2))
    df["discount_pct"] = df["discount_pct"].apply(lambda x: min(100.0, max(0.0, clean_numeric(x, 0.0))))

    # Validate referential integrity
    valid_orders = pd.read_sql("SELECT order_id FROM silver.orders", engine)["order_id"].tolist()
    valid_products = pd.read_sql("SELECT product_id FROM silver.products", engine)["product_id"].tolist()
    before = len(df)
    df = df[df["order_id"].isin(valid_orders) & df["product_id"].isin(valid_products)]
    after = len(df)
    if before != after:
        log.warning("[order_items] Dropped %d orphan items (missing parent)", before - after)

    silver_cols = ["item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"]
    df = df[silver_cols]

    _upsert_to_silver(engine, df, "silver.order_items", "item_id")
    log.info("[order_items] %d rows written to silver", len(df))
    return len(df)


def _upsert_to_silver(engine, df: pd.DataFrame, table: str, key_col: str) -> None:
    """
    Upsert DataFrame into Silver table.
    Deletes existing rows by key then re-inserts (safe for small tables).
    For large tables, use ON CONFLICT DO UPDATE.
    """
    schema, tbl = table.split(".")
    existing_keys = set(
        pd.read_sql(f"SELECT {key_col} FROM {table}", engine)[key_col].tolist()  # noqa: S608
    )
    new_df = df[~df[key_col].isin(existing_keys)]
    update_df = df[df[key_col].isin(existing_keys)]

    if not update_df.empty:
        # Delete and re-insert updated records
        with engine.connect() as conn:
            keys_to_update = tuple(update_df[key_col].tolist())
            if len(keys_to_update) == 1:
                conn.execute(text(f"DELETE FROM {table} WHERE {key_col} = '{keys_to_update[0]}'"))
            else:
                conn.execute(text(f"DELETE FROM {table} WHERE {key_col} IN {keys_to_update}"))  # noqa: S608
            conn.commit()
        update_df.to_sql(tbl, engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=1000)

    if not new_df.empty:
        new_df.to_sql(tbl, engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=1000)


def run_silver_transformer() -> None:
    log.info("=" * 60)
    log.info("DataPulse Silver Transformer — Starting")
    log.info("=" * 60)

    started_at = datetime.utcnow()
    total_rows = 0

    with get_engine() as engine:
        transformers = [
            ("customers", transform_customers),
            ("products", transform_products),
            ("orders", transform_orders),
            ("order_items", transform_order_items),
        ]

        for name, fn in transformers:
            log.info("-" * 40)
            try:
                rows = fn(engine)
                total_rows += rows
            except Exception as exc:
                log.exception("[%s] Transformation failed: %s", name, exc)
                raise

    duration = (datetime.utcnow() - started_at).total_seconds()
    log.info("=" * 60)
    log.info("Silver Transformer — Complete")
    log.info("  Total Rows : %d", total_rows)
    log.info("  Duration   : %.1fs", duration)
    log.info("=" * 60)


if __name__ == "__main__":
    run_silver_transformer()
