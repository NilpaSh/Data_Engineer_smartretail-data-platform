"""
DataPulse — Bronze Layer Loader
================================
Loads raw CSV files into the bronze schema with:
- Duplicate detection (upsert logic)
- Schema validation
- Pipeline run logging
- Error handling & partial load recovery
"""

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

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

# ── Database connection ────────────────────────────────────────────────────
DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'datapulse')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'datapulse123')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'retail_dw')}"
)

DATA_DIR = Path(os.getenv("DATA_OUTPUT_DIR", "./data/raw"))

# ── Table configuration ────────────────────────────────────────────────────
TABLE_CONFIG = {
    "customers": {
        "file": "customers.csv",
        "table": "bronze.raw_customers",
        "dedup_key": "customer_id",
        "expected_columns": [
            "customer_id", "first_name", "last_name", "email", "phone",
            "city", "state", "country", "zip_code", "customer_segment", "registration_date",
        ],
    },
    "products": {
        "file": "products.csv",
        "table": "bronze.raw_products",
        "dedup_key": "product_id",
        "expected_columns": [
            "product_id", "product_name", "category", "subcategory", "brand",
            "cost_price", "selling_price", "stock_quantity", "sku",
        ],
    },
    "orders": {
        "file": "orders.csv",
        "table": "bronze.raw_orders",
        "dedup_key": "order_id",
        "expected_columns": [
            "order_id", "customer_id", "order_date", "ship_date",
            "shipping_method", "order_status", "total_amount", "discount_code",
        ],
    },
    "order_items": {
        "file": "order_items.csv",
        "table": "bronze.raw_order_items",
        "dedup_key": "item_id",
        "expected_columns": [
            "item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct",
        ],
    },
    "events": {
        "file": "events.csv",
        "table": "bronze.raw_events",
        "dedup_key": "event_id",
        "expected_columns": [
            "event_id", "customer_id", "event_type", "product_id",
            "event_timestamp", "session_id", "device_type",
        ],
    },
}


@contextmanager
def get_engine():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


def validate_schema(df: pd.DataFrame, expected_columns: list[str], source: str) -> None:
    """Validates that all expected columns are present."""
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"[{source}] Missing expected columns: {missing}")
    extra = set(df.columns) - set(expected_columns)
    if extra:
        log.warning("[%s] Extra columns found (will be ignored): %s", source, extra)


def get_existing_keys(engine, table: str, key_col: str) -> set:
    """Returns set of already-loaded keys for deduplication."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT {key_col} FROM {table}"))  # noqa: S608
        return {row[0] for row in result}


def load_table(engine, name: str, config: dict) -> dict:
    """
    Load a single CSV into the bronze table.
    Returns stats dict with rows_processed, rows_skipped, status.
    """
    file_path = DATA_DIR / config["file"]
    table = config["table"]
    dedup_key = config["dedup_key"]

    if not file_path.exists():
        log.error("[%s] File not found: %s", name, file_path)
        return {"status": "failed", "rows_processed": 0, "error": f"File not found: {file_path}"}

    log.info("[%s] Reading %s...", name, file_path)
    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    total_rows = len(df)

    # Schema validation
    validate_schema(df, config["expected_columns"], name)
    df = df[config["expected_columns"]]  # Select only expected columns

    # Deduplication: skip rows already in the table
    existing_keys = get_existing_keys(engine, table, dedup_key)
    if existing_keys:
        before = len(df)
        df = df[~df[dedup_key].isin(existing_keys)]
        skipped = before - len(df)
        if skipped:
            log.info("[%s] Skipped %d already-loaded rows", name, skipped)
    else:
        skipped = 0

    if df.empty:
        log.info("[%s] No new rows to load", name)
        return {"status": "success", "rows_processed": 0, "rows_skipped": skipped}

    # Add metadata columns
    df["_source_file"] = config["file"]

    # Write to PostgreSQL
    df.to_sql(
        name=table.split(".")[1],
        schema=table.split(".")[0],
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    new_rows = len(df)
    log.info("[%s] Loaded %d new rows (total in file: %d)", name, new_rows, total_rows)
    return {"status": "success", "rows_processed": new_rows, "rows_skipped": skipped}


def log_pipeline_run(engine, pipeline_name: str, run_date: date, status: str,
                     rows_processed: int, started_at: datetime,
                     completed_at: datetime, error_message: str | None = None) -> None:
    """Records pipeline execution metadata for monitoring."""
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO bronze.pipeline_runs
                    (pipeline_name, run_date, status, rows_processed, started_at, completed_at, error_message)
                VALUES
                    (:pipeline_name, :run_date, :status, :rows_processed, :started_at, :completed_at, :error_message)
            """),
            {
                "pipeline_name": pipeline_name,
                "run_date": run_date,
                "status": status,
                "rows_processed": rows_processed,
                "started_at": started_at,
                "completed_at": completed_at,
                "error_message": error_message,
            },
        )
        conn.commit()


def run_bronze_loader() -> None:
    """Main entry point — loads all source files into the Bronze layer."""
    log.info("=" * 60)
    log.info("DataPulse Bronze Loader — Starting")
    log.info("=" * 60)

    started_at = datetime.utcnow()
    total_rows = 0
    errors = []

    with get_engine() as engine:
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database connection OK")

        for name, config in TABLE_CONFIG.items():
            log.info("-" * 40)
            log.info("Loading: %s", name)
            try:
                result = load_table(engine, name, config)
                total_rows += result.get("rows_processed", 0)
                if result["status"] == "failed":
                    errors.append(f"{name}: {result.get('error', 'unknown error')}")
            except Exception as exc:
                log.exception("[%s] Unexpected error: %s", name, exc)
                errors.append(f"{name}: {exc}")

        completed_at = datetime.utcnow()
        duration = (completed_at - started_at).total_seconds()
        final_status = "failed" if errors else "success"

        log_pipeline_run(
            engine=engine,
            pipeline_name="bronze_loader",
            run_date=started_at.date(),
            status=final_status,
            rows_processed=total_rows,
            started_at=started_at,
            completed_at=completed_at,
            error_message="; ".join(errors) if errors else None,
        )

    log.info("=" * 60)
    log.info("Bronze Loader — Complete")
    log.info("  Status        : %s", final_status.upper())
    log.info("  Rows Loaded   : %d", total_rows)
    log.info("  Duration      : %.1fs", duration)
    if errors:
        log.error("  Errors        : %s", errors)
    log.info("=" * 60)

    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    run_bronze_loader()
