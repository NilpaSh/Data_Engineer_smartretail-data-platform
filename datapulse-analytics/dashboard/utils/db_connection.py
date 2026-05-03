"""
DataPulse — Database connection utility
"""

import os
from functools import lru_cache

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_db_url() -> str:
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER', os.getenv('POSTGRES_USER', 'datapulse'))}:"
        f"{os.getenv('DB_PASSWORD', os.getenv('POSTGRES_PASSWORD', 'datapulse123'))}@"
        f"{os.getenv('DB_HOST', os.getenv('POSTGRES_HOST', 'localhost'))}:"
        f"{os.getenv('DB_PORT', os.getenv('POSTGRES_PORT', '5432'))}/"
        f"{os.getenv('DB_NAME', os.getenv('POSTGRES_DB', 'retail_dw'))}"
    )


@lru_cache(maxsize=1)
def get_engine():
    return create_engine(get_db_url(), pool_pre_ping=True, pool_size=5)


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Results are NOT cached."""
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def check_connection() -> bool:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
