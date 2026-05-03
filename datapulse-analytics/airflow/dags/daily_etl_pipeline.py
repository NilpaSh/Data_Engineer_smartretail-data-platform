"""
DataPulse — Daily ETL Pipeline DAG
====================================
Orchestrates the full pipeline daily at 06:00 UTC:
  1. Generate or ingest source data
  2. Load into Bronze layer
  3. Transform to Silver layer
  4. Run dbt to build Gold layer
  5. Run data quality checks
  6. Notify on success/failure

Demonstrates:
  - TaskGroup organization
  - XCom for passing metrics between tasks
  - Retry logic with exponential backoff
  - SLA monitoring
  - Conditional branching
"""

import logging
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

log = logging.getLogger(__name__)

# ── Default args ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@datapulse.io"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),
}

PROJECT_ROOT = "/opt/airflow/project"


# ── Python callables ─────────────────────────────────────────────────────────

def check_source_files(**context) -> str:
    """Branch: check if new data files are available."""
    import os
    data_dir = f"{PROJECT_ROOT}/data/raw"
    has_files = any(
        f.endswith(".csv") for f in os.listdir(data_dir)
    ) if os.path.exists(data_dir) else False

    if has_files:
        log.info("Source files found — proceeding with ingest")
        return "bronze_load"
    else:
        log.info("No source files found — generating synthetic data first")
        return "generate_data"


def run_bronze_loader(**context) -> dict:
    """Load raw CSVs into Bronze schema and push stats to XCom."""
    sys.path.insert(0, PROJECT_ROOT)
    from ingestion.bronze_loader import run_bronze_loader as load

    try:
        load()
        stats = {"status": "success", "timestamp": datetime.utcnow().isoformat()}
        context["ti"].xcom_push(key="bronze_stats", value=stats)
        return stats
    except Exception as exc:
        raise RuntimeError(f"Bronze loader failed: {exc}") from exc


def run_silver_transformer(**context) -> dict:
    """Transform Bronze → Silver and push row counts to XCom."""
    sys.path.insert(0, PROJECT_ROOT)
    from transformation.silver_transformer import run_silver_transformer as transform

    transform()
    stats = {"status": "success", "timestamp": datetime.utcnow().isoformat()}
    context["ti"].xcom_push(key="silver_stats", value=stats)
    return stats


def validate_data_quality(**context) -> None:
    """Run basic data quality assertions on Silver tables."""
    import os
    import psycopg2

    conn_params = {
        "host":     os.getenv("POSTGRES_HOST", "postgres"),
        "port":     int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname":   os.getenv("POSTGRES_DB", "retail_dw"),
        "user":     os.getenv("POSTGRES_USER", "datapulse"),
        "password": os.getenv("POSTGRES_PASSWORD", "datapulse123"),
    }

    QUALITY_CHECKS = [
        # Name, query, expected: 0 rows means PASS
        ("no_duplicate_customers",
         "SELECT customer_id FROM silver.customers GROUP BY 1 HAVING COUNT(*) > 1",
         "Duplicate customer_ids found"),

        ("no_negative_order_amounts",
         "SELECT order_id FROM silver.orders WHERE total_amount < 0",
         "Orders with negative total_amount found"),

        ("no_future_orders",
         "SELECT order_id FROM silver.orders WHERE order_date > CURRENT_DATE",
         "Orders with future order_date found"),

        ("orders_have_customers",
         """SELECT o.order_id FROM silver.orders o
            LEFT JOIN silver.customers c USING (customer_id)
            WHERE c.customer_id IS NULL""",
         "Orders without matching customers found"),

        ("items_have_products",
         """SELECT i.item_id FROM silver.order_items i
            LEFT JOIN silver.products p USING (product_id)
            WHERE p.product_id IS NULL""",
         "Order items without matching products found"),
    ]

    failures = []
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for check_name, query, error_msg in QUALITY_CHECKS:
                cur.execute(query)
                rows = cur.fetchall()
                if rows:
                    failures.append(f"FAIL [{check_name}]: {error_msg} ({len(rows)} rows)")
                    log.error("DQ FAIL: %s — %d rows", check_name, len(rows))
                else:
                    log.info("DQ PASS: %s", check_name)

    if failures:
        raise ValueError(f"Data quality checks failed:\n" + "\n".join(failures))

    log.info("All data quality checks passed!")


def pipeline_success_callback(context) -> None:
    """Called on successful DAG completion — could push to Slack/PagerDuty."""
    dag_run = context.get("dag_run")
    log.info(
        "Pipeline SUCCESS | DAG: %s | Run: %s | Duration: %s",
        dag_run.dag_id,
        dag_run.run_id,
        datetime.utcnow() - dag_run.start_date,
    )


def pipeline_failure_callback(context) -> None:
    """Called on DAG failure — send alert."""
    exception = context.get("exception")
    task = context.get("task_instance")
    log.error(
        "Pipeline FAILED | Task: %s | Error: %s",
        task.task_id,
        exception,
    )


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="datapulse_daily_etl",
    description="DataPulse daily ETL: Bronze → Silver → Gold (dbt)",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",   # Daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,               # Prevent concurrent runs
    tags=["datapulse", "etl", "daily"],
    on_success_callback=pipeline_success_callback,
    on_failure_callback=pipeline_failure_callback,
    doc_md="""
## DataPulse Daily ETL Pipeline

Runs every day at 06:00 UTC and:
1. Checks for source data files
2. Loads raw data into **Bronze** layer
3. Cleans & types data into **Silver** layer
4. Builds analytical models in **Gold** layer (via dbt)
5. Validates data quality

**Retry policy**: 2 retries with exponential backoff (5m → 10m → 20m)
**SLA**: Must complete within 3 hours
    """,
) as dag:

    start = EmptyOperator(task_id="pipeline_start")
    end = EmptyOperator(task_id="pipeline_end", trigger_rule="none_failed_min_one_success")

    # ── Branch: check for source files ────────────────────────────────────────
    check_files = BranchPythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    # ── Generate synthetic data (only if no files found) ────────────────────
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"cd {PROJECT_ROOT} && python data_generation/generate_ecommerce_data.py",
        doc_md="Generates 2 years of synthetic retail data",
    )

    # ── Bronze Group ──────────────────────────────────────────────────────────
    with TaskGroup("bronze_ingestion", tooltip="Load raw data into Bronze layer") as bronze_group:
        bronze_load = PythonOperator(
            task_id="bronze_load",
            python_callable=run_bronze_loader,
            doc_md="Loads CSV files into bronze.raw_* tables",
        )

    # ── Silver Group ──────────────────────────────────────────────────────────
    with TaskGroup("silver_transformation", tooltip="Clean & type Bronze → Silver") as silver_group:
        silver_transform = PythonOperator(
            task_id="silver_transform",
            python_callable=run_silver_transformer,
            doc_md="Cleans and types data into silver.* tables",
        )

    # ── Data Quality ──────────────────────────────────────────────────────────
    data_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=validate_data_quality,
        doc_md="Validates Silver layer integrity before dbt run",
    )

    # ── Gold Layer via dbt ────────────────────────────────────────────────────
    with TaskGroup("dbt_gold", tooltip="Build analytical Gold layer via dbt") as dbt_group:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {PROJECT_ROOT}/dbt_project && dbt deps",
        )

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"cd {PROJECT_ROOT}/dbt_project && dbt run --target prod",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_project && "
                f"dbt test --target prod --store-failures"
            ),
        )

        dbt_docs = BashOperator(
            task_id="dbt_docs_generate",
            bash_command=f"cd {PROJECT_ROOT}/dbt_project && dbt docs generate --target prod",
        )

        dbt_deps >> dbt_run >> dbt_test >> dbt_docs

    # ── DAG Wiring ────────────────────────────────────────────────────────────
    (
        start
        >> check_files
        >> [generate_data, bronze_load]
    )
    generate_data >> bronze_load
    bronze_load >> silver_transform >> data_quality >> dbt_group >> end
