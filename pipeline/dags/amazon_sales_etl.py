"""
Amazon Sales ETL — Airflow 3.2.0 DAG
Runs: extract → transform → load → cleanup
Schedule: daily at 06:00 UTC
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

# Task logic lives in /opt/pipelines/tasks/ (PYTHONPATH=/opt/pipelines)
from tasks.amazon_tasks import (
    run_extract,
    run_transform,
    run_load,
    run_cleanup,
)

# ---------------------------------------------------------------------------
# Connection params — read from env vars set in the .env template
# ---------------------------------------------------------------------------
_PG_CONN = {
    "host":     os.environ["DB_HOST"],
    "database": os.environ["DB_NAME"],
    "user":     os.environ["DB_USER"],
    "password": os.environ["DB_PASS"],
    "port":     int(os.environ.get("DB_PORT", 5432)),
}

# Path to the source CSV inside the container
_CSV_PATH = os.environ.get(
    "AMAZON_CSV_PATH",
    "/opt/pipelines/data/Amazon Sale Report.csv"
)

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------
_DEFAULT_ARGS = {
    "retries":           2,
    "retry_delay":       timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="amazon_sales_etl",
    description="Full ETL pipeline — Amazon Sales CSV → PostgreSQL star schema",
    schedule="0 6 * * *",           # daily at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,              # avoid overlapping runs on the same data
    default_args=_DEFAULT_ARGS,
    tags=["etl", "amazon", "postgres"],
)
def amazon_sales_etl():

    # ------------------------------------------------------------------ #
    # TASK 1 — Extract                                                    #
    # Reads the CSV from disk and writes a raw parquet to /tmp.           #
    # Returns the temp file path passed to the next task via XCom.        #
    # ------------------------------------------------------------------ #
    @task(queue="default")
    def extract() -> str:
        return run_extract(_CSV_PATH)

    # ------------------------------------------------------------------ #
    # TASK 2 — Transform                                                  #
    # Reads raw parquet, applies all cleaning rules, writes clean parquet. #
    # ------------------------------------------------------------------ #
    @task(queue="default")
    def transform(raw_path: str) -> str:
        return run_transform(raw_path)

    # ------------------------------------------------------------------ #
    # TASK 3 — Load                                                       #
    # Upserts dimensions (product / date / shipment) then inserts facts.  #
    # Returns total row count in FACTS_SALES for logging purposes.        #
    # ------------------------------------------------------------------ #
    @task(queue="default")
    def load(clean_path: str) -> int:
        return run_load(clean_path, _PG_CONN)

    # ------------------------------------------------------------------ #
    # TASK 4 — Cleanup                                                    #
    # Deletes both temp parquet files regardless of upstream result.      #
    # trigger_rule=ALL_DONE ensures it runs even if load fails.           #
    # ------------------------------------------------------------------ #
    @task(queue="default", trigger_rule="all_done")
    def cleanup(raw_path: str, clean_path: str) -> None:
        run_cleanup(raw_path, clean_path)

    # ------------------------------------------------------------------ #
    # WIRE THE DAG                                                        #
    # ------------------------------------------------------------------ #
    raw_path_xcom   = extract()
    clean_path_xcom = transform(raw_path_xcom)
    load(clean_path_xcom)
    cleanup(raw_path_xcom, clean_path_xcom)


# Instantiate the DAG so Airflow picks it up
amazon_sales_etl()