from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ── Couche RAW ────────────────────────────────────────────────────────────────
RAW_ONLINE_RETAIL = Asset("postgres://rfm_db/rfm_db/raw/online_retail")

# ── Couche CLEAN ─────────────────────────────────────────────────────────────
CLEAN_SALES             = Asset("postgres://rfm_db/rfm_db/clean/sales")
CLEAN_STOCK_MOVEMENTS   = Asset("postgres://rfm_db/rfm_db/clean/stock_movements")
CLEAN_CANCELLATIONS     = Asset("postgres://rfm_db/rfm_db/clean/cancellations")
CLEAN_NON_PRODUCT_LINES = Asset("postgres://rfm_db/rfm_db/clean/non_product_lines")

# ── Truncate Tables ─────────────────────────────────────────────────────────────
truncate_req="""
    TRUNCATE clean.sales,
                clean.stock_movements,
                clean.cancellations,
                clean.non_product_lines,
                analytics.customer_rfm
    RESTART IDENTITY CASCADE;"""

with DAG(
    dag_id="dag_clean",
    description="Clean raw.online_retail → 4 tables clean (déclenché par raw.online_retail)",
    schedule=[RAW_ONLINE_RETAIL],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/etl",
    default_args={"owner": "data", "retries": 1},
    tags=["rfm", "clean"],
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Truncate — purge des tables clean.* et analytics.customer_rfm
    #    avant reconstruction complète
    # ─────────────────────────────────────────────────────────────────────────
    truncate = SQLExecuteQueryOperator(
        task_id="truncate",
        conn_id="rfm_db",
        sql=truncate_req,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Clean — raw.online_retail → 4 tables clean
    # ─────────────────────────────────────────────────────────────────────────
    clean = SQLExecuteQueryOperator(
        task_id="clean",
        conn_id="rfm_db",
        sql="02_clean.sql",
        inlets=[RAW_ONLINE_RETAIL],
        outlets=[
            CLEAN_SALES,
            CLEAN_STOCK_MOVEMENTS,
            CLEAN_CANCELLATIONS,
            CLEAN_NON_PRODUCT_LINES,
        ],
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Orchestration
    # ─────────────────────────────────────────────────────────────────────────
    truncate >> clean