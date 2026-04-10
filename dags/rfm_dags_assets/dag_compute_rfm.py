from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ── Couche CLEAN ─────────────────────────────────────────────────────────────
CLEAN_SALES = Asset("postgres://rfm_db/rfm_db/clean/sales")

# ── Couche ANALYTICS ─────────────────────────────────────────────────────────
ANALYTICS_CUSTOMER_RFM = Asset("postgres://rfm_db/rfm_db/analytics/customer_rfm")


with DAG(
    dag_id="dag_compute_rfm",
    description="Calcul RFM depuis clean.sales (déclenché par clean.sales)",
    schedule=[CLEAN_SALES],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/etl",
    default_args={"owner": "data", "retries": 1},
    tags=["rfm", "compute"],
) as dag:

    compute_rfm = SQLExecuteQueryOperator(
        task_id="compute_rfm",
        conn_id="rfm_db",
        sql="03_rfm.sql",
        inlets=[CLEAN_SALES],
        outlets=[ANALYTICS_CUSTOMER_RFM],
    )