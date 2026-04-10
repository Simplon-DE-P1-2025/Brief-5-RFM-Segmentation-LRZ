from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ── Couche ANALYTICS ─────────────────────────────────────────────────────────
ANALYTICS_CUSTOMER_RFM = Asset("postgres://rfm_db/rfm_db/analytics/customer_rfm")


with DAG(
    dag_id="dag_load_segments",
    description="Segmentation RFM → 11 segments (déclenché par analytics.customer_rfm)",
    schedule=[ANALYTICS_CUSTOMER_RFM],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/etl",
    default_args={"owner": "data", "retries": 1},
    tags=["rfm", "segments"],
) as dag:

    load_segments = SQLExecuteQueryOperator(
        task_id="load_segments",
        conn_id="rfm_db",
        sql="04_segments.sql",
        inlets=[ANALYTICS_CUSTOMER_RFM],
    )
