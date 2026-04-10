from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ── Couche CLEAN ──────────────────────────────────────────────────────────────
CLEAN_SALES = Asset("postgres://rfm_db/rfm_db/clean/sales")

# ── Couche ANALYTICS ─────────────────────────────────────────────────────────
ANALYTICS_CUSTOMER_RFM   = Asset("postgres://rfm_db/rfm_db/analytics/customer_rfm")
ANALYTICS_CUSTOMER_RFM_V = Asset("postgres://rfm_db/rfm_db/analytics/customer_rfm_v")

# ── Table produite ────────────────────────────────────────────────────────────
ANALYTICS_CUSTOMER_RFM_HISTORY = Asset("postgres://rfm_db/rfm_db/analytics/customer_rfm_history")


with DAG(
    dag_id="dag_compute_rfm_history",
    description="Snapshot mensuel RFM 24 mois → analytics.customer_rfm_history (déclenché par customer_rfm_v)",
    schedule=[ANALYTICS_CUSTOMER_RFM_V],     # ← après create_views dans la chaîne
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/etl",
    default_args={"owner": "data", "retries": 1},
    tags=["rfm", "history"],
) as dag:

    # Lit   : clean.sales + analytics.customer_rfm
    # Produit : analytics.customer_rfm_history (~24 × ~5 850 ≈ 140 K lignes)
    compute_rfm_history = SQLExecuteQueryOperator(
        task_id="compute_rfm_history",
        conn_id="rfm_db",
        sql="06_history.sql",
        inlets=[CLEAN_SALES, ANALYTICS_CUSTOMER_RFM],
        outlets=[ANALYTICS_CUSTOMER_RFM_HISTORY],
    )
