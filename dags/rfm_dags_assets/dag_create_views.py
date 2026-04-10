from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ── Couche ANALYTICS ─────────────────────────────────────────────────────────
ANALYTICS_CUSTOMER_RFM = Asset("postgres://rfm_db/rfm_db/analytics/customer_rfm")

# ── Vues produites ────────────────────────────────────────────────────────────
ANALYTICS_CUSTOMER_FIRST_PURCHASE = Asset("postgres://rfm_db/rfm_db/analytics/customer_first_purchase")
ANALYTICS_CUSTOMER_RFM_V          = Asset("postgres://rfm_db/rfm_db/analytics/customer_rfm_v")


with DAG(
    dag_id="dag_create_views",
    description="Vues d'enrichissement RFM (déclenché par analytics.customer_rfm)",
    schedule=[ANALYTICS_CUSTOMER_RFM],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/etl",
    default_args={"owner": "data", "retries": 1},
    tags=["rfm", "views"],
) as dag:

    # Crée :
    #   - analytics.customer_first_purchase  (date d'acquisition par client)
    #   - analytics.customer_rfm_v           (customer_rfm + segment_label A.→K. + macro_segment)
    create_views = SQLExecuteQueryOperator(
        task_id="create_views",
        conn_id="rfm_db",
        sql="05_view_rfm_v.sql",
        inlets=[ANALYTICS_CUSTOMER_RFM],
        outlets=[
            ANALYTICS_CUSTOMER_FIRST_PURCHASE,
            ANALYTICS_CUSTOMER_RFM_V,
        ],
    )
