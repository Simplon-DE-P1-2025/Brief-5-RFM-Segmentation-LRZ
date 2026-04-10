import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.sdk import Variable, Asset
from airflow.providers.standard.operators.python import PythonOperator


# ── Couche RAW ────────────────────────────────────────────────────────────────
RAW_ONLINE_RETAIL = Asset("postgres://rfm_db/rfm_db/raw/online_retail")


# ─────────────────────────────────────────────────────────────────────────────
# Callable Python
# ─────────────────────────────────────────────────────────────────────────────

def run_ingest() -> dict:
    """Ingère le fichier XLSX vers raw.online_retail via COPY PostgreSQL."""
    if "/opt/airflow" not in sys.path:
        sys.path.insert(0, "/opt/airflow")

    from etl.ingest import ingest

    dsn = Variable.get(
        "RFM_DB_DSN",
        default=os.getenv("RFM_DB_DSN", "postgresql://rfm_user:rfm_pass@postgres/rfm_db"),
    )
    xlsx_path = Variable.get(
        "DATA_PATH",
        default=os.getenv("DATA_PATH", "/opt/airflow/data/online_retail_II.xlsx"),
    )

    return ingest(dsn=dsn, xlsx_path=xlsx_path, truncate=True)


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_ingest",
    description="Ingestion XLSX → raw.online_retail",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/etl",
    default_args={"owner": "data", "retries": 1},
    tags=["rfm", "ingest"],
) as dag:

    ingest_xlsx = PythonOperator(
        task_id="ingest_xlsx",
        python_callable=run_ingest,
        outlets=[RAW_ONLINE_RETAIL],         # ← publie l'asset
    )
