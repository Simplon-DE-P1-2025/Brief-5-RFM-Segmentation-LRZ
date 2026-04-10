import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.sdk import Variable, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


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
# Paramètres par défaut
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 1,
}

# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_rfm",
    default_args=default_args,
    description="Pipeline RFM complet : ingestion XLSX → clean → RFM → segments → historique",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/etl",
    tags=["rfm"],
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # Groupe 1 — Ingestion
    # XLSX → CSV temporaire → COPY → raw.online_retail
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup(group_id="ingestion") as tg_ingestion:
        ingest_xlsx = PythonOperator(
            task_id="ingest_xlsx",
            python_callable=run_ingest,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Groupe 2 — Transformation & RFM
    # raw.* → clean.* → analytics.customer_rfm
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup(group_id="transform_rfm") as tg_transform:
        clean = SQLExecuteQueryOperator(
            task_id="clean",
            conn_id="rfm_db",
            sql="02_clean.sql",
        )

        compute_rfm = SQLExecuteQueryOperator(
            task_id="compute_rfm",
            conn_id="rfm_db",
            sql="03_rfm.sql",
        )

        clean >> compute_rfm

    # ─────────────────────────────────────────────────────────────────────────
    # Groupe 3 — Segmentation
    # CASE WHEN → 11 segments + vues d'enrichissement
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup(group_id="segmentation") as tg_segmentation:
        load_segments = SQLExecuteQueryOperator(
            task_id="load_segments",
            conn_id="rfm_db",
            sql="04_segments.sql",
        )

        create_views = SQLExecuteQueryOperator(
            task_id="create_views",
            conn_id="rfm_db",
            sql="05_view_rfm_v.sql",
        )

        load_segments >> create_views

    # ─────────────────────────────────────────────────────────────────────────
    # Groupe 4 — Historique RFM
    # Snapshot mensuel 24 mois × ~5 850 clients ≈ 97 K lignes
    # → analytics.customer_rfm_history  (Movements + Cohorts du dashboard)
    # ─────────────────────────────────────────────────────────────────────────
    with TaskGroup(group_id="historique") as tg_historique:
        compute_rfm_history = SQLExecuteQueryOperator(
            task_id="compute_rfm_history",
            conn_id="rfm_db",
            sql="06_history.sql",
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Orchestration inter-groupes
    # ─────────────────────────────────────────────────────────────────────────
    tg_ingestion >> tg_transform >> tg_segmentation >> tg_historique