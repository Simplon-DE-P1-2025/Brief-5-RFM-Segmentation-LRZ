"""
dags/rfm_pipeline_soda.py — Pipeline RFM avec Data Quality Gates (Soda)
======================================================================
Flux : 
Ingest -> Check Raw -> Transform -> Check Silver -> Segment -> Check Gold -> History
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

ETL_DIR = Path("/opt/airflow/etl")
SODA_DIR = Path("/opt/airflow/soda")
DSN = os.getenv("RFM_DB_DSN", "postgresql://rfm_user:rfm_pass@postgres/rfm_db")

# ─────────────────────────────────────────────────────────────────────
# Helpers (SQL & Soda)
# ─────────────────────────────────────────────────────────────────────

def _run_sql_file(filename: str) -> None:
    """Exécute un fichier SQL via psycopg2."""
    import psycopg2
    sql_path = ETL_DIR / filename
    sql = sql_path.read_text(encoding="utf-8")
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()
    print(f"[airflow] {filename} OK")

def _run_soda_scan(check_file: str, data_source: str = "rfm_db") -> None:
    """
    Exécute un scan Soda. 
    Lève une exception en cas d'échec pour stopper le DAG.
    """
    from soda.scan import Scan
    
    print(f"[soda] Lancement du scan : {check_file}")
    scan = Scan()
    scan.set_data_source_name(data_source)
    
    # Chemins vers la config et les fichiers de checks définis précédemment
    scan.add_configuration_yaml_file(file_path=str(SODA_DIR / "configuration.yml"))
    scan.add_soda_checks_yaml_file(file_path=str(SODA_DIR / "checks" / check_file))
    
    result = scan.execute()
    
    if result != 0:
        raise Exception(f"[soda] Échec de Data Quality sur {check_file}. Consultez les logs pour les détails.")
    print(f"[soda] Scan {check_file} passé avec succès !")

# ─────────────────────────────────────────────────────────────────────
# Tasks Callables
# ─────────────────────────────────────────────────────────────────────

def task_ingest_xlsx():
    if "/opt/airflow" not in sys.path:
        sys.path.insert(0, "/opt/airflow")
    from etl.ingest import ingest
    return ingest(dsn=DSN, truncate=True)

def task_transform_clean_rfm():
    # Nettoyage des tables pour l'idempotence (votre helper existant)
    import psycopg2
    conn = psycopg2.connect(DSN)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE clean.sales, clean.stock_movements, analytics.customer_rfm RESTART IDENTITY CASCADE;")
    conn.commit()
    conn.close()

    _run_sql_file("00_functions.sql")
    _run_sql_file("02_clean.sql")
    _run_sql_file("03_rfm.sql")

def task_load_segments():
    _run_sql_file("04_segments.sql")
    _run_sql_file("05_view_rfm_v.sql")

def task_compute_rfm_history():
    _run_sql_file("06_history.sql")

# ─────────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "rfm-team",
    "retries": 0,
}

with DAG(
    dag_id="rfm_pipeline_soda",
    description="Pipeline RFM avec contrôles Soda SQL",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["rfm", "quality", "soda"],
) as dag:

    # 1. Ingestion
    ingest_xlsx = PythonOperator(
        task_id="ingest_xlsx",
        python_callable=task_ingest_xlsx,
    )

    # 2. Check RAW (On vérifie que les données sont là)
    check_raw = PythonOperator(
        task_id="check_raw",
        python_callable=_run_soda_scan,
        op_kwargs={"check_file": "raw_online_retail.yml"}
    )

    # 3. Clean & RFM
    transform_clean_rfm = PythonOperator(
        task_id="transform_clean_rfm",
        python_callable=task_transform_clean_rfm,
    )

    # 4. Check CLEAN SALES (On vérifie le nettoyage et les types)
    check_clean = PythonOperator(
        task_id="check_silver",
        python_callable=_run_soda_scan,
        op_kwargs={"check_file": "clean_sales.yml"}
    )

    # 5. Segments
    load_segments = PythonOperator(
        task_id="load_segments",
        python_callable=task_load_segments,
    )

    # 6. Check ANALYTICS (On vérifie les scores 1-5 et les segments)
    check_analytics = PythonOperator(
        task_id="check_gold",
        python_callable=_run_soda_scan,
        op_kwargs={"check_file": "analytics_customer_rfm.yml"}
    )

    # 7. Histoire
    compute_rfm_history = PythonOperator(
        task_id="compute_rfm_history",
        python_callable=task_compute_rfm_history,
    )

    # ─────────────────────────────────────────────────────────────────────
    # Orchestration avec verrous
    # ─────────────────────────────────────────────────────────────────────
    ingest_xlsx >> check_raw >> transform_clean_rfm >> check_clean >> load_segments >> check_analytics >> compute_rfm_history