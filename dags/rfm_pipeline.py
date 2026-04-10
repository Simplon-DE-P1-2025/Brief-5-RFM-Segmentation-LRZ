"""
dags/rfm_pipeline.py — Pipeline RFM end-to-end orchestré par Airflow
====================================================================

Tâches (dans l'ordre) :

  1. ingest_xlsx           → etl.ingest.ingest()
                              xlsx → CSV temp → COPY → raw.online_retail
  2. transform_clean_rfm   → 00_functions.sql + 02_clean.sql + 03_rfm.sql
                              installe les fonctions SQL partagées
                              (fn_rfm_segment, fn_rfm_macro, vue
                              v_rfm_codes_coverage), puis raw → clean.*
                              → analytics.customer_rfm
  3. load_segments         → 04_segments.sql + 05_view_rfm_v.sql
                              fn_rfm_segment → 11 segments + vues
                              d'enrichissement
  4. compute_rfm_history   → 06_history.sql (extension "matériel supplémentaire")
                              snapshot mensuel sur 25 mois (Online Retail II)
                              ≈ 97 K lignes dans analytics.customer_rfm_history
                              (≈ 4 415 clients/snapshot en moyenne — un client
                              n'apparaît qu'à partir de sa première vente,
                              donc les premiers mois sont moins peuplés)
                              alimente les pages Movements + Cohorts du
                              dashboard v2 (Sprint 5)

Best practices Airflow appliquées :

- Imports lourds (openpyxl, etl.ingest) à l'intérieur des callables, pas au
  top-level → le scheduler peut parser le DAG sans avoir tous les packages
  installés
- Connexion Postgres lue dans l'env (`RFM_DB_DSN` injecté par compose)
- Les SQL files sont lus depuis `/opt/airflow/etl/` (volume monté)
- Idempotence : TRUNCATE des tables aval avant chaque run

Configuration compose :
- volume `./dags:/opt/airflow/dags` (monte ce fichier)
- volume `./etl:/opt/airflow/etl` (monte les SQL et ingest.py)
- volume `./data:/opt/airflow/data` (monte le XLSX)
- env `RFM_DB_DSN=postgresql://rfm_user:rfm_pass@postgres/rfm_db`
- env `_PIP_ADDITIONAL_REQUIREMENTS=openpyxl==3.1.2` à voir comment gérer via un dockerfile
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
DSN = os.getenv("RFM_DB_DSN", "postgresql://rfm_user:rfm_pass@postgres/rfm_db")


# ─────────────────────────────────────────────────────────────────────
# Helpers (importés dynamiquement à l'intérieur des tasks)
# ─────────────────────────────────────────────────────────────────────


def _run_sql_file(filename: str) -> None:
    """Lit un fichier SQL depuis /opt/airflow/etl/ et l'exécute via psycopg2."""
    import psycopg2

    sql_path = ETL_DIR / filename
    sql = sql_path.read_text(encoding="utf-8")
    print(f"[airflow] Exécution {filename} ({len(sql):,} chars)")

    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()
    print(f"[airflow] {filename} OK")


def _truncate_downstream() -> None:
    """TRUNCATE des tables aval (sans toucher à raw.online_retail)."""
    import psycopg2

    print("[airflow] TRUNCATE clean.* + analytics.customer_rfm")
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE clean.sales,
                         clean.stock_movements,
                         clean.cancellations,
                         clean.non_product_lines,
                         analytics.customer_rfm
                RESTART IDENTITY CASCADE;
                """
            )
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────
# Tasks (callables)
# ─────────────────────────────────────────────────────────────────────


def task_ingest_xlsx() -> dict:
    """Étape 01 : XLSX → raw.online_retail. Délègue à etl.ingest.ingest()."""
    # Import lazy : openpyxl n'est chargé que quand cette task tourne,
    # pas au parsing du DAG par le scheduler
    if "/opt/airflow" not in sys.path:
        sys.path.insert(0, "/opt/airflow")
    from etl.ingest import ingest

    result = ingest(dsn=DSN, truncate=True)
    print(f"[airflow] task_ingest_xlsx → {result}")
    return result


def task_transform_clean_rfm() -> None:
    """Étape 00+02+03 : install des fonctions SQL partagées, cascade
    clean, puis calcul RFM via NTILE(5).

    00_functions.sql installe analytics.fn_rfm_segment / fn_rfm_macro
    et la vue de couverture v_rfm_codes_coverage. Ces objets sont
    consommés par 04_segments.sql, 05_view_rfm_v.sql et 06_history.sql.
    Idempotent : CREATE OR REPLACE FUNCTION / VIEW.
    """
    _truncate_downstream()
    _run_sql_file("00_functions.sql")
    _run_sql_file("02_clean.sql")
    _run_sql_file("03_rfm.sql")


def task_load_segments() -> None:
    """Étape 04+05 : assignation des 11 segments + vues d'enrichissement."""
    _run_sql_file("04_segments.sql")
    _run_sql_file("05_view_rfm_v.sql")


def task_compute_rfm_history() -> None:
    """
    Étape 06 : snapshot mensuel rétrospectif pour les pages Movements
    et Cohorts du dashboard v2.

    Exécute etl/06_history.sql qui :
      1. CREATE IF NOT EXISTS analytics.customer_rfm_history
      2. TRUNCATE
      3. Recalcule R/F/M de chaque client à chaque fin de mois en
         utilisant uniquement les ventes ≤ snapshot_date (CTE
         generate_series + agrégat conditionnel)
      4. Reapplique NTILE(5) puis fn_rfm_segment / fn_rfm_macro
      5. INSERT massif (~97 K lignes ≈ 25 snapshots × ~4 415 clients/mois
         en ~18 s sur Online Retail II — un client n'apparaît qu'à
         partir de sa première vente, donc < 25 × 5 852)
    """
    import psycopg2

    _run_sql_file("06_history.sql")

    # Sanity check : compte les snapshots et la distribution macro au dernier
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT snapshot_date) AS n_snapshots,
                    MIN(snapshot_date)            AS first_snap,
                    MAX(snapshot_date)            AS last_snap,
                    COUNT(*)                      AS total_rows
                FROM analytics.customer_rfm_history;
                """
            )
            n_snap, first, last, total = cur.fetchone()
            cur.execute(
                """
                SELECT macro_segment, COUNT(*)
                FROM analytics.customer_rfm_history
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM analytics.customer_rfm_history)
                GROUP BY macro_segment
                ORDER BY macro_segment;
                """
            )
            macros_last = cur.fetchall()
    finally:
        conn.close()

    print(
        f"[airflow] customer_rfm_history : {total:,} lignes, {n_snap} snapshots ({first} → {last})"
    )
    print(f"[airflow] Distribution macro au dernier snapshot {last} :")
    for macro, n in macros_last:
        print(f"[airflow]   {macro:14s} → {n:>5}")


# ─────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "rfm-team",
    "retries": 0,
}

with DAG(
    dag_id="rfm_pipeline",
    description="Pipeline ETL RFM end-to-end (Phase 3 — Tâche A du brief)",
    schedule=None,  # déclenché manuellement
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["rfm", "perso", "phase3"],
    max_active_runs=1,
) as dag:

    ingest_xlsx = PythonOperator(
        task_id="ingest_xlsx",
        python_callable=task_ingest_xlsx,
    )

    transform_clean_rfm = PythonOperator(
        task_id="transform_clean_rfm",
        python_callable=task_transform_clean_rfm,
    )

    load_segments = PythonOperator(
        task_id="load_segments",
        python_callable=task_load_segments,
    )

    compute_rfm_history = PythonOperator(
        task_id="compute_rfm_history",
        python_callable=task_compute_rfm_history,
    )

    ingest_xlsx >> transform_clean_rfm >> load_segments >> compute_rfm_history
