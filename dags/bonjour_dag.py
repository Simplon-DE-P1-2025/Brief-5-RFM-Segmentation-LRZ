from __future__ import annotations

from datetime import datetime
import subprocess

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable


def run_bonjour_script() -> None:

    subprocess.run(
        ["python", "/opt/airflow/etl/bonjour.py"],
        check=True,
    )


with DAG(
    dag_id="bonjour_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "etl"],
) as dag:

    PythonOperator(
        task_id="run_bonjour_script",
        python_callable=run_bonjour_script,
    )
