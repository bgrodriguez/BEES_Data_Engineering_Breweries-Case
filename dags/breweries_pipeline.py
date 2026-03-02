from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    "owner": "bruno",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def run_script(script_name):
    project_path = "/opt/airflow/src"
    script_path = os.path.join(project_path, script_name)

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise Exception(f"Erro ao executar {script_name}: {result.stderr}")

with DAG(
    dag_id="breweries_medallion_pipeline",
    default_args=default_args,
    description="Pipeline Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bees", "medallion"],
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_layer",
        python_callable=run_script,
        op_args=["bronze.py"],
    )

    silver_task = PythonOperator(
        task_id="silver_layer",
        python_callable=run_script,
        op_args=["silver.py"],
    )

    gold_task = PythonOperator(
        task_id="gold_layer",
        python_callable=run_script,
        op_args=["gold.py"],
    )

    bronze_task >> silver_task >> gold_task