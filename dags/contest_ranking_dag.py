import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Fix ModuleNotFoundError

from operators.contest_ranking_ops import extract_contest_ranking

default_args = {
    "owner": "minhduc29",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 15)
}

# Initialize DAG
dag = DAG(
    "contest_ranking_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Extract raw data directly from API and store in local/cloud storage
extract = PythonOperator(
    task_id=f"extract_contest_ranking",
    python_callable=extract_contest_ranking,
    op_args=[4],
    dag=dag
)
