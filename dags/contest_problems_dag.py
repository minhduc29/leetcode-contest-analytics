import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Fix ModuleNotFoundError

from operators.contest_problems_ops import etl_contest_problems, etl_problem_tags

default_args = {
    "owner": "minhduc29",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 17)
}

# Initialize DAG
dag = DAG(
    "contest_problems_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Get all problems from past contests
etl_problems = PythonOperator(
    task_id="etl_contest_problems",
    python_callable=etl_contest_problems,
    op_args=[1],
    dag=dag
)

# Get all tags for each problem
etl_tags = PythonOperator(
    task_id="etl_problem_tags",
    python_callable=etl_problem_tags,
    dag=dag
)

etl_problems >> etl_tags
