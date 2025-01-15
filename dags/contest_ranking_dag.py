import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Fix ModuleNotFoundError

from operators.contest_ranking_ops import extract_contest_ranking, transform_contest_ranking

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

"""
Approach 1: ETL pipeline
- Extract raw data
- Load into local storage/Amazon S3 (Optional)
- Transform locally with pandas
- Reload into local storage/Amazon S3
- Load from S3 to Amazon Redshift

Approach 2: ELT pipeline
- Extract raw data
- Load into Amazon S3
- Transform with AWS Glue
- Load the transformed data to Amazon Redshift
"""

# Extract raw data directly from API and store in local/cloud storage
extract = PythonOperator(
    task_id="extract_contest_ranking",
    python_callable=extract_contest_ranking,
    op_args=[2],
    dag=dag
)

# Approach 1: Transform the data and reload into local/cloud storage
transform = PythonOperator(
    task_id="transform_contest_ranking",
    python_callable=transform_contest_ranking,
    dag=dag
)

extract >> transform

# Approach 2 is done with AWS
