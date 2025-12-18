import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/src')

from job2_cleaner import run_cleaner

with DAG('job2_kafka_to_sqlite', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='run_cleaner_task',
        python_callable=run_cleaner
    )