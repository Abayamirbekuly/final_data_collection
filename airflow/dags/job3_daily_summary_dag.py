import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/src')

from job3_analytics import run_analytics

with DAG('job3_daily_analytics', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='run_analytics_task',
        python_callable=run_analytics
    )