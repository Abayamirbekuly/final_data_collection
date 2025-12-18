import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Указываем Airflow, где искать папку src
sys.path.append('/opt/airflow/src')

# ВАЖНО: убедись, что в job1_producer.py функция называется именно run_producer
from job1_producer import run_producer 

with DAG('job1_api_to_kafka', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='run_producer_task',
        python_callable=run_producer
    )