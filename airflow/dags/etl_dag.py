from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../etl")))
from etl import run_etl

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("countries_etl",
         default_args=default_args,
         start_date=datetime(2025,1,1),
         schedule_interval="@12h",
         catchup=False) as dag:
    
    etl_task = PythonOperator(
        task_id="run_countries_etl",
        python_callable=run_etl
    )