from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

from etl_patient_journey_schedule import build_patient_journey_schedule_window

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'patient_journey_pipeline',
    default_args=default_args,
    description='ETL pipeline to denormalize patient journey data',
    schedule_interval=None,
)

run_pipeline = PythonOperator(
    task_id='run_patient_journey_etl',
    python_callable=build_patient_journey_schedule_window,
    op_args=['postgresql://localhost:5432/msk_db','patient_journey_schedule_window'],
    dag=dag,
)

#orchestrate
run_pipeline
