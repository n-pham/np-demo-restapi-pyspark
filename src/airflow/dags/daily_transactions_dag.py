import logging
from dotenv import load_dotenv, find_dotenv
from os import environ
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

from pipelines.daily_transactions import process_daily_transactions

load_dotenv(find_dotenv())

DAG_SCHEDULE_INTERVAL = environ.get("DAG_SCHEDULE_INTERVAL")
FILE_DATE = environ.get("FILE_DATE", datetime.today().strftime('%Y%m%d'))
FILE_LOCATION = environ.get("FILE_LOCATION")
logging.info(f'AIRFLOW_HOME: {environ.get("AIRFLOW_HOME")}')
logging.info(f'DAG_SCHEDULE_INTERVAL: {DAG_SCHEDULE_INTERVAL}')
logging.info(f'FILE_LOCATION: {FILE_LOCATION}')

default_args = {
    'start_date': datetime(2021, 11, 27),
    'owner': 'airflow'
}

with DAG(
    dag_id='daily_transactions_dag',
    default_args=default_args,
    # TODO Later, use FileSensor to trigger instead of scheduling
    schedule_interval=DAG_SCHEDULE_INTERVAL) as dag:

    process_task = PythonOperator(
            task_id='process_daily_transactions_task',
            python_callable=process_daily_transactions,
            op_kwargs={'file_location': FILE_LOCATION,'date_part': FILE_DATE})

    stop_task = DummyOperator(task_id= "stop")

process_task >> stop_task