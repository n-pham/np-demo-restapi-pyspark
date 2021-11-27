from airflow import DAG
from dotenv import load_dotenv, find_dotenv
from os import environ

load_dotenv(find_dotenv())

print (environ.get("AIRFLOW_HOME"))

DAG_SCHEDULE_INTERVAL = environ.get("DAG_SCHEDULE_INTERVAL")

dag = DAG(
    dag_id='daily_transactions_dag',
    schedule_interval=DAG_SCHEDULE_INTERVAL)