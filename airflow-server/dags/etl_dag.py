import requests
import json
import pandas as pd
import os
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.models.xcom import XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

RECIPES_URL = os.getenv("RECIPES_URL")
API_KEY = os.getenv("API_KEY")
MONGO_URL = os.getenv("MONGO_URL")

def callback_on_failed_task(ti, **context):
    print(f"Failure in the following task: {context['task_instance_key_str']}")

def api_healthcheck(ti, **context):
    headers = { 'x-rapidapi-key': API_KEY, 'x-rapidapi-host': "tasty.p.rapidapi.com"}
    querystring = {"from":"0", "size":"1"}

    try:
        requests.request("GET", RECIPES_URL, headers=headers, params=querystring)
    except Exception as e:
        print(f"Error connecting to the API: {e}")

def fetch_api_data(ti, **context):
    headers = { 'x-rapidapi-key': API_KEY, 'x-rapidapi-host': "tasty.p.rapidapi.com"}
    querystring = {"from":"0", "size":"10"}

    response = response.request("GET", RECIPES_URL, headers=headers, params=querystring)
    # Check if response is NULL    - TO DO
    ti.xcom_push(key='response_data', value=response)
    print(f"Data is sucesfully fetched from {RECIPES_URL}")

def process_response_data(ti, **context):
    response = ti.xcom_pull(key="reponse_data", task_ids=["fetch_api_data"])
    response_json = response.json()
    raise NotImplementedError

def validate_data(ti, **context):
    raise NotImplementedError

def load_data_to_db(ti, **context):
    raise NotImplementedError

def validate_data_load(ti, **context):
    raise NotImplementedError


with DAG(
    dag_id="load_recipes_data",
    schedule_interval=None,
    start_date=datetime(2022,10,28),
    catchup=False,
    tags= ["recipes"],
    default_args={
        "owner": "David",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': callback_on_failed_task
    }
) as dag:

    t1 = PythonOperator(
        task_id='api_healthcheck',
        python_callable=api_healthcheck,
        dag=dag,
        do_xcom_push=True,
        provide_context=True
        )

    t2 = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
        dag=dag,
        provide_context=True
        )
    
    t3 = PythonOperator(
        task_id='process_response_data',
        python_callable=process_response_data,
        dag=dag,
        provide_context=True
        )
    
    t4 = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        dag=dag,
        provide_context=True
        )
    
    t5 = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db,
        dag=dag,
        provide_context=True
        )
    
    t6 = PythonOperator(
        task_id='validate_data_load',
        python_callable=validate_data_load,
        dag=dag,
        provide_context=True
        )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
