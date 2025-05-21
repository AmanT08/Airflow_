from airflow import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

with DAG ('user_information', schedule_interval = '@daily', start_date = datetime(2025,1,1), catchup = False) as dag:
    
    create_table= PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql= '''
            CREATE TABLE IF NOT EXISTS USER_DATA(
            FIRST_NAME TEXT NOT NULL,
            LAST_NAME TEXT NOT NULL,
            AGE INTEGER NOT NULL,
            EMAIL TEXT NOT NULL
            ) 
        '''

    )

    check_api_availaibility = HttpSensor(
        task_id = 'is_api_availaible',
        http_conn_id = 'httpconn',
        endpoint = 'api/'

    )

    get_user_data = SimpleHttpOperator(
        task_id = 'user_data',
        http_conn_id = 'httpconn',
        endpoint = 'api/',
        method = 'GET',
        response_filter = lambda response:json.loads(response.text),
        log_response = True
    )

