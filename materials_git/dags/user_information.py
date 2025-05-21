from airflow import DAG
from datetime import datetime
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pandas import json_normalize


def _store_user():
    hook = PostgresHook(postgres_conn_id = 'postgres')

    file_path = '/tmp/processed_user.csv'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")


    hook.copy_expert(
        sql="COPY users_data FROM stdin WITH DELIMITER as ','",
        filename= file_path
    )

def _process_user(ti):
    user = ti.xcom_pull(task_ids="user_data") 
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)



with DAG ('user_information', schedule_interval = '@daily', start_date = datetime(2025,1,1), catchup = False) as dag:
    
    create_table= PostgresOperator(
        task_id = 'create_table_users',
        postgres_conn_id = 'postgres',
        sql= '''
            CREATE TABLE IF NOT EXISTS users_data (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            ) 
        '''

    )

    is_api_availaible = HttpSensor(
        task_id = 'is_api_availaible',
        http_conn_id = 'httpconn',
        endpoint = 'api/'

    )

    user_data = SimpleHttpOperator(
        task_id = 'user_data',
        http_conn_id = 'httpconn',
        endpoint = 'api/',
        method = 'GET',
        response_filter = lambda response:json.loads(response.text),
        log_response = True
    )

    process_user = PythonOperator(
        task_id ='process_user',
        python_callable = _process_user

    )

    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable = _store_user

    )


    create_table >> is_api_availaible>> user_data >> process_user >> store_user

