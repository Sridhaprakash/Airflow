from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
 
import json
from pandas import json_normalize
from datetime import datetime
 
# Define a function to process user data
def _process_user(ti):
    # Retrieve user data from a previous task
    user = ti.xcom_pull(task_ids="extract_user")
    # Extract details of the first user
    user = user['results'][0]
    # Flatten the user data and save it to a CSV file
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)
 
# Define a function to store user data in PostgreSQL
def _store_user():
    # Create a connection to PostgreSQL
    hook = PostgresHook(postgres_conn_id='postgres')
    # Copy data from a CSV file to the "users" table
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )
 
# Define a DAG (Directed Acyclic Graph)
with DAG('user_processing', start_date=datetime(2022, 1, 1), 
        schedule_interval='@daily', catchup=False) as dag:
    
    # Task to create the "users" table in PostgreSQL
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )
 
    # Task to check if the API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )
 
    # Task to extract user data from an API
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
 
    # Task to process user data using a Python function
    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )
 
    # Task to store user data in PostgreSQL using a Python function
    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )
 
    # Define the task dependencies
    create_table >> is_api_available >> extract_user >> process_user >> store_user
