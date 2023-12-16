# Import necessary modules and classes from Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

# Import datetime module to specify the start date of the DAG
from datetime import datetime

# Define a Directed Acyclic Graph (DAG) named 'group_dag'
# Set the start date, schedule interval, and disable catch-up to avoid backfilling
with DAG('group_dag', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    # Define BashOperator tasks for downloading data (A, B, C) with simulated duration of 10 seconds each
    download_a = BashOperator(
        task_id='download_a',
        bash_command='sleep 10'  # Simulate downloading data A by sleeping for 10 seconds
    )

    download_b = BashOperator(
        task_id='download_b',
        bash_command='sleep 10'  # Simulate downloading data B by sleeping for 10 seconds
    )

    download_c = BashOperator(
        task_id='download_c',
        bash_command='sleep 10'  # Simulate downloading data C by sleeping for 10 seconds
    )

    # Define BashOperator task for checking files with simulated duration of 10 seconds
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'  # Simulate checking files by sleeping for 10 seconds
    )

    # Define BashOperator tasks for transforming data (A, B, C) with simulated duration of 10 seconds each
    transform_a = BashOperator(
        task_id='transform_a',
        bash_command='sleep 10'  # Simulate transforming data A by sleeping for 10 seconds
    )

    transform_b = BashOperator(
        task_id='transform_b',
        bash_command='sleep 10'  # Simulate transforming data B by sleeping for 10 seconds
    )

    transform_c = BashOperator(
        task_id='transform_c',
        bash_command='sleep 10'  # Simulate transforming data C by sleeping for 10 seconds
    )

    # Set task dependencies to create the execution flow
    [download_a, download_b, download_c] >> check_files  # Downloads must be completed before checking files
    check_files >> [transform_a, transform_b, transform_c]  # Checking files must be completed before transforming data
