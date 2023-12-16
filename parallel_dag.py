# Import necessary modules and classes from Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

# Import datetime module to specify the start date of the DAG
from datetime import datetime

# Define a Directed Acyclic Graph (DAG) named 'parallel_dag'
# Set the start date, schedule interval, and disable catch-up to avoid backfilling
with DAG('parallel_dag', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    # Define BashOperator task 'extract_a' to simulate extraction operation A
    extract_a = BashOperator(
        task_id='extract_a',
        bash_command='sleep 1'  # Simulate extraction operation A by sleeping for 1 second
    )

    # Define BashOperator task 'extract_b' to simulate extraction operation B
    extract_b = BashOperator(
        task_id='extract_b',
        bash_command='sleep 1'  # Simulate extraction operation B by sleeping for 1 second
    )

    # Define BashOperator task 'load_a' to simulate loading operation A
    load_a = BashOperator(
        task_id='load_a',
        bash_command='sleep 1'  # Simulate loading operation A by sleeping for 1 second
    )

    # Define BashOperator task 'load_b' to simulate loading operation B
    load_b = BashOperator(
        task_id='load_b',
        bash_command='sleep 1'  # Simulate loading operation B by sleeping for 1 second
    )

    # Define BashOperator task 'transform' to simulate transformation operation
    transform = BashOperator(
        task_id='transform',
        bash_command='sleep 1'  # Simulate transformation operation by sleeping for 1 second
    )

    # Set task dependencies to create the parallel execution flow
    extract_a >> load_a  # 'extract_a' must be completed before 'load_a' starts
    extract_b >> load_b  # 'extract_b' must be completed before 'load_b' starts
    [load_a, load_b] >> transform  # 'load_a' and 'load_b' must be completed before 'transform' starts
