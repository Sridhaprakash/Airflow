# Import necessary modules and classes from Airflow
from airflow import DAG, Dataset
from airflow.decorators import task

# Import datetime module to specify the start date of the DAG
from datetime import date, datetime

# Define a Dataset representing the file path "/tmp/my_file.txt"
my_file = Dataset("/tmp/my_file.txt")

# Define a Directed Acyclic Graph (DAG) named 'consumer'
# Set the schedule to trigger when the 'my_file' Dataset changes
# Set the start date and disable catch-up to avoid backfilling
with DAG(
    dag_id="consumer",
    schedule=[my_file],
    start_date=datetime(2024, 1, 1),
    catchup=False
):

    # Define a task 'read_dataset' using the @task decorator
    @task
    def read_dataset():
        # Open and read the contents of the file specified by 'my_file' Dataset
        with open(my_file.uri, "r") as f:
            print(f.read())

    # Execute the 'read_dataset' task
    read_dataset()
