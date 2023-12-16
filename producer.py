# Import necessary modules and classes from Airflow
from airflow import DAG, Dataset
from airflow.decorators import task

# Import datetime module to specify the start date of the DAG
from datetime import date, datetime

# Define a Dataset representing the file path "/tmp/my_file.txt"
my_file = Dataset("/tmp/my_file.txt")

# Define a Directed Acyclic Graph (DAG) named 'producer'
# Set the schedule to trigger the DAG daily, starting from January 1, 2024
# Disable catch-up to avoid backfilling
with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
):

    # Define a task 'update_dataset' using the @task decorator
    # Specify 'my_file' as an outlet to update the Dataset
    @task(outlets=[my_file])
    def update_dataset():
        # Open the file specified by 'my_file' Dataset in append mode and write "producer update"
        with open(my_file.uri, "a+") as f:
            f.write("producer update")

    # Execute the 'update_dataset' task
    update_dataset()
