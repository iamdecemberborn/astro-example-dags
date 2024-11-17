"""
## Example DAG: Create and Read CSV File

This DAG demonstrates creating a CSV file and reading it in a subsequent task.
It uses Airflow's TaskFlow API to define Python-based tasks and manage dependencies.

The first task creates a CSV file with some sample data, and the second task
reads the file and prints its contents.

"""

import csv
from airflow.decorators import dag, task
from pendulum import datetime
import os

# Define default arguments for the DAG
default_args = {"owner": "Airflow", "retries": 2}

# Define the DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["example", "csv"],
    doc_md=__doc__,
)
def create_and_read_csv():

    # Task to create a CSV file
    @task
    def create_csv():
        """
        Create a CSV file with sample data.
        """
        file_path = "/tmp/sample_data.csv"
        header = ["id", "name", "age"]
        rows = [
            [1, "Alice", 30],
            [2, "Bob", 25],
            [3, "Charlie", 35],
        ]

        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)

        print(f"CSV file created at {file_path}")
        return file_path

    # Task to read the CSV file and print its contents
    @task
    def read_csv(file_path: str):
        """
        Read the created CSV file and print its contents.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found at {file_path}")

        with open(file_path, mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                print(row)

    # Define the dependencies
    file_path = create_csv()
    read_csv(file_path)


# Instantiate the DAG
create_and_read_csv()

