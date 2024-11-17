"""
## Example DAG: Create and Read CSV File with Logging

This DAG demonstrates creating a CSV file and reading it in a subsequent task.
It uses Airflow's TaskFlow API to define Python-based tasks and manage dependencies.

The first task creates a CSV file with some sample data, and the second task
reads the file and prints its contents.

"""

import csv
import logging
from airflow.decorators import dag, task
from pendulum import datetime
import os

import os

# Get the current working directory
current_path = os.getcwd()
print("Current Path:", current_path)

# Navigate to the parent folder
parent_path = os.path.dirname(current_path)
print("Parent Path:", parent_path)

# Navigate to the grandparent folder
grandparent_path = os.path.dirname(parent_path)
print("Grandparent Path:", grandparent_path)

# List all directories and files in the current path
contents = os.listdir(current_path)
print("Contents of Current Path:", contents)



# Define default arguments for the DAG
default_args = {"owner": "Airflow", "retries": 2}

# Set up logging
logger = logging.getLogger("airflow.task")

# Define the DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["example", "csv", "logging"],
    doc_md=__doc__,
)

def create_and_read_csv111():


    # Get the current working directory
    current_path = os.getcwd()
    print("Current Path:", current_path)

    # Navigate to the parent folder
    parent_path = os.path.dirname(current_path)
    print("Parent Path:", parent_path)

    # Navigate to the grandparent folder
    grandparent_path = os.path.dirname(parent_path)
    print("Grandparent Path:", grandparent_path)

    # List all directories and files in the current path
    contents_current = os.listdir(current_path)
    print("Contents of Current Path:", contents_current)

    # List all directories and files in the parent path
    contents_parent = os.listdir(parent_path) if os.path.exists(parent_path) else []
    print("Contents of Parent Path:", contents_parent)

    # List all directories and files in the grandparent path
    contents_grandparent = os.listdir(grandparent_path) if os.path.exists(grandparent_path) else []
    print("Contents of Grandparent Path:", contents_grandparent)


    # Task to create a CSV file
    @task
    def create_csv():

        current_path = os.getcwd()
        print("Current Path:", current_path)
        parent_path = os.path.dirname(current_path)
        print("Parent Path:", parent_path)
        grandparent_path = os.path.dirname(parent_path)
        print("Grandparent Path:", grandparent_path)
        contents = os.listdir(current_path)
        print("Contents of Current Path:", contents)


        # Get the current working directory
        current_path = os.getcwd()
        print("Current Path:", current_path)

        # Navigate to the parent folder
        parent_path = os.path.dirname(current_path)
        print("Parent Path:", parent_path)

        # Navigate to the grandparent folder
        grandparent_path = os.path.dirname(parent_path)
        print("Grandparent Path:", grandparent_path)

        # List all directories and files in the current path
        contents_current = os.listdir(current_path)
        print("Contents of Current Path:", contents_current)

        # List all directories and files in the parent path
        contents_parent = os.listdir(parent_path) if os.path.exists(parent_path) else []
        print("Contents of Parent Path:", contents_parent)

        # List all directories and files in the grandparent path
        contents_grandparent = os.listdir(grandparent_path) if os.path.exists(grandparent_path) else []
        print("Contents of Grandparent Path:", contents_grandparent)






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

        # Log the start of the task
        logger.info("Starting to create CSV file.")

        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)

        # Log file creation
        logger.info(f"CSV file created successfully at {file_path}")
        logger.info(f"File contents: Header: {header}, Rows: {rows}")
        return file_path

    # Task to read the CSV file and print its contents
    @task
    def read_csv(file_path: str):
        """
        Read the created CSV file and print its contents.
        """
        logger.info(f"Starting to read CSV file from {file_path}")

        if not os.path.exists(file_path):
            logger.error(f"CSV file not found at {file_path}")
            raise FileNotFoundError(f"CSV file not found at {file_path}")

        with open(file_path, mode="r") as file:
            reader = csv.reader(file)
            logger.info("CSV file contents:")
            for row in reader:
                logger.info(row)

        logger.info(f"Finished reading CSV file from {file_path}")

    # Define the dependencies
    file_path = create_csv()
    read_csv(file_path)


# Instantiate the DAG
create_and_read_csv111()
