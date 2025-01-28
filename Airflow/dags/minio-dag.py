from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
import os
import pandas as pd
import subprocess
import sys
import pkg_resources
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def check_for_new_files():
    """
    Probes the PostgreSQL database to check if there are new file upload events.
    """

    pg_hook = PostgresHook(postgres_conn_id='postgres')
    sql = """
        SELECT key, value
        FROM events
        WHERE processed IS NULL;
    """
    records = pg_hook.get_records(sql)

    if not records:
        logging.info("No new files to process.")
        return []

    logging.info(f"New files detected: {[record[1] for record in records]}")
    return records


def process_files(**kwargs):
    """
    Processes the files detected from the database and marks them as processed.
    """
    records = kwargs['ti'].xcom_pull(task_ids='check_for_new_files')

    if not records:
        logging.info("No files to process.")
        return

    pg_hook = PostgresHook(postgres_conn_id='postgres')

    for record in records:
        key, value = record
        logging.info(f"Processing file: {key}, uploaded at {value}")

        bucketName, fileName = key.split('/', 1)
        logging.info(f"{bucketName},{fileName}")

        fileProcessor(bucketName, fileName)

        # Mark the file as processed in the database
        update_sql = f"""
            UPDATE events
            SET processed = TRUE
            WHERE key = '{key}';
        """
        pg_hook.run(update_sql)

# Function to install a library if not already installed


def install(package, version="7.0.3"):
    try:
        # Check if the package is installed
        pkg_resources.get_distribution(package)
        logging.info(f"{package} is already installed.")
    except pkg_resources.DistributionNotFound:
        logging.info(f"{package} not found. Installing version {version}...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", f"{package}=={version}"])


def fileProcessor(bucket_name, object_name):
    # Install compatible Minio version if needed
    install('minio', version="7.0.3")
    from minio import Minio
    from minio.error import S3Error

    minio_client = Minio(
        "host.docker.internal:9000",  # MinIO server address
        access_key="ROOTNAME",  # MinIO root user
        secret_key="CHANGEME123",  # MinIO root password
        secure=False  # Disable HTTPS
    )

    try:
        minio_client.bucket_exists("coffee-dataset-example")
        logging.info("Connection to MinIO successful!")
    except S3Error as e:
        logging.info(f"MinIO S3Error: {e}")
    except Exception as e:
        logging.info(f"Unexpected error: {e}")

    # Define the local path to save the file temporarily
    local_file_path = object_name  # You can specify a different path if needed

    try:
        # Download the object from MinIO to a local file
        minio_client.fget_object(bucket_name, object_name, local_file_path)
        logging.info(
            f"File '{object_name}' successfully downloaded from MinIO.")

        # Process the file here
        sendProcessedInfoToDb(local_file_path)

        # After processing, delete the file to release space
        os.remove(local_file_path)
        logging.info(f"File '{object_name}' deleted after processing.")

    except S3Error as e:
        logging.info(f"Error occurred during file download or processing: {e}")
    except Exception as e:
        logging.info(f"Unexpected error: {e}")


def sendProcessedInfoToDb(localFilePath):
    dataCoffeePrice = pd.read_csv(localFilePath)

    coffeePrice = dataCoffeePrice["value"]
    atTime = dataCoffeePrice["date"]

    logging.info(
        f"The coffee price is {coffeePrice.iloc[0]} at date {atTime.iloc[0]}.")

    pg_hook = PostgresHook(postgres_conn_id='postgres')

    insertSql = f"""
                INSERT INTO coffee_price (date, price)
                VALUES ('{atTime.iloc[0]}', '{coffeePrice.iloc[0]}'
                );
    """

    pg_hook.run(insertSql)

    logging.info("Newest price instance added to the coffee_price table")


# Define default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2025, 1, 27),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'file_upload_processor',
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Run every 5 minutes
    catchup=False,
    tags=['file_processing']
)

# Define tasks
check_files_task = PythonOperator(
    task_id='check_for_new_files',
    python_callable=check_for_new_files,
    dag=dag
)

process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    provide_context=True,
    dag=dag
)

# Define task dependencies
check_files_task >> process_files_task
