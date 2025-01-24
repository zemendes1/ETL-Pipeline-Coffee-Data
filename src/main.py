import boto3
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from minio import Minio
from minio.error import S3Error
import time


def connect_to_postgres():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user="myuser",
        password="mypassword",
        database="mydatabase"
    )
    print("Connected to Postgres")
    return conn

def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            country_id SERIAL PRIMARY KEY,
            country VARCHAR(100) UNIQUE,
            coffeeType VARCHAR(100)  
        );
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS coffee_domestic_consumption (
            id SERIAL PRIMARY KEY,
            country_id INT REFERENCES countries(country_id),     
            year VARCHAR(100),
            consumption FLOAT
        );
        """)
        
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS coffee_production (
            id SERIAL PRIMARY KEY,
            country_id INT REFERENCES countries(country_id),     
            year VARCHAR(100),
            production FLOAT
        );
        """)
    
    conn.commit()

def populate_tables(conn, filenames):

    cursor = conn.cursor()

    dataFrameComsumption = pd.read_csv(filenames[0])
    dataFrameProduction = pd.read_csv(filenames[1])
    country_number = 0
    for country in dataFrameComsumption["Country"]:
        cursor.execute("INSERT INTO countries (country, coffeeType) VALUES (%s, %s) ON CONFLICT (country) DO NOTHING  RETURNING country_id", (country, dataFrameComsumption["Coffee type"][country_number]))

        result = cursor.fetchone()  # Get the inserted country_id
        if result is None:
            print(f"Country '{country}' was not inserted, skipping.")
            country_number += 1
            continue  # Skip to the next country if no country_id was returned

        country_id = result[0]

        years = dataFrameComsumption.columns[2:-1]
        
        for year in years:
            cursor.execute ("INSERT INTO coffee_domestic_consumption (country_id, year, consumption) VALUES (%s, %s, %s)", (country_id , year, int(dataFrameComsumption[year][country_number])))

            cursor.execute ("INSERT INTO coffee_production (country_id, year, production) VALUES (%s, %s, %s)", (country_id , year, int(dataFrameProduction[year][country_number])))

        country_number += 1
            
    conn.commit()


def download_from_minio(bucket_name, object_name):
    minio_client = Minio(
        "minio:9000",  # MinIO server address
        access_key="ROOTNAME",  # MinIO root user
        secret_key="CHANGEME123",  # MinIO root password
        secure=False  # Disable HTTPS
    )

    try:
        minio_client.bucket_exists("coffee-dataset-example")
        print("Connection to MinIO successful!")
    except S3Error as e:
        print(f"MinIO S3Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    try:
        minio_client.fget_object(bucket_name, object_name, object_name)
        print(f"File {object_name} successfully downloaded from MinIO.")
    except S3Error as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":

    fileNames =["Coffee_domestic_consumption.csv", "Coffee_production.csv"]
    
    for fileName in fileNames:
        if not os.path.exists(fileName):
            download_from_minio("coffee-dataset-example", fileName)  # Update with your MinIO bucket name
    

    connection = connect_to_postgres()
    create_tables(connection)
    
    populate_tables(connection, fileNames)
