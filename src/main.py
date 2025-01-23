import boto3
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')

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


def download_from_s3(bucket_name, object_name, file_name=None):
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID , aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION_NAME)
    file_name = file_name or object_name
    s3_client.download_file(bucket_name, object_name, file_name)
    
    if os.path.exists(file_name):
        print(f"File {file_name} successfully downloaded.")
    else:
        print(f"File {file_name} not found after download.")


if __name__ == "__main__":

    fileNames =["Coffee_domestic_consumption.csv", "Coffee_production.csv"]
    
    for fileName in fileNames:
        if not os.path.exists(fileName):
            download_from_s3("coffe-dataset-example", fileName)
    

    connection = connect_to_postgres()
    create_tables(connection)
    
    populate_tables(connection, fileNames)
