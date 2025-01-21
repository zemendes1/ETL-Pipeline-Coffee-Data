import boto3
import psycopg2

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

def upload_to_s3(bucket_name, file_name, object_name=None):
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_name, bucket_name, object_name or file_name)
    print(f"Uploaded {file_name} to bucket {bucket_name}")

def download_from_s3(bucket_name, object_name, file_name=None):
    s3_client = boto3.client('s3')
    file_name = file_name or object_name
    s3_client.download_file(bucket_name, object_name, file_name)
    print(f"Downloaded {object_name} from bucket {bucket_name} to {file_name}")


if __name__ == "__main__":
    print("example")
    #download_from_s3("coffe-dataset-example", "Coffee_domestic_consumption.csv", "example.csv")


    # connect_to_postgres()
