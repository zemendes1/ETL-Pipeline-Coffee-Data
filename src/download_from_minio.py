from minio import Minio
from minio.error import S3Error


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
