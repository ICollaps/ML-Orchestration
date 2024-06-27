from airflow.decorators import task
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource, REPLACE


@task
def clean_bucket(source_bucket: str, destination_folder: str):
    
    minio_url = "minio:9000"
    print(f"Connecting to minio on {minio_url}")

    minio_client = Minio(
        minio_url,
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    destination_bucket = "archive"

    try:
        if not minio_client.bucket_exists(destination_bucket):
            minio_client.make_bucket(destination_bucket)

        objects = minio_client.list_objects(source_bucket, recursive=True)

        for obj in objects:
            copy_source = CopySource(source_bucket, obj.object_name)
            destination_object_name = f"{destination_folder}/{obj.object_name}"
            minio_client.copy_object(
                bucket_name=destination_bucket,
                object_name=destination_object_name,
                source=copy_source,
            )
            minio_client.remove_object(source_bucket, obj.object_name)
            print(f"Moved {obj.object_name} to {destination_bucket}/{destination_folder}")

    except S3Error as err:
        print(f"Error occurred: {err}")
