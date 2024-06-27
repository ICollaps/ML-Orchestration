from airflow.decorators import task
from minio import Minio
from airflow.exceptions import AirflowSkipException

@task
def must_train():

    minio_url = "minio:9000"
    print(f"Connecting to minio on {minio_url}")

    # Configuration MinIO
    minio_client = Minio(
        minio_url,
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "output"

    objects = minio_client.list_objects(bucket_name, recursive=True)

    num_files = 0
    minimum_files = 5

    for obj in objects:
        num_files += 1

    print(f"Currently {num_files} in bucket ")

    if num_files > minimum_files:
        return True
    else:
        raise AirflowSkipException(f"Currently {num_files} in {bucket_name}. Must {minimum_files} to trigger a training")
