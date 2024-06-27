from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.clean.main import clean_bucket
from tasks.extract.main import extract
from tasks.transform.main import transform
from tasks.load.main import load
# Default arguments for the DAG



default_args = {
    'owner': 'esgi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='ETL',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
)
def etl_dag():

    cleaned = clean_bucket(source_bucket="data", destination_folder="data")
    extraction_timestamp = extract()
    transform_timestamp = transform(timestamp=extraction_timestamp)
    loaded = load(timestamp=transform_timestamp)


    cleaned >> extraction_timestamp >> transform_timestamp >> loaded

etl_dag_instance = etl_dag()
