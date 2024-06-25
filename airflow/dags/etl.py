from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.extract.main import extract
from tasks.transform.main import transform
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

    extracted = extract()
    transformed = transform()

    extracted >> transformed

etl_dag_instance = etl_dag()
