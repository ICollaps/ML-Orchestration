from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.clean.clean_data import clean_data
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

    cleaned = clean_data()
    extracted = extract()
    transformed = transform()
    loaded = load()

    cleaned >> extracted >> transformed >> loaded

etl_dag_instance = etl_dag()
