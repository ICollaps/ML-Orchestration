from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dataset.datasets import output_bucket
from tasks.must_train.main import must_train
from tasks.train.main import train
from tasks.clean.main import clean_bucket
default_args = {
    'owner': 'esgi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='train',
    schedule=[output_bucket],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
)
def train_dag():


    trigger_train = must_train()
    training = train()
    cleaned = clean_bucket(source_bucket="output", destination_folder="output")

    trigger_train >> training >> cleaned

train_dag_instance = train_dag()