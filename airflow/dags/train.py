from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dataset.datasets import output_bucket
from tasks.must_train.main import must_train
from tasks.train.main import train
from tasks.promot_model.main import promot_model
from tasks.clean.main import clean_bucket
from tasks.queries.query import retreive_all_and_train
from airflow.providers.docker.operators.docker import DockerOperator

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

    trained_model_version = train()

    model_promoted = promot_model(model_version=trained_model_version)

    cleaned_bucket = clean_bucket(source_bucket="output", destination_folder="output")

    trigger_train >> trained_model_version  >> model_promoted >> cleaned_bucket

train_dag_instance = train_dag()