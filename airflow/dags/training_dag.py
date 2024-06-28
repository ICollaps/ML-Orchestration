from datetime import datetime, timedelta

import pandas as pd
from flaml import AutoML
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib
from airflow.decorators import task
import clickhouse_connect
import pandas as pd
from minio import Minio
from io import StringIO
import mlflow

from airflow import DAG
from airflow.decorators import dag, task
from tasks.queries.queries import retreive_all_and_train

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
    description='training_dag',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ml'],
)
def train_dag():

    retrieve_data = retreive_all_and_train('velib')


    retrieve_data

trainning_dag_instance = train_dag()
