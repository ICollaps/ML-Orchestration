from airflow.decorators import task

@task
def load():
    print("load")