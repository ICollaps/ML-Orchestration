from airflow.decorators import task

@task
def train():
    print("Training")
