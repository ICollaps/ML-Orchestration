from airflow.decorators import task
import requests

@task
def train():

    response = requests.post("http://api:8000/train/")
    if response.status_code == 200:
        print("Training succeded")
        print(f'Success: {response.json()}')
    else:
        print(f'Failed: {response.status_code} - {response.text}')
    
    return response.json()
