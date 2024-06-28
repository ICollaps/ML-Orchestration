from airflow.decorators import task
from mlflow import MlflowClient
import requests
from mlflow.exceptions import RestException




@task
def promot_model(model_version: int):
    mlflow_client = MlflowClient(tracking_uri="http://mlflow:5000", registry_uri="http://mlflow:5000")
    model_name = "model"

    # Récupérer les détails du modèle entraîné
    trained_model = mlflow_client.get_model_version(name=model_name, version=model_version)
    trained_model_mse = float(trained_model.tags.get("mse", "inf"))

    try:
        # Essayer de récupérer le modèle en production avec l'alias "champion"
        model_in_prod = mlflow_client.get_model_version_by_alias(model_name, "champion")
        model_in_prod_mse = float(model_in_prod.tags.get("mse", "inf"))

        if trained_model_mse < model_in_prod_mse:
            print(f"Trained model got a higher mse ({trained_model_mse}) than current prod model ({model_in_prod_mse}). New model will be promoted to production")
            mlflow_client.set_registered_model_alias(model_name, "champion", str(trained_model.version))
            print(f'Model registered as {model_name} with alias "champion"')

            print("Sending API restart notification ...")
            response = requests.post("http://api:8000/restart/")
            if response.status_code == 200:
                print("API restart notification sent")
            else:
                print(f'Failed to send API restart: {response.status_code} - {response.text}')
        else:
            print("Current prod model is better")

    except RestException as e:
        if "Registered model alias champion not found" in str(e):
            print("No current champion model found. Promoting the trained model as the new champion.")
            mlflow_client.set_registered_model_alias(model_name, "champion", str(trained_model.version))
            print(f'Model registered as {model_name} with alias "champion"')

            print("Sending API restart notification ...")
            response = requests.post("http://api:8000/restart/")
            if response.status_code == 200:
                print("API restart notification sent")
            else:
                print(f'Failed to send API restart: {response.status_code} - {response.text}')
        else:
            raise

