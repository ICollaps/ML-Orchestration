import mlflow.sklearn
import mlflow

mlflow.set_registry_uri("http://127.0.0.1:5000")
mlflow.set_tracking_uri("http://127.0.0.1:5000")

model_uri = f"models:/model/1"
model = mlflow.sklearn.load_model(model_uri)

print(model)
