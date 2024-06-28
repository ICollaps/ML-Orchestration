import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import clickhouse_connect
import xgboost as xgb
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
import mlflow.pyfunc

# Connexion à la base de données ClickHouse
client = clickhouse_connect.get_client(host='127.0.0.1')
select_velib_query = "SELECT * FROM velib;"

# Récupération des données
data = client.query(select_velib_query)
columns = data.column_names
df = pd.DataFrame(data.result_rows, columns=columns)

# Séparer les caractéristiques (features) de la variable cible (target)
X = df.drop(columns=['num_docks_available', 'id', 'stationCode', 'station_id', 'name'])
y = df['num_docks_available']

# Diviser les données en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow_client = MlflowClient(tracking_uri="http://127.0.0.1:5000", registry_uri="http://127.0.0.1:5000")

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("model")

with mlflow.start_run() as run:
    # Initialiser et entraîner le modèle XGBoost
    model = xgb.XGBRegressor(random_state=42)
    model.fit(X_train, y_train)

    # Faire des prédictions sur l'ensemble de test
    y_pred = model.predict(X_test)

    # Évaluer le modèle
    mse = mean_squared_error(y_test, y_pred)
    print(f'Mean Squared Error: {mse}')

    mlflow.log_metric("mse", mse)

    # Journaliser le modèle directement avec MLflow
    mlflow.sklearn.log_model(model, "model")

    model_name = "model"

    registered_model = mlflow.register_model(f"runs:/{run.info.run_id}/model", model_name, tags={'mse': mse})

    model_version = registered_model.version
    print(f"Registered model version: {model_version}")

# Charger le modèle enregistré depuis MLflow en utilisant la version du modèle
model_uri = f"models:/{model_name}/{model_version}"
loaded_model = mlflow.pyfunc.load_model(model_uri)

# Faire des prédictions avec le modèle chargé
loaded_y_pred = loaded_model.predict(X_test)

# Évaluer le modèle chargé
loaded_mse = mean_squared_error(y_test, loaded_y_pred)
print(f'Mean Squared Error of the loaded model: {loaded_mse}')
