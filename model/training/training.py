import pandas as pd
from flaml import AutoML
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib
import mlflow
import mlflow.sklearn
import logging
# Charger les données depuis le fichier CSV
file_path = '080523-2days-correct.csv'
data = pd.read_csv(file_path)

# Séparer les caractéristiques (features) de la variable cible (target)
X = data.drop(columns=['num_docks_available'])
y = data['num_docks_available']

# Séparer les données en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialiser le modèle AutoML
automl = AutoML()

automl_settings = {
    "time_budget": 120,  # Limite de temps en secondes
    "task": "regression",  # Ou "regression" selon le type de problème
    "log_file_name": "flaml_logs.log",  # Fichier journal pour enregistrer les logs
}

# Entraîner le modèle
automl.fit(X_train, y_train, **automl_settings)

# Faire des prédictions sur l'ensemble de test
y_pred = automl.predict(X_test)

# Évaluer le modèle
mse = mean_squared_error(y_test, y_pred)
print(f'Mean Squared Error: {mse}')

# Afficher les meilleures configurations
print('Best estimator:', automl.best_estimator)
print('Best hyperparameters:', automl.best_config)
print('Best run time:', automl.best_config_train_time)

# Sauvegarder le modèle
model_path = 'flaml_model.pkl'
joblib.dump(automl, model_path)
print(f'Model saved to {model_path}')

# Charger le modèle sauvegardé pour prédiction future
loaded_model = joblib.load(model_path)

# Exemple de prédiction avec le modèle chargé
example_data = X_test.iloc[:1]  # Utiliser la première ligne de X_test comme exemple
example_prediction = loaded_model.predict(example_data)
print(f'Example prediction: {example_prediction}')

mlflow.set_tracking_uri("http://mlflow:5001")  # Update with your MLflow server URI

try:
    with mlflow.start_run() as run:
        mlflow.log_param("time_budget", automl_settings["time_budget"])
        mlflow.log_param("task", automl_settings["task"])
        mlflow.log_param("estimator_list", automl_settings["estimator_list"])
        mlflow.log_metric("mse", mse)
        mlflow.sklearn.log_model(loaded_model, "model")
        mlflow.log_artifact(model_path, artifact_path="model_artifacts")
        logging.info("Model and artifact logged to MLflow")

        # Register the model
        model_uri = f"runs:/{run.info.run_id}/model"
        mlflow.register_model(model_uri, "VelibModel")
        logging.info("Model registered to MLflow")
except Exception as e:
    logging.error(f"MLflow logging failed: {e}")