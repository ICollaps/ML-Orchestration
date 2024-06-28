from airflow.decorators import task
import clickhouse_connect
import pandas as pd
from flaml import AutoML
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error


@task
def retreive_all_and_train(table_name: str):
    client = clickhouse_connect.get_client(host='clickhouse')
    print("Connecting to clickhouse")

    select_velib_query = f"SELECT * FROM {table_name};"

    data = client.query(select_velib_query)
    #print("icici", data.result_rows)
    columns = data.column_names
    #print("columns", columns)
    df_data = pd.DataFrame(data.result_rows, columns=columns)
    print(df_data.columns)

    # Séparer les caractéristiques (features) de la variable cible (target)
    X = df_data.drop(columns=['num_docks_available'])
    y = df_data['num_docks_available']

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
    #automl.fit(X_train, y_train, **automl_settings)

    # Entraîner le modèle
    try:
        automl.fit(X_train, y_train, **automl_settings)
    except Exception as e:
        print(f"AutoML fit failed: {e}")
        return

    # Faire des prédictions sur l'ensemble de test
    y_pred = automl.predict(X_test)
    print("")
    # Évaluer le modèle
    mse = mean_squared_error(y_test, y_pred)
    print(f'Mean Squared Error: {mse}')

    # Afficher les meilleures configurations
    print('Best estimator:', automl.best_estimator)
    print('Best hyperparameters:', automl.best_config)
    print('Best run time:', automl.best_config_train_time)

