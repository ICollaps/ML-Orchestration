from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from clickhouse_connect import Client as ClickhouseClient
import mlflow
import mlflow.tensorflow
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import numpy as np

# ClickHouse configuration
CLICKHOUSE_HOST = 'your_clickhouse_host'
CLICKHOUSE_USER = 'your_clickhouse_user'
CLICKHOUSE_PASSWORD = 'your_clickhouse_password'
CLICKHOUSE_DB = 'your_clickhouse_database'

# MLflow configuration
mlflow.set_tracking_uri("http://your_mlflow_tracking_server")


def query_data():
    client = ClickhouseClient(host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
                              database=CLICKHOUSE_DB)
    query = "SELECT * FROM default.velib"
    df = client.query_dataframe(query)
    df.to_csv('/tmp/data.csv', index=False)  # Save the data for next steps


def preprocess_data():
    df = pd.read_csv('/tmp/data.csv')
    df['stationCode'] = df['stationCode'].astype(str)
    df.drop(columns=['minute'], axis=1, inplace=True)
    df['flag_availability'] = df["futur_availability"].apply(lambda x: 1 if x > 0.0 else 0)
    df.drop(columns='futur_availability', axis=1, inplace=True)
    one_hot_df = pd.get_dummies(df, prefix={'Cluster': 'cluster', 'weekend': 'weekend', 'jour_ferie': 'jour_ferie'},
                                columns=['Cluster', 'weekend', 'jour_ferie'], drop_first=False)
    one_hot_df.to_csv('/tmp/processed_data.csv', index=False)


def split_data():
    df = pd.read_csv('/tmp/processed_data.csv')
    features = df.drop(columns=['stationCode', 'flag_availability'], axis=1)
    target = df['flag_availability']

    feature_scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_features = feature_scaler.fit_transform(features)

    X_train, X_test, y_train, y_test = train_test_split(scaled_features, target, test_size=0.2, shuffle=False)

    X_train = np.reshape(X_train, (X_train.shape[0], 1, X_train.shape[1]))
    X_test = np.reshape(X_test, (X_test.shape[0], 1, X_test.shape[1]))

    np.save('/tmp/X_train.npy', X_train)
    np.save('/tmp/X_test.npy', X_test)
    np.save('/tmp/y_train.npy', y_train)
    np.save('/tmp/y_test.npy', y_test)


def train_model():
    X_train = np.load('/tmp/X_train.npy')
    X_test = np.load('/tmp/X_test.npy')
    y_train = np.load('/tmp/y_train.npy')
    y_test = np.load('/tmp/y_test.npy')

    model = Sequential()
    model.add(Dense(32, activation='relu', input_shape=(1, X_train.shape[2])))
    model.add(Dense(16, activation='relu'))
    model.add(Dense(1, activation='sigmoid'))

    model.compile(optimizer='adam', loss='mean_squared_error', metrics=['accuracy'])

    with mlflow.start_run() as run:
        mlflow.tensorflow.autolog()
        model.fit(X_train, y_train, epochs=3, batch_size=128, validation_split=0.2)

        # Save model
        model.save('/tmp/model.h5')
        mlflow.keras.log_model(model, "model")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        'ml_model_pipeline',
        default_args=default_args,
        description='An ML pipeline that queries ClickHouse, preprocesses data, trains a model, and logs it to MLflow',
        schedule_interval='@daily',
) as dag:
    t1 = PythonOperator(
        task_id='query_data',
        python_callable=query_data,
    )

    t2 = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    t3 = PythonOperator(
        task_id='split_data',
        python_callable=split_data,
    )

    t4 = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    t1 >> t2 >> t3 >> t4
