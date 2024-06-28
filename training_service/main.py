import fastapi
import uvicorn
import pandas as pd
from pydantic import BaseModel
import redis
import hashlib
import json
import clickhouse_connect
import xgboost as xgb
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import mlflow.pyfunc

app = fastapi.FastAPI()
# redis_client = redis.Redis(host='cache', port=6380, db=0)

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow_client = MlflowClient()


@app.post("/train")
async def train():
    clickhouse_client = clickhouse_connect.get_client(host='clickhouse')
    select_velib_query = "SELECT * FROM velib;"
    data = clickhouse_client.query(select_velib_query)
    columns = data.column_names
    df = pd.DataFrame(data.result_rows, columns=columns)
    X = df.drop(columns=['num_docks_available', 'id', 'stationCode', 'station_id', 'name','lon', 'lat'])
    y = df['num_docks_available']    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    mlflow.set_experiment("model")

    with mlflow.start_run() as run:
        model = xgb.XGBRegressor(random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        print(f'Mean Squared Error: {mse}')
        mlflow.log_metric("mse", mse)
        mlflow.sklearn.log_model(model, "model")
        model_name = "model"
        registered_model = mlflow.register_model(f"runs:/{run.info.run_id}/model", model_name, tags={'mse': mse})
        print(f"Registered model version: {registered_model.version}")

    return {"version": registered_model.version}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
