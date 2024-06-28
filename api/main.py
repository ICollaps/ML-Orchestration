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

model = None

class Features(BaseModel):
    num_bikes_available: int
    mechanical: int
    ebike: int
    is_installed: int
    is_returning: int
    is_renting: int
    last_reported: int
    weather: int
    temp: int
    probarain: int
    probafog: int
    probawind70: int
    probawind100: int
    lon: int
    lat: int

def generate_cache_key(features: Features):
    features_dict = features.dict()
    features_str = json.dumps(features_dict, sort_keys=True)
    return hashlib.md5(features_str.encode('utf-8')).hexdigest()

def load_model():
    global model
    try:
        model_name = "model"
        version = mlflow_client.get_model_version_by_alias(model_name, "champion").version
        print(version)
        model_uri = f"models:/{model_name}/{version}"
        model = mlflow.pyfunc.load_model(model_uri)
        print(f"Model version {version} loaded successfully")
    except Exception as e:
        print(f"Error loading model: {e}")

@app.get("/health")
async def health():
    return {"response": "ok"}

@app.post("/predict")
async def predict(features: Features):
    if model is None:
        raise fastapi.HTTPException(status_code=404, detail="Model not loaded.")
    try:
        # cache_key = generate_cache_key(features)
        # cached_result = redis_client.get(cache_key)

        # if cached_result:
        #     print("Result returned from cache")
        #     return {"prediction": json.loads(cached_result)}

        data = pd.DataFrame([features.dict()])
        prediction = model.predict(data)
        predicted_labels = prediction.tolist()
        
        # redis_client.set(cache_key, json.dumps(predicted_labels))
        print("Result stored in cache")
        
        return {"prediction": predicted_labels}

    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=str(e))

@app.post("/restart")
async def restart():
    try:
        await startup()
        return {"status": "Model reloaded successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/train")
async def train():
    clickhouse_client = clickhouse_connect.get_client(host='clickhouse')
    select_velib_query = "SELECT * FROM velib;"
    data = clickhouse_client.query(select_velib_query)
    columns = data.column_names
    df = pd.DataFrame(data.result_rows, columns=columns)
    X = df.drop(columns=['num_docks_available', 'id', 'stationCode', 'station_id', 'name'])
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

    return registered_model.version

async def startup():
    print("Api starting")
    load_model()

async def shutdown():
    print("shutting down ...")

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
