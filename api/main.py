import os
import fastapi
import joblib
import uvicorn
import redis
import json
import hashlib
import pandas as pd
from pydantic import BaseModel

app = fastapi.FastAPI()
model = None


class IrisFeatures(BaseModel):
    sepal_length: float
    sepal_width: float
    petal_length: float
    petal_width: float


def generate_cache_key(features: IrisFeatures):
    features_dict = features.dict()
    features_str = json.dumps(features_dict, sort_keys=True)
    return hashlib.md5(features_str.encode('utf-8')).hexdigest()


def convert_label(predictions: list[int]):
    name_mapping = {
        0: "Iris setosa",
        1: "Iris versicolor",
        2: "Iris virginica"
    }

    return  [name_mapping[pred] for pred in predictions]


@app.get("/health")
async def health():
    return {"response": "ok"}


@app.post("/predict")
async def predict_route(features: IrisFeatures):
    try:
        if model is None:
            raise fastapi.HTTPException(
                status_code=404, detail="Model not loaded")

        cache_key = generate_cache_key(features)

        cached_result = redis_client.get(cache_key)
        if cached_result:
            print("Result returned from cache")
            return {"prediction": json.loads(cached_result)}

        print("Result returned from model")
        data_df = pd.DataFrame([features.dict()])

        prediction = model.predict(data_df)
        predictions = prediction.tolist()

        predicted_labels = convert_label(predictions)

        redis_client.set(cache_key, json.dumps(predicted_labels))
        print("Model result successfully cached")
        return {"prediction": predicted_labels}

    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=str(e))


async def load_model():
    global model, redis_client
    model_file_path = os.getenv("MODEL_PATH", "model.pkl")
    print(f"Model leaded {model_file_path}")
    model = joblib.load(model_file_path)
    print("Model successfully loaded.")

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port)
    print("Connected to redis")


async def shutdown():
    print("shutting down ...")

app.add_event_handler("startup", load_model)
app.add_event_handler("shutdown", shutdown)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
