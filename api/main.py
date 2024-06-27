import fastapi
import uvicorn
import pandas as pd
from pydantic import BaseModel
import mlflow
import mlflow.sklearn
import joblib
import redis
import hashlib
import json


app = fastapi.FastAPI()
# redis_client = redis.Redis(host='cache', port=6380, db=0)


async def load_model():
    global model
    model_file_path = "./model.pkl"
    print(f"Loading model from : {model_file_path}")
    model = joblib.load(model_file_path)
    print("Model loaded")



class Features(BaseModel):
    id: int
    stationCode: int
    station_id: int
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


def generate_cache_key(features: Features):
    features_dict = features.dict()
    features_str = json.dumps(features_dict, sort_keys=True)
    return hashlib.md5(features_str.encode('utf-8')).hexdigest()

@app.get("/health")
async def health():
    return {"response": "ok"}


@app.post("/predict")
async def predict(features: Features):
    if model is None:
        raise fastapi.HTTPException(status_code=404, detail="Model not loaded.")
    try:
        cache_key = generate_cache_key(features)
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
        await load_model()
        return {"status": "Model reloaded successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


async def startup():
    print("Api starting")
    await load_model()

async def shutdown():
    print("shutting down ...")

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
