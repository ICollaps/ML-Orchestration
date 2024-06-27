import fastapi
import uvicorn
import pandas as pd
from pydantic import BaseModel
from minio import Minio
import mlflow
import mlflow.sklearn
import joblib
import flaml

app = fastapi.FastAPI()

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
    num_docks_available: int
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

@app.get("/health")
async def health():
    return {"response": "ok"}


@app.post("/predict")
async def predict(features: Features):
    if model is None:
        raise fastapi.HTTPException(status_code=404, detail="Model not loaded.")
    try:

        data = pd.DataFrame([features.dict()])
        prediction = model.predict(data)
        
        return {"prediction": prediction.tolist()}
    

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
