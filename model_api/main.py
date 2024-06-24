import os
import fastapi
import joblib
import uvicorn
import pandas as pd
from pydantic import BaseModel

app = fastapi.FastAPI()
model = None

class IrisFeatures(BaseModel):
    sepal_length: float
    sepal_width: float
    petal_length: float
    petal_width: float

@app.get("/health")
async def health():
    return {"response": "ok"}

@app.post("/predict")
async def predict_route(features: IrisFeatures):
    if model is None:
        raise fastapi.HTTPException(status_code=404, detail="Le modèle n'est pas chargé.")
    try:
        data_df = pd.DataFrame([features.dict()])
        prediction = model.predict(data_df)
        predictions = prediction.tolist()
        name_mapping = {
            0: "Iris setosa",
            1: "Iris versicolor",
            2: "Iris virginica"
        }
        results = [name_mapping[pred] for pred in predictions]
        return {"prediction": results}
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=str(e))


async def load_model():
    global model
    model_file_path = os.getenv("MODEL_PATH", "knn_model.pkl")
    print(f"Chargement du modèle à partir de : {model_file_path}")
    model = joblib.load(model_file_path)
    print("Modèle chargé avec succès.")

async def shutdown():
    print("shutting down ...")

app.add_event_handler("startup", load_model)
app.add_event_handler("shutdown", shutdown)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
