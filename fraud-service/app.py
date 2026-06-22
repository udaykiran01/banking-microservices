from fastapi import FastAPI
from pydantic import BaseModel
import joblib

app = FastAPI()

model = joblib.load("fraud_model.joblib")

class Transaction(BaseModel):
    amount: float
    account_age_days: int
    transaction_count_24h: int
    avg_transaction_amount: float

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict")
def predict(tx: Transaction):
    features = [[
        tx.amount,
        tx.account_age_days,
        tx.transaction_count_24h,
        tx.avg_transaction_amount
    ]]

    prediction = model.predict(features)[0]
    probability = model.predict_proba(features)[0][1]

    return {
        "fraud": bool(prediction),
        "fraud_probability": float(probability)
    }