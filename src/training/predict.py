# src/training/predict.py
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import mlflow.pyfunc
import pandas as pd
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import text

from training.data import load_raw, clean_city, merge
from training.features import add_features, FEATURE_COLS, HORIZONS
from utils.db import get_engine, ensure_predictions_table
from config import CITIES

load_dotenv()

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def load_production_model(city: str, horizon: int):
    model_name = f"pm10_{city.lower()}_{horizon}d"
    return mlflow.pyfunc.load_model(f"models:/{model_name}/Production")


def get_latest_features(aq: pd.DataFrame, weather: pd.DataFrame, city: str) -> pd.DataFrame:
    cleaned = clean_city(aq, city)
    merged = merge(cleaned, weather)
    featured = add_features(merged)
    city_df = featured[featured["city"] == city].sort_values("date")
    return city_df.iloc[[-1]]


def run_inference(forecast_date: date = None):
    if forecast_date is None:
        forecast_date = date.today()

    mlflow.set_tracking_uri(MLFLOW_URI)

    engine = get_engine()
    ensure_predictions_table(engine)
    aq, weather = load_raw(engine)

    rows = []
    for city in CITIES:
        latest = get_latest_features(aq, weather, city)

        for h in HORIZONS:
            model = load_production_model(city, h)
            pred = model.predict(latest[FEATURE_COLS + ["date"]])[0]

            rows.append({
                "city": city,
                "forecast_date": forecast_date,
                "target_date": forecast_date + pd.Timedelta(days=h),
                "horizon": h,
                "predicted": float(pred),
                "actual": None,
            })

    df = pd.DataFrame(rows)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO raw.predictions (city, forecast_date, target_date, horizon, predicted, actual)
                VALUES (:city, :forecast_date, :target_date, :horizon, :predicted, :actual)
                ON CONFLICT (city, forecast_date, horizon) DO UPDATE SET predicted = EXCLUDED.predicted
            """), row.to_dict())

    engine.dispose()
    print(f"Wrote {len(rows)} predictions for {forecast_date}")


if __name__ == "__main__":
    run_inference()
