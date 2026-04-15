import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from dotenv import load_dotenv

from utils.db import get_engine
from config import CITIES
from api.schemas import ForecastPoint, HistoryPoint, QueryResponse

load_dotenv()

app = FastAPI(title="Air Quality Forecaster API")

@app.get("/forecast/{city}", response_model=list[ForecastPoint])
def get_forecast(city: str):
    city = city.lower()
    if city not in [c.lower() for c in CITIES]:
        raise HTTPException(status_code=404, detail=f"{city} not found")
    
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
                                 SELECT city, forecast_date, target_date, horizon, predicted, actual
                                 FROM raw.predictions
                                 WHERE LOWER(city := city
                                 ORDER BY forecast_date DESC, horizon ASC
                                 LIMIT 7
                                 """), {"city": city}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No forecast found for {city}")
    return [ForecastPoint(**r) for r in rows]


@app.get("/history/{city}", response_model=list[HistoryPoint])
def get_history(city: str, days: int = 30):
    city = city.lower()
    if city not in [c.lower() for c in CITIES]:
        raise HTTPException(status_code=404, detail=f"City '{city}' not found")

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT date, city, pm10
            FROM raw.air_quality
            WHERE LOWER(city) = :city
            ORDER BY date DESC
            LIMIT :days
        """), {"city": city, "days": days}).mappings().all()
    engine.dispose()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No history found for '{city}'")
    return [HistoryPoint(**r) for r in rows]
