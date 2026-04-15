# scripts/daily_pipeline.py
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


import pandas as pd
from datetime import date, datetime
from dotenv import load_dotenv
from sqlalchemy import text

from config import CITIES
from ingestion.airq_api import fetch_air_quality
from ingestion.weather_api import fetch_weather
from utils.db import get_engine, ensure_schema, ensure_predictions_table
from training.predict import run_inference

load_dotenv()


def ingest_air_quality(engine, date_str: str) -> None:
    for city in CITIES:
        data = fetch_air_quality(city)
        df = pd.DataFrame([data])
        df["date"] = pd.to_datetime(df["date"])
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM raw.air_quality WHERE city = :city AND date = :date"), {"city": city, "date": date_str})
            df.to_sql("air_quality", conn, schema="raw", if_exists="append", index=False)
        print(f"{city}: air quality written")


def ingest_weather(engine, date_str: str) -> None:
    for city, info in CITIES.items():
        data = fetch_weather(info["lat"], info["lon"], start_date=date_str, end_date=date_str)
        df = pd.DataFrame(data["hourly"])
        df = df.dropna(subset=["time"])
        df["date"] = pd.to_datetime(df["time"]).dt.date
        df["city"] = city
        df = df.drop(columns=["time"]).groupby(["date", "city"]).mean().reset_index()
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM raw.weather WHERE city = :city AND date = :date"), {"city": city, "date": date_str})
            df.to_sql("weather", conn, schema="raw", if_exists="append", index=False)
        print(f"{city}: weather written")


def update_actuals(engine, date_str: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE raw.predictions p
            SET actual = aq.pm10
            FROM raw.air_quality aq
            WHERE p.city = aq.city
              AND p.target_date = aq.date
              AND p.actual IS NULL
              AND aq.date = :date
        """), {"date": date_str})
    print(f"Actuals updated for {date_str}")


if __name__ == "__main__":
    date_str = date.today().strftime("%Y-%m-%d")
    engine = get_engine()
    ensure_schema(engine, "raw")
    ensure_predictions_table(engine)

    ingest_air_quality(engine, date_str)
    ingest_weather(engine, date_str)
    update_actuals(engine, date_str)
    engine.dispose()

    run_inference()
    print("Done")
