"""
One-time script to bootstrap raw.weather from Open-Meteo archive.
Run from project root: python scripts/load_historical_weather.py
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

import pandas as pd
from dotenv import load_dotenv
from utils.db import get_engine, ensure_schema

from ingestion.weather_api import fetch_weather

from sqlalchemy import text

load_dotenv()

CITIES = {
    # "Berlin":    (52.52, 13.40),
    # "London":    (51.51, -0.13),
    "Paris":     (48.86,  2.35),
    "Amsterdam": (52.37,  4.90),
}

def get_date_range(engine, city: str) -> tuple[str, str]:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MIN(date), MAX(date) FROM raw.air_quality WHERE city = '{city}'")).fetchone()
    return str(result[0]), str(result[1])


def load_weather(engine, city: str) -> None:
    lat, lon = CITIES[city]
    start, end = get_date_range(engine, city)
    data = fetch_weather(lat, lon, start, end)
    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    df["date"] = df["time"].dt.date
    df["city"] = city
    df = df.drop(columns=["time"]).groupby(["date", "city"]).mean().reset_index()
    with engine.begin() as conn:
        df.to_sql("weather", conn, schema="raw", if_exists="append", index=False)
    print(f"{city}: loaded {len(df)} rows ({start} → {end})")



if __name__ == "__main__":
    import time
    engine = get_engine()
    ensure_schema(engine, "raw")
    for city in CITIES:
        load_weather(engine, city)
        time.sleep(2)
    engine.dispose()