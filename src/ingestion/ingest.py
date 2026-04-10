"""
Orchestrates data ingestion: fetches flight departures and weather data
from external APIs and uploads raw JSON to S3 bronze storage.
"""

import os
from datetime import datetime

from dotenv import load_dotenv

from ingestion.flights_api import fetch_departures
from utils.s3 import ensure_bucket, upload_json

from ingestion.weather_api import fetch_weather

load_dotenv()

RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")

def build_s3_key(airport: str, date: str) -> str:
    dt = datetime.strptime(date, "%Y-%m-%d")
    return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{airport}.json"

def ingest_flights(airport: str, start_date: str, end_date: str) -> int:
    ensure_bucket(RAW_BUCKET)
    data = fetch_departures(airport, start_date, end_date)
    key = os.path.join("flights", build_s3_key(airport, start_date))
    upload_json(data, RAW_BUCKET, key)
    return len(data)

def ingest_weather(airport: str, date: str) -> None:

    data = fetch_weather(airport, start_date=date, end_date=date)
    if not data:
        return
    key = os.path.join("weather", build_s3_key(airport, date))
    ensure_bucket(RAW_BUCKET)
    upload_json(data, RAW_BUCKET, key)

if __name__ == "__main__":
    START_DATE = "2024-01-15"
    END_DATE = "2024-01-16"
    AIRPORTS = ["EDDF", "EGLL"]

    for airport in AIRPORTS:
        count = ingest_flights(airport, START_DATE, END_DATE)
        ingest_weather(airport, START_DATE)
        print(f"Uploaded {count} flights for {airport}")
        print(f"Uploaded weather for {airport}")




