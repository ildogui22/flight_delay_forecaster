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

import time

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

def ingest_weather(airport: str, start_date: str, end_date: str = None) -> None:
    if end_date is None:
        end_date = start_date
    data = fetch_weather(airport, start_date=start_date, end_date=end_date)
    if not data:
        return
    key = os.path.join("weather", build_s3_key(airport, start_date))
    ensure_bucket(RAW_BUCKET)
    upload_json(data, RAW_BUCKET, key)


if __name__ == "__main__":
    from datetime import timedelta

    # START_DATE = "2024-01-01"
    # END_DATE = "2024-03-31"
    # AIRPORTS = ["EDDF", "EGLL", "LFPG", "EHAM"]
    START_DATE = "2024-01-01"
    END_DATE = "2024-01-02"
    AIRPORTS = ["EDDF"]

    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    # flights: one day at a time (OpenSky API limit)
    for airport in AIRPORTS:
        current = start

        while current <= end:
            next_day = current + timedelta(days=1)
            count = ingest_flights(airport, current.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d"))
            print(f"{current.strftime('%Y-%m-%d')} | {airport} — {count} flights")
            current += timedelta(days=1)
            time.sleep(1)

    # weather: one call per airport for the full range
    for airport in AIRPORTS:
        ingest_weather(airport, START_DATE, END_DATE)
        print(f"Weather ingested for {airport}")