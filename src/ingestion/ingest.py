import os
from datetime import datetime

from dotenv import load_dotenv

from ingestion.flights_api import fetch_departures
from utils.s3 import ensure_bucket, upload_json

from ingestion.weather_api import fetch_weather, get_airport_coords

load_dotenv()

RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")

def build_s3_key(airport: str, date: str) -> str:
    dt = datetime.strptime(date, "%Y-%m-%d")
    return f"flights/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{airport}.json"

def ingest_flights(airport: str, start_date: str, end_date: str) -> int:
    ensure_bucket(RAW_BUCKET)
    data = fetch_departures(airport, start_date, end_date)
    key = build_s3_key(airport, start_date)
    upload_json(data, RAW_BUCKET, key)
    return len(data)

def ingest_weather(airport: str, date: str) -> None:
    coords = get_airport_coords(airport)
    if coords is None:
        print(f"No coordinates for {airport}, skipping")
        return None
    lat, lon = coords
    data = fetch_weather(lat, lon, start_date=date, end_date=date)
    dt = datetime.strptime(date, "%Y-%m-%d")
    key = f"weather/year={dt.year}/month={dt.month}/day={dt.day}/{airport}.json"
    ensure_bucket(RAW_BUCKET)
    upload_json(data, RAW_BUCKET, key)

if __name__ == "__main__":
    count = ingest_flights("EDDF", "2024-01-01", "2024-01-02")
    print(f"Uploaded {count} flights")


    for airport in ["EDDF", "EGLL"]:
        ingest_weather(airport, "2024-01-01")
        print(f"Uploaded weather for {airport}")


