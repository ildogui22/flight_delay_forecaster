from datetime import datetime
import os
import sys

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import CITIES

from ingestion.airq_api import fetch_air_quality
from ingestion.weather_api import fetch_weather
from utils.s3 import ensure_bucket, upload_json

RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")

def build_s3_key(prefix: str, city: str, date: str) -> str:
    dt = datetime.strptime(date, "%Y-%m-%d")
    return f"{prefix}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{city}.json"

def ingest_air_quality(date: str) -> None:
    ensure_bucket(RAW_BUCKET)
    for city in CITIES:
        try:
            data = fetch_air_quality(city)
            key = build_s3_key("air_quality", city, date)
            upload_json(data, RAW_BUCKET, key)
            print(f"{city}: uploaded air quality to {key}")
        except Exception as e:
            print(f"{city}: ERROR — {e}")

def ingest_weather(date: str) -> None:
    ensure_bucket(RAW_BUCKET)
    for city, info in CITIES.items():
        try:
            data = fetch_weather(info["lat"], info["lon"], start_date=date, end_date=date)
            key = build_s3_key("weather", city, date)
            upload_json(data, RAW_BUCKET, key)
            print(f"{city}: uploaded weather to {key}")
        except Exception as e:
            print(f"{city}: ERROR — {e}")

if __name__ == "__main__":
    date = datetime.today().strftime("%Y-%m-%d")
    ingest_air_quality(date)
    ingest_weather(date)
