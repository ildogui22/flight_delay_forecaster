"""
ETL step 1: reads raw JSON files from S3 bronze storage, cleans and transforms
the data using pandas, and writes Parquet files to S3 silver storage.
"""

import io
import json
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

from config import CITIES
from utils.s3 import get_client, ensure_bucket

load_dotenv()

BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")
BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "ml-pipeline-silver")


def process_air_quality(date: str) -> None:
    dt = datetime.strptime(date, "%Y-%m-%d")
    prefix = f"air_quality/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"

    ensure_bucket(BUCKET_SILVER)
    s3 = get_client()
    response = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix=prefix)
    if "Contents" not in response:
        print(f"No air quality files found at {prefix}")
        return

    for obj in response["Contents"]:
        body = s3.get_object(Bucket=BUCKET_RAW, Key=obj["Key"])["Body"].read()
        data = json.loads(body)
        city = data["city"]

        df = pd.DataFrame([data])
        df["date"] = pd.to_datetime(df["date"])

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)

        out_key = f"air_quality/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{city}.parquet"
        s3.put_object(Bucket=BUCKET_SILVER, Key=out_key, Body=buffer.getvalue())
        print(f"Air quality for {city} written to s3://{BUCKET_SILVER}/{out_key}")



def process_weather(city: str, date: str) -> None:
    ensure_bucket(BUCKET_SILVER)
    dt = datetime.strptime(date, "%Y-%m-%d")
    key = f"weather/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{city}.json"

    s3 = get_client()
    body = s3.get_object(Bucket=BUCKET_RAW, Key=key)["Body"].read()
    hourly = json.loads(body)["hourly"]

    df = pd.DataFrame(hourly)
    df = df.dropna(subset=["time"])
    df["timestamp"] = pd.to_datetime(df["time"], utc=True)
    df["city"] = city
    df = df.drop(columns=["time"])

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    out_key = f"weather/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{city}.parquet"
    s3.put_object(Bucket=BUCKET_SILVER, Key=out_key, Body=buffer.getvalue())
    print(f"Weather for {city} written to s3://{BUCKET_SILVER}/{out_key} — {len(df)} rows")


if __name__ == "__main__":
    DATE = "2026-04-13"

    process_air_quality(DATE)
    for city in CITIES:
        process_weather(city, DATE)
