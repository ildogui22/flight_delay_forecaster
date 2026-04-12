"""
ETL step 1: reads raw JSON files from S3 bronze storage, cleans and transforms
the data using pandas, and writes Parquet files to S3 silver storage.
"""

import io
import json
import os

import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")
BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "ml-pipeline-silver")

from utils.s3 import get_client, ensure_bucket

def process_flights(date: str) -> None:
    
    dt = datetime.strptime(date, "%Y-%m-%d")
    prefix = f"flights/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"

    ensure_bucket(BUCKET_SILVER)
    s3 = get_client()
    response = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix=prefix)
    if "Contents" not in response:
        print(f"No flight files found at {prefix}")
        return
    
    frames = []
    for obj in response["Contents"]:
        body = s3.get_object(Bucket=BUCKET_RAW, Key=obj["Key"])["Body"].read()
        data = json.loads(body)
        print(f"Keys in response: {type(data)}, length: {len(data)}")
        frames.append(pd.DataFrame(data))


    df = pd.concat(frames, ignore_index=True)
    if df.empty or "icao24" not in df.columns:
        print(f"No flight data for {date}, skipping")
        return
    df = df.dropna(subset=["icao24", "firstSeen", "lastSeen", "estDepartureAirport"])
    df = df.drop_duplicates(subset=["icao24", "firstSeen"])
    df["firstSeen_ts"] = pd.to_datetime(df["firstSeen"], unit="s", utc=True)
    df["lastSeen_ts"] = pd.to_datetime(df["lastSeen"], unit="s", utc=True)
    df["duration_minutes"] = (df["lastSeen"] - df["firstSeen"]) / 60
    df["year"] = int(dt.year)
    df["month"] = int(dt.month)
    df["day"] = int(dt.day)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    out_key = f"flights/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/flights.parquet"
    s3.put_object(Bucket=BUCKET_SILVER, Key=out_key, Body=buffer.getvalue())
    print(f"Flights written to s3://{BUCKET_SILVER}/{out_key} — {len(df)} rows")


def process_weather(airport: str, date: str) -> None:
    ensure_bucket(BUCKET_SILVER)
    # year, month, day = date.split("-")
    dt = datetime.strptime(date, "%Y-%m-%d")
    key = f"weather/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{airport}.json"

    s3 = get_client()
    body = s3.get_object(Bucket=BUCKET_RAW, Key=key)["Body"].read()
    hourly = json.loads(body)["hourly"]

    df = pd.DataFrame(hourly)
    df = df.dropna(subset=["time", "temperature_2m"])
    df["timestamp"] = pd.to_datetime(df["time"], utc=True)
    df["airport"] = airport
    df["year"] = int(dt.year)
    df["month"] = int(dt.month)
    df["day"] = int(dt.day)
    df = df.drop(columns=["time"])

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    out_key = f"weather/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{airport}.parquet"
    s3.put_object(Bucket=BUCKET_SILVER, Key=out_key, Body=buffer.getvalue())
    print(f"Weather for {airport} written to s3://{BUCKET_SILVER}/{out_key} — {len(df)} rows")


if __name__ == "__main__":
    DATE = "2024-01-15"
    AIRPORTS = ["EDDF", "EGLL"]

    process_flights(DATE)
    for airport in AIRPORTS:
        process_weather(airport, DATE)
