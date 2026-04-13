import io
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from utils.s3 import get_client
from utils.db import get_engine, ensure_schema

load_dotenv()

BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "ml-pipeline-silver")


def load_air_quality(date: str) -> None:
    from datetime import datetime
    dt = datetime.strptime(date, "%Y-%m-%d")
    prefix = f"air_quality/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"

    s3 = get_client()
    response = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix)
    if "Contents" not in response:
        print(f"No silver air quality files found for {date}")
        return

    frames = []
    for obj in response["Contents"]:
        body = s3.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))

    df = pd.concat(frames, ignore_index=True)
    engine = get_engine()
    with engine.begin() as conn:
        for city in df["city"].unique():
            conn.execute(
                text("DELETE FROM raw.air_quality WHERE city = :city AND date = :date"),
                {"city": city, "date": date}
            )
        df.to_sql("air_quality", conn, schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} air quality rows into raw.air_quality")
    engine.dispose()


def load_weather(city: str, date: str) -> None:
    from datetime import datetime
    dt = datetime.strptime(date, "%Y-%m-%d")
    key = f"weather/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{city}.parquet"

    s3 = get_client()
    body = s3.get_object(Bucket=BUCKET_SILVER, Key=key)["Body"].read()
    df = pd.read_parquet(io.BytesIO(body))

    df["date"] = df["timestamp"].dt.date
    df = df.drop(columns=["timestamp"]).groupby(["date", "city"]).mean().reset_index()

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM raw.weather WHERE city = :city AND date = :date"),
            {"city": city, "date": date}
        )
        df.to_sql("weather", conn, schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} weather rows for {city} into raw.weather")
    engine.dispose()



if __name__ == "__main__":
    from config import CITIES
    DATE = "2026-04-13"

    ensure_schema(get_engine(), "raw")
    load_air_quality(DATE)
    for city in CITIES:
        load_weather(city, DATE)
