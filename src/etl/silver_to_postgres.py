import io
import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from utils.s3 import get_client

from datetime import datetime

load_dotenv()

BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "ml-pipeline-silver")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mlpipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "mlpipeline")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mlpipeline")


def get_engine():
    url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(url)


def ensure_schema(engine, schema: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    except IntegrityError:
        pass


def drop_table_cascade(engine, schema: str, table: str):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE"))


def load_flights(engine, date: str) -> None:
    dt = datetime.strptime(date, "%Y-%m-%d")
    s3 = get_client()
    prefix = f"flights/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    response = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix)
    if "Contents" not in response:
        print("No silver flights found")
        return

    frames = []
    for obj in response["Contents"]:
        body = s3.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))
    
    df = pd.concat(frames, ignore_index=True)
    # drop_table_cascade(engine, "raw", "flights")
    with engine.begin() as conn:
        try:
            conn.execute(text(f"DELETE FROM raw.flights WHERE year={dt.year} AND month={dt.month} AND day={dt.day}"))
        except:
            pass
    df.to_sql("flights", engine, schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} flights rows into raw.flights")

def load_weather(engine, date: str) -> None:
    dt = datetime.strptime(date, "%Y-%m-%d")
    prefix = f"weather/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    s3 = get_client()
    response = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix)
    if "Contents" not in response:
        print("No silver weather files found")
        return

    frames = []
    for obj in response["Contents"]:
        body = s3.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))

    df = pd.concat(frames, ignore_index=True)
    # drop_table_cascade(engine, "raw", "weather")
    with engine.begin() as conn:
        try:
            conn.execute(text(f"DELETE FROM raw.weather WHERE year={dt.year} AND month={dt.month} AND day={dt.day}"))
        except:
            pass
    df.to_sql("weather", engine, schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} weather rows into raw.weather")


if __name__ == "__main__":
    date = "2024-01-01"
    engine = get_engine()
    ensure_schema(engine, "raw")
    load_flights(engine, date)
    load_weather(engine, date)
    engine.dispose()


