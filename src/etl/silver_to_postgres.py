import io
import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from utils.s3 import get_client

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
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()

def load_flights(engine) -> None:
    s3 = get_client()
    response = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix="flights/")
    if "Contents" not in response:
        print("No silver flights found")

    frames = []
    for obj in response["Contents"]:
        body = s3.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))
    
    df = pd.concat(frames, ignore_index=True)
    df.to_sql("flights", engine, schema="raw", if_exists="replace", index=False)
    print(f"Loaded {len(df)} flights rows into raw.flights")

def load_weather(engine) -> None:
    s3 = get_client()
    response = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix="weather/")
    if "Contents" not in response:
        print("No silver weather files found")
        return

    frames = []
    for obj in response["Contents"]:
        body = s3.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))

    df = pd.concat(frames, ignore_index=True)
    df.to_sql("weather", engine, schema="raw", if_exists="replace", index=False)
    print(f"Loaded {len(df)} weather rows into raw.weather")


if __name__ == "__main__":
    engine = get_engine()
    ensure_schema(engine, "raw")
    load_flights(engine)
    load_weather(engine)
    engine.dispose()


