import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
from utils.s3 import get_client

load_dotenv()

BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")


def list_dates(s3, prefix: str) -> list[str]:
    response = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix=prefix)
    if "Contents" not in response:
        return []
    dates = set()
    for obj in response["Contents"]:
        parts = obj["Key"].split("/")
        year  = parts[1].split("=")[1]
        month = parts[2].split("=")[1]
        day   = parts[3].split("=")[1]
        dates.add(f"{year}-{month}-{day}")
    return sorted(dates)


def list_weather_files(s3) -> list[tuple[str, str]]:
    response = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix="weather/")
    if "Contents" not in response:
        return []
    results = []
    for obj in response["Contents"]:
        parts = obj["Key"].split("/")
        year  = parts[1].split("=")[1]
        month = parts[2].split("=")[1]
        day   = parts[3].split("=")[1]
        city  = parts[4].replace(".json", "")
        results.append((city, f"{year}-{month}-{day}"))
    return results


if __name__ == "__main__":
    from etl.bronze_util import process_air_quality, process_weather

    s3 = get_client()
    for date in list_dates(s3, "air_quality/"):
        process_air_quality(date)
    for city, date in list_weather_files(s3):
        process_weather(city, date)
