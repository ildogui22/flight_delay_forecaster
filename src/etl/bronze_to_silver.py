import os
from dotenv import load_dotenv
from utils.s3 import get_client
import json

load_dotenv()

BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")
ENGINE = os.getenv("PROCESSING_ENGINE", "pandas")


def list_flight_dates(s3) -> list[str]:
    response = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix="flights/")
    if "Contents" not in response:
        return []
    dates = set()
    for obj in response["Contents"]:
        parts = obj["Key"].split("/")
        year = parts[1].split("=")[1]
        month = parts[2].split("=")[1]
        day = parts[3].split("=")[1]
        dates.add(f"{year}-{month}-{day}")
    return sorted(dates)


def list_weather_files(s3) -> list[tuple[str, str]]:
    response = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix="weather/")
    if "Contents" not in response:
        return []
    results = []
    for obj in response["Contents"]:
        parts = obj["Key"].split("/")
        year = parts[1].split("=")[1]
        month = parts[2].split("=")[1]
        day = parts[3].split("=")[1]
        airport = parts[4].replace(".json", "")
        results.append((airport, f"{year}-{month}-{day}"))
    return results


# if ENGINE == "spark":
#     from etl.raw_to_silver_spark import build_spark_session, process_flights, process_weather

#     s3 = get_client()
#     spark = build_spark_session()
#     for date in list_flight_dates(s3):
#         process_flights(spark, date)
#     for airport, date in list_weather_files(s3):
#         process_weather(spark, airport, date)
#     spark.stop()
# else:


if __name__ == "__main__":
    from etl.bronze_util import process_flights, process_weather

    s3 = get_client()
    for date in list_flight_dates(s3):
        process_flights(date)
    for airport, date in list_weather_files(s3):
        process_weather(airport, date)
