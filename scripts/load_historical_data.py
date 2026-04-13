"""
One-time script to bootstrap raw.air_quality from historical CSVs.
Run from project root: python scripts/load_historical_data.py
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

import pandas as pd
from dotenv import load_dotenv
from utils.db import get_engine, ensure_schema

load_dotenv()

CSV_FILES = {
    "Berlin":    "data/berlin,-germany-air-quality.csv",
    "London":    "data/london-air-quality.csv",
    "Paris":     "data/paris-air-quality.csv",
    "Amsterdam": "data/amsterdam-air-quality.csv",
}

COLUMNS = ["date", "city", "pm25", "pm10", "o3", "no2", "co"]


def load_csv(engine, city: str, path: str) -> None:
    df = pd.read_csv(path, na_values=[" ", "  ", ""])
    df.columns = df.columns.str.strip()
    df["date"] = pd.to_datetime(df["date"].str.strip())
    df = df.sort_values("date").reset_index(drop=True)
    df["city"] = city
    df = df[COLUMNS]
    with engine.begin() as conn:
        df.to_sql("air_quality", conn, schema="raw", if_exists="append", index=False)
    print(f"{city}: loaded {len(df)} rows")


if __name__ == "__main__":
    engine = get_engine()
    ensure_schema(engine, "raw")
    for city, path in CSV_FILES.items():
        load_csv(engine, city, path)
    engine.dispose()
