import pandas as pd
from sqlalchemy.engine import Engine


def load_raw(engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load raw air quality and weather data from Postgres."""
    aq = pd.read_sql("SELECT * FROM raw.air_quality ORDER BY city, date", engine)
    weather = pd.read_sql("SELECT * FROM raw.weather ORDER BY city, date", engine)
    aq["date"] = pd.to_datetime(aq["date"])
    weather["date"] = pd.to_datetime(weather["date"])
    return aq, weather


def clean_city(aq: pd.DataFrame, city: str, max_gap: int = 7) -> pd.DataFrame:
    """
    Clean pm10 for a single city:
    - Interpolate short gaps (<=max_gap days)
    - Drop long gaps and the max_gap rows after them (corrupted lag window)
    """
    group = aq[aq["city"] == city].sort_values("date").copy()
    group["pm10"] = group["pm10"].interpolate(method="linear", limit=max_gap)
    still_null = group["pm10"].isnull()
    group["drop"] = still_null.rolling(max_gap + 1, min_periods=1).max().astype(bool)
    return group[~group["drop"]].drop(columns=["drop"])


def merge(aq: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    """Left join air quality + weather on city + date, sort chronologically."""
    df = aq.merge(weather, on=["city", "date"], how="left")
    return df.sort_values(["city", "date"]).reset_index(drop=True)
