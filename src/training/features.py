import numpy as np
import pandas as pd

WEATHER_COLS = [
    "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_gusts_10m",
    "wind_direction_10m", "precipitation", "snowfall", "snow_depth",
    "cloud_cover", "surface_pressure", "weather_code"
]

FEATURE_COLS = (
    ["pm10_lag_1d", "pm10_lag_7d", "pm10_rolling_7d"]
    + WEATHER_COLS
    + [
        "wind_x_cloud", "temp_x_humidity", "precip_x_wind", "snow_x_temp",
        "month", "year", "day_of_week",
        "day_of_year_sin", "day_of_year_cos",
        "day_of_week_sin", "day_of_week_cos",
    ]
)

TRAIN_END = "2022-12-31"
META_END = "2024-12-31"
HORIZONS = list(range(1, 8))


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add lag, seasonality and interaction features to the merged dataframe.
    Must be called after merge(), on the full dataset before splitting.
    """
    df = df.copy()

    # lag features — computed per city so lags don't bleed across cities
    df["pm10_lag_1d"] = df.groupby("city")["pm10"].shift(1)
    df["pm10_lag_7d"] = df.groupby("city")["pm10"].shift(7)
    df["pm10_rolling_7d"] = df.groupby("city")["pm10"].transform(
        lambda x: x.shift(1).rolling(7).mean()
    )

    # seasonality
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_year_sin"] = np.sin(2 * np.pi * df["date"].dt.dayofyear / 365)
    df["day_of_year_cos"] = np.cos(2 * np.pi * df["date"].dt.dayofyear / 365)
    df["day_of_week_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["day_of_week_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    # interaction features
    df["wind_x_cloud"] = df["wind_speed_10m"] * df["cloud_cover"]
    df["temp_x_humidity"] = df["temperature_2m"] * df["relative_humidity_2m"]
    df["precip_x_wind"] = df["precipitation"] * df["wind_speed_10m"]
    df["snow_x_temp"] = df["snow_depth"] * df["temperature_2m"]

    return df


def add_targets(df: pd.DataFrame, horizons: list[int] = HORIZONS) -> pd.DataFrame:
    """
    Add target columns for each forecast horizon.
    target_1d = pm10 tomorrow, target_2d = pm10 in 2 days, etc.
    Rows where any target is null (end of series) are dropped.
    """
    df = df.copy()
    for h in horizons:
        df[f"target_{h}d"] = df.groupby("city")["pm10"].shift(-h)
    return df.dropna(subset=[f"target_{h}d" for h in horizons])


def split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Chronological split into train / meta-train / test sets.
    Train: up to TRAIN_END (2022)
    Meta-train: TRAIN_END to META_END (2023-2024)
    Test: after META_END (2025+)
    """
    train = df[df["date"] <= TRAIN_END]
    meta_train = df[(df["date"] > TRAIN_END) & (df["date"] <= META_END)]
    test = df[df["date"] > META_END]
    return train, meta_train, test
