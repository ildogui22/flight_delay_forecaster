# src/training/train.py
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import mlflow
import mlflow.sklearn
import numpy as np
import optuna
import pandas as pd
from dotenv import load_dotenv
from prophet import Prophet
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import xgboost as xgb

from training.data import load_raw, clean_city, merge
from training.features import add_features, add_targets, split, FEATURE_COLS, HORIZONS
from training.evaluate import compute_metrics
from utils.db import get_engine
from config import CITIES

load_dotenv()

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT = "air_quality_pm10"
N_TRIALS = 50


def build_dataset():
    """
    Creates a dictionary containing a Dataframe for each city
    Each Dataframe have air quality and weather merged data, as well engineered features
    """
    engine = get_engine()
    aq, weather = load_raw(engine)
    engine.dispose()

    city_dfs = {}
    for city in CITIES:
        cleaned = clean_city(aq, city)
        merged = merge(cleaned, weather)
        featured = add_features(merged)
        with_targets = add_targets(featured)
        city_dfs[city] = with_targets
    return city_dfs


def tune_xgb(X_train, y_train, X_val, y_val) -> dict:
    """
    Defines Optuna smart hyperparameter tuning fo XGBoost regressor
    """
    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 500),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "max_depth": trial.suggest_int("max_depth", 3, 8),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        }
        model = xgb.XGBRegressor(**params, n_jobs=-1)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_val)
        return -np.sqrt(mean_squared_error(y_val, y_pred))

    study = optuna.create_study(direction="maximize")
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study.optimize(objective, n_trials=N_TRIALS)
    return study.best_params


def train_xgb(X_train, y_train, params: dict):
    model = xgb.XGBRegressor(**params, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    return model


def train_prophet(train_df: pd.DataFrame):
    prophet_df = (
        train_df[["date", "pm10"]]
        .rename(columns={"date": "ds", "pm10": "y"})
        .dropna()
    )
    model = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=True)
    model.fit(prophet_df)
    return model


def prophet_predict(model, dates: pd.Series) -> np.ndarray:
    future = pd.DataFrame({"ds": dates.values})
    forecast = model.predict(future)
    return forecast["yhat"].values


def train_city_horizon(df: pd.DataFrame, horizon: int):
    target_col = f"target_{horizon}d"
    train, val, meta_train, test = split(df)

    X_train = train[FEATURE_COLS].values
    y_train = train[target_col].values
    X_val = val[FEATURE_COLS].values
    y_val = val[target_col].values
    X_meta = meta_train[FEATURE_COLS].values
    y_meta = meta_train[target_col].values
    X_test = test[FEATURE_COLS].values
    y_test = test[target_col].values

    meta_target_dates = meta_train["date"] + pd.Timedelta(days=horizon)
    test_target_dates = test["date"] + pd.Timedelta(days=horizon)

    best_params = tune_xgb(X_train, y_train, X_val, y_val)
    xgb_model = train_xgb(X_train, y_train, best_params)
    prophet_model = train_prophet(train)

    meta_X = np.column_stack([
        xgb_model.predict(X_meta),
        prophet_predict(prophet_model, meta_target_dates),
    ])
    meta_model = LinearRegression().fit(meta_X, y_meta)

    test_X = np.column_stack([
        xgb_model.predict(X_test),
        prophet_predict(prophet_model, test_target_dates),
    ])
    y_pred = meta_model.predict(test_X)
    metrics = compute_metrics(y_test, y_pred)

    return xgb_model, prophet_model, meta_model, best_params, metrics


def run():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    city_dfs = build_dataset()

    for city in CITIES:
        df = city_dfs[city]
        for h in HORIZONS:
            with mlflow.start_run(run_name=f"{city}_horizon_{h}d"):
                mlflow.set_tag("city", city)
                mlflow.set_tag("horizon", f"{h}d")

                xgb_model, prophet_model, meta_model, best_params, metrics = train_city_horizon(df, h)

                mlflow.log_param("city", city)
                mlflow.log_param("horizon", h)
                mlflow.log_params(best_params)
                mlflow.log_metrics(metrics)

                mlflow.sklearn.log_model(
                    meta_model,
                    artifact_path="meta_model",
                    registered_model_name=f"pm10_{city.lower()}_{h}d",
                )

                print(f"{city} +{h}d — RMSE: {metrics['rmse']:.2f}  MAE: {metrics['mae']:.2f}  R²: {metrics['r2']:.3f}")


if __name__ == "__main__":
    run()
