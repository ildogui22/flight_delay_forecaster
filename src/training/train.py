# src/training/train.py
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import mlflow
import mlflow.sklearn
import mlflow.pyfunc
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

def promote_if_best(client, model_name: str, run_id: str, rmse: float):
    """Promote the just-registered model version to Production if it has the best RMSE."""
    # get the version we just registered
    versions = client.get_latest_versions(model_name, stages=["None"])
    if not versions:
        return
    new_version = versions[0].version

    # check if there's already a Production model
    prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    if prod_versions:
        prod_run_id = prod_versions[0].run_id
        prod_rmse = client.get_run(prod_run_id).data.metrics["rmse"]
        if rmse >= prod_rmse:
            print(f"keeping existing Production")
            return

    client.transition_model_version_stage(
        name=model_name,
        version=new_version,
        stage="Production",
        archive_existing_versions=True,
    )
    print(f"-> promoted version {new_version} to Production (RMSE {rmse:.2f})")

class StackedPM10Model(mlflow.pyfunc.PythonModel):
    def __init__(self, xgb_model, prophet_model, meta_model, horizon):
        self.xgb_model = xgb_model
        self.prophet_model = prophet_model
        self.meta_model = meta_model
        self.horizon = horizon

    def predict(self, context, model_input):
        # model_input: DataFrame with FEATURE_COLS + 'date'
        X = model_input[FEATURE_COLS].values
        dates = pd.to_datetime(model_input["date"]) + pd.Timedelta(days=self.horizon)

        xgb_pred = self.xgb_model.predict(X)
        prophet_pred = prophet_predict(self.prophet_model, dates)

        meta_X = np.column_stack([xgb_pred, prophet_pred])
        return self.meta_model.predict(meta_X)



def run():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)
    client = mlflow.tracking.MlflowClient()

    city_dfs = build_dataset()

    for city in CITIES:
        df = city_dfs[city]
        for h in HORIZONS:
            with mlflow.start_run(run_name=f"{city}_horizon_{h}d") as active_run:
                mlflow.set_tag("city", city)
                mlflow.set_tag("horizon", f"{h}d")

                xgb_model, prophet_model, meta_model, best_params, metrics = train_city_horizon(df, h)

                mlflow.log_param("city", city)
                mlflow.log_param("horizon", h)
                mlflow.log_params(best_params)
                mlflow.log_metrics(metrics)

                model_name = f"pm10_{city.lower()}_{h}d"
                stacked = StackedPM10Model(xgb_model, prophet_model, meta_model, h)
                mlflow.pyfunc.log_model(
                    artifact_path="stacked_model",
                    python_model=stacked,
                    registered_model_name=model_name,
                )


                promote_if_best(client, model_name, active_run.info.run_id, metrics["rmse"])
                print(f"{city} +{h}d — RMSE: {metrics['rmse']:.2f}  MAE: {metrics['mae']:.2f}  R²: {metrics['r2']:.3f}")



if __name__ == "__main__":
    run()