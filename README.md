# Air Quality Forecasting ML Pipeline

A machine learning pipeline that forecasts daily PM10 air quality for Berlin, London, Paris, and Amsterdam up to 7 days ahead. Built as a portfolio project to demonstrate end-to-end MLOps practices.

## What it does

Every day, the pipeline fetches fresh air quality readings from the WAQI API and weather data from Open-Meteo, stores them in Postgres, and uses trained models to generate 7-day PM10 forecasts for each city. Predictions are served through a REST API.

## Models

For each city and forecast horizon (1 to 7 days), there is a stacking ensemble made of:

- **XGBoost** trained on weather features and PM10 lag features, with hyperparameters tuned using Optuna
- **Prophet** trained on the PM10 time series to capture weekly and yearly seasonality
- **Linear regression meta-model** that blends the two base model predictions

Training uses strict chronological splits to avoid data leakage. All experiments are tracked in MLflow on Dagshub, and the best model per city/horizon is automatically promoted to Production.

## API
### Requests may take a while as Render sleeps after long downtime

Base URL: `https://air-quality-forecaster-lrz1.onrender.com`

**7-day forecast for a city:**
```bash
curl https://air-quality-forecaster-lrz1.onrender.com/forecast/berlin
```

**Historical PM10 readings:**
```bash
curl https://air-quality-forecaster-lrz1.onrender.com/history/london?days=14
```

Supported cities: `berlin`, `london`, `paris`, `amsterdam`

## Production stack

The entire stack runs on free-tier services:

- **Supabase** — hosted Postgres for air quality, weather, and prediction data
- **Dagshub** — hosted MLflow for experiment tracking and model registry
- **Render** — hosts the FastAPI backend
- **GitHub Actions** — runs the daily pipeline on a cron schedule

## Daily pipeline

GitHub Actions triggers `src/pipeline/pipeline.py` every day at 6am UTC. It checks the database for the last date present, backfills any missing weather data, updates actuals for past predictions where real PM10 has now arrived, and writes fresh 7-day forecasts.
