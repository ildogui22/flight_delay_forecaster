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

## Local setup

Requirements: Docker, Python 3.12

```bash
git clone https://github.com/your-username/your-repo.git
cd ML_pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your credentials, then start the infrastructure:

```bash
docker-compose up -d
```

Bootstrap historical data (one-time):
```bash
python scripts/load_historical_data.py
python scripts/load_historical_weather.py
```

Train models:
```bash
python src/training/train.py
```

Start the API locally:
```bash
uvicorn src.api.main:app --reload --port 8000
```

## Project structure

```
src/
  ingestion/    — WAQI and Open-Meteo API clients
  etl/          — bronze to silver ETL (used in local Airflow pipeline)
  training/     — feature engineering, model training, evaluation, inference
  pipeline/     — daily pipeline script for GitHub Actions
  api/          — FastAPI application
  utils/        — database and S3 utilities
scripts/
  load_historical_data.py
  load_historical_weather.py
airflow/
  dags/         — Airflow DAG kept as local development reference
```

## Tech stack

Python, XGBoost, Prophet, Optuna, MLflow, FastAPI, SQLAlchemy, PostgreSQL, Apache Airflow, Docker
