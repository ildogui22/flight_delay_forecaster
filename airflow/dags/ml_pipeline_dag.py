"""
Daily ML pipeline DAG: ingest → bronze_to_silver → silver_to_postgres → update_actuals → predict
Training is triggered separately (annual retraining only).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/src")

from ingestion.ingest import ingest_air_quality, ingest_weather
from etl.bronze_util import process_air_quality, process_weather
from etl.silver_to_postgres import load_air_quality, load_weather
from training.predict import run_inference
from utils.db import get_engine
from config import CITIES

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_ingest(**context):
    date = str(context["ds"])
    ingest_air_quality(date)
    ingest_weather(date)


def run_bronze_to_silver(**context):
    date = str(context["ds"])
    process_air_quality(date)
    for city in CITIES:
        process_weather(city, date)


def run_silver_to_postgres(**context):
    date = str(context["ds"])
    load_air_quality(date)
    for city in CITIES:
        load_weather(city, date)


def run_update_actuals(**context):
    from sqlalchemy import text
    date = str(context["ds"])
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE raw.predictions p
            SET actual = aq.pm10
            FROM raw.air_quality aq
            WHERE p.city = aq.city
              AND p.target_date = aq.date
              AND p.actual IS NULL
              AND aq.date = :date
        """), {"date": date})
    engine.dispose()


def run_predict(**context):
    from datetime import date as date_type
    ds = context["ds"]
    forecast_date = date_type.fromisoformat(ds)
    run_inference(forecast_date)


with DAG(
    dag_id="ml_pipeline",
    description="Daily air quality ingestion and prediction pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=run_ingest,
        execution_timeout=timedelta(minutes=10),
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    silver_to_postgres = PythonOperator(
        task_id="silver_to_postgres",
        python_callable=run_silver_to_postgres,
    )

    update_actuals = PythonOperator(
        task_id="update_actuals",
        python_callable=run_update_actuals,
    )

    predict = PythonOperator(
        task_id="predict",
        python_callable=run_predict,
        execution_timeout=timedelta(minutes=10),
    )

    ingest >> bronze_to_silver >> silver_to_postgres >> update_actuals >> predict
