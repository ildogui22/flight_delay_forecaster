"""
Daily ML pipeline DAG: ingest → ETL → load to postgres → dbt → train
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
import os
sys.path.insert(0, "/opt/airflow/src")

from ingestion.ingest import ingest_flights, ingest_weather
from etl.bronze_util import process_flights, process_weather
from etl.silver_to_postgres import get_engine, ensure_schema, load_flights, load_weather

AIRPORTS = ["EDDF", "EGLL", "LFPG", "EHAM"]

DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_ingest(**context):
    date = str(context["ds"])
    next_day = str(context["tomorrow_ds"])
    for airport in AIRPORTS:
        ingest_flights(airport, date, next_day)
        ingest_weather(airport, date)

def run_bronze_to_silver(**context):
    date = str(context["ds"])
    process_flights(date)
    for airport in AIRPORTS:
        process_weather(airport, date)

def run_silver_to_postgres(**context):
    date = str(context["ds"])
    engine = get_engine()
    ensure_schema(engine, "raw")
    load_flights(engine, date)
    load_weather(engine, date)
    engine.dispose()


with DAG(
    dag_id="ml_pipeline",
    description="Flight delay prediction pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 3, 1),
    end_date=datetime(2026, 3, 31),
    catchup=True,
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

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    ingest >> bronze_to_silver >> silver_to_postgres >> dbt_run >> dbt_test
