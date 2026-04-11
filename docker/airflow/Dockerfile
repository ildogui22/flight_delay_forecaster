FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y git libpq-dev && apt-get clean

USER airflow

COPY requirements-airflow.txt .
RUN pip install --no-cache-dir "apache-airflow==2.10.4" -r requirements-airflow.txt
