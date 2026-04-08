# ML Pipeline Portfolio Project — Implementation Plan

## Context

A greenfield ML pipeline portfolio project. The goal is to demonstrate end-to-end MLOps skills: API ingestion → distributed processing → SQL transformations → model training → experiment tracking → cloud storage → orchestration → CI/CD.

**Use case**: Predict whether a flight will be delayed (>15 min) using historical flight data from the **OpenSky Network API** (free, no auth required) enriched with weather conditions at the origin airport from the **Open-Meteo API**. Binary classification problem — output: `is_delayed` (0/1).

---

## Technology Stack

| Layer | Technology |
|---|---|
| API source (flights) | OpenSky Network REST API (historical flight data) |
| API source (weather) | Open-Meteo REST API (weather at origin airport) |
| Orchestration | Apache Airflow 2.8 |
| Distributed processing | PySpark 3.5 |
| SQL transformations | dbt-core 1.8 + dbt-postgres |
| Experiment tracking | MLFlow 2.13 |
| Data lake | AWS S3 (bronze/silver/gold layers) |
| Data warehouse | AWS RDS PostgreSQL 15 |
| Container registry | AWS ECR |
| IaC | Terraform 1.8 |
| Containers | Docker + Docker Compose |
| CI/CD | GitHub Actions |
| ML model | scikit-learn (LogisticRegression, RandomForest) + XGBoost (classifier) |

---

## Directory Structure

```
ML_pipeline/
├── .github/
│   └── workflows/
│       ├── ci.yml                   # lint + test on every PR
│       └── cd.yml                   # build, push ECR, deploy on merge to main
│
├── airflow/
│   ├── dags/
│   │   └── ml_pipeline_dag.py       # Main DAG: 6 tasks, daily schedule
│   ├── plugins/                     # custom operators (empty to start)
│   └── logs/                        # gitignored
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_weather_raw.sql      # read from raw schema, cast types
│   │   ├── intermediate/
│   │   │   └── int_weather_features.sql # lag features, rolling averages
│   │   └── marts/
│   │       └── ml_features.sql          # final feature table for training
│   ├── tests/
│   │   └── generic/                     # dbt data quality tests
│   ├── dbt_project.yml
│   └── profiles.yml                     # reads from env vars
│
├── spark/
│   └── jobs/
│       ├── raw_to_silver.py         # S3 JSON → cleaned Parquet on S3
│       └── silver_to_postgres.py    # Parquet → COPY into PostgreSQL raw schema
│
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── flights_api.py           # fetch OpenSky Network flights, upload JSON to S3 bronze
│   │   └── weather_api.py           # fetch Open-Meteo weather at origin airports, upload to S3 bronze
│   ├── training/
│   │   ├── __init__.py
│   │   ├── train.py                 # read from PostgreSQL, train, log to MLFlow
│   │   └── evaluate.py              # load registered model, compute test metrics
│   └── utils/
│       ├── __init__.py
│       ├── s3.py                    # boto3 helpers: upload/download/list
│       └── db.py                    # SQLAlchemy engine factory
│
├── infra/
│   └── terraform/
│       ├── main.tf                  # S3 buckets, RDS, ECR repos, IAM roles
│       ├── variables.tf
│       └── outputs.tf               # RDS endpoint, S3 bucket names
│
├── docker/
│   ├── Dockerfile.airflow           # extends apache/airflow:2.8, adds project deps
│   ├── Dockerfile.spark             # extends bitnami/spark:3.5, adds PySpark jobs
│   └── Dockerfile.mlflow            # extends python:3.11-slim, installs mlflow
│
├── tests/
│   ├── unit/
│   │   ├── test_weather_api.py      # mock HTTP, assert S3 payload shape
│   │   ├── test_train.py            # mock DB read, assert MLFlow run logged
│   │   └── test_s3_utils.py
│   └── integration/
│       └── test_pipeline.py         # uses localstack + test DB
│
├── docker-compose.yml               # local dev: airflow, spark, mlflow, postgres, localstack
├── docker-compose.override.yml      # developer-specific volume mounts
├── pyproject.toml                   # black, isort, flake8, pytest config
├── requirements.txt                 # pinned deps
├── .env.example                     # template for secrets (never committed)
└── README.md
```

---

## Data Flow

```
OpenSky Network API          Open-Meteo API
        │                          │
        ▼                          ▼
S3 bronze/flights/          S3 bronze/weather/
year=YYYY/month=MM/         year=YYYY/month=MM/
day=DD/data.json            day=DD/<airport>.json
        │                          │
        └──────────┬───────────────┘
                   ▼ (spark/jobs/raw_to_silver.py)
           S3 silver/  ← cleaned Parquet, typed schema, partitioned
                   │
                   ▼ (spark/jobs/silver_to_postgres.py)
           PostgreSQL: raw.flights + raw.weather
                   │
                   ▼ (dbt run)
           staging.stg_flights + staging.stg_weather
           intermediate.int_flights_weather  ← joined on (airport, hour)
           marts.ml_features  ← is_delayed label + all features
                   │
                   ▼ (src/training/train.py + MLFlow)
           MLFlow tracking server  ← accuracy, F1, ROC-AUC, model artifact → S3
           MLFlow Model Registry   ← versioned model: "flight-delay-classifier"
                   │
                   ▼ (src/training/evaluate.py)
           PostgreSQL: marts.model_predictions
```

---

## Airflow DAG (`ml_pipeline_dag.py`)

**Schedule**: `@daily`  
**DAG ID**: `ml_pipeline`

```
ingest_weather
      │
spark_raw_to_silver
      │
spark_silver_to_postgres
      │
dbt_transform          ← BashOperator: dbt run + dbt test
      │
train_model            ← PythonOperator: calls src/training/train.py
      │
register_model         ← PythonOperator: promote best run to MLFlow Registry
```

All tasks pass `execution_date` as a parameter to ensure idempotency.

---

## dbt Models

| Layer | Model | Purpose |
|---|---|---|
| Staging | `stg_weather_raw` | Cast columns, rename, filter nulls |
| Intermediate | `int_flights_weather` | Join flights with weather on (origin_airport, departure_hour); compute `is_delayed` |
| Mart | `ml_features` | Final feature select: airline, origin, dest, hour, day_of_week, month, weather features, is_delayed |

dbt tests: `not_null`, `unique` on flight primary keys; `accepted_values` on airline codes.

---

## PySpark Jobs

**`raw_to_silver.py`**:
- Read JSON from S3 bronze with `spark.read.json(path)`
- Enforce schema (StructType)
- Drop nulls, deduplicate, cast timestamps
- Write Parquet to S3 silver with `partitionBy("year","month")`

**`silver_to_postgres.py`**:
- Read Parquet from S3 silver
- Write to PostgreSQL via JDBC (`spark.write.jdbc(...)`)
- Mode: `append` with dedup on `(station_id, timestamp)`

---

## MLFlow Setup

- **Tracking URI**: `http://mlflow:5000` (local) / `http://<ec2-ip>:5000` (prod)
- **Artifact store**: `s3://ml-pipeline-artifacts/mlflow/`
- **Model registry**: PostgreSQL backend (`postgresql+psycopg2://...`)
- **Logged per run**: accuracy, F1, precision, recall, ROC-AUC, hyperparameters, feature importances, model pickle, confusion matrix
- **Promotion**: If F1 > threshold → transition to `Staging`; after manual review → `Production`

---

## Docker Compose Services (local)

| Service | Image | Ports | Purpose |
|---|---|---|---|
| `postgres` | postgres:15 | 5432 | Airflow metadata + data warehouse |
| `airflow-webserver` | docker/Dockerfile.airflow | 8080 | Airflow UI |
| `airflow-scheduler` | docker/Dockerfile.airflow | — | DAG scheduler |
| `spark-master` | docker/Dockerfile.spark | 7077, 8081 | Spark cluster |
| `spark-worker` | docker/Dockerfile.spark | — | Spark worker |
| `mlflow` | docker/Dockerfile.mlflow | 5000 | MLFlow tracking UI |
| `localstack` | localstack/localstack | 4566 | S3 emulation for local dev |

---

## AWS Infrastructure (Terraform)

**S3 Buckets**:
- `ml-pipeline-raw` — bronze JSON data
- `ml-pipeline-processed` — silver/gold Parquet
- `ml-pipeline-artifacts` — MLFlow artifacts, model registry

**RDS PostgreSQL**:
- Instance: `db.t3.micro` (free tier for dev)
- Schemas: `raw`, `staging`, `intermediate`, `marts`

**ECR Repositories**:
- `ml-pipeline/airflow`
- `ml-pipeline/spark`
- `ml-pipeline/mlflow`

**IAM**:
- Role `ml-pipeline-runner` with S3 read/write, RDS connect
- GitHub Actions OIDC role for CI/CD pushes to ECR

---

## CI/CD — GitHub Actions

**`ci.yml`** (triggers on PR):
```
1. checkout
2. python setup (3.11)
3. pip install requirements
4. black --check .
5. isort --check .
6. flake8 .
7. pytest tests/unit/ --cov=src
```

**`cd.yml`** (triggers on push to `main`):
```
1. checkout
2. configure AWS credentials (OIDC)
3. login to ECR
4. docker build + tag each Dockerfile
5. docker push to ECR
6. terraform apply (infra changes)
7. restart ECS tasks / trigger Airflow DAG via API
```

---

## Implementation Phases

### Phase 1 — Foundation
- [ ] `pyproject.toml`, `requirements.txt`, `.env.example`
- [ ] `docker-compose.yml` with all services
- [ ] `docker/` Dockerfiles
- [ ] Verify all services start locally

### Phase 2 — Data Ingestion
- [ ] `src/ingestion/weather_api.py` — Open-Meteo fetch + S3 upload
- [ ] `src/utils/s3.py` — boto3 helpers
- [ ] Unit tests for ingestion

### Phase 3 — Spark Processing
- [ ] `spark/jobs/raw_to_silver.py`
- [ ] `spark/jobs/silver_to_postgres.py`
- [ ] Test with local Spark + LocalStack

### Phase 4 — dbt Transformations
- [ ] `dbt/dbt_project.yml`, `profiles.yml`
- [ ] All 3 model layers
- [ ] dbt tests

### Phase 5 — Model Training + MLFlow
- [ ] `src/training/train.py`
- [ ] `src/training/evaluate.py`
- [ ] MLFlow experiment runs visible in UI

### Phase 6 — Airflow Orchestration
- [ ] `airflow/dags/ml_pipeline_dag.py`
- [ ] Full DAG run end-to-end locally

### Phase 7 — AWS Infrastructure
- [ ] Terraform modules
- [ ] Deploy to AWS and run full pipeline in cloud

### Phase 8 — CI/CD
- [ ] GitHub Actions CI workflow
- [ ] GitHub Actions CD workflow

---

## Verification

1. **Local**: `docker-compose up` → all services healthy
2. **Ingestion**: manually trigger `ingest_weather` task → verify JSON appears in LocalStack S3
3. **Spark**: run `raw_to_silver.py` → verify Parquet in LocalStack, rows in PostgreSQL
4. **dbt**: `dbt run && dbt test` → all models build, all tests pass
5. **MLFlow**: `train.py` → open `localhost:5000`, verify run with metrics logged, model registered
6. **Airflow**: trigger full DAG → all 6 tasks green
7. **CI**: open a PR → GitHub Actions lint + test pass
8. **AWS**: `terraform apply` → run DAG pointing to real S3/RDS → data flows end-to-end
