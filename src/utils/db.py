import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mlpipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "mlpipeline")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mlpipeline")

def get_engine():
    url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(url)

def ensure_schema(engine, schema: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    except IntegrityError:
        pass

def ensure_predictions_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.predictions (
                city          VARCHAR NOT NULL,
                forecast_date DATE NOT NULL,
                target_date   DATE NOT NULL,
                horizon       INT NOT NULL,
                predicted     FLOAT NOT NULL,
                actual        FLOAT,
                UNIQUE (city, forecast_date, horizon)
            )
        """))
