import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SCHEMA = os.getenv("SCHEMA")
LOG_SCHEMA = os.getenv("LOG_SCHEMA")
AIRFLOW_CONN_ID = os.getenv("AIRFLOW_CONN_ID")
# SCHEMA = "DS"
# LOG_SCHEMA = "LOG"

ENGINE_PATH = (
    f"postgresql://airflow:airflow@localhost:5432/postgres"
)
engine = create_engine(ENGINE_PATH)
