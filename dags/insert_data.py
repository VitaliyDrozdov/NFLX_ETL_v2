import os
import sys


from datetime import datetime, time, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf
from airflow.models import Variable


# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# sys.path.append(project_root)

# from sources.core.db_config import engine, SCHEMA, AIRFLOW_CONN_ID
# from sources.main import csv_files, CSVPATH, main

from src.core.db_config import engine, SCHEMA, AIRFLOW_CONN_ID
from src.main import csv_files, main

start_date = datetime.combine(datetime.now().date(), time(1, 0))

sql_script_path = "sql/create_schemas.sql"
default_args = {
    "owner": "drvetall",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "start_date": start_date,
}

with DAG(
    dag_id="insert_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    create_schemas = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id=f"postgres_local",
        sql="sql/create_schemas.sql",
    )
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=f"postgres_local",
        sql="sql/create_tables.sql",
    )
    insert_into_db = PythonOperator(
        task_id="insert_into_db",
        python_callable=main,
        op_kwargs={"filenames": csv_files},
    )
    create_schemas >> insert_into_db >> create_tables
    # create_schemas >> create_tables
