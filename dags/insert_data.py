import os

from dotenv import load_dotenv
from datetime import datetime, time, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from src.core.db_config import engine, SCHEMA, AIRFLOW_CONN_ID
from src.main import csv_files, main
from src.f101_manager import f_101_main

start_date = datetime.combine(datetime.now().date(), time(1, 0))

load_dotenv()

CONND_ID = os.getenv("AIRFLOW_CONN_ID")

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
        conn_id=CONND_ID,
        sql="sql/create_schemas.sql",
    )
    insert_into_db = PythonOperator(
        task_id="insert_into_db",
        python_callable=main,
        op_kwargs={"filenames": csv_files},
    )
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONND_ID,
        sql="sql/create_tables.sql",
    )
    create_fill_account_balance_f = SQLExecuteQueryOperator(
        task_id="create_fill_account_balance_f",
        conn_id=CONND_ID,
        sql="sql/fill_account_balance_f.sql",
    )
    create_fill_account_turnover_f = SQLExecuteQueryOperator(
        task_id="create_fill_account_turnover_f",
        conn_id=CONND_ID,
        sql="sql/fill_account_turnover_f.sql",
    )
    create_fill_f101_round_f = SQLExecuteQueryOperator(
        task_id="create_fill_f101_round_f",
        conn_id=CONND_ID,
        sql="sql/fill_f101_round_f.sql",
    )
    do_procedures = SQLExecuteQueryOperator(
        task_id="do_procedures",
        conn_id=CONND_ID,
        sql="sql/do_procedures.sql",
    )

    export_from_db = PythonOperator(
        task_id="export_from_db",
        python_callable=f_101_main,
    )
    (
        create_schemas
        >> insert_into_db
        >> create_tables
        >> [
            create_fill_account_balance_f,
            create_fill_account_turnover_f,
            create_fill_f101_round_f,
        ]
        >> export_from_db
    )
