import os

from dotenv import load_dotenv

from src.core.manage_tables import (
    create_tables,
    drop_table_if_exists,
)
from src.core.db_config import engine, SCHEMA
from src.core.parser import load_to_db, read_data
from src.core.runtime import log_execution

load_dotenv()

CSVPATH = os.getenv("AIRFLOW_CONN_ID")

csv_files = [
    "ft_balance_f.csv",
    "ft_posting_f.csv",
    "md_account_d.csv",
    "md_currency_d.csv",
    "md_exchange_rate_d.csv",
    "md_ledger_account_s.csv",
]


@log_execution
def proccess(filename):
    table_name = filename.split(".")[0]
    drop_table_if_exists(engine, table_name)
    create_tables(engine)
    filepath = os.path.join("./Исходные_файлы", filename)
    encoding = "utf-8"
    # для этого файла стандартная utf-8 не подходит
    if "md_currency_d.csv" in filename:
        encoding = "cp1252"
    data = read_data(filepath, encoding=encoding)
    load_to_db(data, table_name, engine, schema=SCHEMA)


def main(filenames):
    """Основной ETL процесс."""
    if filenames:
        for filename in filenames:
            proccess(filename)
        engine.dispose()


if __name__ == "__main__":
    main(csv_files)
