import pandas as pd

from .db_config import SCHEMA
from .logging_config import logger


def read_data(file_path, encoding="utf-8", delimiter=";"):
    """Функция загруки данных из csv.
    Args:
        file_path: dataset.
        encoding (str, optional): Кодировка. Defaults to "utf-8".
        delimeter (str, optional): Разделитель.Defaults to ";".
    """
    try:
        logger.info(f"Чтение данных из файла: {file_path}")
        data = pd.read_csv(
            file_path,
            parse_dates=True,
            delimiter=delimiter,
            encoding=encoding,
        )
        return data
    except Exception as err:
        logger.error(f"Ошибка при чтении данных из: {file_path}; {err}")


def clean_data(data, dropna):
    """Функция для очистки данных.
    Args:
        data: dataset.
        dropna (bool): Удалять ли строки с Null значениями.
    """
    if dropna:
        data.dropna(how="any", inplace=True)
    data.drop_duplicates(inplace=True)
    data.columns = data.columns.str.lower()
    if "currency_code" in data.columns:
        data["currency_code"] = (
            data["currency_code"].astype(int).astype(str).str[:3]
        )
    if "code_iso_char" in data.columns:
        data["code_iso_char"] = data["code_iso_char"].apply(
            lambda x: "" if len(x) < 2 else x
        )
    return data


def load_to_db(
    data, table_name, engine, schema=SCHEMA, clean=True, dropna=True
):
    """Загрузка данных в БД.

    Args:
        data: dataset для загрузки.
        table_name (str): наименование таблицы.
        engine: соединение с БД. Например, через SQLAlchemy.
        schema (str, optional): Наименование схемы в БД. Defaults to SCHEMA.
        clean (bool, optional): Очистка данных. Defaults to True.
        dropna (bool, optional): Удалять ли строки с Null значениями.
    """
    try:
        if clean:
            cleaned_data = clean_data(data, dropna=dropna)
        else:
            cleaned_data = data

        cleaned_data.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
        )
        logger.info("Данные загружены в БД.")

    except Exception as err:
        logger.error(
            (f"\nОшибка при загрузке данных в таблицу {table_name}: {err}\n")
        )


def export_to_csv(table_name, csv_path, engine, schema="DM"):
    sql_query = f'SELECT * FROM "{schema}".{table_name}'
    try:
        df = pd.read_sql_query(sql_query, engine)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig", sep=";")
        logger.info(
            f"Данные из '{table_name}' экспортированы в CSV: {csv_path}"
        )
    except Exception as e:
        logger.error(
            f"Ошибка при экспорте данных из таблицы '{table_name}' в CSV: {e}"
        )
