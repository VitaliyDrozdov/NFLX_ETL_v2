import time
from datetime import datetime
from functools import wraps
from .logging_config import logger, log_to_db


def log_execution(func):
    """Декортатор для логирования в консоли и в log файл."""

    @wraps(func)
    def wrapper(filename):
        table_name = filename.split(".")[0]
        start_time = datetime.now()
        logger.info(f"Начало загрузки данных для таблицы: {table_name}.")
        time.sleep(5)
        result = func(filename)
        end_time = datetime.now()
        # logger.info(f"Окончание загрузки данных для таблицы: {table_name}.")
        log_to_db(table_name, start_time, end_time)
        return result

    return wrapper
