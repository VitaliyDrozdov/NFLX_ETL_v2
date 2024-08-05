import logging
import sys

from sqlalchemy.orm import sessionmaker

from .db_config import engine


def setup_logging():
    """ "Настройки логирования."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - func(%(funcName)s)- [%(levelname)s] - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


logger = setup_logging()


def log_to_db(table_name, start_time, end_time):
    """Функция записи логов в таблицу в БД

    Args:
        table_name (str): наименоване таблицы.
        start_time (datetime): начальное время выполнения.
        end_time (datetime): конечное время выполнения.
    """
    try:
        from .manage_tables import ETLLog

        Session = sessionmaker(bind=engine)
        session = Session()
        duration = end_time - start_time
        log_entry = ETLLog(
            table_name=table_name,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
        )
        session.add(log_entry)
        session.commit()

    except Exception as err:
        logger.error(
            f"Ошибка при записи лога для таблицы '{table_name}': {err}"
        )
    finally:
        session.close()
