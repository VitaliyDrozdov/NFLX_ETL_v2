from sqlalchemy import (
    CHAR,
    Column,
    Date,
    Interval,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    TIMESTAMP,
)
from sqlalchemy.ext.declarative import declarative_base

from .db_config import LOG_SCHEMA, SCHEMA
from .logging_config import logger

Base = declarative_base()
metadata = MetaData()


class FT_BALANCE_F(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "ft_balance_f"
    on_date = Column(Date, primary_key=True, nullable=False)
    account_rk = Column(Numeric, primary_key=True, nullable=False)
    currency_rk = Column(Numeric)
    balance_out = Column(Float)


FT_POSTING_F = Table(
    "ft_posting_f",
    metadata,
    Column("oper_date", Date, nullable=False),
    Column("credit_account_rk", Numeric, nullable=False),
    Column("debet_account_rk", Numeric, nullable=False),
    Column("credit_amount", Float),
    Column("debet_amount", Float),
    schema=SCHEMA,
)


class MD_ACCOUNT_D(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_account_d"
    data_actual_date = Column(Date, primary_key=True, nullable=False)
    data_actual_end_date = Column(Date, nullable=False)
    account_rk = Column(Numeric, primary_key=True, nullable=False)
    account_number = Column(String(20), nullable=False)
    char_type = Column(CHAR(1), nullable=False)
    currency_rk = Column(Numeric, nullable=False)
    currency_code = Column(String(3), nullable=False)


class MD_CURRENCY_D(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_currency_d"
    currency_rk = Column(Numeric, primary_key=True, nullable=False)
    data_actual_date = Column(Date, primary_key=True, nullable=False)
    data_actual_end_date = Column(Date)
    currency_code = Column(String(3))
    code_iso_char = Column(String(3))


class MD_EXCHANGE_RATE_D(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_exchange_rate_d"
    data_actual_date = Column(Date, primary_key=True, nullable=False)
    data_actual_end_date = Column(Date)
    currency_rk = Column(Numeric, primary_key=True, nullable=False)
    reduced_cource = Column(Float)
    code_iso_num = Column(String(3))


class MD_LEDGER_ACCOUNT_S(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_ledger_account_s"
    ledger_account = Column(Integer, primary_key=True, nullable=False)
    start_date = Column(Date, primary_key=True, nullable=False)
    chapter = Column(CHAR(1))
    chapter_name = Column(String(16))
    section_number = Column(Integer)
    section_name = Column(String(22))
    subsection_name = Column(String(21))
    ledger1_account = Column(Integer)
    ledger1_account_name = Column(String(47))
    ledger_account_name = Column(String(153))
    characteristic = Column(CHAR(1))
    is_resident = Column(Integer)
    is_reserve = Column(Integer)
    is_reserved = Column(Integer)
    is_loan = Column(Integer)
    is_reserved_assets = Column(Integer)
    is_overdue = Column(Integer)
    is_interest = Column(Integer)
    pair_account = Column(String(5))
    end_date = Column(Date)
    is_rub_only = Column(Integer)
    min_term = Column(CHAR(1))
    min_term_measure = Column(CHAR(1))
    max_term = Column(CHAR(1))
    max_term_measure = Column(CHAR(1))
    ledger_acc_full_name_translit = Column(CHAR(1))
    is_revaluation = Column(CHAR(1))
    is_correct = Column(CHAR(1))


class ETLLog(Base):
    __tablename__ = "etl_log"
    __table_args__ = {"schema": LOG_SCHEMA}

    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), nullable=False)
    start_time = Column(TIMESTAMP, nullable=False)
    end_time = Column(TIMESTAMP, nullable=False)
    duration = Column(Interval)


def create_tables(engine):
    """Создание таблиц в БД.
    Args:
        engine: соединение с БД.
    """
    try:
        logger.info("Создание таблиц.")
        Base.metadata.create_all(engine, checkfirst=True)
        metadata.create_all(engine, checkfirst=True)
        logger.info("Таблицы созданы.")
    except Exception as err:
        logger.error(f"Ошибка при создании таблиц: {err}")
    return Base.metadata, metadata


def drop_table_if_exists(engine, table_name, schema=SCHEMA):
    """Удаление таблиц, если существуют.

    Args:
        engine: соединение с БД.
        table_name (str): наименование таблицы.
        schema (str, optional): Наименование схемы. Defaults to SCHEMA.
    """
    try:
        metadata.reflect(bind=engine, schema=schema)
        logger.info(f"Удаление существующей таблицы: {table_name}")
        table = Table(
            table_name,
            metadata,
            autoload_with=engine,
            schema=schema,
        )
        table.drop(bind=engine)
        logger.info(f"Таблица '{table_name}' удалена.")
    except Exception as err:
        logger.error(f"\nОшибка при удалении таблицы: {err}\n")
