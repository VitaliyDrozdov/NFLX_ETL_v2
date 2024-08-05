# ETL-процесс для обновления данных в БД и выгруки формы 101 банковского отчета v2

Ссылка на видео: https://cloud.mail.ru/public/trmM/6XYVu7hy7

## Описание проекта<a name="description"></a>

## Обновленная версия ETL процесса https://github.com/VitaliyDrozdov/NFLX_ETL. 
##  Добавлен Airflow.

ETL процесс состоит из следующих этапов:

**1. Загрузка данных из .csv файлов в БД.**

**2. Формирование необходимых витрин данных.**

**3. Формирование таблицы для формы 101.**

**4. Выгрузка данных из БД в .csv файл.**



### Используемый стек:<a name="stack"></a>

- Python
- Pandas
- SQLAlchemy
- PostgreSQL
- Airflow

### Установка проекта локально <a name="local-install"></a>

1. **Склонировать репозиторий:**

   ```bash
   git clone https://github.com/VitaliyDrozdov/NFLX_ETL

2. **Подготавливаем переменные окружения к работе:**

*Копируем файл*  .env.example с новым названием .env и заполняем его необходимыми данными:

```shell
cp .env.example .env
```

3. **Запуск Airflow в Docker контейнерах:**
### Запускаем Airflow в контейнере:

*В корневой директории вводим ```docker compose up -d```*

```shell
docker compose up -d
```
## Airflow будет доступен по адресу:
<h3>
    <a href="http://127.0.0.1:8080//">http://127.0.0.1:8080/</a>
</h3>

Данные для входа настриваются в .env файле. 
По-умолчанию:
login: airflow
password: airflow

4. **Настройка подключения к БД:**

В Airflow Connections настраиваем подключение к базе данных. Вводим данные из .env. По-умолчанию:
Connection Id = postgres_local
host = host.docker.internal
login = airflow
password = airflow
port = 5432


5. **Запускаем DAG через web интерфейс:**

## Расчитываем витрины данных:

В результате в БД в схеме "DS" создадутся таблицы аналогичные именам файлов и заполнятся данными из .csv файлов. Логи выполнения будут доступные в таблице "etl_log" в схеме "LOG".

В схеме "DM" создадутся таблицы  account_turnover_f, account_balance_f, fill_f101_round_f и заполняется рассчитанными данными.

В папке data_mart (по-умолчанию) появится файл fill_f101_round_f.csv с данными из таблицы.
