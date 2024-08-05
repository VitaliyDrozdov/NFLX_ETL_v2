# ETL-процесс для обновления данных в БД и выгруки формы 101 банковского отчета.

Ссылка на видео: https://cloud.mail.ru/public/trmM/6XYVu7hy7

## Описание проекта<a name="description"></a>
ETL процесс состоит из следующий этапов:

**1. Загруказ данных из .csv файлов в БД.**

**2. Формирование необходимых витрин данных.**

**3. Формирование таблицы для формы 101.**

**4. Выгрузка данных из БД в .csv файл.**



### Используемый стек:<a name="stack"></a>

- Python
- Pandas
- SQLAlchemy
- PostgreSQL

### Установка проекта локально <a name="local-install"></a>

1. **Склонировать репозиторий:**

   ```bash
   git clone https://github.com/VitaliyDrozdov/NFLX_ETL

2. **Создаем и активируем виртуальное окружение:**

* Для Linux/macOS

    ```
    python3 -m venv env
    source env/bin/activate
    python3 -m pip install --upgrade pip
    ```

* Для windows

    ```
    python -m venv venv
    source venv/scripts/activate
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    ```


3. **Подготавливаем переменные окружения к работе:**

*Копируем файл*  .env.example с новым названием .env и заполняем его необходимыми данными:

```shell
cp .env.example .env
```

### Экспортируем данные в БД:

*Создаем в БД новые схемы ```"DS"``` и ```"LOG"```*


*В корневой папке проекта создаем папку ```"Иcходные файлы"``` и копируем необходимые файлы с таблицами в формате .csv*

*Запускаем скрипт ```main.py```*

```shell
python main.py
```
В результате в БД в схеме "DS" создадутся таблицы аналогичные именам файлов и заполнятся данными из .csv файлов. Логи выполнения будут доступные в таблице "etl_log" в схеме "LOG".

## Расчитываем витрины данных:

В папке data_mart лежат необходимые скрипты sql для создания нужных таблиц и процедур.


```shell
cd data_mart
```

Последовательно выполняем скрипты из файлов:

```shell
create_tables
fill_account_turnover_f
fill_account_balance_f
fill_f101_round_f
do_procedures
```
В схеме "DM" создадутся таблицы  account_turnover_f, account_balance_f, fill_f101_round_f и заполняется рассчитанными данными.

## Выгружаем полученные данные из таблицы f101_round_f:

Запускаем скрипт ```f101_manager.py``` в корне проекта:
```shell
python f101_manager.py
```
В папке data_mart (по-умолчанию) появится файл fill_f101_round_f.csv с данными из таблицы.
