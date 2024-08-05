FROM apache/airflow:2.9.3
# WORKDIR /sources
USER root
COPY requirements.txt ./requirements.txt
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ./requirements.txt
# COPY ./src /opt/airflow/src/