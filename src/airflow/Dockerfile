FROM apache/airflow:2.10.3-python3.11

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

COPY src /opt/airflow/src
ENV PYTHONPATH=/opt/airflow/src