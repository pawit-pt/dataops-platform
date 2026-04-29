FROM apache/airflow:2.8.1-python3.11

# Install the Trino provider for TrinoOperator
RUN pip install --no-cache-dir "apache-airflow-providers-trino" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt" && \
    airflow version
