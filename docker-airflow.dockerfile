
FROM apache/airflow:2.9.3

USER airflow

RUN pip install \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres

RUN pip install --no-cache-dir dbt-core dbt-snowflake