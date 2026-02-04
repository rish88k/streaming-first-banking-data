from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'banking_end_to_end_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    # STEP 1: LOAD (Bronze Layer)
    load_transactions = CopyFromExternalStageToSnowflakeOperator(
        task_id='load_transactions_raw',
        snowflake_conn_id='snowflake_conn',
        stage='BRONZE.BANKING_RAW_STAGE',
        table='RAW_TRANSACTIONS',
        schema='BRONZE',
        file_format="(TYPE = 'JSON')",
        files=['transactions/*.json']
    )

    # STEP 2: TRANSFORM (Silver & Gold Layers via dbt)
    # This command runs your dbt models inside the Airflow container
    dbt_transform = BashOperator(
        task_id='dbt_transform_banking',
        bash_command="""
            cd /opt/airflow/banking_dbt && \
            dbt run --profiles-dir . --target dev && \
            dbt test --profiles-dir . --target dev
        """
    )

    # The dependency chain: Load must finish before Transform starts
    load_transactions >> dbt_transform