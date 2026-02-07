from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'minio_to_snowflake_ingestion_v2',
    default_args=default_args,
    description='Modern Snowflake Ingestion using CopyFromExternalStage',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    # 1. Ensure the landing schema exists
    create_schema = CopyFromExternalStageToSnowflakeOperator(
        task_id='create_bronze_schema',
        snowflake_conn_id='snowflake_conn',
        sql="CREATE SCHEMA IF NOT EXISTS BRONZE;"
    )

    # 2. Define the Stage (The connection to MinIO)
    # It is better to keep this as a standalone SQL command
    create_stage = CopyFromExternalStageToSnowflakeOperator(
        task_id='create_external_stage',
        snowflake_conn_id='snowflake_conn',
        sql="""
            CREATE STAGE IF NOT EXISTS BRONZE.BANKING_RAW_STAGE
            URL = 's3://banking-raw/'
            CREDENTIALS = (AWS_KEY_ID = 'admin' AWS_SECRET_KEY = 'password')
            ENDPOINT = 'http://minio:9000';
        """
    )

    # 3. Use the new replacement for the deprecated S3ToSnowflakeOperator
    load_accounts = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_accounts',
        snowflake_conn_id='snowflake_conn',
        stage='BRONZE.BANKING_RAW_STAGE',
        table='RAW_ACCOUNTS',
        schema='BRONZE',
        file_format="(TYPE = 'JSON')",
        files=['accounts/*.json'] # You can also use pattern matching here
    )

    load_customers = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_customers',
        snowflake_conn_id='snowflake_conn',
        stage='BRONZE.BANKING_RAW_STAGE',
        table='RAW_CUSTOMERS',
        schema='BRONZE',
        file_format="(TYPE = 'JSON')",
        files=['customers/*.json']
    )

    # Updated Dependency Chain
    create_schema >> create_stage >> [load_accounts, load_customers]