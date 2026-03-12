from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator




@dag(
    dag_id=f"transfer_from_minio_to_snowflake_transactions",
    start_date=datetime(2026, 2, 7),
    schedule="*/1 * * * *",
    tags=["transfer"],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries":2,
        "retry_delay": timedelta(minutes=5)
    })

def transfer_data_transactions():

    create_bronze_table= SQLExecuteQueryOperator(
        task_id="create_raw",
        conn_id= "warehouse_id",
        sql= """ CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
                    raw_json VARIANT,
                    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());
                  """
         )

    create_stage= SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id= "warehouse_id",
        sql= """ CREATE STAGE IF NOT EXISTS incoming  
                 URL = 's3://de-project-banking-pipeline-dev-2/transactions'
                 STORAGE_INTEGRATION = my_s3_integration
                 FILE_FORMAT = (TYPE = 'JSON'); """
         )
    
    load_json = SQLExecuteQueryOperator(
        task_id="copy_json_to_variant",
        conn_id="warehouse_id",
        sql="""
            COPY INTO RAW_TRANSACTIONS (raw_json)
            FROM @incoming
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """
        )

    create_bronze_table >> create_stage >> load_json

transfer_data_transactions()


@dag(
    dag_id=f"transfer_from_minio_to_snowflake_accounts",
    start_date=datetime(2026, 2, 7),
    schedule="*/1 * * * *",
    tags=["transfer"],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries":2,
        "retry_delay": timedelta(minutes=5)
    })

def transfer_data_accounts():
    create_bronze_table= SQLExecuteQueryOperator(
        task_id="create_raw",
        conn_id= "warehouse_id",
        sql= """ CREATE TABLE IF NOT EXISTS RAW_ACCOUNTS (
                    raw_json VARIANT,
                    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());
                  """
         )

    create_stage= SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id= "warehouse_id",
        sql= """ CREATE STAGE IF NOT EXISTS incoming  
                 URL = 's3://de-project-banking-pipeline-dev-2/accounts'
                 STORAGE_INTEGRATION = my_s3_integration
                 FILE_FORMAT = (TYPE = 'JSON'); """
         )
    load_json = SQLExecuteQueryOperator(
        task_id="copy_json_to_variant",
        conn_id="warehouse_id",
        sql="""
            COPY INTO RAW_ACCOUNTS (raw_json)
            FROM @incoming
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """
        )

    create_bronze_table >> create_stage >> load_json

transfer_data_accounts()

@dag(
    dag_id=f"transfer_from_minio_to_snowflake_customers",
    start_date=datetime(2026, 2, 7),
    schedule="*/1 * * * *",
    tags=["transfer"],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries":2,
        "retry_delay": timedelta(minutes=5)
    })

def transfer_data_customers():

    create_bronze_table= SQLExecuteQueryOperator(
        task_id="create_raw",
        conn_id= "warehouse_id",
        sql= """ CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
                    raw_json VARIANT,
                    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());
                  """
         )
    create_stage= SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id= "warehouse_id",
        sql= """ CREATE STAGE IF NOT EXISTS incoming  
                 URL = 's3://de-project-banking-pipeline-dev-2/customers'
                 STORAGE_INTEGRATION = my_s3_integration
                 FILE_FORMAT = (TYPE = 'JSON'); """
         )
    load_json = SQLExecuteQueryOperator(
        task_id="copy_json_to_variant",
        conn_id="warehouse_id",
        sql="""
            COPY INTO RAW_CUSTOMERS (raw_json)
            FROM @incoming
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """
        )
    create_bronze_table >> create_stage >> load_json

transfer_data_customers()

