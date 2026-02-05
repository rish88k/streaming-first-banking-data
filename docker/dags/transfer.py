from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator




@dag(
    dag_id=f"transfer_from_minio_to_snowflake",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    tags=["transfer"],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries":2,
        "retry_delay": timedelta(minutes=5)
    })

def transfer_data():

    create_bronze_table= SQLExecuteQueryOperator(
        task_id="create_bronze",
        conn_id= "SNOWFLAKE_DEFAULT",
        sql= """ CREATE TABLE IF NOT EXISTS BRONZE (
                    raw_json VARIANT,
                    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());
                  """
         )

    create_stage= SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id= "SNOWFLAKE_DEFAULT",
        sql= """ CREATE STAGE incoming  
                 URL = 's3://de-project-banking-pipeline-dev-1/transactions'
                 STORAGE_INTEGRATION = my_s3_integration
                 FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE); """
         )
    
    load_json= CopyFromExternalStageToSnowflakeOperator(
        task_id="copy",
        snowflake_conn_id= "SNOWFLAKE_DEFAULT",
        stage="incoming",
        table="BRONZE",
        file_format="(TYPE= 'JSON')",
        copy_options="ON_ERROR= 'continue'"
    )
    
    create_bronze_table >> create_stage >> load_json


transfer_data()


        


