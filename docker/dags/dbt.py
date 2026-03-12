from airflow.decorators import dag, task    
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# Environment variables for dbt - Ensure these point to your specific directories
DBT_ENV = {
    'DBT_PROFILES_DIR': '/Users/rish88k/data_analysis/de_project/dbt/trans_snowf',
    'DBT_PROJECT_DIR': '/Users/rish88k/data_analysis/de_project/dbt/trans_snowf',
    'PATH': '/Users/rish88k/data_analysis/de_project/dbt/venv/bin:' + '/usr/local/bin:/usr/bin:/bin'
}

default_args = {
    'owner': 'risheek',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ------------------------------------------------------------------
# DAG 1: Bronze Layer (Staging)
# Goal: Unpack raw Kafka/MinIO data into incremental tables.
# ------------------------------------------------------------------
@dag(
    dag_id='banking_01_bronze_layer',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),  # Runs frequently to ingest CDC data
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['banking', 'bronze', 'incremental']
)
def bronze_pipeline():

    # Using BashOperator for robust dbt CLI execution
    build_bronze_acc = BashOperator(
        task_id='build_bronze_acc',
        bash_command='dbt build --select bronze_acc',
        env=DBT_ENV
    )

    build_bronze_cust = BashOperator(
        task_id='build_bronze_cust',
        bash_command='dbt build --select bronze_cust',
        env=DBT_ENV
    )

    build_bronze_trans = BashOperator(
        task_id='build_bronze_trans',
        bash_command='dbt build --select bronze_trans',
        env=DBT_ENV
    )

    # All three independent streams run in parallel
    [build_bronze_acc, build_bronze_cust, build_bronze_trans]

# ------------------------------------------------------------------
# DAG 2: Aggregation Layer (Gold)
# Goal: Build summary tables for analytics once Bronze data is ready.
# ------------------------------------------------------------------
@dag(
    dag_id='banking_02_aggregation_layer',
    default_args=default_args,
    schedule_interval='0 */6 * * *', # Runs every 6 hours (at the start of the hour)
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['banking', 'gold', 'aggregations']
)
def aggregation_pipeline():

    # This builds the 5 models in your aggregations folder
    build_gold_metrics = BashOperator(
        task_id='build_aggregations',
        bash_command='dbt build --select path:models/aggregations',
        env=DBT_ENV
    )

    # Final project-wide test to ensure everything is valid
    #test_full_project = BashOperator(
    #    task_id='dbt_test_all',
    #    bash_command='dbt test',
    #    env=DBT_ENV
    #)

    [build_gold_metrics]

# Instantiate the DAGs
bronze_dag = bronze_pipeline()
agg_dag = aggregation_pipeline()