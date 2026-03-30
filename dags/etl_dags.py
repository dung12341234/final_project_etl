from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'folon',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    description='ETL pipeline for student adaptability data',
    schedule_interval='@hourly',  # runs once per day
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id='run_etl_script',
        bash_command='python /opt/airflow/scripts/etl_pipeline.py'
    )