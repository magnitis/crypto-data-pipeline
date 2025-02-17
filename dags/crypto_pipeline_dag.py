from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "crypto_market_data_pipeline",
    default_args=default_args,
    description="Crypto Market Data Pipeline DAG",
    schedule_interval="@continuous",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "market_data"],
    max_active_runs=1,
) as dag:

    # Task to run the crypto data pipeline
    run_pipeline = BashOperator(
        task_id="run_crypto_pipeline",
        bash_command="python /opt/airflow/crypto_data_pipeline.py --mode rest",
    )

    # Set task dependencies
    run_pipeline
