from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAIN = PROJECT_ROOT / "main.py"

default_args = {
    "owner": "finance_admin",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "finance_hub_pipeline",
    default_args=default_args,
    description="FinanceHub: UCI retail ETL to PostgreSQL and Azure SQL",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1_ingest = BashOperator(
    task_id="ingest_data",
    bash_command=f'python "{MAIN}" --step ingest',
    dag=dag,
)

t2_transform = BashOperator(
    task_id="transform_and_analyze",
    bash_command=f'python "{MAIN}" --step transform',
    dag=dag,
)

t3_load = BashOperator(
    task_id="load_to_data_warehouse",
    bash_command=f'python "{MAIN}" --step load',
    dag=dag,
)

t1_ingest >> t2_transform >> t3_load