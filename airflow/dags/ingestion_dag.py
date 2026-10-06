from datetime import datetime
import sys
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.ingestion import process_landing_files

def run_ingestion():
    results = process_landing_files()
    for result in results:
        print(result)

with DAG(
    dag_id="de2_landing_to_raw_ingestion",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["DE2", "ingestion"],
) as dag:

    ingest_files = PythonOperator(
        task_id="detect_validate_route_log",
        python_callable=run_ingestion,
    )


