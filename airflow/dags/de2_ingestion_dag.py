from datetime import datetime
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(
    0,
    str(PROJECT_ROOT),
)

from ingestion.ingestion import (
    detect_files,
    process_file,
    write_ingestion_log,
)


LANDING_DIR = PROJECT_ROOT / "data" / "landing"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
LOG_PATH = PROJECT_ROOT / "logs" / "ingestion_log.csv"


# ============================================================
# TASK 1 — DETECT
# ============================================================

def detect_task(**context):

    files = detect_files(LANDING_DIR)

    if not files:
        raise FileNotFoundError(
            "No daily telecom CSV files found in landing zone."
        )

    file_names = [
        str(file)
        for file in files
    ]

    context["ti"].xcom_push(
        key="detected_files",
        value=file_names,
    )


# ============================================================
# TASK 2 — VALIDATE
# ============================================================

def validate_task(**context):

    files = context["ti"].xcom_pull(
        task_ids="detect",
        key="detected_files",
    )

    records = []

    for file_path in files:

        record = process_file(
            file_path,
            RAW_DIR,
            REJECTED_DIR,
        )

        records.append(record)

    context["ti"].xcom_push(
        key="ingestion_records",
        value=records,
    )


# ============================================================
# TASK 3 — ROUTE
# ============================================================

def route_task(**context):

    records = context["ti"].xcom_pull(
        task_ids="validate",
        key="ingestion_records",
    )

    if not records:
        raise RuntimeError(
            "No ingestion records were produced."
        )

    for record in records:

        print(
            f"{record['filename']} -> "
            f"{record['status']} | "
            f"{record['reason']}"
        )


# ============================================================
# TASK 4 — LOG
# ============================================================

def log_task(**context):

    records = context["ti"].xcom_pull(
        task_ids="validate",
        key="ingestion_records",
    )

    write_ingestion_log(
        records,
        LOG_PATH,
    )

    for record in records:
        print(
            f"AUDIT | "
            f"{record['filename']} | "
            f"{record['status']} | "
            f"{record['row_count']} | "
            f"{record['reason']}"
        )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="de2_ingestion_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["DE2", "ingestion"],
) as dag:

    detect = PythonOperator(
        task_id="detect",
        python_callable=detect_task,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
    )

    route = PythonOperator(
        task_id="route",
        python_callable=route_task,
    )

    log = PythonOperator(
        task_id="log",
        python_callable=log_task,
    )

    detect >> validate >> route >> log