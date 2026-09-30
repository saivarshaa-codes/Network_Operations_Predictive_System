from datetime import datetime
import subprocess
import sys
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SPARK_SCRIPT = PROJECT_ROOT / "spark" / "telecom_pipeline.py"

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REFERENCE_PATH = PROJECT_ROOT / "data" / "reference" / "milano-grid.geojson"


# ============================================================
# DE3 — SPARK PROCESSING
# ============================================================

def run_spark():
    """
    Launch the reusable PySpark processing job.

    Airflow orchestrates the job.
    All business logic remains inside telecom_pipeline.py.
    """

    print("SPARK_JOB_START")
    print(f"INPUT={RAW_DIR}")
    print(f"OUTPUT={PROCESSED_DIR}")
    print(f"REFERENCE={REFERENCE_PATH}")

    command = [
        sys.executable,
        str(SPARK_SCRIPT),
        "--input",
        str(RAW_DIR),
        "--output",
        str(PROCESSED_DIR),
        "--reference",
        str(REFERENCE_PATH),
    ]

    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        check=False,
    )

    if result.returncode != 0:
        print(
            f"SPARK_JOB_STATUS=FAILED | "
            f"EXIT_CODE={result.returncode}"
        )

        raise RuntimeError(
            f"Spark job failed with exit code "
            f"{result.returncode}"
        )

    print("SPARK_JOB_STATUS=SUCCESS")


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="de3_spark_processing",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["DE3", "spark", "processing"],
) as dag:

    spark_process = PythonOperator(
        task_id="run_spark_processing",
        python_callable=run_spark,
    )