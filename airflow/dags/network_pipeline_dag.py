from __future__ import annotations

import csv
import json
import logging
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException



PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
from ingestion import ingestion

LANDING_DIR = PROJECT_ROOT / "data" / "landing"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
REFERENCE_PATH = PROJECT_ROOT / "data" / "reference" / "milano-grid.geojson"

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
WAREHOUSE_DB = PROJECT_ROOT / "data" / "warehouse" / "network_ops.db"

INGESTION_SCRIPT = PROJECT_ROOT / "ingestion" / "ingestion.py"
SPARK_SCRIPT = PROJECT_ROOT / "spark" / "telecom_pipeline.py"
WAREHOUSE_SCRIPT = PROJECT_ROOT / "warehouse" / "build_warehouse.py"
ML2_SCRIPT = PROJECT_ROOT / "ML" / "ML2" / "feature_engineering.py"
ML6_SCRIPT = PROJECT_ROOT / "ML" / "ML6" / "batch_score.py"

FEATURE_TABLE = "network_feature_table"
RISK_TABLE = "network_risk_scores"

LOG_DIR = PROJECT_ROOT / "logs"
STATUS_PATH = LOG_DIR / "pipeline_status.json"

logger = logging.getLogger("network_pipeline")


def write_failure_status(context):
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    dag_run = context.get("dag_run")
    failed_task = context.get("task_instance") or context.get("ti")

    run_id = str(dag_run.run_id) if dag_run else "unknown"

    failed_task_id = (
        failed_task.task_id
        if failed_task
        else "unknown"
    )

    task_ids = [
        "ingest",
        "validate",
        "spark_process",
        "load_warehouse",
        "ml2_features",
        "ml6_batch_score",
        "quality_check",
        "notify",
    ]

    per_task_status = {
        task_id: "NOT_RUN"
        for task_id in task_ids
    }

    if failed_task_id in task_ids:
        failed_index = task_ids.index(failed_task_id)

        for task_id in task_ids[:failed_index]:
            per_task_status[task_id] = "SUCCESS"

        per_task_status[failed_task_id] = "FAILED"

    rows_published = 0
    as_of = None
    feature_rows = 0
    risk_rows = 0

    if WAREHOUSE_DB.exists():
        try:
            with sqlite3.connect(WAREHOUSE_DB) as conn:
                rows_published = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_network_activity
                    """
                ).fetchone()[0]

                as_of = conn.execute(
                    """
                    SELECT MAX(timestamp)
                    FROM dim_time
                    """
                ).fetchone()[0]

                feature_rows = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM sqlite_master
                    WHERE type = 'table'
                    AND name = 'network_feature_table'
                    """
                ).fetchone()[0]

                if feature_rows:
                    feature_rows = conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM network_feature_table
                        """
                    ).fetchone()[0]

                risk_rows = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM sqlite_master
                    WHERE type = 'table'
                    AND name = 'network_risk_scores'
                    """
                ).fetchone()[0]

                if risk_rows:
                    risk_rows = conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM network_risk_scores
                        """
                    ).fetchone()[0]

        except sqlite3.Error as exc:
            logger.warning(
                "Could not read warehouse evidence: %s",
                exc,
            )

    status = {
        "run_id": run_id,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "FAILED",
        "failed_task": failed_task_id,
        "error": str(
            context.get("exception")
            or context.get("reason")
            or "Unknown failure"
        ),
        "per_task_status": per_task_status,
        "rows_in": 0,
        "rows_rejected": 0,
        "nulls_handled": 0,
        "rows_published": rows_published,
        "AS_OF": as_of,
        "ml2_feature_rows": feature_rows,
        "ml6_risk_rows": risk_rows,
        "warehouse": {
            "database": str(WAREHOUSE_DB),
        },
    }

    STATUS_PATH.write_text(
        json.dumps(
            status,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.error(
        "PIPELINE_STATUS=FAILED run_id=%s failed_task=%s status_file=%s",
        run_id,
        failed_task_id,
        STATUS_PATH,
    )


@dag(
    dag_id="network_operations_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["network", "telecom", "de7", "ml"],
    description="End-to-end network operations pipeline with ML feature generation and batch risk scoring",
    on_failure_callback=write_failure_status,
)
def network_operations_pipeline():

    @task(task_id="ingest")
    def ingest():
        if not LANDING_DIR.exists():
            raise AirflowException(
                f"Landing directory does not exist: {LANDING_DIR}"
            )

        files = sorted(
            LANDING_DIR.glob("sms-call-internet-mi-*.csv")
        )

        if not files:
            raise AirflowException(
                f"No incoming CSV files found in {LANDING_DIR}"
            )

        logger.info(
            "INGEST_FILES=%s",
            len(files),
        )

        sys.path.insert(
            0,
            str(INGESTION_SCRIPT.parent),
        )


        try:
            detected = ingestion.detect_files()
        except TypeError:
            detected = ingestion.detect_files(
                LANDING_DIR
            )

        if not detected:
            raise AirflowException(
                "No files detected by ingestion module."
            )

        logger.info(
            "INGEST_COMPLETE=%s",
            len(detected),
        )

        return len(detected)

    @task(task_id="validate")
    def validate():
        sys.path.insert(
            0,
            str(INGESTION_SCRIPT.parent),
        )

        results = ingestion.process_landing_files()

        validated = sum(
            1
            for result in results
            if result.get("status") == "ACCEPTED"
        )

        rejected = sum(
            1
            for result in results
            if result.get("status") == "REJECTED"
        )

        skipped = sum(
            1
            for result in results
            if result.get("status") == "SKIPPED"
        )

        logger.info(
            "VALIDATION_COMPLETE validated=%s rejected=%s skipped=%s",
            validated,
            rejected,
            skipped,
        )

        if validated == 0 and skipped == 0:
            raise AirflowException(
                "Validation produced no accepted or previously processed files."
            )

        return {
            "validated": validated,
            "rejected": rejected,
            "skipped": skipped,
        }

    @task(
        task_id="spark_process",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def spark_process():
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

        logger.info(
            "SPARK_COMMAND=%s",
            " ".join(command),
        )

        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            text=True,
            capture_output=True,
        )

        if result.stdout:
            logger.info(
                "SPARK_OUTPUT:\n%s",
                result.stdout,
            )

        if result.stderr:
            logger.warning(
                "SPARK_STDERR:\n%s",
                result.stderr,
            )

        if result.returncode != 0:
            raise AirflowException(
                f"SP7 failed with exit code {result.returncode}"
            )

        logger.info(
            "SPARK_PROCESS=SUCCESS"
        )

    @task(
        task_id="load_warehouse",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def load_warehouse():
        command = [
            sys.executable,
            str(WAREHOUSE_SCRIPT),
        ]

        logger.info(
            "WAREHOUSE_COMMAND=%s",
            " ".join(command),
        )

        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            text=True,
            capture_output=True,
        )

        if result.stdout:
            logger.info(
                "WAREHOUSE_OUTPUT:\n%s",
                result.stdout,
            )

        if result.stderr:
            logger.warning(
                "WAREHOUSE_STDERR:\n%s",
                result.stderr,
            )

        if result.returncode != 0:
            raise AirflowException(
                f"Warehouse build failed with exit code "
                f"{result.returncode}"
            )

        if not WAREHOUSE_DB.exists():
            raise AirflowException(
                "Warehouse command succeeded but database was not created."
            )

        logger.info(
            "LOAD_WAREHOUSE=SUCCESS"
        )

        return "SUCCESS"

    @task(task_id="ml2_features")
    def ml2_features():
        if not ML2_SCRIPT.exists():
            raise AirflowException(
                f"ML2 feature engineering script not found: {ML2_SCRIPT}"
            )

        if not WAREHOUSE_DB.exists():
            raise AirflowException(
                "ML2 cannot run because the warehouse database does not exist."
            )

        command = [
            sys.executable,
            str(ML2_SCRIPT),
        ]

        logger.info(
            "ML2_COMMAND=%s",
            " ".join(command),
        )

        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            text=True,
            capture_output=True,
        )

        if result.stdout:
            logger.info(
                "ML2_OUTPUT:\n%s",
                result.stdout,
            )

        if result.stderr:
            logger.warning(
                "ML2_STDERR:\n%s",
                result.stderr,
            )

        if result.returncode != 0:
            raise AirflowException(
                f"ML2 feature engineering failed with exit code "
                f"{result.returncode}"
            )

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                AND name = ?
                """,
                (FEATURE_TABLE,),
            ).fetchone()[0]

            if not exists:
                raise AirflowException(
                    "ML2 completed successfully but "
                    f"{FEATURE_TABLE} was not created."
                )

            row_count = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {FEATURE_TABLE}
                """
            ).fetchone()[0]

        if row_count == 0:
            raise AirflowException(
                f"ML2 completed but {FEATURE_TABLE} contains zero rows."
            )

        logger.info(
            "ML2_FEATURES=SUCCESS rows=%s",
            row_count,
        )

        return row_count

    @task(task_id="ml6_batch_score")
    def ml6_batch_score():
        if not ML6_SCRIPT.exists():
            raise AirflowException(
                f"ML6 batch scoring script not found: {ML6_SCRIPT}"
            )

        if not WAREHOUSE_DB.exists():
            raise AirflowException(
                "ML6 cannot run because the warehouse database does not exist."
            )

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            feature_exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                AND name = ?
                """,
                (FEATURE_TABLE,),
            ).fetchone()[0]

            if not feature_exists:
                raise AirflowException(
                    "ML6 cannot run because "
                    f"{FEATURE_TABLE} does not exist."
                )

            feature_rows = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {FEATURE_TABLE}
                """
            ).fetchone()[0]

            if feature_rows == 0:
                raise AirflowException(
                    f"ML6 cannot run because {FEATURE_TABLE} is empty."
                )

        command = [
            sys.executable,
            str(ML6_SCRIPT),
        ]

        logger.info(
            "ML6_COMMAND=%s",
            " ".join(command),
        )

        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            text=True,
            capture_output=True,
        )

        if result.stdout:
            logger.info(
                "ML6_OUTPUT:\n%s",
                result.stdout,
            )

        if result.stderr:
            logger.warning(
                "ML6_STDERR:\n%s",
                result.stderr,
            )

        if result.returncode != 0:
            raise AirflowException(
                f"ML6 batch scoring failed with exit code "
                f"{result.returncode}"
            )

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            risk_exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                AND name = ?
                """,
                (RISK_TABLE,),
            ).fetchone()[0]

            if not risk_exists:
                raise AirflowException(
                    "ML6 completed successfully but "
                    f"{RISK_TABLE} was not created."
                )

            risk_rows = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {RISK_TABLE}
                """
            ).fetchone()[0]

            duplicate_rows = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT grid_id, timestamp
                    FROM {RISK_TABLE}
                    GROUP BY grid_id, timestamp
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]

            invalid_scores = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {RISK_TABLE}
                WHERE risk_score < 0
                   OR risk_score > 1
                """
            ).fetchone()[0]

            invalid_levels = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {RISK_TABLE}
                WHERE risk_level NOT IN ('LOW', 'MEDIUM', 'HIGH')
                """
            ).fetchone()[0]

            invalid_versions = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {RISK_TABLE}
                WHERE model_version != 'ML3-v1'
                """
            ).fetchone()[0]

        if risk_rows == 0:
            raise AirflowException(
                f"ML6 completed but {RISK_TABLE} contains zero rows."
            )

        if duplicate_rows != 0:
            raise AirflowException(
                f"ML6 validation failed: {duplicate_rows} duplicate risk rows."
            )

        if invalid_scores != 0:
            raise AirflowException(
                f"ML6 validation failed: {invalid_scores} invalid risk scores."
            )

        if invalid_levels != 0:
            raise AirflowException(
                f"ML6 validation failed: {invalid_levels} invalid risk levels."
            )

        if invalid_versions != 0:
            raise AirflowException(
                f"ML6 validation failed: {invalid_versions} invalid model versions."
            )

        report_path = (
            PROJECT_ROOT
            / "ML"
            / "ML6"
            / "top20_operational_attention_report.md"
        )

        if not report_path.exists():
            raise AirflowException(
                "ML6 completed but the top-20 operational attention "
                "report was not created."
            )

        logger.info(
            "ML6_BATCH_SCORE=SUCCESS rows=%s",
            risk_rows,
        )

        return risk_rows

    @task(task_id="quality_check")
    def quality_check():
        LOG_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        run_id = (
            f"manual__"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        )

        run_timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        if not WAREHOUSE_DB.exists():
            raise AirflowException(
                f"Warehouse database not found: {WAREHOUSE_DB}"
            )

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            rows_published = conn.execute(
                """
                SELECT COUNT(*)
                FROM fact_network_activity
                """
            ).fetchone()[0]

            as_of = conn.execute(
                """
                SELECT MAX(timestamp)
                FROM dim_time
                """
            ).fetchone()[0]

            grid_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM dim_grid
                """
            ).fetchone()[0]

            duplicate_facts = conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT time_key, grid_key
                    FROM fact_network_activity
                    GROUP BY time_key, grid_key
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]

            feature_exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                AND name = ?
                """,
                (FEATURE_TABLE,),
            ).fetchone()[0]

            risk_exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                AND name = ?
                """,
                (RISK_TABLE,),
            ).fetchone()[0]

            feature_rows = (
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {FEATURE_TABLE}
                    """
                ).fetchone()[0]
                if feature_exists
                else 0
            )

            risk_rows = (
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {RISK_TABLE}
                    """
                ).fetchone()[0]
                if risk_exists
                else 0
            )

        if duplicate_facts != 0:
            raise AirflowException(
                f"Quality check failed: {duplicate_facts} "
                "duplicate fact keys."
            )

        if rows_published == 0:
            raise AirflowException(
                "Quality check failed: zero published rows."
            )

        if as_of is None:
            raise AirflowException(
                "Quality check failed: AS_OF is NULL."
            )

        if not feature_exists or feature_rows == 0:
            raise AirflowException(
                "Quality check failed: ML2 feature table is missing or empty."
            )

        if not risk_exists or risk_rows == 0:
            raise AirflowException(
                "Quality check failed: ML6 risk table is missing or empty."
            )

        rows_in = 0

        for csv_file in RAW_DIR.glob(
            "sms-call-internet-mi-*.csv"
        ):
            with open(
                csv_file,
                "r",
                encoding="utf-8",
                newline="",
            ) as f:
                reader = csv.reader(f)
                next(reader, None)
                rows_in += sum(
                    1
                    for _ in reader
                )

        status = {
            "run_id": run_id,
            "run_timestamp": run_timestamp,
            "status": "SUCCESS",
            "per_task_status": {
                "ingest": "SUCCESS",
                "validate": "SUCCESS",
                "spark_process": "SUCCESS",
                "load_warehouse": "SUCCESS",
                "ml2_features": "SUCCESS",
                "ml6_batch_score": "SUCCESS",
                "quality_check": "SUCCESS",
            },
            "rows_in": rows_in,
            "rows_rejected": 0,
            "nulls_handled": 0,
            "rows_published": rows_published,
            "AS_OF": as_of,
            "ml2_feature_rows": feature_rows,
            "ml6_risk_rows": risk_rows,
            "warehouse": {
                "database": str(WAREHOUSE_DB),
                "grid_count": grid_count,
            },
        }

        STATUS_PATH.write_text(
            json.dumps(
                status,
                indent=2,
            ),
            encoding="utf-8",
        )

        logger.info(
            "PIPELINE_STATUS=%s",
            STATUS_PATH,
        )

        return str(STATUS_PATH)

    @task(
        task_id="notify",
        trigger_rule="all_success",
    )
    def notify(status_path: str):
        status = json.loads(
            Path(status_path).read_text(
                encoding="utf-8"
            )
        )

        logger.info(
            "PIPELINE_SUCCESS run_id=%s AS_OF=%s rows_published=%s ml2_rows=%s ml6_rows=%s",
            status["run_id"],
            status["AS_OF"],
            status["rows_published"],
            status["ml2_feature_rows"],
            status["ml6_risk_rows"],
        )

        print(
            "\n"
            "============================================\n"
            "NETWORK PIPELINE SUCCESS\n"
            "============================================\n"
            f"AS_OF: {status['AS_OF']}\n"
            f"Rows published: {status['rows_published']}\n"
            f"ML2 feature rows: {status['ml2_feature_rows']}\n"
            f"ML6 risk rows: {status['ml6_risk_rows']}\n"
            f"Status file: {status_path}\n"
            "============================================"
        )

    ingest_task = ingest()
    validate_task = validate()
    spark_task = spark_process()
    warehouse_task = load_warehouse()
    ml2_task = ml2_features()
    ml6_task = ml6_batch_score()
    quality_task = quality_check()
    notify_task = notify(quality_task)

    (
        ingest_task
        >> validate_task
        >> spark_task
        >> warehouse_task
        >> ml2_task
        >> ml6_task
        >> quality_task
        >> notify_task
    )


network_operations_pipeline()