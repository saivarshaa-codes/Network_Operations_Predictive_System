from __future__ import annotations

import csv
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

REQUIRED_COLUMNS = {
    "Timestamp",
    "Grid",
    "CountryCode",
    "SMSIn",
    "SMSOut",
    "CallIn",
    "CallOut",
    "Internet",
}

ACTIVITY_COLUMNS = [
    "SMSIn",
    "SMSOut",
    "CallIn",
    "CallOut",
    "Internet",
]

FILE_PATTERN = "sms-call-internet-mi-*.csv"


# ============================================================
# 1. DETECT
# ============================================================

def detect_files(landing_dir: str | Path) -> list[Path]:
    """
    Detect daily Milan telecom CSV files in the landing zone.

    Only files matching sms-call-internet-mi-*.csv are considered.
    """

    landing = Path(landing_dir)

    if not landing.exists():
        raise FileNotFoundError(
            f"Landing directory does not exist: {landing}"
        )

    return sorted(
        file
        for file in landing.glob(FILE_PATTERN)
        if file.is_file()
    )


# ============================================================
# 2. SCHEMA VALIDATION
# ============================================================

def validate_schema(file_path: str | Path) -> tuple[bool, str]:
    """
    Validate that the daily CSV contains all required raw columns.
    """

    file_path = Path(file_path)

    try:
        df = pd.read_csv(file_path, nrows=0)
    except Exception as exc:
        return False, f"Unable to read CSV: {exc}"

    columns = set(df.columns)

    missing_columns = REQUIRED_COLUMNS - columns

    if missing_columns:
        return (
            False,
            "Schema validation failed: missing columns: "
            + ", ".join(sorted(missing_columns)),
        )

    return True, "Schema validation passed"


# ============================================================
# 3. MINIMUM QUALITY VALIDATION
# ============================================================

def validate_minimum_quality(
    file_path: str | Path,
) -> tuple[bool, int, str]:
    """
    Validate minimum quality requirements.

    Checks:
    - file can be read
    - file is not empty
    - timestamps are valid
    - activity values are numeric
    - activity values are non-negative
    - grid IDs are within 1-10000
    """

    file_path = Path(file_path)

    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        return False, 0, f"Quality validation failed: unable to read CSV: {exc}"

    row_count = len(df)

    if row_count == 0:
        return False, row_count, "Quality validation failed: file is empty"

    # Timestamp validation
    timestamps = pd.to_datetime(
        df["Timestamp"],
        errors="coerce",
    )

    invalid_timestamps = timestamps.isna().sum()

    if invalid_timestamps > 0:
        return (
            False,
            row_count,
            "Quality validation failed: "
            f"{invalid_timestamps} malformed timestamp value(s)",
        )

    # Activity validation
    for column in ACTIVITY_COLUMNS:

        numeric_values = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        invalid_numeric = numeric_values.isna().sum()

        if invalid_numeric > 0:
            return (
                False,
                row_count,
                "Quality validation failed: "
                f"non-numeric values found in {column}",
            )

        negative_values = (numeric_values < 0).sum()

        if negative_values > 0:
            return (
                False,
                row_count,
                "Quality validation failed: "
                f"negative activity value(s) found in {column}",
            )

    # Grid ID validation
    grid_ids = pd.to_numeric(
        df["Grid"],
        errors="coerce",
    )

    invalid_grid_ids = (
        grid_ids.isna()
        | (grid_ids < 1)
        | (grid_ids > 10000)
    ).sum()

    if invalid_grid_ids > 0:
        return (
            False,
            row_count,
            "Quality validation failed: "
            f"{invalid_grid_ids} invalid Grid value(s); expected 1-10000",
        )

    return True, row_count, "Minimum quality validation passed"


# ============================================================
# 4. ROUTE
# ============================================================

def route_file(
    file_path: str | Path,
    status: str,
    reason: str,
    raw_dir: str | Path,
    rejected_dir: str | Path,
) -> str:
    """
    Route a validated file to raw or an invalid file to rejected.

    Already processed files are skipped rather than duplicated.
    """

    file_path = Path(file_path)
    raw_dir = Path(raw_dir)
    rejected_dir = Path(rejected_dir)

    raw_dir.mkdir(parents=True, exist_ok=True)
    rejected_dir.mkdir(parents=True, exist_ok=True)

    raw_target = raw_dir / file_path.name
    rejected_target = rejected_dir / file_path.name

    # Prevent duplicate routing on DAG re-runs.
    if raw_target.exists() or rejected_target.exists():
        return "SKIPPED_ALREADY_PROCESSED"

    if status == "VALID":
        shutil.copy2(file_path, raw_target)
        return "RAW"

    shutil.copy2(file_path, rejected_target)
    return "REJECTED"


# ============================================================
# 5. PROCESS ONE FILE
# ============================================================

def process_file(
    file_path: str | Path,
    raw_dir: str | Path,
    rejected_dir: str | Path,
) -> dict:
    """
    Validate and route one incoming file.
    """

    file_path = Path(file_path)

    # Schema check
    schema_valid, schema_reason = validate_schema(file_path)

    if not schema_valid:
        destination = route_file(
            file_path,
            "INVALID",
            schema_reason,
            raw_dir,
            rejected_dir,
        )

        return {
            "filename": file_path.name,
            "status": (
                "REJECTED"
                if destination == "REJECTED"
                else destination
            ),
            "row_count": 0,
            "reason": schema_reason,
        }

    # Minimum quality check
    quality_valid, row_count, quality_reason = (
        validate_minimum_quality(file_path)
    )

    if not quality_valid:
        destination = route_file(
            file_path,
            "INVALID",
            quality_reason,
            raw_dir,
            rejected_dir,
        )

        return {
            "filename": file_path.name,
            "status": (
                "REJECTED"
                if destination == "REJECTED"
                else destination
            ),
            "row_count": row_count,
            "reason": quality_reason,
        }

    # Valid file
    destination = route_file(
        file_path,
        "VALID",
        "Validation passed",
        raw_dir,
        rejected_dir,
    )

    return {
        "filename": file_path.name,
        "status": (
            "ACCEPTED"
            if destination == "RAW"
            else destination
        ),
        "row_count": row_count,
        "reason": quality_reason,
    }


# ============================================================
# 6. AUDIT LOG
# ============================================================

def write_ingestion_log(
    records: list[dict],
    log_path: str | Path,
) -> None:
    """
    Write one audit record for every file processed.
    """

    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "filename",
        "status",
        "row_count",
        "reason",
        "processed_at",
    ]

    file_exists = log_path.exists()

    with log_path.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        if not file_exists:
            writer.writeheader()

        processed_at = datetime.now().isoformat()

        for record in records:
            writer.writerow(
                {
                    **record,
                    "processed_at": processed_at,
                }
            )


# ============================================================
# 7. COMPLETE INGESTION RUN
# ============================================================

def run_ingestion(
    landing_dir: str | Path,
    raw_dir: str | Path,
    rejected_dir: str | Path,
    log_path: str | Path,
) -> list[dict]:
    """
    Execute detect -> validate -> route -> log.
    """

    files = detect_files(landing_dir)

    if not files:
        raise FileNotFoundError(
            "No daily files found matching "
            f"{FILE_PATTERN}"
        )

    records = []

    for file_path in files:

        record = process_file(
            file_path,
            raw_dir,
            rejected_dir,
        )

        records.append(record)

    write_ingestion_log(
        records,
        log_path,
    )

    return records