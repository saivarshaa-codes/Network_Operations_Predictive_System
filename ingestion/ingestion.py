from __future__ import annotations

import csv
import shutil
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

LANDING_DIR = PROJECT_ROOT / "data" / "landing"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
REFERENCE_DIR = PROJECT_ROOT / "data" / "reference"
LOG_DIR = PROJECT_ROOT / "logs"

AUDIT_LOG = LOG_DIR / "ingestion_log.csv"

FILE_PATTERN = "sms-call-internet-mi-*.csv"

REQUIRED_COLUMNS = {
    "datetime",
    "CellID",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
}

ACTIVITY_COLUMNS = [
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]


# ============================================================
# 1. DETECT
# ============================================================

def detect_files(landing_dir: Path = LANDING_DIR) -> list[Path]:
    """
    Detect daily Milan telecom CSV files in the landing zone.

    Only files matching sms-call-internet-mi-*.csv are considered.
    Static reference files are not part of this flow.
    """
    
    landing_dir = Path(landing_dir)

    if not landing_dir.exists():
        return []

    return sorted(
        path
        for path in landing_dir.glob(FILE_PATTERN)
        if path.is_file()
    )


# ============================================================
# 2. VALIDATE SCHEMA
# ============================================================

def validate_schema(file_path: Path) -> tuple[bool, str]:
    """
    Validate that the daily CSV contains all required raw columns.
    """
    file_path = Path(file_path)

    try:
        with file_path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.reader(file)
            header = next(reader, [])

    except Exception as exc:
        return False, f"Unable to read file: {exc}"

    columns = {column.strip() for column in header}
    missing = REQUIRED_COLUMNS - columns

    if missing:
        return False, (
            "SCHEMA_VALIDATION_FAILED: "
            f"missing required columns: {', '.join(sorted(missing))}"
        )

    return True, "schema_valid"


# ============================================================
# 3. VALIDATE MINIMUM QUALITY
# ============================================================

def validate_minimum_quality(file_path: Path) -> tuple[bool, int, str]:
    """
    Validate minimum data quality.

    Checks:
    - file contains data rows
    - datetime can be parsed
    - CellID is present and within 1-10000
    - activity values are non-negative
    """
    file_path = Path(file_path)

    row_count = 0

    try:
        with file_path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)

            for row_number, row in enumerate(reader, start=2):
                row_count += 1

                # Timestamp validation
                timestamp = row.get("datetime", "").strip()

                try:
                    datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return (
                        False,
                        row_count,
                        f"QUALITY_VALIDATION_FAILED: malformed datetime "
                        f"at row {row_number}: {timestamp}",
                    )

                # Grid validation
                cell_id = row.get("CellID", "").strip()

                if not cell_id:
                    return (
                        False,
                        row_count,
                        f"QUALITY_VALIDATION_FAILED: missing CellID "
                        f"at row {row_number}",
                    )

                try:
                    cell_id_value = int(cell_id)
                except ValueError:
                    return (
                        False,
                        row_count,
                        f"QUALITY_VALIDATION_FAILED: invalid CellID "
                        f"at row {row_number}: {cell_id}",
                    )

                if not 1 <= cell_id_value <= 10000:
                    return (
                        False,
                        row_count,
                        f"QUALITY_VALIDATION_FAILED: CellID outside "
                        f"1-10000 at row {row_number}: {cell_id_value}",
                    )

                # Activity validation
                for column in ACTIVITY_COLUMNS:
                    value = row.get(column, "").strip()

                    # Blank activity is allowed at this stage.
                    if value == "":
                        continue

                    try:
                        numeric_value = float(value)
                    except ValueError:
                        return (
                            False,
                            row_count,
                            f"QUALITY_VALIDATION_FAILED: non-numeric "
                            f"{column} at row {row_number}: {value}",
                        )

                    if numeric_value < 0:
                        return (
                            False,
                            row_count,
                            f"QUALITY_VALIDATION_FAILED: negative "
                            f"{column} at row {row_number}: {numeric_value}",
                        )

    except Exception as exc:
        return (
            False,
            row_count,
            f"QUALITY_VALIDATION_FAILED: unable to read file: {exc}",
        )

    if row_count == 0:
        return False, 0, "QUALITY_VALIDATION_FAILED: file contains no data rows"

    return True, row_count, "quality_valid"


# ============================================================
# 4. ROUTE
# ============================================================

def route_file(
    file_path: Path,
    valid: bool,
    reason: str,
    row_count: int,
) -> dict:
    """
    Route a validated file to raw or rejected.

    Re-running an already-routed file is logged as SKIPPED rather
    than silently duplicating it.
    """
    file_path = Path(file_path)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if valid:
        destination = RAW_DIR / file_path.name
        status = "ACCEPTED"
    else:
        destination = REJECTED_DIR / file_path.name
        status = "REJECTED"

    # Explicit rerun/idempotency behaviour.
    if destination.exists():
        status = "SKIPPED"
        final_reason = f"Already processed; existing file: {destination}"
    else:
        shutil.copy2(file_path, destination)
        final_reason = reason

    return {
        "filename": file_path.name,
        "status": status,
        "row_count": row_count,
        "reason": final_reason,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# 5. AUDIT LOG
# ============================================================

def write_audit_record(record: dict) -> None:
    """
    Append one machine-readable ingestion audit record.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    fields = [
        "filename",
        "status",
        "row_count",
        "reason",
        "processed_at",
    ]

    file_exists = AUDIT_LOG.exists()

    with AUDIT_LOG.open(
        "a",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(file, fieldnames=fields)

        if not file_exists:
            writer.writeheader()

        writer.writerow(record)


# ============================================================
# 6. PROCESS LANDING FILES
# ============================================================

def process_landing_files() -> list[dict]:
    """
    Execute the complete DE2 ingestion flow:

        detect → schema validation
               → quality validation
               → route
               → audit log
    """
    results = []

    for file_path in detect_files():

        schema_valid, schema_reason = validate_schema(file_path)

        if not schema_valid:
            result = route_file(
                file_path=file_path,
                valid=False,
                reason=schema_reason,
                row_count=0,
            )

            write_audit_record(result)
            results.append(result)
            continue

        quality_valid, row_count, quality_reason = (
            validate_minimum_quality(file_path)
        )

        result = route_file(
            file_path=file_path,
            valid=quality_valid,
            reason=quality_reason,
            row_count=row_count,
        )

        write_audit_record(result)
        results.append(result)

    return results


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    results = process_landing_files()

    for result in results:
        print(result)