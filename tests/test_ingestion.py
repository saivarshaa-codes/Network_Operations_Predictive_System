from pathlib import Path

import pandas as pd

from ingestion.ingestion import (
    detect_files,
    validate_schema,
    validate_minimum_quality,
    route_file,
)


def create_valid_file(path: Path):

    df = pd.DataFrame(
        {
            "Timestamp": [
                "2013-11-01 00:00:00",
                "2013-11-01 01:00:00",
            ],
            "Grid": [1, 2],
            "CountryCode": [39, 39],
            "SMSIn": [10.0, 20.0],
            "SMSOut": [5.0, 10.0],
            "CallIn": [2.0, 3.0],
            "CallOut": [1.0, 2.0],
            "Internet": [50.0, 60.0],
        }
    )

    df.to_csv(path, index=False)


def create_invalid_file(path: Path):

    df = pd.DataFrame(
        {
            "Timestamp": [
                "2013-11-01 00:00:00",
                "2013-11-01 01:00:00",
            ],
            "Grid": [1, 2],
            "CountryCode": [39, 39],
            "SMSIn": [-10.0, 20.0],
            "SMSOut": [5.0, 10.0],
            "CallIn": [2.0, 3.0],
            "CallOut": [1.0, 2.0],
            "Internet": [50.0, 60.0],
        }
    )

    df.to_csv(path, index=False)


def test_detect_files(tmp_path):

    landing = tmp_path / "landing"
    landing.mkdir()

    valid = landing / "sms-call-internet-mi-2013-11-01.csv"

    create_valid_file(valid)

    files = detect_files(landing)

    assert len(files) == 1
    assert files[0].name == valid.name


def test_valid_file_passes_validation(tmp_path):

    file_path = tmp_path / "valid.csv"

    create_valid_file(file_path)

    valid_schema, _ = validate_schema(file_path)

    valid_quality, row_count, _ = (
        validate_minimum_quality(file_path)
    )

    assert valid_schema is True
    assert valid_quality is True
    assert row_count == 2


def test_negative_activity_is_rejected(tmp_path):

    file_path = tmp_path / "invalid.csv"

    create_invalid_file(file_path)

    valid, row_count, reason = (
        validate_minimum_quality(file_path)
    )

    assert valid is False
    assert row_count == 2
    assert "negative activity" in reason


def test_invalid_file_is_routed_to_rejected(tmp_path):

    landing = tmp_path / "landing"
    raw = tmp_path / "raw"
    rejected = tmp_path / "rejected"

    landing.mkdir()
    raw.mkdir()
    rejected.mkdir()

    invalid = landing / "sms-call-internet-mi-invalid.csv"

    create_invalid_file(invalid)

    destination = route_file(
        invalid,
        "INVALID",
        "negative activity",
        raw,
        rejected,
    )

    assert destination == "REJECTED"
    assert (rejected / invalid.name).exists()


def test_already_processed_file_is_skipped(tmp_path):

    landing = tmp_path / "landing"
    raw = tmp_path / "raw"
    rejected = tmp_path / "rejected"

    landing.mkdir()
    raw.mkdir()
    rejected.mkdir()

    valid = landing / "sms-call-internet-mi-2013-11-01.csv"

    create_valid_file(valid)

    # Simulate previous successful processing.
    (raw / valid.name).write_text(
        valid.read_text()
    )

    destination = route_file(
        valid,
        "VALID",
        "Validation passed",
        raw,
        rejected,
    )

    assert destination == "SKIPPED_ALREADY_PROCESSED"