from pathlib import Path
import csv
import tempfile
import unittest

from ingestion.ingestion import (
    detect_files,
    validate_schema,
    validate_minimum_quality,
    route_file,
)


class TestIngestion(unittest.TestCase):

    def create_csv(self, directory, filename, columns, rows):
        """Create a temporary CSV file for testing."""
        file_path = Path(directory) / filename

        with file_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

        return file_path

    def valid_row(self):
        """Return one valid telecom activity record."""
        return {
            "datetime": "2013-11-01 00:00:00",
            "CellID": "1",
            "countrycode": "39",
            "smsin": "10",
            "smsout": "5",
            "callin": "3",
            "callout": "2",
            "internet": "100",
        }

    # --------------------------------------------------------
    # 1. DETECTION
    # --------------------------------------------------------

    def test_detect_files(self):
        """Only daily activity CSVs matching the required pattern are detected."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            self.create_csv(
                temp_path,
                "sms-call-internet-mi-2013-11-01.csv",
                list(self.valid_row().keys()),
                [self.valid_row()],
            )

            self.create_csv(
                temp_path,
                "sms-call-internet-mi-2013-11-02.txt",
                list(self.valid_row().keys()),
                [self.valid_row()],
            )

            detected = detect_files(temp_path)

            self.assertEqual(len(detected), 1)
            self.assertEqual(
                detected[0].name,
                "sms-call-internet-mi-2013-11-01.csv",
            )

    # --------------------------------------------------------
    # 2. VALID FILE — SCHEMA
    # --------------------------------------------------------

    def test_valid_file_passes_schema(self):
        """A file containing all required columns passes schema validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = self.create_csv(
                temp_dir,
                "valid.csv",
                list(self.valid_row().keys()),
                [self.valid_row()],
            )

            valid, reason = validate_schema(file_path)

            self.assertTrue(valid)
            self.assertEqual(reason, "schema_valid")

    # --------------------------------------------------------
    # 3. INVALID FILE — MISSING INTERNET COLUMN
    # --------------------------------------------------------

    def test_invalid_file_fails_schema(self):
        """A file missing the internet column is rejected by schema validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_columns = [
                "datetime",
                "CellID",
                "countrycode",
                "smsin",
                "smsout",
                "callin",
                "callout",
            ]

            invalid_row = {
                key: value
                for key, value in self.valid_row().items()
                if key != "internet"
            }

            file_path = self.create_csv(
                temp_dir,
                "invalid.csv",
                invalid_columns,
                [invalid_row],
            )

            valid, reason = validate_schema(file_path)

            self.assertFalse(valid)
            self.assertIn("SCHEMA_VALIDATION_FAILED", reason)
            self.assertIn("internet", reason)

    # --------------------------------------------------------
    # 4. VALID FILE — QUALITY
    # --------------------------------------------------------

    def test_valid_file_passes_quality(self):
        """A valid telecom record passes minimum quality validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = self.create_csv(
                temp_dir,
                "valid.csv",
                list(self.valid_row().keys()),
                [self.valid_row()],
            )

            valid, row_count, reason = validate_minimum_quality(file_path)

            self.assertTrue(valid)
            self.assertEqual(row_count, 1)
            self.assertEqual(reason, "quality_valid")

    # --------------------------------------------------------
    # 5. VALID FILE — ROUTING
    # --------------------------------------------------------

    def test_valid_file_routes_to_raw(self):
        """A valid file is routed to the raw directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            source = self.create_csv(
                temp_path,
                "valid.csv",
                list(self.valid_row().keys()),
                [self.valid_row()],
            )

            import ingestion.ingestion as ingestion_module

            original_raw = ingestion_module.RAW_DIR
            original_rejected = ingestion_module.REJECTED_DIR
            original_log = ingestion_module.LOG_DIR

            try:
                ingestion_module.RAW_DIR = temp_path / "raw"
                ingestion_module.REJECTED_DIR = temp_path / "rejected"
                ingestion_module.LOG_DIR = temp_path / "logs"

                result = route_file(
                    source,
                    valid=True,
                    reason="quality_valid",
                    row_count=1,
                )

                self.assertEqual(result["status"], "ACCEPTED")
                self.assertTrue(
                    (temp_path / "raw" / "valid.csv").exists()
                )

            finally:
                ingestion_module.RAW_DIR = original_raw
                ingestion_module.REJECTED_DIR = original_rejected
                ingestion_module.LOG_DIR = original_log

    # --------------------------------------------------------
    # 6. INVALID FILE — ROUTING
    # --------------------------------------------------------

    def test_invalid_file_routes_to_rejected(self):
        """An invalid file is routed to the rejected directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            source = self.create_csv(
                temp_path,
                "invalid.csv",
                ["datetime", "CellID"],
                [{
                    "datetime": "2013-11-01 00:00:00",
                    "CellID": "1",
                }],
            )

            import ingestion.ingestion as ingestion_module

            original_raw = ingestion_module.RAW_DIR
            original_rejected = ingestion_module.REJECTED_DIR
            original_log = ingestion_module.LOG_DIR

            try:
                ingestion_module.RAW_DIR = temp_path / "raw"
                ingestion_module.REJECTED_DIR = temp_path / "rejected"
                ingestion_module.LOG_DIR = temp_path / "logs"

                reason = (
                    "SCHEMA_VALIDATION_FAILED: "
                    "missing required columns: internet"
                )

                result = route_file(
                    source,
                    valid=False,
                    reason=reason,
                    row_count=0,
                )

                self.assertEqual(result["status"], "REJECTED")
                self.assertIn("internet", result["reason"])
                self.assertTrue(
                    (temp_path / "rejected" / "invalid.csv").exists()
                )

            finally:
                ingestion_module.RAW_DIR = original_raw
                ingestion_module.REJECTED_DIR = original_rejected
                ingestion_module.LOG_DIR = original_log

    # --------------------------------------------------------
    # 7. RERUN / IDEMPOTENCY
    # --------------------------------------------------------

    def test_rerun_existing_file_is_skipped(self):
        """Re-running an already routed file is logged as SKIPPED."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            source = self.create_csv(
                temp_path,
                "valid.csv",
                list(self.valid_row().keys()),
                [self.valid_row()],
            )

            import ingestion.ingestion as ingestion_module

            original_raw = ingestion_module.RAW_DIR
            original_rejected = ingestion_module.REJECTED_DIR
            original_log = ingestion_module.LOG_DIR

            try:
                ingestion_module.RAW_DIR = temp_path / "raw"
                ingestion_module.REJECTED_DIR = temp_path / "rejected"
                ingestion_module.LOG_DIR = temp_path / "logs"

                first_result = route_file(
                    source,
                    valid=True,
                    reason="quality_valid",
                    row_count=1,
                )

                second_result = route_file(
                    source,
                    valid=True,
                    reason="quality_valid",
                    row_count=1,
                )

                self.assertEqual(first_result["status"], "ACCEPTED")
                self.assertEqual(second_result["status"], "SKIPPED")
                self.assertIn(
                    "Already processed",
                    second_result["reason"],
                )

            finally:
                ingestion_module.RAW_DIR = original_raw
                ingestion_module.REJECTED_DIR = original_rejected
                ingestion_module.LOG_DIR = original_log


if __name__ == "__main__":
    unittest.main()

