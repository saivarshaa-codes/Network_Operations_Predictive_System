
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    DoubleType,
)

import os
import sys

from logger_config import get_logger
logger = get_logger("NetworkActivityIngestion")


# =========================================================
# Environment Configuration
# =========================================================

os.environ["HADOOP_HOME"] = (
    r"C:\Users\saivarshaa.sujee\pyspark_demo\hadoop"
)

os.environ["PATH"] = (
    os.environ["PATH"]
    + r";C:\Users\saivarshaa.sujee\pyspark_demo\hadoop\bin"
)

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# =========================================================
# Configuration
# =========================================================

FILE_PATTERN = "sms-call-internet-mi-*.csv"


# =========================================================
# 1. Create SparkSession
# =========================================================

spark = (
    SparkSession.builder
    .appName("NetworkActivityIngestion")
    .getOrCreate()
)

logger.info("SparkSession created successfully")


# =========================================================
# 2. Define schema for the ACTUAL RAW CSV
# =========================================================

RAW_SCHEMA = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])

logger.info("Manual schema defined for raw network activity files")


# =========================================================
# 3. Raw → Canonical column mapping
# =========================================================

COLUMN_MAPPING = {
    "datetime": "timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet_activity",
}

logger.info("Raw-to-canonical column mapping configured")


# =========================================================
# 4. Load and normalize raw network data
# =========================================================

def load_raw_network_data(data_folder):

    logger.info("Starting network data ingestion")

    data_path = Path(data_folder)

    file_pattern = str(
        data_path / FILE_PATTERN
    )

    logger.info(
        "Reading files using pattern: %s",
        file_pattern
    )

    # Read all raw CSV files using the actual raw schema
    raw_network_df = (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(file_pattern)
    )

    logger.info(
        "Raw network activity files loaded successfully"
    )

    # Add source file for traceability
    raw_network_df = (
        raw_network_df
        .withColumn(
            "input_file_name",
            input_file_name()
        )
    )

    logger.info(
        "Source filename added for record-level traceability"
    )

    # Convert raw names to canonical names
    for raw_name, canonical_name in COLUMN_MAPPING.items():

        raw_network_df = (
            raw_network_df
            .withColumnRenamed(
                raw_name,
                canonical_name
            )
        )

    logger.info(
        "Raw column names converted to canonical project names"
    )

    return raw_network_df


# =========================================================
# 5. Validate ingestion
# =========================================================

def validate_ingestion(raw_network_df, data_folder):

    logger.info("Starting ingestion validation")

    data_path = Path(data_folder)

    # Files that SHOULD have been loaded
    expected_files = list(
        data_path.glob(FILE_PATTERN)
    )

    expected_file_count = len(expected_files)

    logger.info(
        "Expected input files found: %d",
        expected_file_count
    )

    # -----------------------------------------------------
    # Basic counts
    # -----------------------------------------------------

    row_count = (
        raw_network_df.count()
    )

    logger.info(
        "Total records loaded: %d",
        row_count
    )

    file_count = (
        raw_network_df
        .select("input_file_name")
        .distinct()
        .count()
    )

    logger.info(
        "Distinct source files represented in DataFrame: %d",
        file_count
    )

    unique_grid_count = (
        raw_network_df
        .select("grid_id")
        .distinct()
        .count()
    )

    logger.info(
        "Distinct grid IDs found: %d",
        unique_grid_count
    )

    country_code_count = (
        raw_network_df
        .select("country_code")
        .distinct()
        .count()
    )

    logger.info(
        "Distinct country codes found: %d",
        country_code_count
    )

    timestamp_count = (
        raw_network_df
        .select("timestamp")
        .distinct()
        .count()
    )

    logger.info(
        "Distinct timestamps found: %d",
        timestamp_count
    )

    # -----------------------------------------------------
    # Validation checks
    # -----------------------------------------------------

    null_filename_count = (
        raw_network_df
        .filter(
            col("input_file_name").isNull()
        )
        .count()
    )

    invalid_grid_count = (
        raw_network_df
        .filter(
            (col("grid_id") < 1)
            | (col("grid_id") > 10000)
        )
        .count()
    )

    partition_count = (
        raw_network_df
        .rdd
        .getNumPartitions()
    )

    expected_timestamp_count = (
        expected_file_count * 24
    )

    logger.info(
        "Expected timestamp count based on daily files: %d",
        expected_timestamp_count
    )

    logger.info(
        "Null source filename records: %d",
        null_filename_count
    )

    logger.info(
        "Invalid grid ID records: %d",
        invalid_grid_count
    )

    logger.info(
        "Spark partition count: %d",
        partition_count
    )

    # -----------------------------------------------------
    # Validation status
    # -----------------------------------------------------

    file_check = (
        file_count == expected_file_count
    )

    timestamp_check = (
        timestamp_count == expected_timestamp_count
    )

    grid_check = (
        invalid_grid_count == 0
    )

    filename_check = (
        null_filename_count == 0
    )

    if file_check:
        logger.info(
            "File count validation passed"
        )
    else:
        logger.error(
            "File count validation failed: expected=%d, loaded=%d",
            expected_file_count,
            file_count
        )

    if timestamp_check:
        logger.info(
            "Timestamp count validation passed"
        )
    else:
        logger.error(
            "Timestamp count validation failed: expected=%d, actual=%d",
            expected_timestamp_count,
            timestamp_count
        )

    if grid_check:
        logger.info(
            "Grid ID range validation passed"
        )
    else:
        logger.error(
            "Grid ID validation failed: %d invalid records found",
            invalid_grid_count
        )

    if filename_check:
        logger.info(
            "Source filename traceability validation passed"
        )
    else:
        logger.error(
            "Source filename validation failed: %d null filenames found",
            null_filename_count
        )

    logger.info(
        "Ingestion validation completed"
    )

    # -----------------------------------------------------
    # Console report
    # -----------------------------------------------------

    print(
        "\n========== INGESTION REPORT =========="
    )

    print(
        f"Expected files        : {expected_file_count}"
    )

    print(
        f"Files loaded          : {file_count}"
    )

    print(
        f"Row count             : {row_count}"
    )

    print(
        f"Unique grids          : {unique_grid_count}"
    )

    print(
        f"Country-code values   : {country_code_count}"
    )

    print(
        f"Distinct timestamps   : {timestamp_count}"
    )

    print(
        f"Expected timestamps   : {expected_timestamp_count}"
    )

    print(
        f"Null source filenames : {null_filename_count}"
    )

    print(
        f"Invalid grid IDs      : {invalid_grid_count}"
    )

    print(
        f"Spark partitions      : {partition_count}"
    )

    return {
        "expected_file_count": expected_file_count,
        "file_count": file_count,
        "row_count": row_count,
        "unique_grid_count": unique_grid_count,
        "country_code_count": country_code_count,
        "timestamp_count": timestamp_count,
        "expected_timestamp_count": expected_timestamp_count,
        "invalid_grid_count": invalid_grid_count,
        "null_filename_count": null_filename_count,
        "partition_count": partition_count,
    }


# =========================================================
# 6. Main
# =========================================================

if __name__ == "__main__":

    try:

        logger.info(
            "========== NETWORK INGESTION STARTED =========="
        )

        DATA_FOLDER = (
            r"D:\Network Operations Predictive System\data"
        )

        # -------------------------------------------------
        # Load raw data
        # -------------------------------------------------

        raw_network_df = (
            load_raw_network_data(
                DATA_FOLDER
            )
        )

        # -------------------------------------------------
        # Display canonical schema
        # -------------------------------------------------

        print(
            "\n========== CANONICAL SCHEMA =========="
        )

        raw_network_df.printSchema()

        logger.info(
            "Canonical schema generated successfully"
        )

        # -------------------------------------------------
        # Validate ingestion
        # -------------------------------------------------

        report = validate_ingestion(
            raw_network_df,
            DATA_FOLDER
        )

        # -------------------------------------------------
        # Display sample
        # -------------------------------------------------

        print(
            "\n========== SAMPLE DATA =========="
        )

        raw_network_df.show(
            5,
            truncate=False
        )

        logger.info(
            "Sample records displayed successfully"
        )

        logger.info(
            "Network ingestion completed successfully"
        )

    except Exception:

        logger.exception(
            "Network ingestion pipeline failed"
        )

        raise

    finally:

        spark.stop()

        logger.info(
            "SparkSession stopped"
        )

        logger.info(
            "========== NETWORK INGESTION FINISHED =========="
        )

        print(
            "Spark Stopped Successfully"
        )
