from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    lit,
    when,
    to_date,
    hour,
    date_format,
    unix_timestamp,
)
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
logger = get_logger("NetworkActivityCleaning")

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

OUTPUT_PATH = r"D:\Network Operations Predictive System\outputs\sp2_cleaned_network_activity"

# =========================================================
# SparkSession
# =========================================================

spark = (
    SparkSession.builder
    .appName("NetworkActivityCleaning")
    .config("spark.driver.memory","8g")
    .config("spark.executor.memory","8g")
    .getOrCreate()
)

logger.info("SP2 SparkSession created")


# =========================================================
# Actual RAW CSV schema
# =========================================================

RAW_SCHEMA = StructType([
    StructField(
        "datetime",
        TimestampType(),
        True
    ),
    StructField(
        "CellID",
        IntegerType(),
        True
    ),
    StructField(
        "countrycode",
        IntegerType(),
        True
    ),
    StructField(
        "smsin",
        DoubleType(),
        True
    ),
    StructField(
        "smsout",
        DoubleType(),
        True
    ),
    StructField(
        "callin",
        DoubleType(),
        True
    ),
    StructField(
        "callout",
        DoubleType(),
        True
    ),
    StructField(
        "internet",
        DoubleType(),
        True
    ),
])


# =========================================================
# Raw → Canonical mapping
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


# =========================================================
# 1. Load and rename raw columns
# =========================================================

def load_network_data(data_folder):

    logger.info(
        "Starting network data ingestion"
    )

    data_path = Path(data_folder)
    file_pattern = str(
        data_path / FILE_PATTERN
    )

    logger.info(
        "Using file pattern: %s",
        FILE_PATTERN
    )

    df = (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(file_pattern)
    )

    # Add source filename for traceability
    df = df.withColumn(
        "input_file_name",
        input_file_name()
    )

    # Rename raw columns to canonical names
    for raw_name, canonical_name in COLUMN_MAPPING.items():

        df = df.withColumnRenamed(
            raw_name,
            canonical_name
        )

    logger.info(
        "Raw columns renamed to canonical project names"
    )

    logger.info(
        "Source filename added for traceability"
    )

    return df


# =========================================================
# 2. Verify / cast data types
# =========================================================

def standardize_types(df):

    logger.info(
        "Starting data type standardization"
    )

    standardized_df = (
        df
        .withColumn(
            "timestamp",
            col("timestamp").cast("timestamp")
        )
        .withColumn(
            "grid_id",
            col("grid_id").cast("integer")
        )
        .withColumn(
            "country_code",
            col("country_code").cast("integer")
        )
        .withColumn(
            "sms_in",
            col("sms_in").cast("double")
        )
        .withColumn(
            "sms_out",
            col("sms_out").cast("double")
        )
        .withColumn(
            "call_in",
            col("call_in").cast("double")
        )
        .withColumn(
            "call_out",
            col("call_out").cast("double")
        )
        .withColumn(
            "internet_activity",
            col("internet_activity").cast("double")
        )
    )

    logger.info(
        "Data type standardization completed"
    )

    return standardized_df


# =========================================================
# 3. Verify hourly cadence
# =========================================================

def verify_hourly_cadence(df, data_folder):

    logger.info(
        "Starting hourly cadence validation"
    )

    data_path = Path(data_folder)

    expected_files = list(
        data_path.glob(FILE_PATTERN)
    )

    expected_file_count = len(
        expected_files
    )

    expected_timestamp_count = (
        expected_file_count * 24
    )

    actual_timestamp_count = (
        df
        .select("timestamp")
        .distinct()
        .count()
    )

    # Order timestamps
    timestamp_df = (
        df
        .select("timestamp")
        .distinct()
        .orderBy("timestamp")
    )

    # Convert timestamps to seconds
    timestamp_seconds = (
        timestamp_df
        .withColumn(
            "timestamp_seconds",
            unix_timestamp("timestamp")
        )
    )

    timestamp_values = [
        row.timestamp_seconds
        for row in timestamp_seconds.collect()
    ]

    cadence_violations = 0

    for previous, current in zip(
        timestamp_values,
        timestamp_values[1:]
    ):

        if current - previous != 3600:
            cadence_violations += 1

    cadence_valid = (
        actual_timestamp_count
        == expected_timestamp_count
        and cadence_violations == 0
    )

    logger.info(
        "Expected files: %d",
        expected_file_count
    )

    logger.info(
        "Expected timestamps: %d",
        expected_timestamp_count
    )

    logger.info(
        "Actual timestamps: %d",
        actual_timestamp_count
    )

    logger.info(
        "Cadence violations: %d",
        cadence_violations
    )

    if cadence_valid:
        logger.info(
            "Hourly cadence validation passed"
        )
    else:
        logger.error(
            "Hourly cadence validation failed"
        )

    print("\n========== SP2 CADENCE CHECK ==========")

    print(
        f"Expected files       : "
        f"{expected_file_count}"
    )

    print(
        f"Expected timestamps  : "
        f"{expected_timestamp_count}"
    )

    print(
        f"Actual timestamps    : "
        f"{actual_timestamp_count}"
    )

    print(
        f"Cadence violations   : "
        f"{cadence_violations}"
    )

    print(
        f"Hourly cadence valid : "
        f"{cadence_valid}"
    )

    return {
        "expected_timestamp_count":
            expected_timestamp_count,
        "actual_timestamp_count":
            actual_timestamp_count,
        "cadence_violations":
            cadence_violations,
        "cadence_valid":
            cadence_valid,
    }

# =========================================================
# 4. Pandas vs Spark validation
# =========================================================

def validate_pandas_vs_spark(clean_network_df):
    """
    Compare Spark and pandas calculations on one full day.

    The comparison is performed at the project analytics grain:
    timestamp + grid_id.
    """

    logger.info(
        "Starting pandas-versus-Spark validation"
    )

    # -----------------------------------------------------
    # Select one complete day
    # -----------------------------------------------------

    selected_date = (
        clean_network_df
        .select("date")
        .distinct()
        .orderBy("date")
        .first()["date"]
    )

    logger.info(
        "Selected validation date: %s",
        selected_date
    )

    spark_day_df = (
        clean_network_df
        .filter(col("date") == selected_date)
    )

    # -----------------------------------------------------
    # Spark calculation
    # -----------------------------------------------------

    spark_result = (
        spark_day_df
        .groupBy(
            "timestamp",
            "grid_id"
        )
        .sum("total_activity")
        .withColumnRenamed(
            "sum(total_activity)",
            "spark_total_activity"
        )
    )

    # -----------------------------------------------------
    # Convert selected day to pandas
    # -----------------------------------------------------

    pandas_input = (
        spark_day_df
        .select(
            "timestamp",
            "grid_id",
            "total_activity"
        )
        .toPandas()
    )

    # -----------------------------------------------------
    # Equivalent pandas calculation
    # -----------------------------------------------------

    pandas_result = (
        pandas_input
        .groupby(
            ["timestamp", "grid_id"],
            as_index=False
        )["total_activity"]
        .sum()
        .rename(
            columns={
                "total_activity":
                    "pandas_total_activity"
            }
        )
    )

    # -----------------------------------------------------
    # Convert Spark result to pandas
    # -----------------------------------------------------

    spark_result_pd = (
        spark_result
        .toPandas()
    )

    # -----------------------------------------------------
    # Sort both results for deterministic comparison
    # -----------------------------------------------------

    spark_result_pd = spark_result_pd.sort_values(
        ["timestamp", "grid_id"]
    ).reset_index(drop=True)

    pandas_result = pandas_result.sort_values(
        ["timestamp", "grid_id"]
    ).reset_index(drop=True)

    # -----------------------------------------------------
    # Compare row counts
    # -----------------------------------------------------

    row_count_match = (
        len(spark_result_pd)
        == len(pandas_result)
    )

    # -----------------------------------------------------
    # Compare values
    # -----------------------------------------------------

    values_match = False

    if row_count_match:

        values_match = (
            spark_result_pd[
                "spark_total_activity"
            ]
            .round(10)
            .equals(
                pandas_result[
                    "pandas_total_activity"
                ].round(10)
            )
        )

    validation_passed = (
        row_count_match
        and values_match
    )

    logger.info(
        "Pandas rows: %d",
        len(pandas_result)
    )

    logger.info(
        "Spark rows: %d",
        len(spark_result_pd)
    )

    logger.info(
        "Pandas-vs-Spark row count match: %s",
        row_count_match
    )

    logger.info(
        "Pandas-vs-Spark value match: %s",
        values_match
    )

    if validation_passed:
        logger.info(
            "Pandas-versus-Spark validation PASSED"
        )
    else:
        logger.error(
            "Pandas-versus-Spark validation FAILED"
        )

    # -----------------------------------------------------
    # Display validation report
    # -----------------------------------------------------

    print(
        "\n========== PANDAS VS SPARK VALIDATION =========="
    )

    print(
        f"Validation date       : {selected_date}"
    )

    print(
        f"Spark rows            : {len(spark_result_pd)}"
    )

    print(
        f"Pandas rows           : {len(pandas_result)}"
    )

    print(
        f"Row count match       : {row_count_match}"
    )

    print(
        f"Value match           : {values_match}"
    )

    print(
        f"Validation passed     : {validation_passed}"
    )

    return {
        "validation_date": selected_date,
        "spark_rows": len(spark_result_pd),
        "pandas_rows": len(pandas_result),
        "row_count_match": row_count_match,
        "value_match": values_match,
        "validation_passed": validation_passed,
    }



# =========================================================
# 5. Clean and quarantine records
# =========================================================

def clean_network_data(df):

    logger.info(
        "SP2 cleaning started"
    )

    # Preserve original data before cleaning
    preserved_df = df

    records_before = (
        preserved_df.count()
    )

    logger.info(
        "Records before cleaning: %d",
        records_before
    )

    activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    ]

    # -----------------------------------------------------
    # Profile activity NULLs BEFORE replacing them
    # -----------------------------------------------------

    null_counts = {}

    for column_name in activity_columns:

        null_counts[column_name] = (
            preserved_df
            .filter(
                col(column_name).isNull()
            )
            .count()
        )

        logger.info(
            "Activity NULL count - %s: %d",
            column_name,
            null_counts[column_name]
        )

    total_activity_null_values = sum(
        null_counts.values()
    )

    logger.info(
        "Total activity NULL values: %d",
        total_activity_null_values
    )

    # -----------------------------------------------------
    # Identify invalid records
    # -----------------------------------------------------

    invalid_condition = (
        col("grid_id").isNull()
        | col("timestamp").isNull()
        | (
            col("sms_in").isNotNull()
            & (col("sms_in") < 0)
        )
        | (
            col("sms_out").isNotNull()
            & (col("sms_out") < 0)
        )
        | (
            col("call_in").isNotNull()
            & (col("call_in") < 0)
        )
        | (
            col("call_out").isNotNull()
            & (col("call_out") < 0)
        )
        | (
            col("internet_activity").isNotNull()
            & (col("internet_activity") < 0)
        )
    )

    rejected_records_df = (
        preserved_df
        .filter(invalid_condition)
    )

    rejected_record_count = (
        rejected_records_df.count()
    )

    logger.info(
        "Rejected records: %d",
        rejected_record_count
    )

    # -----------------------------------------------------
    # Keep valid records
    # -----------------------------------------------------

    valid_df = (
        preserved_df
        .filter(~invalid_condition)
    )

    # -----------------------------------------------------
    # Curated-layer NULL → 0
    # -----------------------------------------------------

    clean_network_df = valid_df

    for column_name in activity_columns:

        clean_network_df = (
            clean_network_df
            .withColumn(
                column_name,
                when(
                    col(column_name).isNull(),
                    lit(0.0)
                )
                .otherwise(
                    col(column_name)
                )
            )
        )

    logger.info(
        "Applied curated-layer NULL-to-zero rule"
    )

    logger.info(
        "Activity NULL values handled: %d",
        total_activity_null_values
    )

    # -----------------------------------------------------
    # Create activity indicators
    # -----------------------------------------------------

    clean_network_df = (
        clean_network_df

        .withColumn(
            "total_sms",
            col("sms_in")
            + col("sms_out")
        )

        .withColumn(
            "total_calls",
            col("call_in")
            + col("call_out")
        )

        .withColumn(
            "total_activity",
            col("total_sms")
            + col("total_calls")
            + col("internet_activity")
        )
    )

    logger.info(
        "Created total_sms, total_calls and total_activity"
    )

    # -----------------------------------------------------
    # Derive time features
    # -----------------------------------------------------

    clean_network_df = (
        clean_network_df

        .withColumn(
            "date",
            to_date(col("timestamp"))
        )

        .withColumn(
            "hour",
            hour(col("timestamp"))
        )

        .withColumn(
            "day_of_week",
            date_format(
                col("timestamp"),
                "EEEE"
            )
        )
    )

    logger.info(
        "Derived date, hour and day_of_week"
    )

    records_after = (
        clean_network_df.count()
    )

    logger.info(
        "Records after cleaning: %d",
        records_after
    )

    logger.info(
        "SP2 cleaning completed successfully"
    )

    # -----------------------------------------------------
    # Cleaning report
    # -----------------------------------------------------

    cleaning_report = {
        "records_before":
            records_before,

        "records_after":
            records_after,

        "rejected_records":
            rejected_record_count,

        "activity_null_counts":
            null_counts,

        "activity_null_values_handled":
            total_activity_null_values,
    }

    return (
        clean_network_df,
        rejected_records_df,
        cleaning_report,
    )


# =========================================================
# Main
# =========================================================

if __name__ == "__main__":

    try:

        logger.info(
            "========== SP2 PIPELINE STARTED =========="
        )

        DATA_FOLDER = (
            r"D:\Network Operations Predictive System\data"
        )

        # -------------------------------------------------
        # Load raw data
        # -------------------------------------------------

        raw_network_df = (
            load_network_data(
                DATA_FOLDER
            )
        )

        raw_record_count = (
            raw_network_df.count()
        )

        logger.info(
            "Raw record count: %d",
            raw_record_count
        )

        print(
            "\n========== RAW RECORD COUNT =========="
        )

        print(
            raw_record_count
        )

        # -------------------------------------------------
        # Standardize data types
        # -------------------------------------------------

        standardized_df = (
            standardize_types(
                raw_network_df
            )
        )

        print(
            "\n========== STANDARDIZED SCHEMA =========="
        )

        standardized_df.printSchema()

        logger.info(
            "Standardized schema generated"
        )

        # -------------------------------------------------
        # Verify cadence
        # -------------------------------------------------

        cadence_report = (
            verify_hourly_cadence(
                standardized_df,
                DATA_FOLDER
            )
        )

        # -------------------------------------------------
        # Clean data
        # -------------------------------------------------

        (
            clean_network_df,
            rejected_records_df,
            cleaning_report,
        ) = clean_network_data(
            standardized_df
        )

        # -------------------------------------------------
        # Pandas vs Spark validation
        # -------------------------------------------------

        pandas_spark_report = validate_pandas_vs_spark(
            clean_network_df
        )
        (
            clean_network_df
            .write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )
        logger.info("Cleaned data written to parquet at: %s", OUTPUT_PATH)


        


        # -------------------------------------------------
        # Final report
        # -------------------------------------------------

        print(
            "\n========== SP2 CLEANING REPORT =========="
        )

        print(
            "Records before cleaning :",
            cleaning_report[
                "records_before"
            ]
        )

        print(
            "Records after cleaning  :",
            cleaning_report[
                "records_after"
            ]
        )

        print(
            "Rejected records        :",
            cleaning_report[
                "rejected_records"
            ]
        )

        print(
            "Activity NULL counts    :"
        )

        for (
            column_name,
            count_value
        ) in (
            cleaning_report[
                "activity_null_counts"
            ].items()
        ):

            print(
                f"  {column_name}: "
                f"{count_value}"
            )

        print(
            "Activity NULL values handled:",
            cleaning_report[
                "activity_null_values_handled"
            ]
        )

        logger.info(
            "SP2 cleaning report generated"
        )

        # -------------------------------------------------
        # Clean network schema
        # -------------------------------------------------

        print(
            "\n========== CLEAN NETWORK SCHEMA =========="
        )

        clean_network_df.printSchema()

        # -------------------------------------------------
        # Clean data sample
        # -------------------------------------------------

        print(
            "\n========== CLEAN DATA SAMPLE =========="
        )

        clean_network_df.show(
            5,
            truncate=False
        )

        # -------------------------------------------------
        # Rejected data sample
        # -------------------------------------------------

        print(
            "\n========== REJECTED DATA SAMPLE =========="
        )

        rejected_records_df.show(
            5,
            truncate=False
        )

        logger.info(
            "SP2 output samples displayed"
        )

        # -------------------------------------------------
        # Stop Spark
        # -------------------------------------------------

        spark.stop()

        logger.info(
            "Spark stopped successfully"
        )

        logger.info(
            "========== SP2 PIPELINE COMPLETED =========="
        )

        print(
            "\nSpark Stopped Successfully"
        )

    except Exception:

        logger.exception(
            "SP2 pipeline failed"
        )

        try:
            spark.stop()
        except Exception:
            pass

        raise