import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    date_format,
    hour,
    lit,
    to_date,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)


# ============================================================
# ENVIRONMENT
# ============================================================

os.environ["HADOOP_HOME"] = (
    r"C:\Users\saivarshaa.sujee\pyspark_demo\hadoop"
)

os.environ["PATH"] += (
    r";C:\Users\saivarshaa.sujee\pyspark_demo\hadoop\bin"
)

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

LOG_DIR = PROJECT_ROOT / "logs"

APPLICATION_LOG = LOG_DIR / "application.log"
ERROR_LOG = LOG_DIR / "errors.log"


# ============================================================
# LOGGING
# ============================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("telecom_pipeline")
logger.setLevel(logging.INFO)

# Prevent duplicate messages if logging is configured elsewhere.
logger.propagate = False

if not logger.handlers:

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    )

    # Terminal
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Normal application log
    application_handler = logging.FileHandler(
        APPLICATION_LOG,
        mode="a",
        encoding="utf-8",
    )
    application_handler.setLevel(logging.INFO)
    application_handler.setFormatter(formatter)

    # Error-only log
    error_handler = logging.FileHandler(
        ERROR_LOG,
        mode="a",
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(application_handler)
    logger.addHandler(error_handler)


# ============================================================
# RAW DATA SCHEMA
# ============================================================

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


# Raw dataset name -> canonical project name
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


# ============================================================
# READ RAW
# ============================================================

def read_raw(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """
    Read all valid raw telecom CSV files.

    Expected filename pattern:
        sms-call-internet-mi-*.csv
    """

    input_dir = Path(input_path)

    if not input_dir.exists():
        raise FileNotFoundError(
            f"Input folder does not exist: {input_path}"
        )

    input_files = list(
        input_dir.glob("sms-call-internet-mi-*.csv")
    )

    if not input_files:
        raise FileNotFoundError(
            f"No valid input files found in: {input_path}"
        )

    logger.info(
        "INPUT_FILES=%d | INPUT_PATH=%s",
        len(input_files),
        input_path,
    )

    df = (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(
            str(
                input_dir /
                "sms-call-internet-mi-*.csv"
            )
        )
    )

    for raw_name, canonical_name in COLUMN_MAPPING.items():

        df = df.withColumnRenamed(
            raw_name,
            canonical_name,
        )

    input_rows = df.count()

    logger.info(
        "INPUT_ROWS=%d",
        input_rows,
    )

    return df


# ============================================================
# CLEAN
# ============================================================

def clean(
    df: DataFrame,
) -> tuple[DataFrame, int, int]:
    """
    Apply the core SP2 cleaning rules.

    Returns:
        clean_df
        rejected_rows
        nulls_handled
    """

    # --------------------------------------------------------
    # 1. Identify invalid records
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 2. Count and remove invalid records
    # --------------------------------------------------------

    rejected_rows = (
        df
        .filter(invalid_condition)
        .count()
    )

    clean_network_df = (
        df
        .filter(~invalid_condition)
    )

    # --------------------------------------------------------
    # 3. Handle NULL activity values
    # --------------------------------------------------------

    activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    ]

    nulls_handled = sum(
        clean_network_df
        .filter(col(column).isNull())
        .count()
        for column in activity_columns
    )

    for column in activity_columns:

        clean_network_df = (
            clean_network_df
            .withColumn(
                column,
                when(
                    col(column).isNull(),
                    lit(0.0),
                ).otherwise(
                    col(column),
                ),
            )
        )

    # --------------------------------------------------------
    # 4. Derive activity features
    # --------------------------------------------------------

    clean_network_df = (
        clean_network_df
        .withColumn(
            "total_sms",
            col("sms_in") + col("sms_out"),
        )
        .withColumn(
            "total_calls",
            col("call_in") + col("call_out"),
        )
        .withColumn(
            "total_activity",
            col("sms_in")
            + col("sms_out")
            + col("call_in")
            + col("call_out")
            + col("internet_activity"),
        )
    )

    # --------------------------------------------------------
    # 5. Derive time features
    # --------------------------------------------------------

    clean_network_df = (
        clean_network_df
        .withColumn(
            "date",
            to_date(col("timestamp")),
        )
        .withColumn(
            "hour",
            hour(col("timestamp")),
        )
        .withColumn(
            "day_of_week",
            date_format(
                col("timestamp"),
                "EEEE",
            ),
        )
    )

    logger.info(
        "REJECTED_ROWS=%d",
        rejected_rows,
    )

    logger.info(
        "NULLS_HANDLED=%d",
        nulls_handled,
    )

    return (
        clean_network_df,
        rejected_rows,
        nulls_handled,
    )


# ============================================================
# AGGREGATE
# ============================================================

def aggregate(
    df: DataFrame,
) -> DataFrame:
    """
    Create one record per grid and timestamp.

    Country-level records are aggregated to the
    canonical grid + timestamp grain.
    """

    hourly_grid_summary = (
        df
        .groupBy(
            "timestamp",
            "grid_id",
        )
        .agg(
            F.sum("sms_in").alias("sms_in"),
            F.sum("sms_out").alias("sms_out"),
            F.sum("call_in").alias("call_in"),
            F.sum("call_out").alias("call_out"),
            F.sum(
                "internet_activity"
            ).alias(
                "internet_activity"
            ),
        )
        .withColumn(
            "date",
            F.to_date("timestamp"),
        )
        .withColumn(
            "hour",
            F.hour("timestamp"),
        )
        .withColumn(
            "day_of_week",
            F.date_format(
                "timestamp",
                "EEEE",
            ),
        )
        .withColumn(
            "total_sms",
            F.col("sms_in")
            + F.col("sms_out"),
        )
        .withColumn(
            "total_calls",
            F.col("call_in")
            + F.col("call_out"),
        )
        .withColumn(
            "total_activity",
            F.col("total_sms")
            + F.col("total_calls")
            + F.col("internet_activity"),
        )
        .withColumn(
            "internet_share",
            F.when(
                F.col("total_activity") > 0,
                F.col("internet_activity")
                / F.col("total_activity"),
            ).otherwise(0.0),
        )
    )

    output_rows = hourly_grid_summary.count()

    logger.info(
        "AGGREGATED_OUTPUT_ROWS=%d",
        output_rows,
    )

    return hourly_grid_summary


# ============================================================
# ENRICH
# ============================================================

def enrich(
    spark: SparkSession,
    hourly_grid_summary: DataFrame,
    reference_path: str,
) -> DataFrame:
    """
    Validate the static geographic reference.

    Geometry remains in milano-grid.geojson and is NOT
    duplicated into the hourly analytics fact table.

    grid_id provides the logical link between the analytics
    data and the static geographic reference.
    """

    reference_file = Path(reference_path)

    # --------------------------------------------------------
    # 1. Validate GeoJSON exists
    # --------------------------------------------------------

    if not reference_file.exists():
        raise FileNotFoundError(
            f"Reference GeoJSON not found: "
            f"{reference_path}"
        )

    # --------------------------------------------------------
    # 2. Validate hourly analytics structure
    # --------------------------------------------------------

    if "grid_id" not in hourly_grid_summary.columns:

        raise ValueError(
            "hourly_grid_summary must contain grid_id."
        )

    if "geometry" in hourly_grid_summary.columns:

        raise ValueError(
            "Geometry must not be included in "
            "hourly_grid_summary."
        )

    # --------------------------------------------------------
    # 3. Log geographic-reference policy
    # --------------------------------------------------------

    logger.info(
        "REFERENCE_PATH=%s",
        reference_path,
    )

    logger.info(
        "GEOGRAPHIC_REFERENCE_STATUS=VALID"
    )

    logger.info(
        "GEOMETRY_POLICY=Polygon geometry remains in "
        "the static GeoJSON reference and is not "
        "duplicated in hourly analytics."
    )

    # No geometry is added to the fact table.
    return hourly_grid_summary


# ============================================================
# WRITE OUTPUTS
# ============================================================

def write_outputs(
    clean_df: DataFrame,
    hourly_grid_summary: DataFrame,
    output_path: str,
) -> None:
    """
    Write processed activity, hourly analytics and
    dashboard summary outputs.
    """

    output_dir = Path(output_path)

    activity_path = (
        output_dir / "activity"
    )

    hourly_path = (
        output_dir / "hourly_grid_summary"
    )

    dashboard_path = (
        output_dir / "dashboard_summary"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # 1. Clean activity
    # --------------------------------------------------------

    (
        clean_df
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(
            str(activity_path)
        )
    )

    logger.info(
        "ACTIVITY_OUTPUT=%s",
        activity_path,
    )

    # --------------------------------------------------------
    # 2. Hourly grid summary
    # --------------------------------------------------------

    (
        hourly_grid_summary
        .write
        .mode("overwrite")
        .parquet(
            str(hourly_path)
        )
    )

    logger.info(
        "HOURLY_SUMMARY_OUTPUT=%s",
        hourly_path,
    )

    # --------------------------------------------------------
    # 3. Small dashboard summary
    # --------------------------------------------------------

    dashboard_summary = (
        hourly_grid_summary
        .groupBy("grid_id")
        .agg(
            F.sum(
                "total_activity"
            ).alias(
                "total_activity"
            )
        )
        .orderBy(
            F.col(
                "total_activity"
            ).desc()
        )
        .limit(10)
    )

    (
        dashboard_summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .option(
            "header",
            True,
        )
        .csv(
            str(dashboard_path)
        )
    )

    logger.info(
        "DASHBOARD_OUTPUT=%s",
        dashboard_path,
    )


# ============================================================
# ROUND-TRIP VALIDATION
# ============================================================

def validate_round_trip(
    spark: SparkSession,
    hourly_grid_summary: DataFrame,
    output_path: str,
) -> None:
    """
    Read the written hourly Parquet back and validate:

    - exact row count
    - exact schema
    - duplicate grid_id + timestamp groups
    - absence of geometry
    """

    hourly_path = (
        Path(output_path)
        / "hourly_grid_summary"
    )

    reloaded_df = spark.read.parquet(
        str(hourly_path)
    )

    original_count = (
        hourly_grid_summary.count()
    )

    reloaded_count = (
        reloaded_df.count()
    )

    logger.info(
        "ROUND_TRIP_ORIGINAL_ROWS=%d",
        original_count,
    )

    logger.info(
        "ROUND_TRIP_RELOADED_ROWS=%d",
        reloaded_count,
    )

    # --------------------------------------------------------
    # Row count validation
    # --------------------------------------------------------

    if original_count != reloaded_count:

        raise AssertionError(
            "Round-trip row count mismatch."
        )

    logger.info(
        "ROUND_TRIP_ROW_COUNT=PASS"
    )

    # --------------------------------------------------------
    # Schema validation
    # --------------------------------------------------------

    if (
        hourly_grid_summary.schema
        != reloaded_df.schema
    ):

        raise AssertionError(
            "Round-trip schema mismatch."
        )

    logger.info(
        "ROUND_TRIP_SCHEMA=PASS"
    )

    # --------------------------------------------------------
    # Geometry validation
    # --------------------------------------------------------

    if "geometry" in reloaded_df.columns:

        raise AssertionError(
            "Geometry column found in "
            "hourly_grid_summary."
        )

    logger.info(
        "ROUND_TRIP_GEOMETRY=PASS"
    )

    # --------------------------------------------------------
    # Duplicate validation
    # --------------------------------------------------------

    duplicate_groups = (
        reloaded_df
        .groupBy(
            "grid_id",
            "timestamp",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    logger.info(
        "ROUND_TRIP_DUPLICATE_GROUPS=%d",
        duplicate_groups,
    )

    if duplicate_groups != 0:

        raise AssertionError(
            "Duplicate (grid_id, timestamp) "
            "groups found after round trip."
        )

    logger.info(
        "ROUND_TRIP_DUPLICATES=PASS"
    )


# ============================================================
# JOB CONTRACT
# ============================================================

def write_job_contract(
    output_path: str,
) -> None:
    """
    Write the executable job contract.
    """

    contract = """
# Telecom Spark Pipeline — Job Contract

## Input

The job expects a directory containing raw telecom
CSV files matching:

sms-call-internet-mi-*.csv

Required raw columns:

- datetime
- CellID
- countrycode
- smsin
- smsout
- callin
- callout
- internet

## Processing

1. Read all matching raw CSV files.
2. Apply the controlled raw-to-canonical column mapping.
3. Reject records with:
   - missing grid_id
   - missing timestamp
   - negative activity values
4. Replace remaining NULL activity measures with zero.
5. Derive total_sms, total_calls and total_activity.
6. Derive date, hour and day_of_week.
7. Aggregate country-level records to grid_id + timestamp.
8. Calculate hourly analytics including internet_share.
9. Validate the static Milan GeoJSON reference.
10. Keep Polygon geometry outside the hourly fact table.

## Outputs

output_path/activity/

Clean activity data in Parquet,
partitioned by date.

output_path/hourly_grid_summary/

One record per grid_id + timestamp in Parquet.
No geometry column is stored.

output_path/dashboard_summary/

Small CSV summary containing the top 10 grids
by total activity.

output_path/JOB_CONTRACT.md

Description of the pipeline input, processing,
outputs and failure conditions.

## Geographic Reference

The Milan GeoJSON remains a separate static reference
for map rendering.

The analytics data links to geography using grid_id.

## Failure Conditions

The job fails with a non-zero exit code when:

- the input folder does not exist
- no matching raw CSV files are found
- the reference GeoJSON does not exist
- required columns are missing
- geometry is incorrectly present in hourly analytics
- round-trip validation fails
- an unexpected processing error occurs
"""


    contract_path = (
        Path(output_path)
        / "JOB_CONTRACT.md"
    )

    contract_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    contract_path.write_text(
        contract.strip(),
        encoding="utf-8",
    )

    logger.info(
        "JOB_CONTRACT=%s",
        contract_path,
    )


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    start_time = datetime.now()

    logger.info(
        "JOB_START=%s",
        start_time.isoformat(),
    )

    parser = argparse.ArgumentParser(
        description=(
            "Network Operations & "
            "Predictive Intelligence Spark Pipeline"
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Directory containing raw telecom CSV files"
        ),
    )

    parser.add_argument(
        "--output",
        required=True,
        help=(
            "Output directory for processed "
            "and analytics data"
        ),
    )

    parser.add_argument(
        "--reference",
        required=True,
        help=(
            "Path to milano-grid.geojson"
        ),
    )

    args = parser.parse_args()

    spark = None

    try:

        # ----------------------------------------------------
        # Start Spark
        # ----------------------------------------------------

        spark = (
            SparkSession.builder
            .appName(
                "TelecomNetworkPipeline"
            )
            .config(
                "spark.driver.memory",
                "4g",
            )
            .config(
                "spark.executor.memory",
                "4g",
            )
            .getOrCreate()
        )

        # ----------------------------------------------------
        # 1. READ
        # ----------------------------------------------------

        raw_df = read_raw(
            spark,
            args.input,
        )

        # ----------------------------------------------------
        # 2. CLEAN
        # ----------------------------------------------------

        clean_df, rejected_rows, nulls_handled = clean(
            raw_df
        )

        # ----------------------------------------------------
        # 3. AGGREGATE
        # ----------------------------------------------------

        hourly_grid_summary = aggregate(
            clean_df
        )

        # ----------------------------------------------------
        # 4. ENRICH
        # ----------------------------------------------------

        hourly_grid_summary = enrich(
            spark,
            hourly_grid_summary,
            args.reference,
        )

        # ----------------------------------------------------
        # 5. WRITE
        # ----------------------------------------------------

        write_outputs(
            clean_df,
            hourly_grid_summary,
            args.output,
        )

        # ----------------------------------------------------
        # 6. OUTPUT COUNTS
        # ----------------------------------------------------

        clean_output_rows = (
            clean_df.count()
        )

        hourly_output_rows = (
            hourly_grid_summary.count()
        )

        logger.info(
            "OUTPUT_ROWS_CLEAN_ACTIVITY=%d",
            clean_output_rows,
        )

        logger.info(
            "OUTPUT_ROWS_HOURLY_GRID_SUMMARY=%d",
            hourly_output_rows,
        )

        # ----------------------------------------------------
        # 7. ROUND-TRIP VALIDATION
        # ----------------------------------------------------

        validate_round_trip(
            spark,
            hourly_grid_summary,
            args.output,
        )

        # ----------------------------------------------------
        # 8. JOB CONTRACT
        # ----------------------------------------------------

        write_job_contract(
            args.output
        )

        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        end_time = datetime.now()

        duration = (
            end_time - start_time
        ).total_seconds()

        logger.info(
            "JOB_END=%s",
            end_time.isoformat(),
        )

        logger.info(
            "JOB_DURATION_SECONDS=%.3f",
            duration,
        )

        logger.info(
            "JOB_STATUS=SUCCESS"
        )

        return 0

    except Exception as exc:

        # exception traceback goes to both
        # application.log and errors.log
        logger.exception(
            "JOB_STATUS=FAILED | ERROR=%s",
            str(exc),
        )

        return 1

    finally:

        if spark is not None:
            spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(main())

