# ============================================================
# SP6 — Write Processed & Analytics Data
# ============================================================

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    max,
    min,
    sum,
    to_date
)
from pyspark.sql.types import StructType

from logger_config import get_logger


logger = get_logger("WriteProcessedAnalytics")


# ============================================================
# Environment Configuration
# ============================================================

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


# ============================================================
# Configuration
# ============================================================

# Clean activity data produced by SP2
CLEAN_ACTIVITY_PATH = (
    r"D:\Network Operations Predictive System\outputs\sp2_cleaned_network_activity"
)

# Hourly grid-level analytics produced by SP3
HOURLY_GRID_SUMMARY_PATH = (
    r"D:\Network Operations Predictive System"
    r"\outputs\sp3_network_aggregations"
    r"\hourly_grid_summary"
)

# SP6 output directory
PROCESSED_ANALYTICS_OUTPUT_PATH = (
    r"D:\Network Operations Predictive System"
    r"\outputs\sp6_processed_analytics"
)

# Static geographic reference used for map rendering
MILAN_GRID_GEOJSON_PATH = (
    r"D:\Network Operations Predictive System\data\milano-grid.geojson"
)

# ============================================================
# SparkSession
# ============================================================

spark = (
    SparkSession.builder
    .appName("WriteProcessedAnalytics")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.driver.memory","8g")
    .config("spark.executor.memory","8g")
    .getOrCreate()
)

logger.info("SP6 SparkSession created")


# ============================================================
# 1. Load Existing SP2 Clean Activity Data
# ============================================================

def load_clean_activity(input_path):
    """
    Load the already-cleaned activity data produced by SP2.
    """

    logger.info(
        "Loading SP2 clean activity data from: %s",
        input_path,
    )

    df = spark.read.parquet(input_path)

    logger.info(
        "SP2 clean activity data loaded successfully"
    )

    print("\n========== SP2 CLEAN ACTIVITY INPUT ==========")

    df.printSchema()

    print(
        "Input row count :",
        df.count(),
    )

    return df


# ============================================================
# 2. Prepare Date Column
# ============================================================

def prepare_date_column(activity_df):
    """
    Ensure the clean activity DataFrame contains a date column.

    If date already exists, reuse it.
    Otherwise derive it from timestamp.
    """

    if "date" in activity_df.columns:

        logger.info(
            "Date column already exists."
        )

        return activity_df

    if "timestamp" not in activity_df.columns:

        raise ValueError(
            "Cannot create date column because "
            "timestamp column is missing."
        )

    logger.info(
        "Date column not found. Deriving date from timestamp."
    )

    activity_df = activity_df.withColumn(
        "date",
        to_date(col("timestamp"))
    )

    return activity_df


# ============================================================
# 3. Write Processed Activity Partitioned by Date
# ============================================================

def write_processed_activity(
    activity_df,
    output_path,
):
    """
    Write clean activity data as Parquet partitioned by date.
    """

    output = Path(output_path)

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    activity_output = str(
        output / "activity"
    )

    logger.info(
        "Writing processed activity to: %s",
        activity_output,
    )

    (
        activity_df
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(activity_output)
    )

    print(
        "\n========== PROCESSED ACTIVITY =========="
    )

    print(
        "Output path :",
        activity_output,
    )

    print(
        "Format      : Parquet"
    )

    print(
        "Partitioned by : date"
    )

    logger.info(
        "Processed activity written successfully"
    )

    return activity_output


# ============================================================
# 4. Load Existing SP3 Hourly Grid Summary
# ============================================================

def load_hourly_grid_summary(input_path):
    """
    Load the hourly_grid_summary already produced by SP3.
    """

    logger.info(
        "Loading SP3 hourly_grid_summary from: %s",
        input_path,
    )

    df = spark.read.parquet(input_path)

    print(
        "\n========== SP3 HOURLY GRID SUMMARY =========="
    )

    df.printSchema()

    print(
        "Row count :",
        df.count(),
    )

    return df


# ============================================================
# 5. Prepare Hourly Grid Summary
# ============================================================

def prepare_hourly_grid_summary(hourly_df):
    """
    Prepare the SP3 hourly summary for SP6.

    Geometry must not exist in the analytics fact table.
    """

    # Geometry must not be duplicated into hourly analytics data.

    geometry_columns = [
        column
        for column in hourly_df.columns
        if column.lower() in (
            "geometry",
            "centroid_longitude",
            "centroid_latitude",
        )
    ]

    if geometry_columns:

        logger.info(
            "Removing geographic columns from hourly_grid_summary: %s",
            geometry_columns,
        )

        hourly_df = hourly_df.drop(
            *geometry_columns
        )

    # Required analytical identity columns
    required_columns = [
        "grid_id",
        "timestamp",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in hourly_df.columns
    ]

    if missing_columns:

        raise ValueError(
            "hourly_grid_summary is missing required "
            f"columns: {missing_columns}"
        )

    # Check uniqueness before writing.
    duplicate_count = (
        hourly_df
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

    print(
        "\n========== HOURLY SUMMARY VALIDATION =========="
    )

    print(
        "Duplicate (grid_id, timestamp) groups :",
        duplicate_count,
    )

    assert duplicate_count == 0, (
        "Duplicate (grid_id, timestamp) records "
        "found in hourly_grid_summary."
    )

    # Ensure geometry is absent.
    assert "geometry" not in hourly_df.columns, (
        "Geometry column must not exist in "
        "hourly_grid_summary."
    )

    logger.info(
        "Hourly grid summary validation passed."
    )

    return hourly_df


# ============================================================
# 6. Write Hourly Grid Summary
# ============================================================

def write_hourly_grid_summary(
    hourly_df,
    output_path,
):
    """
    Write one-record-per-grid-hour analytics data as Parquet.
    """

    output = Path(output_path)

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    hourly_output = str(
        output / "hourly_grid_summary"
    )

    logger.info(
        "Writing hourly_grid_summary to: %s",
        hourly_output,
    )

    (
        hourly_df
        .write
        .mode("overwrite")
        .parquet(hourly_output)
    )

    print(
        "\n========== HOURLY GRID SUMMARY OUTPUT =========="
    )

    print(
        "Output path :",
        hourly_output,
    )

    print(
        "Format      : Parquet"
    )

    print(
        "Geometry    : Not included"
    )

    return hourly_output


# ============================================================
# 7. Create Dashboard Summary
# ============================================================

def create_dashboard_summary(
    hourly_df,
):
    """
    Create a small dashboard-friendly summary.

    The summary contains total activity by grid and is
    intentionally kept small for easy CSV inspection.
    """

    dashboard_summary = (
        hourly_df
        .groupBy("grid_id")
        .agg(
            sum("total_activity")
            .alias("total_activity")
        )
        .orderBy(
            col("total_activity").desc()
        )
        .limit(10)
    )

    return dashboard_summary


# ============================================================
# 8. Write Dashboard Summary as CSV
# ============================================================

def write_dashboard_summary(
    dashboard_df,
    output_path,
):
    """
    Write the small dashboard summary as CSV.
    """

    output = Path(output_path)

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = str(
        output / "dashboard_summary"
    )

    logger.info(
        "Writing dashboard summary CSV to: %s",
        csv_path,
    )

    (
        dashboard_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(csv_path)
    )

    print(
        "\n========== DASHBOARD SUMMARY =========="
    )

    dashboard_df.show(
        10,
        truncate=False,
    )

    print(
        "CSV output directory :",
        csv_path,
    )

    return csv_path


# ============================================================
# 9. Write Equivalent Parquet for File-Size Comparison
# ============================================================

def write_dashboard_parquet(
    dashboard_df,
    output_path,
):
    """
    Write the same dashboard dataset as Parquet so that
    CSV and Parquet file sizes can be compared fairly.
    """

    output = Path(output_path)

    parquet_path = str(
        output / "dashboard_summary_parquet"
    )

    (
        dashboard_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(parquet_path)
    )

    return parquet_path


# ============================================================
# 10. Calculate Directory Size
# ============================================================

def get_directory_size(path):
    """
    Calculate total size of all files inside a directory.
    """

    total_size = 0

    path = Path(path)

    if not path.exists():
        return 0

    for file in path.rglob("*"):

        if file.is_file():
            total_size += file.stat().st_size

    return total_size


# ============================================================
# 11. Compare CSV and Parquet Sizes
# ============================================================

def compare_file_sizes(
    csv_path,
    parquet_path,
):
    """
    Compare the physical sizes of equivalent CSV
    and Parquet representations.
    """

    csv_size = get_directory_size(
        csv_path
    )

    parquet_size = get_directory_size(
        parquet_path
    )

    print(
        "\n========== FILE SIZE COMPARISON =========="
    )

    print(
        f"CSV size     : {csv_size:,} bytes"
    )

    print(
        f"Parquet size : {parquet_size:,} bytes"
    )

    if parquet_size < csv_size:

        reduction = (
            (csv_size - parquet_size)
            / csv_size
        ) * 100

        print(
            f"Parquet is approximately "
            f"{reduction:.2f}% smaller."
        )

    else:

        print(
            "Parquet is not smaller for this small "
            "dashboard dataset."
        )

    print(
        "\nColumnar storage benefit:"
    )

    print(
        "- Parquet stores data by column."
    )

    print(
        "- Spark can read only the required columns."
    )

    print(
        "- Parquet supports compression."
    )

    print(
        "- This makes Parquet suitable for "
        "analytics workloads."
    )

    logger.info(
        "CSV size: %d bytes",
        csv_size,
    )

    logger.info(
        "Parquet size: %d bytes",
        parquet_size,
    )

    return csv_size, parquet_size


# ============================================================
# 12. Validate Partition Folders
# ============================================================

def validate_partitions(
    processed_activity_path,
):
    """
    Verify that the processed activity directory
    contains date-based partition folders.
    """

    path = Path(
        processed_activity_path
    )

    partition_folders = sorted(
        [
            folder.name
            for folder in path.iterdir()
            if folder.is_dir()
            and folder.name.startswith("date=")
        ]
    )

    print(
        "\n========== DATE PARTITION VALIDATION =========="
    )

    print(
        "Number of date partitions :",
        len(partition_folders),
    )

    print(
        "Partition folders:"
    )

    for folder in partition_folders:

        print(
            " -",
            folder,
        )

    assert partition_folders, (
        "No date partition folders found."
    )

    logger.info(
        "Date partition validation passed."
    )

    return partition_folders


# ============================================================
# 13. Round-Trip Validation
# ============================================================

def validate_round_trip(
    output_path,
    original_df,
):
    """
    Read the written Parquet data back and validate:

    1. Exact row count
    2. Exact schema
    3. No duplicate grid_id + timestamp
    """

    print(
        "\n========== ROUND-TRIP VALIDATION =========="
    )

    reloaded_df = spark.read.parquet(
        output_path
    )

    original_count = original_df.count()

    reloaded_count = reloaded_df.count()

    print(
        "Original row count :",
        original_count,
    )

    print(
        "Reloaded row count :",
        reloaded_count,
    )

    assert original_count == reloaded_count, (
        "Round-trip row count mismatch."
    )

    print(
        "Row count validation : PASS"
    )

    schema_match = (
        original_df.schema
        == reloaded_df.schema
    )

    print(
        "Schema match :",
        schema_match,
    )

    assert schema_match, (
        "Round-trip schema does not match "
        "the original schema."
    )

    print(
        "Schema validation : PASS"
    )

    duplicate_count = (
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

    print(
        "Duplicate (grid_id, timestamp) groups :",
        duplicate_count,
    )

    assert duplicate_count == 0, (
        "Duplicate (grid_id, timestamp) records "
        "found after round trip."
    )

    print(
        "Duplicate validation : PASS"
    )

    logger.info(
        "Round-trip validation completed successfully."
    )

    return True


# ============================================================
# 14. Validate No Geometry in Analytics Output
# ============================================================

def validate_no_geometry(
    hourly_output_path,
):
    """
    Confirm that hourly_grid_summary does not
    contain geometry.
    """

    df = spark.read.parquet(
        hourly_output_path
    )

    print(
        "\n========== GEOMETRY VALIDATION =========="
    )

    print(
        "Hourly summary columns:"
    )

    print(
        df.columns
    )

    assert "geometry" not in df.columns, (
        "Geometry column found in hourly_grid_summary."
    )

    print(
        "Geometry column : NOT PRESENT"
    )

    print(
        "Geometry validation : PASS"
    )

    return True


# ============================================================
# 15. Main Pipeline
# ============================================================

if __name__ == "__main__":

    try:

        logger.info(
            "========== SP6 PIPELINE STARTED =========="
        )

        output_root = Path(
            PROCESSED_ANALYTICS_OUTPUT_PATH
        )

        output_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        # ----------------------------------------------------
        # Load SP2 clean activity
        # ----------------------------------------------------

        activity_df = load_clean_activity(
            CLEAN_ACTIVITY_PATH
        )

        # ----------------------------------------------------
        # Prepare date
        # ----------------------------------------------------

        activity_df = prepare_date_column(
            activity_df
        )

        # ----------------------------------------------------
        # Write processed activity
        # ----------------------------------------------------

        processed_activity_path = (
            write_processed_activity(
                activity_df,
                PROCESSED_ANALYTICS_OUTPUT_PATH,
            )
        )

        # ----------------------------------------------------
        # Validate date partitions
        # ----------------------------------------------------

        validate_partitions(
            processed_activity_path
        )

        # ----------------------------------------------------
        # Load SP3 hourly grid summary
        # ----------------------------------------------------

        hourly_df = load_hourly_grid_summary(
            HOURLY_GRID_SUMMARY_PATH
        )

        # ----------------------------------------------------
        # Prepare hourly summary
        # ----------------------------------------------------

        hourly_df = prepare_hourly_grid_summary(
            hourly_df
        )

        # ----------------------------------------------------
        # Write hourly summary
        # ----------------------------------------------------

        hourly_output_path = (
            write_hourly_grid_summary(
                hourly_df,
                PROCESSED_ANALYTICS_OUTPUT_PATH,
            )
        )

        # ----------------------------------------------------
        # Validate no geometry
        # ----------------------------------------------------

        validate_no_geometry(
            hourly_output_path
        )

        # ----------------------------------------------------
        # Round-trip validation
        # ----------------------------------------------------

        validate_round_trip(
            hourly_output_path,
            hourly_df,
        )

        # ----------------------------------------------------
        # Create dashboard summary
        # ----------------------------------------------------

        dashboard_df = create_dashboard_summary(
            hourly_df
        )

        # ----------------------------------------------------
        # Write CSV
        # ----------------------------------------------------

        dashboard_csv_path = (
            write_dashboard_summary(
                dashboard_df,
                PROCESSED_ANALYTICS_OUTPUT_PATH,
            )
        )

        # ----------------------------------------------------
        # Write equivalent Parquet for size comparison
        # ----------------------------------------------------

        dashboard_parquet_path = (
            write_dashboard_parquet(
                dashboard_df,
                PROCESSED_ANALYTICS_OUTPUT_PATH,
            )
        )

        # ----------------------------------------------------
        # Compare CSV and Parquet sizes
        # ----------------------------------------------------

        compare_file_sizes(
            dashboard_csv_path,
            dashboard_parquet_path,
        )

        # ----------------------------------------------------
        # Retain GeoJSON separately
        # ----------------------------------------------------

        print(
            "\n========== STATIC GEOJSON REFERENCE =========="
        )

        print(
            "GeoJSON reference :",
            MILAN_GRID_GEOJSON_PATH,
        )

        print(
            "Geometry remains in the static "
            "GeoJSON reference."
        )

        # ----------------------------------------------------
        # Final completion
        # ----------------------------------------------------

        print(
            "\n========== SP6 COMPLETION =========="
        )

        print(
            "SP6 processed and analytics data "
            "outputs completed successfully."
        )

        print(
            "\nAcceptance evidence captured:"
        )

        print(
            "1. Clean activity written as Parquet"
        )

        print(
            "2. Activity partitioned by date"
        )

        print(
            "3. hourly_grid_summary written as Parquet"
        )

        print(
            "4. Geometry excluded from hourly analytics"
        )

        print(
            "5. Dashboard summary written as CSV"
        )

        print(
            "6. Parquet round-trip row count validated"
        )

        print(
            "7. Parquet round-trip schema validated"
        )

        print(
            "8. Duplicate grid_id + timestamp check passed"
        )

        print(
            "9. CSV vs Parquet file size compared"
        )

        print(
            "10. Static GeoJSON retained separately"
        )

        logger.info(
            "========== SP6 PIPELINE COMPLETED =========="
        )

        spark.stop()

    except Exception:

        logger.exception(
            "SP6 pipeline failed"
        )

        try:
            spark.stop()
        except Exception:
            pass

        raise