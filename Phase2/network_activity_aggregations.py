from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    date_format,
    hour,
    row_number,
    sum,
    avg,

    round,
    to_date,
    when,
)


from logger_config import get_logger


logger = get_logger("NetworkActivityAggregation")

import os
import sys
os.environ["HADOOP_HOME"] = r"C:\Users\saivarshaa.sujee\pyspark_demo\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\saivarshaa.sujee\pyspark_demo\hadoop\bin"
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
# =========================================================
# Configuration
# =========================================================

SP2_INPUT_PATH = (
    r"D:\Network Operations Predictive System\outputs\sp2_cleaned_network_activity"
)

SP3_OUTPUT_PATH = (
    r"D:\Network Operations Predictive System\outputs\sp3_network_aggregations"
)


# =========================================================
# SparkSession
# =========================================================

spark = (
    SparkSession.builder
    .appName("NetworkActivityAggregation")
    .getOrCreate()
)

logger.info("SP3 SparkSession created")


# =========================================================
# Load SP2 Parquet
# =========================================================

def load_sp2_data(input_path):
    """
    Load the cleaned network activity data
    produced by SP2.
    """

    logger.info(
        "Loading SP2 Parquet data from: %s",
        input_path
    )

    df = spark.read.parquet(input_path)

    logger.info(
        "SP2 Parquet data loaded successfully"
    )

    return df


# =========================================================
# Collapse country-code records
# =========================================================

def aggregate_to_grid_hour(df):
    """
    Collapse country-code-level records into
    one record per timestamp + grid_id.

    Activity measures are summed across
    country-code categories.
    """

    logger.info(
        "Starting country-code aggregation"
    )

    activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    ]

    hourly_grid_summary = (
        df
        .groupBy(
            "timestamp",
            "grid_id"
        )
        .agg(
            *[
                sum(col(column_name))
                .alias(column_name)
                for column_name in activity_columns
            ]
        )
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
        "Country-code records collapsed to grid-hour grain"
    )

    return hourly_grid_summary


# =========================================================
# Derive activity metrics
# =========================================================

def derive_activity_metrics(df):
    """
    Calculate total SMS, total calls,
    total activity and internet share.
    """

    logger.info(
        "Deriving activity metrics"
    )

    result = (
        df
        .withColumn(
            "total_sms",
            col("sms_in") + col("sms_out")
        )
        .withColumn(
            "total_calls",
            col("call_in") + col("call_out")
        )
        .withColumn(
            "total_activity",
            col("total_sms")
            + col("total_calls")
            + col("internet_activity")
        )
        .withColumn(
            "internet_share",
            round(
                when(
                    col("total_activity") == 0,
                    0
                ).otherwise(
                    col("internet_activity")
                    / col("total_activity")
                ),
                4
            )
        )
    )

    logger.info(
        "Activity metrics derived successfully"
    )

    return result


# =========================================================
# Validate canonical grain
# =========================================================

def validate_hourly_grid_grain(df, clean_row_count):
    """
    Verify that hourly_grid_summary contains
    exactly one record per timestamp + grid_id
    and that aggregation reduced the row count.
    """

    total_rows = df.count()

    distinct_grain_rows = (
        df
        .select(
            "timestamp",
            "grid_id"
        )
        .distinct()
        .count()
    )

    # Acceptance check 1: no duplicate grid_id + timestamp
    no_duplicate_grain = (
        total_rows == distinct_grain_rows
    )

    assert no_duplicate_grain, (
        "Duplicate (grid_id, timestamp) records found."
    )

    # Acceptance check 2: aggregation reduced row count
    row_count_reduced = (
        total_rows < clean_row_count
    )

    assert row_count_reduced, (
        "Hourly grid row count must be less than clean input row count."
    )

    logger.info(
        "Clean input rows: %d",
        clean_row_count
    )

    logger.info(
        "Hourly grid rows: %d",
        total_rows
    )

    logger.info(
        "Distinct timestamp + grid_id rows: %d",
        distinct_grain_rows
    )

    logger.info(
        "No duplicate grain: %s",
        no_duplicate_grain
    )

    logger.info(
        "Row count reduced: %s",
        row_count_reduced
    )

    print(
        "\n========== SP3 GRAIN VALIDATION =========="
    )

    print(
        "Clean input rows              :",
        clean_row_count
    )

    print(
        "Hourly grid rows              :",
        total_rows
    )

    print(
        "Distinct timestamp + grid_id :",
        distinct_grain_rows
    )

    print(
        "No duplicate grain            :",
        no_duplicate_grain
    )

    print(
        "Row count reduced             :",
        row_count_reduced
    )

    return no_duplicate_grain and row_count_reduced

# =========================================================
# Daily traffic summary
# =========================================================

def create_daily_traffic_summary(df):
    """
    Aggregate activity by date and grid_id for daily traffic summary
    """

    logger.info(
        "Creating daily traffic summary"
    )

    daily_traffic_summary = (
        df
        .groupBy(
            "date",
            "grid_id"
        )
        .agg(
            sum("total_sms")
            .alias("total_sms"),

            sum("total_calls")
            .alias("total_calls"),

            sum("internet_activity")
            .alias("internet_activity"),

            sum("total_activity")
            .alias("total_activity"),

            avg("total_activity")
            .alias("avg_hourly_activity")
        )
        .orderBy(
            "date",
            col("total_activity").desc()
        )
    )

    logger.info(
        "Daily traffic summary created"
    )

    return daily_traffic_summary


# =========================================================
# Top 10 high-activity grids
# =========================================================

def create_hotspot_ranking(
    daily_traffic_summary,
    start_date=None,
    end_date=None
):
    """
    Identify the top 10 highest-activity grids
    for a selected date window.
    """

    logger.info(
        "Creating hotspot ranking"
    )

    ranking_df = daily_traffic_summary

    # Apply selected window
    if start_date is not None:
        ranking_df = ranking_df.filter(
            col("date") >= start_date
        )

    if end_date is not None:
        ranking_df = ranking_df.filter(
            col("date") <= end_date
        )

    # Aggregate activity across selected window
    ranking_df = (
        ranking_df
        .groupBy("grid_id")
        .agg(
            sum("total_activity").alias("total_activity"),
            sum("total_sms").alias("total_sms"),
            sum("total_calls").alias("total_calls"),
            sum("internet_activity").alias(
                "internet_activity"
            )
        )
    )

    # Rank grids by total activity
    window_spec = Window.orderBy(
        col("total_activity").desc()
    )

    hotspot_ranking = (
        ranking_df
        .withColumn(
            "rank",
            row_number().over(window_spec)
        )
        .filter(
            col("rank") <= 10
        )
        .orderBy("rank")
    )

    logger.info(
        "Top 10 hotspot ranking created"
    )

    return hotspot_ranking


# =========================================================
# Peak activity hour
# =========================================================

def calculate_peak_activity_hour(df):
    """
    Calculate the hour with the highest
    overall network activity.

    Activity is aggregated across all grids.
    """

    logger.info(
        "Calculating peak activity hour"
    )

    peak_hour = (
        df
        .groupBy("hour")
        .agg(
            sum("total_activity")
            .alias("total_activity"),

            avg("total_activity")
            .alias("average_grid_activity")
        )
        .orderBy(
            col("total_activity").desc()
        )
        .limit(1)
    )

    logger.info(
        "Peak activity hour calculated"
    )

    return peak_hour


# =========================================================
#  Overall internet share
# =========================================================

def calculate_internet_share(df):
    """
    Calculate the share of overall activity
    contributed by internet activity.
    """

    logger.info(
        "Calculating overall internet share"
    )

    internet_share = (
        df
        .agg(
            sum("internet_activity")
            .alias("total_internet_activity"),

            sum("total_activity")
            .alias("total_network_activity")
        )
        .withColumn(
            "internet_share",
            round(
                when(
                    col("total_network_activity") == 0,
                    0
                ).otherwise(
                    col("total_internet_activity") / col("total_network_activity")
                ),
                4
            )
        )
    )

    logger.info(
        "Overall internet share calculated"
    )

    return internet_share


# =========================================================
#  Save SP3 outputs
# =========================================================

def save_sp3_outputs(
    hourly_grid_summary,
    daily_traffic_summary,
    hotspot_ranking,
):

    output_path = Path(SP3_OUTPUT_PATH)

    hourly_path = str(
        output_path / "hourly_grid_summary"
    )

    daily_path = str(
        output_path / "daily_traffic_summary"
    )

    hotspot_path = str(
        output_path / "hotspot_ranking"
    )

   

    logger.info(
        "Saving SP3 outputs"
    )

    hourly_grid_summary.write.mode(
        "overwrite"
    ).parquet(hourly_path)

    daily_traffic_summary.write.mode(
        "overwrite"
    ).parquet(daily_path)

    hotspot_ranking.write.mode( 
        "overwrite"
    ).parquet(hotspot_path)

    
    logger.info(
        "SP3 outputs saved successfully"
    )


# =========================================================
# Main
# =========================================================

if __name__ == "__main__":

    try:

        logger.info(
            "========== SP3 PIPELINE STARTED =========="
        )

        # -------------------------------------------------
        # Load SP2 output
        # -------------------------------------------------

        network_df = load_sp2_data(
            SP2_INPUT_PATH
        )
        print("\n========== SP2 INPUT COUNTS ==========")

        unique_grid_count = network_df.select("grid_id").distinct().count()
        unique_country_count = network_df.select("country_code").distinct().count()

        print("Unique grid_id     :", unique_grid_count)
        print("Unique country_code:", unique_country_count)
        print("Number of distinct dates:", network_df.select("date").distinct().count())



        print(
            "\n========== SP2 INPUT SCHEMA =========="
        )
    

        network_df.printSchema()


        # -------------------------------------------------
        # Country-code aggregation
        # -------------------------------------------------

        hourly_grid_summary = (
            aggregate_to_grid_hour(
                network_df
            )
        )

        # -------------------------------------------------
        # Activity metrics
        # -------------------------------------------------

        hourly_grid_summary = (
            derive_activity_metrics(
                hourly_grid_summary
            )
        )


        # -------------------------------------------------
        # Validate canonical grain
        # -------------------------------------------------

        clean_row_count = network_df.count()

        validate_hourly_grid_grain(
            hourly_grid_summary,
            clean_row_count
        )
        # -------------------------------------------------
        # Daily summary
        # -------------------------------------------------

        daily_traffic_summary = (
            create_daily_traffic_summary(
                hourly_grid_summary
            )
        )

        # -------------------------------------------------
        # Hotspot ranking
        # -------------------------------------------------

        hotspot_ranking = create_hotspot_ranking(
        daily_traffic_summary,
        start_date="2013-11-01",
        end_date="2013-11-03"
    )

        # -------------------------------------------------
        # Peak activity hour
        # -------------------------------------------------

        peak_hour = (
            calculate_peak_activity_hour(
                hourly_grid_summary
            )
        )

        # -------------------------------------------------
        # Internet share
        # -------------------------------------------------

        internet_share = (
            calculate_internet_share(
                hourly_grid_summary
            )
        )

        # -------------------------------------------------
        # Display results
        # -------------------------------------------------

        print(
            "\n========== HOURLY GRID SUMMARY =========="
        )

        hourly_grid_summary.show(
            10,
            truncate=False
        )

        print(
            "\n========== DAILY TRAFFIC SUMMARY =========="
        )

        daily_traffic_summary.show(
            10,
            truncate=False
        )

        print(
            "\n========== TOP 10 HOTSPOTS =========="
        )

        hotspot_ranking.show(
            10,
            truncate=False
        )

        print(
            "\n========== PEAK ACTIVITY HOUR =========="
        )

        peak_hour.show(
            truncate=False
        )

        print(
            "\n========== INTERNET SHARE =========="
        )

        internet_share.show(
            truncate=False
        )

        # -------------------------------------------------
        # Save outputs
        # -------------------------------------------------

        save_sp3_outputs(
            hourly_grid_summary,
            daily_traffic_summary,
            hotspot_ranking,
        )

        logger.info(
            "========== SP3 PIPELINE COMPLETED =========="
        )

        print(
            "\nSP3 Pipeline Completed Successfully"
        )

        spark.stop()

    except Exception:

        logger.exception(
            "SP3 pipeline failed"
        )

        try:
            spark.stop()
        except Exception:
            pass

        raise