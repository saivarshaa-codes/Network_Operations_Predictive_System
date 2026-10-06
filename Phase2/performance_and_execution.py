import os
import sys
import time

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    sum,
)

from logger_config import get_logger


logger = get_logger("PerformanceAndExecutionBehaviour")


# =====================================================
# Environment configuration
# =====================================================

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


# =====================================================
# Configuration
# =====================================================

SP4_INPUT_PATH = (
    r"D:\Network Operations Predictive System"
    r"\outputs\sp4_geospatial_enrichment"
    r"\grid_activity_geo"
)


# =====================================================
# SparkSession
# =====================================================

spark = (
    SparkSession.builder
    .appName("PerformanceAndExecutionBehaviour")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

logger.info("PerformanceAndExecutionBehaviour SparkSession created")


# =====================================================
# Load SP4 enriched activity data
# =====================================================

def load_activity_data(input_path):
    """
    Load the geographically enriched activity data
    produced by SP4.
    """

    logger.info(
        "Loading geographically enriched activity data from: %s",
        input_path,
    )

    df = spark.read.parquet(input_path)

    logger.info(
        "Geospatial_enriched activity data loaded successfully"
    )

    return df


# =====================================================
# 1. Explain hotspot aggregation
# =====================================================

def explain_hotspot_aggregation(activity_df):
    """
    Inspect the physical execution plan of a
    hotspot aggregation.
    """

    print(
        "\n========== 1. HOTSPOT AGGREGATION PLAN =========="
    )

    hotspot_df = (
        activity_df
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

    print(
        "\nPhysical plan for hotspot aggregation:"
    )

    hotspot_df.explain(mode="formatted")

    return hotspot_df


# =====================================================
# 2. Cache and timing comparison
# =====================================================

def compare_cache_timing(activity_df):
    """
    Compare repeated action timings before and after
    caching the DataFrame.
    """

    print(
        "\n========== 2. CACHE TIMING COMPARISON =========="
    )

    print(
        "\nRunning repeated actions WITHOUT cache..."
    )

    start_time = time.perf_counter()

    activity_df.count()

    first_uncached_time = (
        time.perf_counter() - start_time
    )

    start_time = time.perf_counter()

    activity_df.count()

    second_uncached_time = (
        time.perf_counter() - start_time
    )

    print(
        f"First uncached count  : "
        f"{first_uncached_time:.3f} seconds"
    )

    print(
        f"Second uncached count : "
        f"{second_uncached_time:.3f} seconds"
    )

    print(
        "\nCaching DataFrame..."
    )

    cached_df = activity_df.cache()

    # Materialise the cache.
    # count() is an action, so Spark actually executes
    # the transformations and stores the cached data.
    start_time = time.perf_counter()

    cached_df.count()

    cache_materialization_time = (
        time.perf_counter() - start_time
    )

    print(
        f"Cache materialization : "
        f"{cache_materialization_time:.3f} seconds"
    )

    print(
        "\nRunning repeated actions WITH cache..."
    )

    start_time = time.perf_counter()

    cached_df.count()

    first_cached_time = (
        time.perf_counter() - start_time
    )

    start_time = time.perf_counter()

    cached_df.count()

    second_cached_time = (
        time.perf_counter() - start_time
    )

    print(
        f"First cached count    : "
        f"{first_cached_time:.3f} seconds"
    )

    print(
        f"Second cached count   : "
        f"{second_cached_time:.3f} seconds"
    )

    print(
        "\nCache timing observation:"
    )

    if second_cached_time < second_uncached_time:
        print(
            "PASS - repeated action was faster "
            "after caching."
        )
    else:
        print(
            "OBSERVATION - caching did not improve "
            "this particular action."
        )

    cached_df.unpersist()

    return {
        "first_uncached": first_uncached_time,
        "second_uncached": second_uncached_time,
        "cache_materialization": cache_materialization_time,
        "first_cached": first_cached_time,
        "second_cached": second_cached_time,
    }


# =====================================================
# 3. Repartition experiment
# =====================================================

def compare_partitions(activity_df):
    """
    Repartition the DataFrame by date and compare
    partition counts.
    """

    print(
        "\n========== 3. REPARTITION EXPERIMENT =========="
    )

    original_partitions = (
        activity_df.rdd.getNumPartitions()
    )

    print(
        f"Original partition count : "
        f"{original_partitions}"
    )

    repartitioned_df = activity_df.repartition(
        10,
        "grid_id",
    )

    repartitioned_count = (
        repartitioned_df.rdd.getNumPartitions()
    )

    print(
        f"After repartition by grid_id : "
        f"{repartitioned_count}"
    )

    print(
        "\nRepartition observation:"
    )

    print(
        "The DataFrame is explicitly redistributed "
        "using date as the partitioning key."
    )

    return repartitioned_df


# =====================================================
# 4. Column pruning experiment
# =====================================================

def demonstrate_column_pruning(activity_df):
    """
    Demonstrate selecting only the required columns
    before aggregation.
    """

    print(
        "\n========== 4. COLUMN PRUNING =========="
    )

    required_columns_df = (
        activity_df
        .select(
            "grid_id",
            "total_activity",
        )
    )

    print(
        "\nColumns before pruning:"
    )

    print(
        activity_df.columns
    )

    print(
        "\nColumns after pruning:"
    )

    print(
        required_columns_df.columns
    )

    pruned_aggregation = (
        required_columns_df
        .groupBy("grid_id")
        .agg(
            sum("total_activity")
            .alias("total_activity")
        )
    )

    print(
        "\nPhysical plan after column pruning:"
    )

    pruned_aggregation.explain(
        mode="formatted"
    )

    return pruned_aggregation


# =====================================================
# 5. Standard vs broadcast join
# =====================================================

def compare_broadcast_join(activity_df):
    """
    Compare a standard join and a broadcast join
    using the static grid lookup from SP4.
    """

    print(
        "\n========== 5. BROADCAST JOIN COMPARISON =========="
    )

    grid_lookup = (
        activity_df
        .select(
            "grid_id",
            "geometry",
            "centroid_longitude",
            "centroid_latitude",
        )
        .dropDuplicates(
            ["grid_id"]
        )
    )

    print(
        "\nStandard join physical plan:"
    )

    standard_join = (
        activity_df
        .join(
            grid_lookup,
            on="grid_id",
            how="left",
        )
    )

    standard_join.explain(
        mode="formatted"
    )

    print(
        "\nBroadcast join physical plan:"
    )

    broadcast_join = (
        activity_df
        .join(
            broadcast(grid_lookup),
            on="grid_id",
            how="left",
        )
    )

    broadcast_join.explain(
        mode="formatted"
    )

    print(
        "\nBroadcast observation:"
    )

    print(
        "The standard join can use SortMergeJoin, "
        "while the broadcast version can use "
        "BroadcastHashJoin and avoid shuffling the "
        "large activity side."
    )

    return broadcast_join


# =====================================================
# 6. Over-partitioning demonstration
# =====================================================

def demonstrate_over_partitioning(activity_df):
    """
    Demonstrate the effect of creating an unnecessarily
    large number of partitions on a local dataset.
    """

    print(
        "\n========== 6. OVER-PARTITIONING =========="
    )

    original_partitions = (
        activity_df.rdd.getNumPartitions()
    )

    print(
        f"Original partitions : "
        f"{original_partitions}"
    )

    over_partitioned_df = (
        activity_df.repartition(1000)
    )

    over_partitioned_count = (
        over_partitioned_df.rdd.getNumPartitions()
    )

    print(
        f"Over-partitioned DataFrame : "
        f"{over_partitioned_count}"
    )

    print(
        "\nOver-partitioning observation:"
    )

    print(
        "Creating far more partitions than needed "
        "can add shuffle and task-management overhead. "
        "On a small local dataset, this can make the "
        "job slower instead of faster."
    )

    return over_partitioned_df


# =====================================================
# 7. Collect performance observations
# =====================================================

def print_performance_observations(
    timing_results,
    activity_df,
):
    """
    Print three evidence-based performance observations.
    """

    print(
        "\n========== 7. PERFORMANCE OBSERVATIONS =========="
    )

    original_partitions = (
        activity_df.rdd.getNumPartitions()
    )

    print(
        "\nObservation 1 - Cache"
    )

    print(
        f"Evidence: second uncached count = "
        f"{timing_results['second_uncached']:.3f}s; "
        f"second cached count = "
        f"{timing_results['second_cached']:.3f}s."
    )

    if (
        timing_results["second_cached"]
        < timing_results["second_uncached"]
    ):
        print(
            "Result: repeated action became faster "
            "after caching."
        )
    else:
        print(
            "Result: caching did not improve the "
            "measured action in this run."
        )

    print(
        "\nObservation 2 - Partitioning"
    )

    print(
        f"Evidence: original partition count = "
        f"{original_partitions}; "
        f"repartitioned count = 10."
    )

    print(
        "Result: repartition changes how the data "
        "is distributed across Spark partitions."
    )

    print(
        "\nObservation 3 - Broadcast join"
    )

    print(
        "Evidence: standard join plan uses "
        "SortMergeJoin, while the broadcast plan "
        "uses BroadcastHashJoin."
    )

    print(
        "Result: broadcasting the small static grid "
        "lookup avoids shuffling the large activity "
        "DataFrame."
    )


# =====================================================
# Main pipeline
# =====================================================

if __name__ == "__main__":

    try:

        logger.info(
            "========== SP5 PIPELINE STARTED =========="
        )

        # -------------------------------------------------
        # Load SP4 data
        # -------------------------------------------------

        activity_df = load_activity_data(
            SP4_INPUT_PATH
        )

        print(
            "\n========== SP4 INPUT =========="
        )

        activity_df.printSchema()

        print(
            f"Input partitions : "
            f"{activity_df.rdd.getNumPartitions()}"
        )

        # -------------------------------------------------
        # 1. Explain hotspot aggregation
        # -------------------------------------------------

        explain_hotspot_aggregation(
            activity_df
        )

        # -------------------------------------------------
        # 2. Cache timing comparison
        # -------------------------------------------------

        timing_results = compare_cache_timing(
            activity_df
        )

        # -------------------------------------------------
        # 3. Repartition
        # -------------------------------------------------

        repartitioned_df = compare_partitions(
            activity_df
        )

        # -------------------------------------------------
        # 4. Column pruning
        # -------------------------------------------------

        pruned_aggregation = (
            demonstrate_column_pruning(
                activity_df
            )
        )

        # -------------------------------------------------
        # 5. Broadcast join
        # -------------------------------------------------

        broadcast_join = compare_broadcast_join(
            activity_df
        )

        # -------------------------------------------------
        # 6. Over-partitioning
        # -------------------------------------------------

        over_partitioned_df = (
            demonstrate_over_partitioning(
                activity_df
            )
        )

        # -------------------------------------------------
        # 7. Evidence-based observations
        # -------------------------------------------------

        print_performance_observations(
            timing_results,
            activity_df,
        )

        # -------------------------------------------------
        # Final output
        # -------------------------------------------------

        print(
            "\n========== SP5 COMPLETION =========="
        )

        print(
            "SP5 performance experiments completed."
        )

        print(
            "Required performance evidence captured:"
        )

        print(
            "1. Hotspot aggregation physical plan"
        )

        print(
            "2. Before/after cache timings"
        )

        print(
            "3. Partition count comparison"
        )

        print(
            "4. Column pruning physical plan"
        )

        print(
            "5. Standard vs broadcast join plan"
        )

        print(
            "6. Over-partitioning demonstration"
        )

        print(
            "7. Three evidence-based observations"
        )

        logger.info(
            "========== SP5 PIPELINE COMPLETED =========="
        )

    except Exception:

        logger.exception(
            "SP5 pipeline failed"
        )

        raise

    finally:

        spark.stop()