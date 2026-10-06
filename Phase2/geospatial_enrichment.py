import json
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    countDistinct,
    max,
    min,
    round,
    sum,
)

from logger_config import get_logger

logger = get_logger("GeospatialEnrichment")


os.environ["HADOOP_HOME"] = r"C:\Users\saivarshaa.sujee\pyspark_demo\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\saivarshaa.sujee\pyspark_demo\hadoop\bin"
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


#===========================
# Configuration
#===========================
SP3_INPUT_PATH = (
    r"D:\Network Operations Predictive System\outputs\sp3_network_aggregations\hourly_grid_summary"
)

GEOJSON_PATH = (
    r"D:\Network Operations Predictive System\data\milano-grid.geojson"
)

SP4_OUTPUT_PATH = (
    r"D:\Network Operations Predictive System\outputs\sp4_geospatial_enrichment"
 
)

#=======================================
# SparkSession
#=======================================
spark = (
    SparkSession.builder
    .appName("GeospatialEnrichment")
    .config("spark.python.worker.connect.timeout", "60s")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

logger.info("SP4 SparkSession created")

#=====================================
# Load SP3 activity data
#=====================================
def load_activity_data(input_path):
    """
    Load the hourly grid activity data produced by SP3.
    """

    logger.info(
        "Loading SP3 hourly activity data from: %s",
        input_path,
    )

    df = spark.read.parquet(input_path)

    logger.info(
        "SP3 hourly activity data loaded successfully"
    )

    return df

#=============================================
# Inspect and process the Milan GeoJSON
#=============================================
def load_milan_grid_geojson(geojson_path):
    """
    Load the Milan grid GeoJSON and create a lookup
    containing grid_id and geometry.
    """

    logger.info(
        "Loading Milan grid GeoJSON from: %s",
        geojson_path,
    )

    with open(
        geojson_path,
        "r",
        encoding="utf-8",
    ) as file:
        geojson = json.load(file)

    # Structural inspection
    geojson_type = geojson.get("type")
    features = geojson.get("features", [])

    logger.info(
        "GeoJSON type: %s",
        geojson_type,
    )

    logger.info(
        "Number of GeoJSON features: %d",
        len(features),
    )

    if geojson_type != "FeatureCollection":
        raise ValueError(
            "GeoJSON top-level type must be FeatureCollection."
        )

    if not features:
        raise ValueError(
            "GeoJSON contains no features."
        )

    grid_records = []

    for feature in features:

        properties = feature.get("properties", {})
        geometry = feature.get("geometry")

        grid_id = properties.get("cellId")

        if grid_id is None:
            raise ValueError(
                "GeoJSON feature is missing properties.cellId."
            )

        if geometry is None:
            raise ValueError(
                f"Grid {grid_id} has missing geometry."
            )

        geometry_type = geometry.get("type")

        if geometry_type not in (
            "Polygon",
            "MultiPolygon",
        ):
            raise ValueError(
                f"Unexpected geometry type for grid "
                f"{grid_id}: {geometry_type}"
            )

        grid_records.append(
            (
                int(grid_id),
                json.dumps(geometry),
            )
        )

    grid_lookup = spark.createDataFrame(
        grid_records,
        [
            "grid_id",
            "geometry",
        ],
    ).cache()

    grid_lookup.count()

    logger.info(
        "Milan grid lookup created with %d records",
        len(grid_records),
    )

    return grid_lookup

#========================================
# Validate the geographic reference data
#========================================
def validate_geojson_grid(grid_lookup):
    """
    Validate grid identifiers, uniqueness, geometry presence
    and geographic coordinate ranges.
    """

    total_grids = grid_lookup.count()

    distinct_grids = (
        grid_lookup
        .select("grid_id")
        .distinct()
        .count()
    )

    missing_geometry = (
        grid_lookup
        .filter(col("geometry").isNull())
        .count()
    )

    invalid_grid_range = (
        grid_lookup
        .filter(
            (col("grid_id") < 1)
            | (col("grid_id") > 10000)
        )
        .count()
    )

    assert total_grids == distinct_grids, (
        "Duplicate grid_id values found in GeoJSON."
    )

    assert missing_geometry == 0, (
        "Missing geometry found in GeoJSON."
    )

    assert invalid_grid_range == 0, (
        "GeoJSON contains grid_id values outside 1-10000."
    )

    logger.info(
        "GeoJSON total grids: %d",
        total_grids,
    )

    logger.info(
        "GeoJSON distinct grids: %d",
        distinct_grids,
    )

    logger.info(
        "Missing geometries: %d",
        missing_geometry,
    )

    logger.info(
        "Invalid grid IDs: %d",
        invalid_grid_range,
    )

    print(
        "\n========== GEOJSON VALIDATION =========="
    )

    print(
        "Total grid features       :",
        total_grids,
    )

    print(
        "Distinct grid IDs        :",
        distinct_grids,
    )

    print(
        "Missing geometries       :",
        missing_geometry,
    )

    print(
        "Invalid grid IDs         :",
        invalid_grid_range,
    )

    return True

#==============================
# Derive polygon centroids
#==============================
def add_grid_centroids(grid_lookup):
    """
    Calculate longitude and latitude centroid coordinates
    from each grid polygon.
    """

    from shapely.geometry import shape

    records = grid_lookup.collect()

    centroid_records = []

    for row in records:

        geometry = json.loads(row["geometry"])
        polygon = shape(geometry)

        if not polygon.is_valid:
            raise ValueError(
                f"Invalid geometry for grid {row['grid_id']}."
            )

        centroid = polygon.centroid

        centroid_records.append(
            (
                row["grid_id"],
                row["geometry"],
                float(centroid.x),
                float(centroid.y),
            )
        )

    centroid_lookup = spark.createDataFrame(
        centroid_records,
        [
            "grid_id",
            "geometry",
            "centroid_longitude",
            "centroid_latitude",
        ],
    )

    logger.info(
        "Centroids calculated successfully"
    )

    return centroid_lookup
#======================================================================
#Validate the geographic spot-check for a named grid and adjacent grids
#=======================================================================
def validate_geographic_spot_check(grid_lookup):
    """
    Verify a named grid centroid and confirm that
    grid 1 and grid 2 are adjacent.
    """

    # Named grid spot-check
    named_grid = (
        grid_lookup
        .filter(col("grid_id") == 5000)
        .select(
            "grid_id",
            "centroid_longitude",
            "centroid_latitude",
        )
        .first()
    )

    print("\n========== GEOGRAPHIC SPOT-CHECK ==========")

    print(
        "Named grid centroid       :",
        named_grid,
    )

    # Grid 1 and Grid 2 comparison
    grid_1 = (
        grid_lookup
        .filter(col("grid_id") == 1)
        .select(
            "centroid_longitude",
            "centroid_latitude",
        )
        .first()
    )

    grid_2 = (
        grid_lookup
        .filter(col("grid_id") == 2)
        .select(
            "centroid_longitude",
            "centroid_latitude",
        )
        .first()
    )

    print("Grid 1 centroid           :", grid_1)
    print("Grid 2 centroid           :", grid_2)

    # They must not be identical
    assert grid_1 != grid_2, (
        "Grid 1 and Grid 2 centroids are identical."
    )

    # Grid 1 and Grid 2 should be geographically close
    longitude_difference = abs(
        grid_1["centroid_longitude"]
        - grid_2["centroid_longitude"]
    )

    latitude_difference = abs(
        grid_1["centroid_latitude"]
        - grid_2["centroid_latitude"]
    )

    assert longitude_difference < 0.01, (
        "Grid 1 and Grid 2 are not geographically adjacent."
    )

    assert latitude_difference < 0.01, (
        "Grid 1 and Grid 2 are not geographically adjacent."
    )

    print(
        "Grid 1 vs Grid 2         : PASS - adjacent"
    )

#=========================================================
# Geographic validation using actual polygon geometry
#==========================================================
def validate_geographic_geometry(grid_lookup):
    """
    Validate polygon geometry using Shapely and verify that
    all coordinates use the expected longitude/latitude order.
    """

    from shapely.geometry import shape

    records = grid_lookup.collect()

    invalid_geometry_count = 0
    invalid_coordinate_count = 0
    polygon_count = 0
    multipolygon_count = 0

    for row in records:

        geometry = json.loads(row["geometry"])
        polygon = shape(geometry)

        if not polygon.is_valid:
            invalid_geometry_count += 1

        if geometry["type"] == "Polygon":
            polygon_count += 1
        elif geometry["type"] == "MultiPolygon":
            multipolygon_count += 1

        min_x, min_y, max_x, max_y = polygon.bounds

        # GeoJSON coordinates are longitude, latitude.
        # Milan should fall within normal European longitude
        # and latitude ranges.
        if not (
            -180 <= min_x <= 180
            and -180 <= max_x <= 180
            and -90 <= min_y <= 90
            and -90 <= max_y <= 90
        ):
            invalid_coordinate_count += 1

    assert invalid_geometry_count == 0, (
        "Invalid polygon geometry detected."
    )

    assert invalid_coordinate_count == 0, (
        "Invalid longitude/latitude coordinates detected."
    )

    logger.info(
        "Valid polygon geometries: %d",
        len(records) - invalid_geometry_count,
    )

    logger.info(
        "Polygon geometries: %d",
        polygon_count,
    )

    logger.info(
        "MultiPolygon geometries: %d",
        multipolygon_count,
    )

    print(
        "\n========== GEOGRAPHIC VALIDATION =========="
    )

    print(
        "Invalid geometries       :",
        invalid_geometry_count,
    )

    print(
        "Invalid coordinates      :",
        invalid_coordinate_count,
    )

    print(
        "Polygon geometries       :",
        polygon_count,
    )

    print(
        "MultiPolygon geometries  :",
        multipolygon_count,
    )

    print(
        "Geographic validation    :",
        "PASS",
    )

    return True

# =====================================================
# Validate activity coverage before and after enrichment
# =====================================================
def validate_enrichment(
    activity_df,
    enriched_df,
    grid_lookup,
):
    """
    Validate that the left join preserves activity rows
    and that every activity grid has geographic metadata.
    """

    activity_row_count = activity_df.count()
    enriched_row_count = enriched_df.count()

    print(
        f"Activity rows before join : {activity_row_count}"
    )
    print(
        f"Activity rows after join  : {enriched_row_count}"
    )

    assert activity_row_count == enriched_row_count, (
        "Row count changed after join. "
        "The GeoJSON lookup may contain duplicate grid_id values."
    )

    print("Row count validation     : PASS")

    # Distinct activity grids
    activity_grid_count = (
        activity_df
        .select("grid_id")
        .distinct()
        .count()
    )

    # Distinct enriched grids
    enriched_grid_count = (
        enriched_df
        .select("grid_id")
        .distinct()
        .count()
    )

    # Grids without matching geometry
    missing_geometry_count = (
        enriched_df
        .filter(col("geometry").isNull())
        .select("grid_id")
        .distinct()
        .count()
    )

    coverage_percentage = (
        (
            activity_grid_count
            - missing_geometry_count
        )
        / activity_grid_count
    ) * 100

    lookup_grid_count = (
        grid_lookup
        .select("grid_id")
        .distinct()
        .count()
    )

    assert enriched_grid_count == activity_grid_count, (
        "Left join changed the number of activity grids."
    )

    assert missing_geometry_count == 0, (
        "Some activity grids could not be geographically enriched."
    )

    assert coverage_percentage == 100.0, (
        "Geospatial enrichment coverage is below 100%."
    )

    logger.info(
        "Activity grids before join: %d",
        activity_grid_count,
    )
    logger.info(
        "Activity grids after join: %d",
        enriched_grid_count,
    )
    logger.info(
        "GeoJSON lookup grids: %d",
        lookup_grid_count,
    )
    logger.info(
        "Missing geometry grids: %d",
        missing_geometry_count,
    )
    logger.info(
        "Enrichment coverage: %.2f%%",
        coverage_percentage,
    )

    print("\n========== SP4 JOIN VALIDATION ==========")
    print(
        "Activity grids before join :",
        activity_grid_count,
    )
    print(
        "Activity grids after join  :",
        enriched_grid_count,
    )
    print(
        "GeoJSON lookup grids       :",
        lookup_grid_count,
    )
    print(
        "Missing geometry grids     :",
        missing_geometry_count,
    )
    print(
        "Enrichment coverage        :",
        f"{coverage_percentage:.2f}%",
    )

    return coverage_percentage

#=======================================================
# Compare normal and broadcast joins
#=======================================================
def compare_join_plans(
    activity_df,
    grid_lookup,
):
    """
    Compare the Spark execution plans for a standard join
    and a broadcast join.
    """

    print(
        "\n========== STANDARD JOIN PLAN =========="
    )

    standard_join = activity_df.join(
        grid_lookup,
        on="grid_id",
        how="left",
    )

    standard_join.explain(
        mode="formatted"
    )

    print(
        "\n========== BROADCAST JOIN PLAN =========="
    )

    broadcast_join = activity_df.join(
        broadcast(grid_lookup),
        on="grid_id",
        how="left",
    )

    broadcast_join.explain(
        mode="formatted"
    )

    return broadcast_join

#===============================================
# Create the enriched activity dataset
#===============================================
def create_enriched_activity(
    activity_df,
    grid_lookup,
):
    """
    Enrich network activity with geographic geometry
    using a broadcast left join.
    """

    enriched_df = (
        activity_df
        .join(
            broadcast(grid_lookup),
            on="grid_id",
            how="left",
        )
        .select(
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_activity",
            "geometry",
            "centroid_longitude",
            "centroid_latitude",
        ).cache()
    )

    enriched_df.count()

    logger.info(
        "Geospatial enrichment completed"
    )

    return enriched_df

#=====================================================
# Create top high-activity grids with geometry
#=====================================================
def create_top_activity_grids(
    enriched_df,
    start_date=None,
    end_date=None,
):
    """
    Identify the top 10 high-activity grids for the
    selected date window and retain their geometry.
    """

    ranking_df = enriched_df

    if start_date is not None:
        ranking_df = ranking_df.filter(
            col("date") >= start_date
        )

    if end_date is not None:
        ranking_df = ranking_df.filter(
            col("date") <= end_date
        )

    top_grids = (
        ranking_df
        .groupBy(
            "grid_id",
            "geometry",
            "centroid_longitude",
            "centroid_latitude",
        )
        .agg(
            sum("total_activity")
            .alias("total_activity")
        )
        .orderBy(
            col("total_activity").desc()
        )
        .limit(10)
    )

    logger.info(
        "Top activity grids with geometry created"
    )

    return top_grids

#================================================
# Save SP4 outputs
#================================================
def save_sp4_outputs(
    enriched_df
):
    """
    Save the enriched activity dataset and top geographic
    hotspots as Parquet.
    """

    output_path = Path(SP4_OUTPUT_PATH)

    enriched_path = str(
        output_path / "grid_activity_geo"
    )

    enriched_df.write.mode(
        "overwrite"
    ).parquet(enriched_path)

    logger.info(
        "SP4 outputs saved successfully"
    )

#========================================
# Main pipeline
#========================================
if __name__ == "__main__":

    try:

        logger.info(
            "========== SP4 PIPELINE STARTED =========="
        )

        # Load SP3 activity data
        network_df = load_activity_data(
            SP3_INPUT_PATH
        )

        # Load and normalize GeoJSON
        grid_lookup = load_milan_grid_geojson(
            GEOJSON_PATH
        )

        print(
            "\n========== GEOJSON LOOKUP =========="
        )

        grid_lookup.show(
            5,
            truncate=False
        )

        # Validate geographic reference data
        validate_geojson_grid(
            grid_lookup
        )

        validate_geographic_geometry(
            grid_lookup
        )

        # Calculate centroids
        grid_lookup = add_grid_centroids(
            grid_lookup
        )

        validate_geographic_spot_check(
            grid_lookup
        )
        # Compare normal and broadcast plans
        compare_join_plans(
            network_df,
            grid_lookup
        )

        # Create enriched dataset
        grid_activity_geo_df = (
            create_enriched_activity(
                network_df,
                grid_lookup
            )
        )

        # Validate enrichment
        validate_enrichment(
            network_df,
            grid_activity_geo_df,
            grid_lookup
        )

        # Create top geographic hotspots
        top_activity_grids = (
            create_top_activity_grids(
                grid_activity_geo_df,
                start_date="2013-11-01",
                end_date="2013-11-03",
            )
        )

        # Display enriched data
        print(
            "\n========== GEOGRAPHICALLY ENRICHED ACTIVITY =========="
        )

        grid_activity_geo_df.show(
            10,
            truncate=False
        )

        print(
            "\n========== TOP ACTIVITY GRIDS WITH GEOMETRY =========="
        )

        top_activity_grids.show(
            10,
            truncate=False
        )

        # Save outputs
        save_sp4_outputs(
            grid_activity_geo_df
        )

        logger.info(
            "========== SP4 PIPELINE COMPLETED =========="
        )

        print(
            "\nSP4 Pipeline Completed Successfully"
        )

        spark.stop()

    except Exception:

        logger.exception(
            "SP4 pipeline failed"
        )

        try:
            spark.stop()
        except Exception:
            pass

        raise