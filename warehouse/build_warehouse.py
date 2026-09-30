from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# DE6 — WAREHOUSE BUILD
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

HOURLY_SUMMARY = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "hourly_grid_summary"
)

REFERENCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "milano-grid.geojson"
)

WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
DB_PATH = WAREHOUSE_DIR / "network_ops.db"

BATCH_SIZE = 100000


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("DE6_Warehouse_Load")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HELPERS
# ============================================================

def normalize_grid_id(value):
    """Keep grid IDs consistent between Spark and GeoJSON."""
    if value is None:
        return None

    value = str(value).strip()

    if value.endswith(".0"):
        value = value[:-2]

    return value


def load_geojson():
    """Load the static Milan grid reference."""
    with open(REFERENCE_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def get_centroid(geometry):
    """
    Calculate an approximate centroid from polygon vertices.

    The centroid is stored only in dim_grid.
    Full polygon geometry is not stored in fact_network_activity.
    """

    if not geometry:
        return None, None

    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates", [])

    if geometry_type == "Polygon":
        rings = coordinates

    elif geometry_type == "MultiPolygon":
        rings = [
            ring
            for polygon in coordinates
            for ring in polygon
        ]

    else:
        return None, None

    points = []

    for ring in rings:
        for point in ring:
            if len(point) >= 2:
                longitude, latitude = point[0], point[1]
                points.append((longitude, latitude))

    if not points:
        return None, None

    longitude = sum(point[0] for point in points) / len(points)
    latitude = sum(point[1] for point in points) / len(points)

    return latitude, longitude


# ============================================================
# START
# ============================================================

print("\nDE6 — WAREHOUSE BUILD")
print("=" * 60)

print(f"SOURCE={HOURLY_SUMMARY}")
print(f"REFERENCE={REFERENCE_PATH}")
print(f"DATABASE={DB_PATH}")
print(f"BATCH_SIZE={BATCH_SIZE}")


# ============================================================
# VALIDATE INPUT PATHS
# ============================================================

if not HOURLY_SUMMARY.exists():
    raise FileNotFoundError(
        f"Hourly summary does not exist: {HOURLY_SUMMARY}"
    )

if not REFERENCE_PATH.exists():
    raise FileNotFoundError(
        f"GeoJSON reference does not exist: {REFERENCE_PATH}"
    )


# ============================================================
# READ SPARK OUTPUT
# ============================================================

print("\nReading Spark output...")

source_df = spark.read.parquet(str(HOURLY_SUMMARY))

source_df = (
    source_df
    .withColumn("grid_id", F.col("grid_id").cast("string"))
    .withColumn("timestamp", F.to_timestamp("timestamp"))
)

required_columns = {
    "grid_id",
    "timestamp",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_sms",
    "total_calls",
    "total_activity",
    "internet_share",
}

missing = required_columns - set(source_df.columns)

if missing:
    raise RuntimeError(
        f"Missing required source columns: {sorted(missing)}"
    )


# ============================================================
# SOURCE VALIDATION
# ============================================================

source_count = source_df.count()

duplicate_source_groups = (
    source_df
    .groupBy("grid_id", "timestamp")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_source_groups != 0:
    raise AssertionError(
        f"FAIL: source contains {duplicate_source_groups} "
        f"duplicate grid/timestamp groups"
    )

print(f"SOURCE_ROWS={source_count}")
print("CHECK_SOURCE_GRAIN=PASS")


source_grid_ids = {
    normalize_grid_id(row["grid_id"])
    for row in (
        source_df
        .select("grid_id")
        .distinct()
        .collect()
    )
}

print(f"SOURCE_DISTINCT_GRIDS={len(source_grid_ids)}")


# Cache because the source is reused for dimensions,
# fact loading and validation.
source_df = source_df.cache()
source_df.count()


# ============================================================
# CREATE WAREHOUSE
# ============================================================

WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

if DB_PATH.exists():
    DB_PATH.unlink()

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Enable foreign-key enforcement in SQLite.
cursor.execute("PRAGMA foreign_keys = ON")


# ============================================================
# TABLES
# ============================================================

cursor.executescript(
    """
    CREATE TABLE dim_time (
        time_key INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL UNIQUE,
        date TEXT NOT NULL,
        hour INTEGER NOT NULL,
        day_of_week INTEGER NOT NULL
    );

    CREATE TABLE dim_grid (
        grid_key INTEGER PRIMARY KEY,
        grid_id TEXT NOT NULL UNIQUE,
        centroid_latitude REAL,
        centroid_longitude REAL,
        geometry_reference TEXT
    );

    CREATE TABLE fact_network_activity (
        time_key INTEGER NOT NULL,
        grid_key INTEGER NOT NULL,

        sms_in REAL NOT NULL,
        sms_out REAL NOT NULL,
        call_in REAL NOT NULL,
        call_out REAL NOT NULL,
        internet_activity REAL NOT NULL,

        total_sms REAL NOT NULL,
        total_calls REAL NOT NULL,
        total_activity REAL NOT NULL,
        internet_share REAL,

        PRIMARY KEY (time_key, grid_key),

        FOREIGN KEY (time_key)
            REFERENCES dim_time(time_key),

        FOREIGN KEY (grid_key)
            REFERENCES dim_grid(grid_key)
    );
    """
)

conn.commit()

print("TABLES_CREATED=PASS")


# ============================================================
# DIM_TIME
# ============================================================

print("\nBuilding dim_time...")

time_df = (
    source_df
    .select("timestamp")
    .distinct()
    .orderBy("timestamp")
    .withColumn("date", F.to_date("timestamp"))
    .withColumn("hour", F.hour("timestamp"))
    .withColumn("day_of_week", F.dayofweek("timestamp"))
)

time_rows = time_df.collect()

time_key_map = {}

for key, row in enumerate(time_rows, start=1):

    timestamp = row["timestamp"].strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    date_value = row["date"].strftime(
        "%Y-%m-%d"
    )

    time_key_map[timestamp] = key

    cursor.execute(
        """
        INSERT INTO dim_time
        (
            time_key,
            timestamp,
            date,
            hour,
            day_of_week
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            key,
            timestamp,
            date_value,
            row["hour"],
            row["day_of_week"],
        ),
    )

conn.commit()

print(f"DIM_TIME_ROWS={len(time_rows)}")


# ============================================================
# DIM_GRID
# ============================================================

print("\nBuilding dim_grid from GeoJSON...")

geojson = load_geojson()

grid_rows = []

for feature in geojson.get("features", []):

    properties = feature.get("properties", {})
    geometry = feature.get("geometry")

    grid_id = (
        properties.get("grid_id")
        or properties.get("CellID")
        or properties.get("cellId")
        or properties.get("cell_id")
        or properties.get("id")
    )

    if grid_id is None:
        continue

    grid_id = normalize_grid_id(grid_id)

    latitude, longitude = get_centroid(geometry)

    grid_rows.append(
        (
            grid_id,
            latitude,
            longitude,
            REFERENCE_PATH.name,
        )
    )


# Remove duplicate IDs from reference.
grid_map = {}

for row in grid_rows:
    grid_map[row[0]] = row

grid_rows = list(grid_map.values())

reference_grid_ids = {
    row[0]
    for row in grid_rows
}

print(f"REFERENCE_DISTINCT_GRIDS={len(reference_grid_ids)}")


# Validate that source and reference contain the same grids.
missing_in_reference = source_grid_ids - reference_grid_ids
extra_in_reference = reference_grid_ids - source_grid_ids

if missing_in_reference:
    raise AssertionError(
        "FAIL: source grids missing from GeoJSON: "
        f"{sorted(missing_in_reference)[:10]}"
    )

if extra_in_reference:
    raise AssertionError(
        "FAIL: GeoJSON contains grids absent from source: "
        f"{sorted(extra_in_reference)[:10]}"
    )

print("CHECK_SOURCE_REFERENCE_GRIDS=PASS")


for key, row in enumerate(grid_rows, start=1):

    grid_id, latitude, longitude, geometry_ref = row

    cursor.execute(
        """
        INSERT INTO dim_grid
        (
            grid_key,
            grid_id,
            centroid_latitude,
            centroid_longitude,
            geometry_reference
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            key,
            grid_id,
            latitude,
            longitude,
            geometry_ref,
        ),
    )

conn.commit()


grid_key_map = {
    row[1]: row[0]
    for row in cursor.execute(
        "SELECT grid_key, grid_id FROM dim_grid"
    ).fetchall()
}

print(f"DIM_GRID_ROWS={len(grid_rows)}")


# ============================================================
# FACT LOAD
# ============================================================

print("\nLoading fact_network_activity...")
print(f"Batch size = {BATCH_SIZE}")

fact_df = source_df.select(
    "grid_id",
    "timestamp",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_sms",
    "total_calls",
    "total_activity",
    "internet_share",
)

batch = []
fact_count = 0

for row in fact_df.toLocalIterator():

    timestamp = row["timestamp"].strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    grid_id = normalize_grid_id(row["grid_id"])

    time_key = time_key_map.get(timestamp)
    grid_key = grid_key_map.get(grid_id)

    if time_key is None:
        raise RuntimeError(
            f"No dim_time key for {timestamp}"
        )

    if grid_key is None:
        raise RuntimeError(
            f"No dim_grid key for {grid_id}"
        )

    batch.append(
        (
            time_key,
            grid_key,
            float(row["sms_in"] or 0),
            float(row["sms_out"] or 0),
            float(row["call_in"] or 0),
            float(row["call_out"] or 0),
            float(row["internet_activity"] or 0),
            float(row["total_sms"] or 0),
            float(row["total_calls"] or 0),
            float(row["total_activity"] or 0),
            (
                float(row["internet_share"])
                if row["internet_share"] is not None
                else None
            ),
        )
    )

    if len(batch) >= BATCH_SIZE:

        cursor.executemany(
            """
            INSERT INTO fact_network_activity
            (
                time_key,
                grid_key,
                sms_in,
                sms_out,
                call_in,
                call_out,
                internet_activity,
                total_sms,
                total_calls,
                total_activity,
                internet_share
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )

        conn.commit()

        fact_count += len(batch)

        print(
            f"FACT_BATCH_INSERTED={len(batch)} "
            f"TOTAL={fact_count}"
        )

        batch.clear()


# Insert remaining rows.
if batch:

    cursor.executemany(
        """
        INSERT INTO fact_network_activity
        (
            time_key,
            grid_key,
            sms_in,
            sms_out,
            call_in,
            call_out,
            internet_activity,
            total_sms,
            total_calls,
            total_activity,
            internet_share
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        batch,
    )

    conn.commit()

    fact_count += len(batch)

    print(
        f"FACT_BATCH_INSERTED={len(batch)} "
        f"TOTAL={fact_count}"
    )


# ============================================================
# INDEXES
# ============================================================

print("\nCreating indexes...")

cursor.executescript(
    """
    CREATE INDEX idx_fact_grid
        ON fact_network_activity(grid_key);

    CREATE INDEX idx_fact_time
        ON fact_network_activity(time_key);

    CREATE INDEX idx_fact_grid_time
        ON fact_network_activity(grid_key, time_key);

    CREATE INDEX idx_dim_time_timestamp
        ON dim_time(timestamp);
    """
)

conn.commit()

print("INDEXES_CREATED=PASS")


# ============================================================
# ACCEPTANCE CHECKS
# ============================================================

print("\n")
print("=" * 60)
print("DE6 ACCEPTANCE CHECKS")
print("=" * 60)


# ------------------------------------------------------------
# CHECK 1 — FACT HAS NO GEOMETRY
# ------------------------------------------------------------

fact_columns = [
    row[1]
    for row in cursor.execute(
        "PRAGMA table_info(fact_network_activity)"
    ).fetchall()
]

if any("geometry" in column.lower() for column in fact_columns):
    raise AssertionError(
        "FAIL: geometry exists in fact_network_activity"
    )

print("CHECK_1_FACT_NO_GEOMETRY=PASS")


# ------------------------------------------------------------
# CHECK 2 — DIM_GRID COUNT + UNIQUENESS
# ------------------------------------------------------------

warehouse_grid_count = cursor.execute(
    "SELECT COUNT(*) FROM dim_grid"
).fetchone()[0]

duplicate_grid_keys = cursor.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT grid_id
        FROM dim_grid
        GROUP BY grid_id
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if warehouse_grid_count != len(source_grid_ids):
    raise AssertionError(
        f"FAIL: dim_grid={warehouse_grid_count}, "
        f"source_distinct_grids={len(source_grid_ids)}"
    )

if duplicate_grid_keys != 0:
    raise AssertionError(
        "FAIL: duplicate grid_id values in dim_grid"
    )

print(
    f"CHECK_2_DIM_GRID=PASS "
    f"({warehouse_grid_count} grids, no duplicates)"
)


# ------------------------------------------------------------
# CHECK 3 — FACT ROW COUNT
# ------------------------------------------------------------

warehouse_fact_count = cursor.execute(
    """
    SELECT COUNT(*)
    FROM fact_network_activity
    """
).fetchone()[0]

if warehouse_fact_count != source_count:
    raise AssertionError(
        f"FAIL: fact={warehouse_fact_count}, "
        f"source={source_count}"
    )

print(
    f"CHECK_3_FACT_ROW_COUNT=PASS "
    f"({warehouse_fact_count})"
)


# ------------------------------------------------------------
# CHECK 4 — FACT GRAIN / NO FAN-OUT
# ------------------------------------------------------------

duplicate_facts = cursor.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT time_key, grid_key
        FROM fact_network_activity
        GROUP BY time_key, grid_key
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if duplicate_facts != 0:
    raise AssertionError(
        "FAIL: duplicate fact grain detected"
    )

print("CHECK_4_NO_FACT_FANOUT=PASS")


# ------------------------------------------------------------
# CHECK 5 — SPARK VS SQL AGGREGATE
# ------------------------------------------------------------

spark_total = (
    source_df
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet_activity").alias("internet_activity"),
        F.sum("total_sms").alias("total_sms"),
        F.sum("total_calls").alias("total_calls"),
        F.sum("total_activity").alias("total_activity"),
    )
    .collect()[0]
)

sql_total = cursor.execute(
    """
    SELECT
        SUM(sms_in),
        SUM(sms_out),
        SUM(call_in),
        SUM(call_out),
        SUM(internet_activity),
        SUM(total_sms),
        SUM(total_calls),
        SUM(total_activity)
    FROM fact_network_activity
    """
).fetchone()


def assert_equal(name, spark_value, sql_value, tolerance=1e-4):
    if abs(float(spark_value) - float(sql_value)) > tolerance:
        raise AssertionError(
            f"FAIL: {name}: Spark={spark_value}, SQL={sql_value}"
        )


aggregate_columns = [
    ("sms_in", spark_total["sms_in"], sql_total[0]),
    ("sms_out", spark_total["sms_out"], sql_total[1]),
    ("call_in", spark_total["call_in"], sql_total[2]),
    ("call_out", spark_total["call_out"], sql_total[3]),
    (
        "internet_activity",
        spark_total["internet_activity"],
        sql_total[4],
    ),
    ("total_sms", spark_total["total_sms"], sql_total[5]),
    ("total_calls", spark_total["total_calls"], sql_total[6]),
    (
        "total_activity",
        spark_total["total_activity"],
        sql_total[7],
    ),
]

for name, spark_value, sql_value in aggregate_columns:
    assert_equal(name, spark_value, sql_value)

print("CHECK_5_SPARK_SQL_AGGREGATE=PASS")


# ------------------------------------------------------------
# CHECK 6 — INDEXES
# ------------------------------------------------------------

indexes = cursor.execute(
    """
    SELECT name
    FROM sqlite_master
    WHERE type = 'index'
      AND name NOT LIKE 'sqlite_%'
    """
).fetchall()

index_names = {row[0] for row in indexes}

required_indexes = {
    "idx_fact_grid",
    "idx_fact_time",
    "idx_fact_grid_time",
    "idx_dim_time_timestamp",
}

missing_indexes = required_indexes - index_names

if missing_indexes:
    raise AssertionError(
        f"FAIL: missing indexes: {sorted(missing_indexes)}"
    )

print(
    f"CHECK_6_INDEXES=PASS "
    f"({len(index_names)} indexes)"
)


# ------------------------------------------------------------
# CHECK 7 — FOREIGN KEY INTEGRITY
# ------------------------------------------------------------

foreign_key_errors = cursor.execute(
    "PRAGMA foreign_key_check"
).fetchall()

if foreign_key_errors:
    raise AssertionError(
        f"FAIL: foreign-key violations: {foreign_key_errors[:5]}"
    )

print("CHECK_7_FOREIGN_KEYS=PASS")


# ============================================================
# SAMPLE ANALYTICS QUERIES
# ============================================================

print("\n")
print("=" * 60)
print("SAMPLE SQL QUERIES")
print("=" * 60)


# ------------------------------------------------------------
# QUERY 1 — TOP GRIDS
# ------------------------------------------------------------

print("\n1. TOP 10 GRIDS BY TOTAL ACTIVITY")

rows = cursor.execute(
    """
    SELECT
        g.grid_id,
        SUM(f.total_activity) AS total_activity
    FROM fact_network_activity f
    JOIN dim_grid g
        ON f.grid_key = g.grid_key
    GROUP BY g.grid_id
    ORDER BY total_activity DESC
    LIMIT 10
    """
).fetchall()

for row in rows:
    print(row)


# ------------------------------------------------------------
# QUERY 2 — HOURLY TREND
# ------------------------------------------------------------

print("\n2. HOURLY ACTIVITY TREND")

rows = cursor.execute(
    """
    SELECT
        t.timestamp,
        SUM(f.total_activity) AS total_activity
    FROM fact_network_activity f
    JOIN dim_time t
        ON f.time_key = t.time_key
    GROUP BY t.timestamp
    ORDER BY t.timestamp
    LIMIT 10
    """
).fetchall()

for row in rows:
    print(row)


# ------------------------------------------------------------
# QUERY 3 — INTERNET-HEAVY WINDOWS
# ------------------------------------------------------------

print("\n3. INTERNET-HEAVY WINDOWS")

rows = cursor.execute(
    """
    SELECT
        t.timestamp,
        SUM(f.internet_activity) AS internet_activity
    FROM fact_network_activity f
    JOIN dim_time t
        ON f.time_key = t.time_key
    GROUP BY t.timestamp
    ORDER BY internet_activity DESC
    LIMIT 10
    """
).fetchall()

for row in rows:
    print(row)


# ============================================================
# FINAL VALIDATION
# ============================================================

print("\n")
print("=" * 60)
print("DE6 COMPLETE")
print("=" * 60)

print(f"DATABASE={DB_PATH}")
print(f"DIM_TIME_ROWS={len(time_rows)}")
print(f"DIM_GRID_ROWS={warehouse_grid_count}")
print(f"FACT_ROWS={warehouse_fact_count}")
print(f"BATCH_SIZE={BATCH_SIZE}")
print("STATUS=SUCCESS")


# ============================================================
# CLEANUP
# ============================================================

conn.close()

source_df.unpersist()

spark.stop()

