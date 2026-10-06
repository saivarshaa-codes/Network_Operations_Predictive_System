from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

DB_PATH = Path("data/warehouse/network_ops.db")
FEATURE_TABLE = "network_feature_table"

# Feature windows
RECENT_HOURS = 6
FEATURE_HOURS = 24
PRIOR_HOURS = 24

# Required source columns
REQUIRED_COLUMNS = [
    "grid_id",
    "timestamp",
    "total_activity",
    "internet_activity",
]

# Final feature columns
FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# LOAD SOURCE DATA
# ============================================================

def load_source_data(conn: sqlite3.Connection) -> pd.DataFrame:
    logger.info("Loading source data from warehouse...")

    query = """
        SELECT
            g.grid_id,
            t.timestamp,
            f.total_activity,
            f.internet_activity
        FROM fact_network_activity AS f
        JOIN dim_time AS t
            ON f.time_key = t.time_key
        JOIN dim_grid AS g
            ON f.grid_key = g.grid_key
        ORDER BY
            g.grid_id,
            t.timestamp
    """

    df = pd.read_sql_query(query, conn)

    if df.empty:
        raise ValueError("Source query returned zero rows.")

    missing = [
        col for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required source columns: {missing}"
        )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="raise",
    )

    df["total_activity"] = pd.to_numeric(
        df["total_activity"],
        errors="raise",
    )

    df["internet_activity"] = pd.to_numeric(
        df["internet_activity"],
        errors="raise",
    )

    return df


# ============================================================
# SOURCE VALIDATION
# ============================================================

def validate_source_data(df: pd.DataFrame) -> None:
    logger.info("Validating source data...")

    # --------------------------------------------------------
    # Required values
    # --------------------------------------------------------

    null_counts = df[REQUIRED_COLUMNS].isnull().sum()

    null_counts = null_counts[null_counts > 0]

    if not null_counts.empty:
        raise ValueError(
            "Null values found in required source columns:\n"
            f"{null_counts}"
        )

    # --------------------------------------------------------
    # Duplicate grid/timestamp rows
    # --------------------------------------------------------

    duplicate_count = int(
        df.duplicated(
            subset=["grid_id", "timestamp"]
        ).sum()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate "
            "(grid_id, timestamp) rows."
        )

    # --------------------------------------------------------
    # Negative activity
    # --------------------------------------------------------

    if (df["total_activity"] < 0).any():
        raise ValueError(
            "Negative total_activity values found."
        )

    if (df["internet_activity"] < 0).any():
        raise ValueError(
            "Negative internet_activity values found."
        )

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    df.sort_values(
        ["grid_id", "timestamp"],
        inplace=True,
    )

    df.reset_index(drop=True, inplace=True)

    logger.info(
        "Source validation passed: %s rows, %s grids.",
        len(df),
        df["grid_id"].nunique(),
    )


# ============================================================
# GAP REPORT
# ============================================================

def find_timestamp_gaps(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Finds missing/non-hourly transitions.

    IMPORTANT:
    We do NOT fill these gaps.

    A missing hour must not be interpreted as an observed
    zero-activity hour.
    """

    work = df[
        ["grid_id", "timestamp"]
    ].copy()

    work["previous_timestamp"] = (
        work.groupby("grid_id")["timestamp"]
        .shift(1)
    )

    work["time_difference"] = (
        work["timestamp"]
        - work["previous_timestamp"]
    )

    gaps = work[
        work["previous_timestamp"].notna()
        & (
            work["time_difference"]
            != pd.Timedelta(hours=1)
        )
    ].copy()

    return gaps


# ============================================================
# VECTORIZED FEATURE ENGINEERING
# ============================================================

def calculate_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculates leakage-safe ML2 features.

    Feature timestamp = t.

    Every feature uses observations at or before t.

    ------------------------------------------------------------
    24-hour features
    ------------------------------------------------------------

        t-23 ... t

    ------------------------------------------------------------
    Recent window
    ------------------------------------------------------------

        t-5 ... t

    ------------------------------------------------------------
    Prior baseline
    ------------------------------------------------------------

        t-29 ... t-6

    ------------------------------------------------------------
    Future target
    ------------------------------------------------------------

        t+1

    t+1 is NEVER used by this function.
    """

    logger.info("Calculating ML2 features...")

    work = df.copy()

    work.sort_values(
        ["grid_id", "timestamp"],
        inplace=True,
    )

    work.reset_index(drop=True, inplace=True)

    # ========================================================
    # IMPORTANT:
    #
    # Pandas rolling() works on ROW COUNT.
    #
    # Therefore we first create a continuous hourly sequence
    # per grid for feature calculation.
    #
    # Missing timestamps are represented as NaN.
    #
    # We DO NOT replace them with zero.
    #
    # This prevents a missing hour from becoming fake activity.
    # ========================================================

    grids = []

    for grid_id, group in work.groupby(
        "grid_id",
        sort=False,
    ):
        group = group.copy()

        group = group.set_index("timestamp")

        full_index = pd.date_range(
            start=group.index.min(),
            end=group.index.max(),
            freq="h",
        )

        group = group.reindex(full_index)

        group["grid_id"] = grid_id

        grids.append(group)

    hourly = pd.concat(grids)

    hourly.index.name = "timestamp"

    hourly.reset_index(inplace=True)

    hourly.sort_values(
        ["grid_id", "timestamp"],
        inplace=True,
    )

    hourly.reset_index(drop=True, inplace=True)

    # ========================================================
    # GROUPS
    # ========================================================

    activity = hourly.groupby(
        "grid_id",
        sort=False,
    )["total_activity"]

    internet = hourly.groupby(
        "grid_id",
        sort=False,
    )["internet_activity"]

    # ========================================================
    # 24-HOUR AVERAGE
    #
    # t-23 ... t
    #
    # min_periods=24 means ALL 24 actual hourly observations
    # must exist.
    # ========================================================

    hourly["avg_activity"] = activity.transform(
        lambda s: s.rolling(
            window=FEATURE_HOURS,
            min_periods=FEATURE_HOURS,
        ).mean()
    )

    # ========================================================
    # 24-HOUR PEAK
    # ========================================================

    rolling_peak = activity.transform(
        lambda s: s.rolling(
            window=FEATURE_HOURS,
            min_periods=FEATURE_HOURS,
        ).max()
    )

    # ========================================================
    # PEAK RATIO
    #
    # peak / average
    # ========================================================

    hourly["peak_ratio"] = np.where(
        hourly["avg_activity"] > 0,
        rolling_peak / hourly["avg_activity"],
        0.0,
    )

    # ========================================================
    # ACTIVE HOURS
    #
    # Number of observed hours with activity > 0.
    #
    # If any hour is missing, the 24-hour feature becomes NaN
    # because min_periods=24 requires all observations.
    # ========================================================

    hourly["active_hours"] = activity.transform(
        lambda s: s.gt(0).rolling(
            window=FEATURE_HOURS,
            min_periods=FEATURE_HOURS,
        ).sum()
    )

    # ========================================================
    # VARIABILITY
    #
    # Population standard deviation.
    # ========================================================

    hourly["variability"] = activity.transform(
        lambda s: s.rolling(
            window=FEATURE_HOURS,
            min_periods=FEATURE_HOURS,
        ).std(ddof=0)
    )

    # ========================================================
    # INTERNET SHARE
    #
    # sum(internet_activity)
    # ----------------------
    # sum(total_activity)
    #
    # over t-23 ... t
    # ========================================================

    rolling_internet = internet.transform(
        lambda s: s.rolling(
            window=FEATURE_HOURS,
            min_periods=FEATURE_HOURS,
        ).sum()
    )

    rolling_total = activity.transform(
        lambda s: s.rolling(
            window=FEATURE_HOURS,
            min_periods=FEATURE_HOURS,
        ).sum()
    )

    hourly["internet_share"] = np.where(
        rolling_total > 0,
        rolling_internet / rolling_total,
        0.0,
    )

    # ========================================================
    # RECENT 6-HOUR MEAN
    #
    # t-5 ... t
    # ========================================================

    recent_mean = activity.transform(
        lambda s: s.rolling(
            window=RECENT_HOURS,
            min_periods=RECENT_HOURS,
        ).mean()
    )

    # ========================================================
    # PRIOR 24-HOUR MEAN
    #
    # t-29 ... t-6
    #
    # First calculate a 24-hour rolling mean:
    #
    # at t-6 -> t-29 ... t-6
    #
    # Then shift it forward by 6 rows so that:
    #
    # at t -> t-29 ... t-6
    # ========================================================

    prior_mean = activity.transform(
        lambda s: (
            s.rolling(
                window=PRIOR_HOURS,
                min_periods=PRIOR_HOURS,
            )
            .mean()
            .shift(RECENT_HOURS)
        )
    )

    # ========================================================
    # ACTIVITY GROWTH
    #
    # recent 6h vs previous 24h
    #
    # (recent - prior) / prior
    # ========================================================

    hourly["activity_growth"] = np.where(
        prior_mean > 0,
        (recent_mean - prior_mean) / prior_mean,
        0.0,
    )

    # ========================================================
    # FEATURE TIMESTAMP
    # ========================================================

    hourly.rename(
        columns={
            "timestamp": "feature_timestamp"
        },
        inplace=True,
    )

    # ========================================================
    # REQUIRE REAL t+1
    #
    # This feature row represents prediction at t+1.
    #
    # Therefore t+1 must actually exist.
    #
    # We only retain rows whose next observed timestamp is
    # exactly one hour later.
    # ========================================================

    hourly["next_timestamp"] = (
        hourly.groupby("grid_id")[
            "feature_timestamp"
        ].shift(-1)
    )

    has_valid_next_hour = (
        hourly["next_timestamp"]
        ==
        hourly["feature_timestamp"]
        + pd.Timedelta(hours=1)
    )

    hourly = hourly[
        has_valid_next_hour
    ].copy()

    # ========================================================
    # Keep only complete feature rows.
    #
    # activity_growth requires:
    #
    # t-29 ... t
    #
    # Therefore at least 30 consecutive hourly observations
    # are required.
    # ========================================================

    hourly.dropna(
        subset=FEATURE_COLUMNS,
        inplace=True,
    )

    # ========================================================
    # Final table
    # ========================================================

    result = hourly[
        [
            "grid_id",
            "feature_timestamp",
            *FEATURE_COLUMNS,
        ]
    ].copy()

    # ========================================================
    # Remove accidental infinities
    # ========================================================

    result.replace(
        [np.inf, -np.inf],
        np.nan,
        inplace=True,
    )

    result.dropna(
        subset=FEATURE_COLUMNS,
        inplace=True,
    )

    result.reset_index(drop=True, inplace=True)

    return result


# ============================================================
# FEATURE VALIDATION
# ============================================================

def validate_feature_table(
    feature_df: pd.DataFrame,
) -> None:

    logger.info("Validating generated feature table...")

    expected_columns = [
        "grid_id",
        "feature_timestamp",
        *FEATURE_COLUMNS,
    ]

    if feature_df.columns.tolist() != expected_columns:
        raise ValueError(
            "Unexpected feature table columns.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {feature_df.columns.tolist()}"
        )

    # --------------------------------------------------------
    # Empty table
    # --------------------------------------------------------

    if feature_df.empty:
        raise ValueError(
            "Feature table contains zero rows."
        )

    # --------------------------------------------------------
    # Duplicate feature rows
    # --------------------------------------------------------

    duplicate_count = int(
        feature_df.duplicated(
            subset=[
                "grid_id",
                "feature_timestamp",
            ]
        ).sum()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate "
            "(grid_id, feature_timestamp) rows."
        )

    # --------------------------------------------------------
    # Nulls
    # --------------------------------------------------------

    if feature_df[FEATURE_COLUMNS].isnull().any().any():
        raise ValueError(
            "Null values found in generated features."
        )

    # --------------------------------------------------------
    # Infinity
    # --------------------------------------------------------

    numeric_values = feature_df[
        FEATURE_COLUMNS
    ].to_numpy()

    if not np.isfinite(numeric_values).all():
        raise ValueError(
            "Infinite values found in generated features."
        )

    # --------------------------------------------------------
    # Active hours
    # --------------------------------------------------------

    if (
        (feature_df["active_hours"] < 0)
        |
        (feature_df["active_hours"] > 24)
    ).any():
        raise ValueError(
            "active_hours contains values outside [0, 24]."
        )

    # --------------------------------------------------------
    # Internet share
    # --------------------------------------------------------

    if (
        (feature_df["internet_share"] < 0)
        |
        (feature_df["internet_share"] > 1)
    ).any():
        raise ValueError(
            "internet_share contains values outside [0, 1]."
        )

    logger.info(
        "Feature validation passed: %s rows.",
        len(feature_df),
    )


# ============================================================
# STRICT LEAKAGE TEST
# ============================================================

def test_no_future_feature_leakage() -> None:
    """
    Strong leakage test.

    We calculate features at t.

    Then we massively modify observations AFTER t.

    Features at t must remain identical.

    If any feature changes, the implementation is using
    future information and the test fails.
    """

    logger.info(
        "Running strict future-leakage test..."
    )

    start = pd.Timestamp(
        "2020-01-01 00:00:00"
    )

    timestamps = pd.date_range(
        start=start,
        periods=36,
        freq="h",
    )

    test_df = pd.DataFrame(
        {
            "grid_id": "TEST_GRID",
            "timestamp": timestamps,
            "total_activity": np.arange(
                1,
                37,
                dtype=float,
            ),
            "internet_activity": np.arange(
                1,
                37,
                dtype=float,
            ) * 0.5,
        }
    )

    validate_source_data(test_df)

    original_features = calculate_features(
        test_df
    )

    # --------------------------------------------------------
    # Choose a timestamp where:
    #
    # t-29 ... t exists
    # t+1 exists
    # --------------------------------------------------------

    test_timestamp = (
        start
        + pd.Timedelta(hours=30)
    )

    original_row = original_features[
        original_features["feature_timestamp"]
        == test_timestamp
    ]

    if len(original_row) != 1:
        raise AssertionError(
            "Leakage test could not locate expected "
            "feature timestamp."
        )

    # --------------------------------------------------------
    # Modify ONLY future rows.
    # --------------------------------------------------------

    modified_df = test_df.copy()

    future_mask = (
        modified_df["timestamp"]
        > test_timestamp
    )

    modified_df.loc[
        future_mask,
        "total_activity",
    ] = 999999999.0

    modified_df.loc[
        future_mask,
        "internet_activity",
    ] = 888888888.0

    # --------------------------------------------------------
    # Recalculate
    # --------------------------------------------------------

    modified_features = calculate_features(
        modified_df
    )

    modified_row = modified_features[
        modified_features["feature_timestamp"]
        == test_timestamp
    ]

    if len(modified_row) != 1:
        raise AssertionError(
            "Leakage test could not locate modified "
            "feature timestamp."
        )

    # --------------------------------------------------------
    # Compare every feature
    # --------------------------------------------------------

    for column in FEATURE_COLUMNS:

        original_value = float(
            original_row.iloc[0][column]
        )

        modified_value = float(
            modified_row.iloc[0][column]
        )

        if not np.isclose(
            original_value,
            modified_value,
            rtol=1e-12,
            atol=1e-12,
        ):
            raise AssertionError(
                "FUTURE DATA LEAKAGE DETECTED: "
                f"'{column}' at {test_timestamp} "
                "changed after modifying future data."
            )

    logger.info(
        "STRICT LEAKAGE TEST PASSED."
    )


# ============================================================
# SAVE TABLE
# ============================================================

def save_feature_table(
    conn: sqlite3.Connection,
    feature_df: pd.DataFrame,
) -> None:

    logger.info(
        "Replacing '%s'...",
        FEATURE_TABLE,
    )

    # --------------------------------------------------------
    # Explicit replacement
    # --------------------------------------------------------

    conn.execute(
        f"DROP TABLE IF EXISTS {FEATURE_TABLE}"
    )

    conn.commit()

    # --------------------------------------------------------
    # Write new table
    # --------------------------------------------------------

    feature_df.to_sql(
        FEATURE_TABLE,
        conn,
        if_exists="fail",
        index=False,
    )

    # --------------------------------------------------------
    # Index
    # --------------------------------------------------------

    conn.execute(
        f"""
        CREATE INDEX idx_network_feature_table_grid_time
        ON {FEATURE_TABLE}
        (
            grid_id,
            feature_timestamp
        )
        """
    )

    conn.commit()

    logger.info(
        "Created '%s' with %s rows.",
        FEATURE_TABLE,
        len(feature_df),
    )


# ============================================================
# DATABASE VERIFICATION
# ============================================================

def verify_database_table(
    conn: sqlite3.Connection,
) -> None:

    logger.info(
        "Verifying SQLite table..."
    )

    # --------------------------------------------------------
    # Row count
    # --------------------------------------------------------

    row_count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {FEATURE_TABLE}
        """
    ).fetchone()[0]

    if row_count == 0:
        raise ValueError(
            f"{FEATURE_TABLE} contains zero rows."
        )

    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    schema = pd.read_sql_query(
        f"""
        PRAGMA table_info({FEATURE_TABLE})
        """,
        conn,
    )

    actual_columns = schema[
        "name"
    ].tolist()

    expected_columns = [
        "grid_id",
        "feature_timestamp",
        *FEATURE_COLUMNS,
    ]

    if actual_columns != expected_columns:
        raise ValueError(
            "Database table schema mismatch.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {actual_columns}"
        )

    # --------------------------------------------------------
    # Duplicate check directly against SQLite
    # --------------------------------------------------------

    duplicate_count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                grid_id,
                feature_timestamp
            FROM {FEATURE_TABLE}
            GROUP BY
                grid_id,
                feature_timestamp
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_count > 0:
        raise ValueError(
            "Duplicate (grid_id, feature_timestamp) "
            "rows found in SQLite table."
        )

    logger.info(
        "Database verification passed: %s rows.",
        row_count,
    )


# ============================================================
# SUMMARY
# ============================================================

def log_summary(
    feature_df: pd.DataFrame,
) -> None:

    logger.info("-" * 70)
    logger.info("ML2 FEATURE SUMMARY")
    logger.info("-" * 70)

    logger.info(
        "Rows: %s",
        len(feature_df),
    )

    logger.info(
        "Grids: %s",
        feature_df["grid_id"].nunique(),
    )

    logger.info(
        "Feature timestamp range: %s -> %s",
        feature_df["feature_timestamp"].min(),
        feature_df["feature_timestamp"].max(),
    )

    logger.info(
        "Features: %s",
        ", ".join(FEATURE_COLUMNS),
    )

    logger.info("-" * 70)


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    logger.info("=" * 70)
    logger.info(
        "ML2 — Leakage-Safe Feature Engineering"
    )
    logger.info("=" * 70)

    # ========================================================
    # 1. Leakage test FIRST
    #
    # Do not touch the production table if this fails.
    # ========================================================

    test_no_future_feature_leakage()

    # ========================================================
    # 2. Check database
    # ========================================================

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}"
        )

    # ========================================================
    # 3. Connect
    # ========================================================

    with sqlite3.connect(DB_PATH) as conn:

        # ----------------------------------------------------
        # 4. Load
        # ----------------------------------------------------

        source_df = load_source_data(conn)

        # ----------------------------------------------------
        # 5. Validate
        # ----------------------------------------------------

        validate_source_data(source_df)

        # ----------------------------------------------------
        # 6. Report gaps
        # ----------------------------------------------------

        gaps = find_timestamp_gaps(
            source_df
        )

        if not gaps.empty:

            logger.warning(
                "Found %s timestamp gaps/non-hourly "
                "transitions in source data.",
                len(gaps),
            )

            logger.warning(
                "These gaps will NOT be filled with zero."
            )

            logger.warning(
                "\n%s",
                gaps[
                    [
                        "grid_id",
                        "previous_timestamp",
                        "timestamp",
                        "time_difference",
                    ]
                ].to_string(index=False),
            )

        else:

            logger.info(
                "No timestamp gaps found."
            )

        # ----------------------------------------------------
        # 7. Calculate features
        # ----------------------------------------------------

        feature_df = calculate_features(
            source_df
        )

        # ----------------------------------------------------
        # 8. Validate generated features
        # ----------------------------------------------------

        validate_feature_table(
            feature_df
        )

        # ----------------------------------------------------
        # 9. Save/replace table
        # ----------------------------------------------------

        save_feature_table(
            conn,
            feature_df,
        )

        # ----------------------------------------------------
        # 10. Verify SQLite
        # ----------------------------------------------------

        verify_database_table(
            conn
        )

    # ========================================================
    # 11. Summary
    # ========================================================

    log_summary(
        feature_df
    )

    logger.info("=" * 70)
    logger.info("ML2 COMPLETE")
    logger.info("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()