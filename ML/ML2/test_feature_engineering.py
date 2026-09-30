from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# Import ML2 feature engineering module
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ML2_DIR = PROJECT_ROOT / "ml" / "ml2"

sys.path.insert(0, str(ML2_DIR))

from feature_engineering import calculate_features


# ============================================================
# Test configuration
# ============================================================

GRID_ID = "4821"
START_TIMESTAMP = "2013-11-03 00:00:00"

FEATURE_TIMESTAMP_INDEX = 50

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# ============================================================
# Build deterministic test data
# ============================================================

def build_test_data() -> pd.DataFrame:
    """
    Build 72 hourly observations for one grid.

    This provides:
    - 24 hours prior history
    - 24 hours recent history
    - future observations after feature_timestamp
    """

    timestamps = pd.date_range(
        START_TIMESTAMP,
        periods=72,
        freq="h",
    )

    rows = []

    for i, timestamp in enumerate(timestamps):

        total_activity = 100.0 + (i % 12) * 10.0
        internet_activity = total_activity * 0.60

        rows.append(
            {
                "grid_id": GRID_ID,
                "timestamp": timestamp,
                "total_activity": total_activity,
                "internet_activity": internet_activity,
            }
        )

    return pd.DataFrame(rows)


# ============================================================
# Get feature row for one timestamp
# ============================================================

def get_feature_row(
    df: pd.DataFrame,
    feature_timestamp: pd.Timestamp,
) -> pd.Series:

    features = calculate_features(df)

    row = features[
        features["feature_timestamp"] == feature_timestamp
    ]

    if len(row) != 1:
        raise AssertionError(
            f"Expected exactly one feature row for "
            f"{GRID_ID} at {feature_timestamp}, "
            f"found {len(row)}."
        )

    return row.iloc[0]


# ============================================================
# Leakage test
# ============================================================

def test_future_data_does_not_change_past_features() -> None:
    """
    Features at time t must depend only on data through t.

    We first calculate features normally.

    Then we deliberately replace ALL activity after t
    with extreme values.

    Features at t must remain unchanged.

    If a feature accidentally uses future data, this test fails.
    """

    original_df = build_test_data()

    feature_timestamp = original_df.loc[
        FEATURE_TIMESTAMP_INDEX,
        "timestamp",
    ]

    future_start = feature_timestamp + pd.Timedelta(hours=1)

    # --------------------------------------------------------
    # Calculate original features
    # --------------------------------------------------------

    original_row = get_feature_row(
        original_df,
        feature_timestamp,
    )

    # --------------------------------------------------------
    # Inject extreme future data
    # --------------------------------------------------------

    future_df = original_df.copy()

    future_mask = (
        future_df["timestamp"] > feature_timestamp
    )

    future_df.loc[
        future_mask,
        "total_activity",
    ] = 1_000_000_000.0

    future_df.loc[
        future_mask,
        "internet_activity",
    ] = 900_000_000.0

    # Confirm future rows were actually modified
    assert future_df.loc[
        future_mask,
        "timestamp",
    ].min() == future_start

    # --------------------------------------------------------
    # Calculate features again
    # --------------------------------------------------------

    future_row = get_feature_row(
        future_df,
        feature_timestamp,
    )

    # --------------------------------------------------------
    # Compare every ML2 feature
    # --------------------------------------------------------

    for column in FEATURE_COLUMNS:

        original_value = float(
            original_row[column]
        )

        future_value = float(
            future_row[column]
        )

        assert np.isclose(
            original_value,
            future_value,
            rtol=1e-10,
            atol=1e-10,
        ), (
            f"LEAKAGE DETECTED: feature '{column}' "
            f"at {feature_timestamp} changed after "
            f"future data was injected.\n"
            f"Original value: {original_value}\n"
            f"After future injection: {future_value}"
        )

    print()
    print("ML2 LEAKAGE TEST PASSED")
    print(f"Grid: {GRID_ID}")
    print(f"Feature timestamp: {feature_timestamp}")
    print(f"Injected future data from: {future_start}")
    print("Result: features at t are unchanged.")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    test_future_data_does_not_change_past_features()

    print()
    print("All ML2 leakage checks passed.")