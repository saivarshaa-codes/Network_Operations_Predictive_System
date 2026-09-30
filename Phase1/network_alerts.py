import logging
from pathlib import Path
import pandas as pd
from usage_processor import UsageProcessor
from logger_config import get_logger

logger = get_logger("NetworkAlertGenerator")

# =========================================================
# Rule Configuration
# =========================================================

HIGH_THRESHOLD = 1.5
DROP_THRESHOLD = 0.5
SPIKE_THRESHOLD = 2.0

ACTIVITY_FLOOR_PERCENTILE = 0.10


# =========================================================
# 1. Calculate Within-Day Baseline
# =========================================================

def calculate_within_day_baseline(df):
    """
    Calculate the leave-one-out median baseline.

    For every grid/hour, the current hour is excluded
    from its own baseline.
    """
    result = df.copy()
    result["baseline_activity"] = pd.NA

    for grid_id, group in result.groupby("grid_id"):
        values = group["total_activity"].tolist()
        indices = group.index.tolist()

        for position, row_index in enumerate(indices):
            # Exclude the current hour
            other_values = (
                values[:position] +
                values[position + 1:]
            )
            result.loc[row_index, "baseline_activity"] = (
                pd.Series(other_values).median()
            )

    result["baseline_activity"] = pd.to_numeric(
        result["baseline_activity"]
    )

    return result


# =========================================================
# 2. Calculate Activity Floor
# =========================================================

def calculate_activity_floor(df):
    """
    Calculate the activity floor from the
    10th percentile of daily grid activity.
    """

    daily_grid_activity = (
        df.groupby("grid_id")["total_activity"]
        .sum()
    )

    floor = daily_grid_activity.quantile(
        ACTIVITY_FLOOR_PERCENTILE
    )

    return floor


# =========================================================
# 3. Generate Alerts
# =========================================================

def generate_alerts(df):
    """
    Apply HIGH_ACTIVITY, ACTIVITY_SPIKE
    and ACTIVITY_DROP rules.
    """

    required_columns = {
        "grid_id",
        "timestamp",
        "total_activity"
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    # NP3 must operate on grid/hour data
    if df.duplicated(
        ["grid_id", "timestamp"]
    ).any():
        raise ValueError(
            "Input contains duplicate grid/hour records."
        )

    data = df.copy()

    # Sort so previous hour is correct
    data = data.sort_values(
        ["grid_id", "timestamp"]
    ).reset_index(drop=True)

    # -----------------------------------------------------
    # Baseline
    # -----------------------------------------------------

    data = calculate_within_day_baseline(data)

    # -----------------------------------------------------
    # Daily activity floor
    # -----------------------------------------------------

    activity_floor = calculate_activity_floor(data)

    daily_totals = (
        data.groupby("grid_id")["total_activity"]
        .sum()
        .rename("daily_activity")
    )

    data = data.merge(
        daily_totals,
        on="grid_id",
        how="left"
    )

    data["eligible"] = (
        data["daily_activity"] >= activity_floor
    )

    # -----------------------------------------------------
    # Previous-hour activity
    # -----------------------------------------------------

    data["previous_activity"] = (
        data.groupby("grid_id")["total_activity"]
        .shift(1)
    )

    alerts = []

    # -----------------------------------------------------
    # Evaluate every grid/hour
    # -----------------------------------------------------

    for _, row in data.iterrows():

        if not row["eligible"]:
            continue

        current = row["total_activity"]
        baseline = row["baseline_activity"]
        previous = row["previous_activity"]

        grid_id = row["grid_id"]
        timestamp = row["timestamp"]

        # -------------------------------------------------
        # HIGH_ACTIVITY
        # -------------------------------------------------

        if current > baseline * HIGH_THRESHOLD:

            alerts.append({
                "grid_id": grid_id,
                "timestamp": timestamp,
                "alert_type": "HIGH_ACTIVITY",
                "current_activity": current,
                "baseline_activity": baseline,
                "reason": (
                    f"HIGH_ACTIVITY: current activity "
                    f"{current:.2f} is above "
                    f"{HIGH_THRESHOLD:.1f}x baseline activity "
                    f"{baseline:.2f}"
                )
            })

        # -------------------------------------------------
        # ACTIVITY_DROP
        # -------------------------------------------------

        if current < baseline * DROP_THRESHOLD:

            alerts.append({
                "grid_id": grid_id,
                "timestamp": timestamp,
                "alert_type": "ACTIVITY_DROP",
                "current_activity": current,
                "baseline_activity": baseline,
                "reason": (
                    f"ACTIVITY_DROP: current activity "
                    f"{current:.2f} is below "
                    f"{DROP_THRESHOLD:.1f}x baseline activity "
                    f"{baseline:.2f}"
                )
            })

        # -------------------------------------------------
        # ACTIVITY_SPIKE
        # -------------------------------------------------

        if (
            pd.notna(previous)
            and previous > 0
            and current > previous * SPIKE_THRESHOLD
        ):

            alerts.append({
                "grid_id": grid_id,
                "timestamp": timestamp,
                "alert_type": "ACTIVITY_SPIKE",
                "current_activity": current,
                "baseline_activity": baseline,
                "reason": (
                    f"ACTIVITY_SPIKE: current activity "
                    f"{current:.2f} is more than "
                    f"{SPIKE_THRESHOLD:.1f}x the previous-hour "
                    f"activity {previous:.2f}; "
                    f"baseline activity is {baseline:.2f}"
                )
            })

    alerts_df = pd.DataFrame(alerts)

    return alerts_df, activity_floor


# =========================================================
# 4. Validate Alerts
# =========================================================

def validate_alerts(alerts_df, grid_hour_data):
    """Run NP3 acceptance checks."""

    required_columns = [
        "grid_id",
        "timestamp",
        "alert_type",
        "current_activity",
        "baseline_activity",
        "reason"
    ]

    missing = [
        column
        for column in required_columns
        if column not in alerts_df.columns
    ]

    if missing:
        raise AssertionError(
            f"Alert output missing columns: {missing}"
        )

    # Every alert grid must exist in analytics data
    valid_grids = set(
        grid_hour_data["grid_id"].unique()
    )

    invalid_grids = set(
        alerts_df["grid_id"].unique()
    ) - valid_grids

    if invalid_grids:
        raise AssertionError(
            f"Unknown grid_id values in alerts: "
            f"{sorted(invalid_grids)}"
        )

    # Every alert must have a human-readable reason
    if alerts_df["reason"].isna().any():
        raise AssertionError(
            "Some alerts have no reason."
        )

    if (alerts_df["reason"].str.len() == 0).any():
        raise AssertionError(
            "Some alerts have an empty reason."
        )

    # Current and baseline must be present
    if alerts_df[
        ["current_activity", "baseline_activity"]
    ].isna().any().any():
        raise AssertionError(
            "Alert contains missing activity values."
        )

    logger.info(
        "VALIDATION PASSED: validate_alerts()"
    )

# =========================================================
# 5. Operational Summary
# =========================================================

def print_summary(alerts_df, grid_hour_data):
    """Print short operational alert summary."""

    total_grid_hours = len(grid_hour_data)
    total_alerts = len(alerts_df)
    alerted_grid_hours =0


    if total_grid_hours == 0:
        alert_proportion = 0
    else:
        alerted_grid_hours = (
            alerts_df[
                ["grid_id", "timestamp"]
            ]
            .drop_duplicates()
            .shape[0]
        )

        alert_proportion = (
            alerted_grid_hours /
            total_grid_hours
        )

    print("\n========== NP3 ALERT SUMMARY ==========")

    print("\nAlerts by type:")
    print(
        alerts_df["alert_type"]
        .value_counts()
        if not alerts_df.empty
        else "No alerts generated."
    )

    print("\nTop 10 grids by alert count:")

    if not alerts_df.empty:
        print(
            alerts_df["grid_id"]
            .value_counts()
            .head(10)
        )
    else:
        print("No alerts generated.")

    print(
        f"\nAlerted grid-hours: "
        f"{alerted_grid_hours if total_grid_hours else 0}"
        f" / {total_grid_hours}"
    )

    print(
        f"Proportion of grid-hours alerted: "
        f"{alert_proportion:.2%}"
    )

    print("=======================================\n")



# =========================================================
# 7. Main Execution
# =========================================================

if __name__ == "__main__":

    # -----------------------------------------------------
    # NP2: Create grid/hour analytics
    # -----------------------------------------------------

    processor = UsageProcessor(
        file_path="D:\\Network Operations Predictive System\\data\\sms-call-internet-mi-2013-11-01.csv"
    )

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.derive_activity_features()
    processor.aggregate_to_grid_time()

    grid_hour_data = processor.grid_hour_data

    # -----------------------------------------------------
    # NP3: Generate alerts
    # -----------------------------------------------------

    alerts_df, activity_floor = generate_alerts(
        grid_hour_data
    )

    logger.info(
        "activity_floor=%f",
        activity_floor
    )

    logger.info(
        "input_grid_hour_rows=%d",
        len(grid_hour_data)
    )

    logger.info(
        "alert_rows=%d",
        len(alerts_df)
    )

    # -----------------------------------------------------
    # Validate
    # -----------------------------------------------------

    validate_alerts(
        alerts_df,
        grid_hour_data
    )

    # -----------------------------------------------------
    # Export alerts
    # -----------------------------------------------------

    output_dir = Path("outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    alert_path = (
        output_dir /
        "network_alerts.csv"
    )

    alerts_df.to_csv(
        alert_path,
        index=False
    )

    logger.info(
        "alerts_output=%s",
        alert_path
    )

    # -----------------------------------------------------
    # Summary
    # -----------------------------------------------------

    print_summary(
        alerts_df,
        grid_hour_data
    )

