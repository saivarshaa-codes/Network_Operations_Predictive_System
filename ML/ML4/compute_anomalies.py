from __future__ import annotations

import json
import pickle
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "network_ops.db"

NP3_ALERTS_PATH = PROJECT_ROOT / "outputs" / "network_alerts.csv"

if not NP3_ALERTS_PATH.exists():
    NP3_ALERTS_PATH = (
        PROJECT_ROOT
        / "phase1"
        / "outputs"
        / "network_alerts.csv"
    )

if not NP3_ALERTS_PATH.exists():
    NP3_ALERTS_PATH = (
        PROJECT_ROOT
        / "Phase1"
        / "outputs"
        / "network_alerts.csv"
    )

ML3_MODEL_PATH = (
    PROJECT_ROOT
    / "ML"
    / "ML3"
    / "outputs"
    / "risk_classifier.pkl"
)

ML3_PREDICTIONS_PATH = (
    PROJECT_ROOT
    / "ML"
    / "ML3"
    / "outputs"
    / "risk_predictions.csv"
)

ML4_DIR = PROJECT_ROOT / "ML" / "ML4"

ANOMALY_OUTPUT_PATH = (
    ML4_DIR / "network_anomaly_scores.csv"
)

COMPARISON_OUTPUT_PATH = (
    ML4_DIR / "ml4_three_way_comparison.csv"
)

REPORT_PATH = (
    ML4_DIR / "ml4_anomaly_report.md"
)

JSON_REPORT_PATH = (
    ML4_DIR / "ml4_anomaly_evaluation.json"
)

HIGH_THRESHOLD = 0.50
LOW_THRESHOLD = -0.50


def calculate_generalized_baseline(
    df: pd.DataFrame,
    group_keys: list[str],
) -> pd.DataFrame:
    work = df.copy()

    work["baseline_activity"] = (
        work
        .groupby(group_keys)["total_activity"]
        .transform("median")
    )

    global_grid_median = (
        work
        .groupby("grid_id")["total_activity"]
        .transform("median")
    )

    work["baseline_activity"] = (
        work["baseline_activity"]
        .fillna(global_grid_median)
    )

    return work


def load_warehouse_activity() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            """
            SELECT
                CAST(g.grid_id AS TEXT) AS grid_id,
                t.timestamp,
                f.total_activity
            FROM fact_network_activity AS f
            JOIN dim_grid AS g
                ON f.grid_key = g.grid_key
            JOIN dim_time AS t
                ON f.time_key = t.time_key
            ORDER BY grid_id, timestamp
            """,
            conn,
        )

    if df.empty:
        raise ValueError(
            "No warehouse activity rows were found."
        )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="raise",
    )

    df["grid_id"] = df["grid_id"].astype(str)

    return df


def verify_bucket_history(
    df: pd.DataFrame,
) -> dict:
    work = df.copy()

    work["hour_of_day"] = (
        work["timestamp"].dt.hour
    )

    bucket_counts = (
        work
        .groupby(
            ["grid_id", "hour_of_day"]
        )
        .size()
    )

    if bucket_counts.empty:
        raise ValueError(
            "No grid/hour-of-day buckets were found."
        )

    result = {
        "minimum_observations": int(
            bucket_counts.min()
        ),
        "maximum_observations": int(
            bucket_counts.max()
        ),
        "average_observations": float(
            bucket_counts.mean()
        ),
        "bucket_count": int(
            len(bucket_counts)
        ),
    }

    return result


def compute_anomalies(
    df: pd.DataFrame,
) -> pd.DataFrame:
    work = df.copy()

    work["percentage_deviation"] = np.where(
        work["baseline_activity"] > 0,
        (
            work["total_activity"]
            - work["baseline_activity"]
        )
        / work["baseline_activity"],
        0.0,
    )

    work["anomaly_score"] = (
        work["percentage_deviation"]
        .abs()
    )

    work["direction"] = "normal"

    work.loc[
        work["percentage_deviation"]
        > HIGH_THRESHOLD,
        "direction",
    ] = "high"

    work.loc[
        work["percentage_deviation"]
        < LOW_THRESHOLD,
        "direction",
    ] = "low"

    work["reason"] = (
        "NORMAL: Activity within expected historical bounds."
    )

    high_mask = (
        work["direction"] == "high"
    )

    low_mask = (
        work["direction"] == "low"
    )

    work.loc[high_mask, "reason"] = (
        "ANOMALY_HIGH: Current activity "
        + work.loc[
            high_mask,
            "total_activity",
        ].map(
            lambda x: f"{x:.2f}"
        )
        + " is "
        + (
            work.loc[
                high_mask,
                "percentage_deviation",
            ]
            * 100
        ).map(
            lambda x: f"{x:.1f}%"
        )
        + " above the historical hour-of-day "
        "baseline of "
        + work.loc[
            high_mask,
            "baseline_activity",
        ].map(
            lambda x: f"{x:.2f}"
        )
        + "."
    )

    work.loc[low_mask, "reason"] = (
        "ANOMALY_LOW: Current activity "
        + work.loc[
            low_mask,
            "total_activity",
        ].map(
            lambda x: f"{x:.2f}"
        )
        + " is "
        + (
            work.loc[
                low_mask,
                "percentage_deviation",
            ].abs()
            * 100
        ).map(
            lambda x: f"{x:.1f}%"
        )
        + " below the historical hour-of-day "
        "baseline of "
        + work.loc[
            low_mask,
            "baseline_activity",
        ].map(
            lambda x: f"{x:.2f}"
        )
        + "."
    )

    work["ml4_high"] = (
        work["direction"] == "high"
    ).astype(int)

    work["ml4_low"] = (
        work["direction"] == "low"
    ).astype(int)

    work["ml4_anomaly"] = (
        work["direction"] != "normal"
    ).astype(int)

    return work


def load_np3_alerts() -> pd.DataFrame:
    if not NP3_ALERTS_PATH.exists():
        return pd.DataFrame(
            columns=[
                "grid_id",
                "timestamp",
                "np3_flag",
            ]
        )

    np3 = pd.read_csv(
        NP3_ALERTS_PATH
    )

    required = {
        "grid_id",
        "timestamp",
        "alert_type",
    }

    missing = required - set(
        np3.columns
    )

    if missing:
        raise ValueError(
            "NP3 alerts file is missing columns: "
            + ", ".join(sorted(missing))
        )

    np3["grid_id"] = (
        np3["grid_id"].astype(str)
    )

    np3["timestamp"] = pd.to_datetime(
        np3["timestamp"],
        errors="raise",
    )

    np3 = np3[
        np3["alert_type"]
        == "HIGH_ACTIVITY"
    ].copy()

    np3 = np3[
        [
            "grid_id",
            "timestamp",
        ]
    ].drop_duplicates()

    np3["np3_flag"] = 1

    return np3


def load_ml3_predictions() -> pd.DataFrame:
    if not ML3_PREDICTIONS_PATH.exists():
        raise FileNotFoundError(
            "ML3 prediction file was not found: "
            f"{ML3_PREDICTIONS_PATH}"
        )

    predictions = pd.read_csv(
        ML3_PREDICTIONS_PATH
    )

    required = {
        "grid_id",
        "timestamp",
        "risk_probability",
        "prediction",
    }

    missing = required - set(
        predictions.columns
    )

    if missing:
        raise ValueError(
            "ML3 prediction file is missing columns: "
            + ", ".join(sorted(missing))
        )

    predictions["grid_id"] = (
        predictions["grid_id"].astype(str)
    )

    predictions["timestamp"] = pd.to_datetime(
        predictions["timestamp"],
        errors="raise",
    )

    predictions["ml3_probability"] = (
        predictions["risk_probability"]
        .astype(float)
    )

    predictions["ml3_risk"] = (
        predictions["prediction"]
        .astype(int)
    )

    predictions = predictions[
        [
            "grid_id",
            "timestamp",
            "ml3_probability",
            "ml3_risk",
        ]
    ].drop_duplicates(
        subset=[
            "grid_id",
            "timestamp",
        ]
    )

    return predictions


def build_three_way_comparison(
    anomaly_df: pd.DataFrame,
) -> pd.DataFrame:
    np3 = load_np3_alerts()

    ml3 = load_ml3_predictions()

    comparison = anomaly_df[
        [
            "grid_id",
            "timestamp",
            "ml4_high",
            "ml4_low",
            "ml4_anomaly",
            "anomaly_score",
            "percentage_deviation",
            "direction",
        ]
    ].copy()

    comparison = comparison.merge(
        np3,
        on=[
            "grid_id",
            "timestamp",
        ],
        how="left",
        validate="one_to_one",
    )

    comparison["np3_flag"] = (
        comparison["np3_flag"]
        .fillna(0)
        .astype(int)
    )

    comparison = comparison.merge(
        ml3,
        on=[
            "grid_id",
            "timestamp",
        ],
        how="left",
        validate="one_to_one",
    )

    comparison["ml3_risk"] = (
        comparison["ml3_risk"]
        .fillna(0)
        .astype(int)
    )

    comparison["ml3_probability"] = (
        comparison["ml3_probability"]
        .fillna(0.0)
    )

    comparison["all_three"] = (
        (
            comparison["np3_flag"] == 1
        )
        & (
            comparison["ml3_risk"] == 1
        )
        & (
            comparison["ml4_high"] == 1
        )
    ).astype(int)

    comparison["np3_ml3_only"] = (
        (
            comparison["np3_flag"] == 1
        )
        & (
            comparison["ml3_risk"] == 1
        )
        & (
            comparison["ml4_high"] == 0
        )
    ).astype(int)

    comparison["np3_ml4_only"] = (
        (
            comparison["np3_flag"] == 1
        )
        & (
            comparison["ml3_risk"] == 0
        )
        & (
            comparison["ml4_high"] == 1
        )
    ).astype(int)

    comparison["ml3_ml4_only"] = (
        (
            comparison["np3_flag"] == 0
        )
        & (
            comparison["ml3_risk"] == 1
        )
        & (
            comparison["ml4_high"] == 1
        )
    ).astype(int)

    comparison["np3_only"] = (
        (
            comparison["np3_flag"] == 1
        )
        & (
            comparison["ml3_risk"] == 0
        )
        & (
            comparison["ml4_high"] == 0
        )
    ).astype(int)

    comparison["ml3_only"] = (
        (
            comparison["np3_flag"] == 0
        )
        & (
            comparison["ml3_risk"] == 1
        )
        & (
            comparison["ml4_high"] == 0
        )
    ).astype(int)

    comparison["ml4_only"] = (
        (
            comparison["np3_flag"] == 0
        )
        & (
            comparison["ml3_risk"] == 0
        )
        & (
            comparison["ml4_high"] == 1
        )
    ).astype(int)

    comparison["none_flagged"] = (
        (
            comparison["np3_flag"] == 0
        )
        & (
            comparison["ml3_risk"] == 0
        )
        & (
            comparison["ml4_high"] == 0
        )
    ).astype(int)

    return comparison


def calculate_comparison_metrics(
    comparison: pd.DataFrame,
) -> dict:
    total = len(comparison)

    np3_count = int(
        comparison["np3_flag"].sum()
    )

    ml3_count = int(
        comparison["ml3_risk"].sum()
    )

    ml4_count = int(
        comparison["ml4_high"].sum()
    )

    all_three = int(
        comparison["all_three"].sum()
    )

    np3_ml3_only = int(
        comparison["np3_ml3_only"].sum()
    )

    np3_ml4_only = int(
        comparison["np3_ml4_only"].sum()
    )

    ml3_ml4_only = int(
        comparison["ml3_ml4_only"].sum()
    )

    np3_only = int(
        comparison["np3_only"].sum()
    )

    ml3_only = int(
        comparison["ml3_only"].sum()
    )

    ml4_only = int(
        comparison["ml4_only"].sum()
    )

    none_flagged = int(
        comparison["none_flagged"].sum()
    )

    return {
        "total_records": total,
        "np3_alerts": np3_count,
        "ml3_risks": ml3_count,
        "ml4_high_anomalies": ml4_count,
        "all_three": all_three,
        "np3_ml3_only": np3_ml3_only,
        "np3_ml4_only": np3_ml4_only,
        "ml3_ml4_only": ml3_ml4_only,
        "np3_only": np3_only,
        "ml3_only": ml3_only,
        "ml4_only": ml4_only,
        "none_flagged": none_flagged,
        "ml3_ml4_agreement": float(
            np.mean(
                comparison["ml3_risk"]
                == comparison["ml4_high"]
            )
        ),
        "np3_ml4_agreement": float(
            np.mean(
                comparison["np3_flag"]
                == comparison["ml4_high"]
            )
        ),
    }


def generate_disagreement_summary(
    metrics: dict,
) -> list[str]:
    explanations = []

    if metrics["np3_only"] > 0:
        explanations.append(
            "NP3-only cases indicate activity that satisfies "
            "the current-day high-activity rule but is not "
            "identified as an ML3 future risk or ML4 historical "
            "high anomaly."
        )

    if metrics["ml3_only"] > 0:
        explanations.append(
            "ML3-only cases indicate future high-activity risk "
            "without a current NP3 alert or current ML4 high "
            "historical anomaly."
        )

    if metrics["ml4_only"] > 0:
        explanations.append(
            "ML4-only cases indicate current activity that is "
            "unusual for the grid's historical hour-of-day "
            "behaviour without a current NP3 alert or ML3 "
            "future-risk prediction."
        )

    if metrics["all_three"] > 0:
        explanations.append(
            "Cases flagged by all three mechanisms represent "
            "the strongest multi-signal operational attention "
            "cases because current rules, future risk, and "
            "historical deviation agree."
        )

    return explanations


def save_outputs(
    scored_df: pd.DataFrame,
    comparison: pd.DataFrame,
    bucket_stats: dict,
    metrics: dict,
):
    ML4_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_columns = [
        "grid_id",
        "timestamp",
        "total_activity",
        "baseline_activity",
        "percentage_deviation",
        "anomaly_score",
        "direction",
        "reason",
    ]

    scored_df[
        output_columns
    ].to_csv(
        ANOMALY_OUTPUT_PATH,
        index=False,
    )

    comparison.to_csv(
        COMPARISON_OUTPUT_PATH,
        index=False,
    )

    explanations = (
        generate_disagreement_summary(
            metrics
        )
    )

    report_lines = [
        "# ML4 Anomaly Baseline Evaluation Report",
        "",
        "## Overview",
        "",
        "- Baseline: historical median by grid and hour of day.",
        "- High anomaly threshold: +50% deviation.",
        "- Low anomaly threshold: -50% deviation.",
        "- Anomaly score: absolute percentage deviation.",
        "- Direction: high, low, or normal.",
        "",
        "## Historical Bucket Validation",
        "",
        (
            f"- Grid/hour buckets: "
            f"{bucket_stats['bucket_count']:,}"
        ),
        (
            f"- Minimum observations per bucket: "
            f"{bucket_stats['minimum_observations']}"
        ),
        (
            f"- Maximum observations per bucket: "
            f"{bucket_stats['maximum_observations']}"
        ),
        (
            f"- Average observations per bucket: "
            f"{bucket_stats['average_observations']:.2f}"
        ),
        "",
        "## Three-Way Comparison",
        "",
        "| Mechanism | Flagged Count |",
        "|---|---:|",
        (
            f"| NP3 | "
            f"{metrics['np3_alerts']:,} |"
        ),
        (
            f"| ML3 | "
            f"{metrics['ml3_risks']:,} |"
        ),
        (
            f"| ML4 high anomaly | "
            f"{metrics['ml4_high_anomalies']:,} |"
        ),
        (
            f"| All three | "
            f"{metrics['all_three']:,} |"
        ),
        (
            f"| NP3 + ML3 only | "
            f"{metrics['np3_ml3_only']:,} |"
        ),
        (
            f"| NP3 + ML4 only | "
            f"{metrics['np3_ml4_only']:,} |"
        ),
        (
            f"| ML3 + ML4 only | "
            f"{metrics['ml3_ml4_only']:,} |"
        ),
        (
            f"| NP3 only | "
            f"{metrics['np3_only']:,} |"
        ),
        (
            f"| ML3 only | "
            f"{metrics['ml3_only']:,} |"
        ),
        (
            f"| ML4 only | "
            f"{metrics['ml4_only']:,} |"
        ),
        (
            f"| None | "
            f"{metrics['none_flagged']:,} |"
        ),
        "",
        "## Agreement",
        "",
        (
            f"- ML3 vs ML4 agreement: "
            f"{metrics['ml3_ml4_agreement']:.4f}"
        ),
        (
            f"- NP3 vs ML4 agreement: "
            f"{metrics['np3_ml4_agreement']:.4f}"
        ),
        "",
        "## Disagreement Interpretation",
        "",
    ]

    if explanations:
        for explanation in explanations:
            report_lines.append(
                f"- {explanation}"
            )
    else:
        report_lines.append(
            "- No disagreement category contained any records."
        )

    report_lines.extend(
        [
            "",
            "## Operational Interpretation",
            "",
            "NP3 identifies current elevated activity using its rule-based baseline.",
            "",
            "ML3 predicts future elevated activity using recent activity behaviour available through the prediction timestamp.",
            "",
            "ML4 identifies current activity that deviates materially from the historical hour-of-day behaviour of the same grid.",
            "",
            "The three mechanisms answer different operational questions, so disagreement is informative rather than automatically a defect.",
            "",
            "A positive signal is an operational attention signal for investigation. It is not evidence of a confirmed network fault.",
            "",
            "## Output Files",
            "",
            f"- Anomaly scores: `{ANOMALY_OUTPUT_PATH}`",
            f"- Three-way comparison: `{COMPARISON_OUTPUT_PATH}`",
        ]
    )

    REPORT_PATH.write_text(
        "\n".join(report_lines),
        encoding="utf-8",
    )

    json_payload = {
        "model": "ML4 generalized historical anomaly baseline",
        "baseline": {
            "type": "grid_id + hour_of_day historical median",
            "high_threshold": HIGH_THRESHOLD,
            "low_threshold": LOW_THRESHOLD,
            "score": "absolute percentage deviation",
        },
        "bucket_history": bucket_stats,
        "comparison": metrics,
        "outputs": {
            "anomaly_scores": str(
                ANOMALY_OUTPUT_PATH
            ),
            "three_way_comparison": str(
                COMPARISON_OUTPUT_PATH
            ),
            "report": str(
                REPORT_PATH
            ),
        },
    }

    JSON_REPORT_PATH.write_text(
        json.dumps(
            json_payload,
            indent=2,
        ),
        encoding="utf-8",
    )


def main():
    ML4_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    activity = load_warehouse_activity()

    bucket_stats = verify_bucket_history(
        activity
    )

    activity["hour_of_day"] = (
        activity["timestamp"].dt.hour
    )

    baseline_df = (
        calculate_generalized_baseline(
            activity,
            [
                "grid_id",
                "hour_of_day",
            ],
        )
    )

    scored_df = compute_anomalies(
        baseline_df
    )

    comparison = build_three_way_comparison(
        scored_df
    )

    metrics = calculate_comparison_metrics(
        comparison
    )

    save_outputs(
        scored_df,
        comparison,
        bucket_stats,
        metrics,
    )

    print()
    print("=" * 64)
    print("ML4 — HISTORICAL ANOMALY BASELINE")
    print("=" * 64)
    print()

    print("BASELINE")
    print("-" * 64)
    print("Baseline type     : Grid + hour-of-day median")
    print(
        f"High threshold    : +{HIGH_THRESHOLD:.0%}"
    )
    print(
        f"Low threshold     : {LOW_THRESHOLD:.0%}"
    )
    print("Anomaly score     : Absolute percentage deviation")
    print()

    print("DATA")
    print("-" * 64)
    print(
        f"Warehouse rows    : {len(activity):,}"
    )
    print(
        f"Grid/hour buckets : "
        f"{bucket_stats['bucket_count']:,}"
    )
    print(
        f"Min history       : "
        f"{bucket_stats['minimum_observations']} observations"
    )
    print(
        f"Max history       : "
        f"{bucket_stats['maximum_observations']} observations"
    )
    print(
        f"Avg history       : "
        f"{bucket_stats['average_observations']:.2f} observations"
    )
    print()

    print("ANOMALY SUMMARY")
    print("-" * 64)
    print(
        f"High anomalies    : "
        f"{int(scored_df['ml4_high'].sum()):,}"
    )
    print(
        f"Low anomalies     : "
        f"{int(scored_df['ml4_low'].sum()):,}"
    )
    print(
        f"Normal            : "
        f"{int((scored_df['direction'] == 'normal').sum()):,}"
    )
    print()

    print("THREE-WAY COMPARISON")
    print("-" * 64)
    print(
        f"NP3 alerts        : "
        f"{metrics['np3_alerts']:,}"
    )
    print(
        f"ML3 risks         : "
        f"{metrics['ml3_risks']:,}"
    )
    print(
        f"ML4 high anomalies: "
        f"{metrics['ml4_high_anomalies']:,}"
    )
    print(
        f"All three         : "
        f"{metrics['all_three']:,}"
    )
    print(
        f"NP3 + ML3 only    : "
        f"{metrics['np3_ml3_only']:,}"
    )
    print(
        f"NP3 + ML4 only    : "
        f"{metrics['np3_ml4_only']:,}"
    )
    print(
        f"ML3 + ML4 only    : "
        f"{metrics['ml3_ml4_only']:,}"
    )
    print(
        f"NP3 only          : "
        f"{metrics['np3_only']:,}"
    )
    print(
        f"ML3 only          : "
        f"{metrics['ml3_only']:,}"
    )
    print(
        f"ML4 only          : "
        f"{metrics['ml4_only']:,}"
    )
    print(
        f"None flagged      : "
        f"{metrics['none_flagged']:,}"
    )
    print()

    print("AGREEMENT")
    print("-" * 64)
    print(
        f"ML3 vs ML4        : "
        f"{metrics['ml3_ml4_agreement']:.4f}"
    )
    print(
        f"NP3 vs ML4        : "
        f"{metrics['np3_ml4_agreement']:.4f}"
    )
    print()

    print("OUTPUTS")
    print("-" * 64)
    print(
        f"Anomaly scores    : "
        f"{ANOMALY_OUTPUT_PATH}"
    )
    print(
        f"Three-way compare : "
        f"{COMPARISON_OUTPUT_PATH}"
    )
    print(
        f"Evaluation report : "
        f"{REPORT_PATH}"
    )
    print(
        f"JSON report       : "
        f"{JSON_REPORT_PATH}"
    )
    print()

    print("=" * 64)
    print("ML4 COMPLETED SUCCESSFULLY")
    print("=" * 64)
    print()


if __name__ == "__main__":
    main()