from __future__ import annotations

import json
import pickle
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.tree import DecisionTreeClassifier


DB_PATH = Path("data/warehouse/network_ops.db")
FEATURE_TABLE = "network_feature_table"

OUTPUT_DIR = Path("ML/ML3/outputs")
MODEL_PATH = OUTPUT_DIR / "risk_classifier.pkl"
PREDICTIONS_PATH = OUTPUT_DIR / "risk_predictions.csv"
METRICS_PATH = OUTPUT_DIR / "ml3_metrics.json"
REPORT_PATH = OUTPUT_DIR / "ml3_evaluation_report.md"

RANDOM_STATE = 42
SURGE_FACTOR = 1.5
OPERATIONAL_THRESHOLD = 0.85

TRAIN_RATIO = 0.60
VALIDATION_RATIO = 0.20

FEATURES = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
    "recent_6h_mean",
    "recent_6h_std",
    "recent_6h_peak_ratio",
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
]

ML2_FEATURES = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


def load_ml2_features() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT
                grid_id,
                feature_timestamp,
                avg_activity,
                activity_growth,
                active_hours,
                peak_ratio,
                variability,
                internet_share
            FROM {FEATURE_TABLE}
            ORDER BY grid_id, feature_timestamp
            """,
            conn,
        )

    if df.empty:
        raise RuntimeError("ML2 feature table is empty.")

    df["feature_timestamp"] = pd.to_datetime(
        df["feature_timestamp"]
    )

    df = df.sort_values(
        ["grid_id", "feature_timestamp"]
    ).reset_index(drop=True)

    return df

def load_activity() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            """
            SELECT
                g.grid_id,
                t.timestamp,
                f.total_activity,
                f.internet_activity
            FROM fact_network_activity AS f
            JOIN dim_grid AS g
                ON f.grid_key = g.grid_key
            JOIN dim_time AS t
                ON f.time_key = t.time_key
            ORDER BY g.grid_id, t.timestamp
            """,
            conn,
        )

    if df.empty:
        raise RuntimeError("Warehouse activity table is empty.")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["grid_id", "timestamp"]).reset_index(drop=True)

    return df


def build_extra_features(activity: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for grid_id, group in activity.groupby("grid_id", sort=False):
        group = group.sort_values("timestamp").copy()

        group = group.set_index("timestamp")

        recent_mean = group["total_activity"].rolling(
            6,
            min_periods=6,
        ).mean()

        recent_std = group["total_activity"].rolling(
            6,
            min_periods=6,
        ).std(ddof=0)

        recent_peak = group["total_activity"].rolling(
            6,
            min_periods=6,
        ).max()

        group["recent_6h_mean"] = recent_mean
        group["recent_6h_std"] = recent_std

        group["recent_6h_peak_ratio"] = np.where(
            recent_mean > 0,
            recent_peak / recent_mean,
            0.0,
        )

        hours = group.index.hour
        day_of_week = group.index.dayofweek

        group["hour_sin"] = np.sin(2 * np.pi * hours / 24)
        group["hour_cos"] = np.cos(2 * np.pi * hours / 24)

        group["day_sin"] = np.sin(2 * np.pi * day_of_week / 7)
        group["day_cos"] = np.cos(2 * np.pi * day_of_week / 7)

        group["grid_id"] = grid_id
        group = group.reset_index()

        frames.append(group)

    return pd.concat(frames, ignore_index=True)


def build_target(activity: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for grid_id, group in activity.groupby("grid_id", sort=False):
        group = group.sort_values("timestamp").copy()
        group = group.set_index("timestamp")

        baseline = group["total_activity"].rolling(
            24,
            min_periods=24,
        ).median()

        future_activity = group["total_activity"].shift(-1)
        next_timestamp = group.index.to_series().shift(-1)

        target = (
            (next_timestamp - group.index == pd.Timedelta(hours=1))
            & baseline.notna()
            & future_activity.notna()
            & (future_activity > SURGE_FACTOR * baseline)
        ).astype(int)

        result = pd.DataFrame(
            {
                "grid_id": grid_id,
                "feature_timestamp": group.index,
                "target": target,
            }
        )

        frames.append(result)

    return pd.concat(frames, ignore_index=True)


def build_ml3_dataset(
    ml2: pd.DataFrame,
    activity: pd.DataFrame,
) -> pd.DataFrame:
    extra = build_extra_features(activity)

    extra = extra[
        [
            "grid_id",
            "timestamp",
            "recent_6h_mean",
            "recent_6h_std",
            "recent_6h_peak_ratio",
            "hour_sin",
            "hour_cos",
            "day_sin",
            "day_cos",
        ]
    ].copy()

    extra = extra.rename(
        columns={
            "timestamp": "feature_timestamp",
        }
    )

    target = build_target(activity)

    df = ml2.merge(
        extra,
        on=["grid_id", "feature_timestamp"],
        how="inner",
        validate="one_to_one",
    )

    df = df.merge(
        target,
        on=["grid_id", "feature_timestamp"],
        how="inner",
        validate="one_to_one",
    )

    df = df.sort_values(
        ["feature_timestamp", "grid_id"]
    ).reset_index(drop=True)

    required_columns = FEATURES + ["target"]

    df = df.dropna(
        subset=required_columns
    ).copy()

    df = df[
        np.isfinite(
            df[FEATURES].to_numpy()
        ).all(axis=1)
    ].copy()

    df["target"] = df["target"].astype(int)

    return df


def run_leakage_test(
    ml2: pd.DataFrame,
    activity: pd.DataFrame,
) -> bool:
    cutoff = activity["timestamp"].max() - pd.Timedelta(hours=24)

    baseline_activity = build_extra_features(activity)

    baseline_activity = baseline_activity[
        baseline_activity["timestamp"] <= cutoff
    ].copy()

    future_activity = activity[
        activity["timestamp"] > cutoff
    ].copy()

    if future_activity.empty:
        return True

    perturbed = activity.copy()

    perturbed.loc[
        perturbed["timestamp"] > cutoff,
        "total_activity",
    ] *= 1000.0

    perturbed_features = build_extra_features(perturbed)

    original = baseline_activity.merge(
        perturbed_features,
        on=["grid_id", "timestamp"],
        suffixes=("_original", "_perturbed"),
        how="inner",
    )

    for column in [
        "recent_6h_mean",
        "recent_6h_std",
        "recent_6h_peak_ratio",
        "hour_sin",
        "hour_cos",
        "day_sin",
        "day_cos",
    ]:
        a = original[f"{column}_original"].to_numpy()
        b = original[f"{column}_perturbed"].to_numpy()

        if not np.allclose(
            a,
            b,
            equal_nan=True,
        ):
            return False

    return True


def chronological_split(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    timestamps = (
        df["feature_timestamp"]
        .sort_values()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    n_timestamps = len(timestamps)

    train_end_index = int(
        n_timestamps * TRAIN_RATIO
    )

    validation_end_index = int(
        n_timestamps * (TRAIN_RATIO + VALIDATION_RATIO)
    )

    train_end_timestamp = timestamps.iloc[
        train_end_index - 1
    ]

    validation_end_timestamp = timestamps.iloc[
        validation_end_index - 1
    ]

    train = df[
        df["feature_timestamp"] <= train_end_timestamp
    ].copy()

    validation = df[
        (df["feature_timestamp"] > train_end_timestamp)
        & (
            df["feature_timestamp"]
            <= validation_end_timestamp
        )
    ].copy()

    test = df[
        df["feature_timestamp"] > validation_end_timestamp
    ].copy()

    return train, validation, test


def train_model(
    train: pd.DataFrame,
    validation: pd.DataFrame,
) -> DecisionTreeClassifier:
    development = pd.concat(
        [train, validation],
        ignore_index=True,
    )

    model = DecisionTreeClassifier(
        criterion="entropy",
        max_depth=7,
        min_samples_split=20,
        min_samples_leaf=10,
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )

    model.fit(
        development[FEATURES],
        development["target"],
    )

    return model


def evaluate_model(
    model: DecisionTreeClassifier,
    test: pd.DataFrame,
) -> tuple[dict, pd.DataFrame]:
    probabilities = model.predict_proba(
        test[FEATURES]
    )

    positive_class_index = list(
        model.classes_
    ).index(1)

    risk_probability = probabilities[
        :, positive_class_index
    ]

    predictions = (
        risk_probability >= 0.50
    ).astype(int)

    evaluated = test[
        [
            "grid_id",
            "feature_timestamp",
            "target",
        ]
    ].copy()

    evaluated["risk_probability"] = risk_probability
    evaluated["prediction"] = predictions

    metrics = {
        "accuracy": float(
            accuracy_score(
                test["target"],
                predictions,
            )
        ),
        "precision": float(
            precision_score(
                test["target"],
                predictions,
                zero_division=0,
            )
        ),
        "recall": float(
            recall_score(
                test["target"],
                predictions,
                zero_division=0,
            )
        ),
        "f1": float(
            f1_score(
                test["target"],
                predictions,
                zero_division=0,
            )
        ),
        "positive_rate": float(
            test["target"].mean()
        ),
        "predicted_positive_rate": float(
            predictions.mean()
        ),
        "test_rows": int(len(test)),
        "test_positive_rows": int(
            test["target"].sum()
        ),
        "confusion_matrix": confusion_matrix(
            test["target"],
            predictions,
        ).tolist(),
    }

    return metrics, evaluated


def calculate_np3_high_activity(
    activity: pd.DataFrame,
) -> pd.DataFrame:
    frames = []

    for grid_id, group in activity.groupby(
        "grid_id",
        sort=False,
    ):
        group = group.sort_values(
            "timestamp"
        ).copy()

        group = group.set_index("timestamp")

        baseline = group[
            "total_activity"
        ].rolling(
            24,
            min_periods=24,
        ).median()

        current_activity = group[
            "total_activity"
        ]

        np3_high_activity = (
            current_activity
            > SURGE_FACTOR * baseline
        ).astype(int)

        np3_high_activity[
            baseline.isna()
        ] = np.nan

        result = pd.DataFrame(
            {
                "grid_id": grid_id,
                "timestamp": group.index,
                "np3_high_activity": np3_high_activity,
            }
        )

        frames.append(result)

    return pd.concat(
        frames,
        ignore_index=True,
    )


def np3_comparison(
    test_predictions: pd.DataFrame,
    activity: pd.DataFrame,
) -> dict:
    np3 = calculate_np3_high_activity(
        activity
    )

    np3 = np3.rename(
        columns={
            "timestamp": "feature_timestamp",
        }
    )

    comparison = test_predictions.merge(
        np3,
        on=[
            "grid_id",
            "feature_timestamp",
        ],
        how="left",
        validate="one_to_one",
    )

    comparison = comparison.dropna(
        subset=["np3_high_activity"]
    ).copy()

    comparison["np3_high_activity"] = (
        comparison["np3_high_activity"]
        .astype(int)
    )

    comparison["model_high_risk"] = (
        comparison["risk_probability"]
        >= OPERATIONAL_THRESHOLD
    ).astype(int)

    comparison["agreement"] = (
        comparison["model_high_risk"]
        == comparison["np3_high_activity"]
    )

    model_high_risk = int(
        comparison["model_high_risk"].sum()
    )

    np3_high_activity = int(
        comparison["np3_high_activity"].sum()
    )

    agreement_count = int(
        comparison["agreement"].sum()
    )

    total_comparable = int(
        len(comparison)
    )

    return {
        "model_high_risk": model_high_risk,
        "np3_high_activity": np3_high_activity,
        "agreement_count": agreement_count,
        "comparable_rows": total_comparable,
        "agreement_rate": (
            float(agreement_count / total_comparable)
            if total_comparable
            else 0.0
        ),
    }


def save_serving_predictions(
    model: DecisionTreeClassifier,
    df: pd.DataFrame,
) -> pd.DataFrame:
    probabilities = model.predict_proba(
        df[FEATURES]
    )

    positive_class_index = list(
        model.classes_
    ).index(1)

    risk_probability = probabilities[
        :, positive_class_index
    ]

    predictions = (
        risk_probability >= 0.50
    ).astype(int)

    output = pd.DataFrame(
        {
            "grid_id": df["grid_id"].astype(str),
            "timestamp": df[
                "feature_timestamp"
            ].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "target": df["target"].astype(int),
            "risk_probability": risk_probability,
            "prediction": predictions,
        }
    )

    output.to_csv(
        PREDICTIONS_PATH,
        index=False,
    )

    return output


def save_model(
    model: DecisionTreeClassifier,
) -> None:
    artifact = {
        "model": model,
        "model_version": "ML3-v1",
        "model_name": "decision_tree",
        "features": FEATURES,
        "threshold": OPERATIONAL_THRESHOLD,
        "surge_factor": SURGE_FACTOR,
    }

    with MODEL_PATH.open(
        "wb"
    ) as file:
        pickle.dump(
            artifact,
            file,
        )


def save_metrics(
    metrics: dict,
    np3_metrics: dict,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
    serving: pd.DataFrame,
) -> None:
    payload = {
        "model_version": "ML3-v1",
        "model_name": "decision_tree",
        "features": FEATURES,
        "surge_factor": SURGE_FACTOR,
        "operational_threshold": OPERATIONAL_THRESHOLD,
        "split": {
            "train_rows": int(len(train)),
            "validation_rows": int(len(validation)),
            "test_rows": int(len(test)),
            "train_start": train[
                "feature_timestamp"
            ].min().isoformat(),
            "train_end": train[
                "feature_timestamp"
            ].max().isoformat(),
            "validation_start": validation[
                "feature_timestamp"
            ].min().isoformat(),
            "validation_end": validation[
                "feature_timestamp"
            ].max().isoformat(),
            "test_start": test[
                "feature_timestamp"
            ].min().isoformat(),
            "test_end": test[
                "feature_timestamp"
            ].max().isoformat(),
        },
        "test_metrics": metrics,
        "np3_comparison": np3_metrics,
        "serving": {
            "rows": int(len(serving)),
            "start": serving["timestamp"].min(),
            "end": serving["timestamp"].max(),
        },
    }

    with METRICS_PATH.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            payload,
            file,
            indent=2,
        )


def save_report(
    metrics: dict,
    np3_metrics: dict,
    model: DecisionTreeClassifier,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
    serving: pd.DataFrame,
) -> None:
    importance = sorted(
        zip(
            FEATURES,
            model.feature_importances_,
        ),
        key=lambda x: x[1],
        reverse=True,
    )

    report = f"""# ML3 — Future High-Activity Risk Classifier

## Problem

Using recent hourly activity behaviour available through time `t`, predict whether a grid will meet the defined elevated-activity condition at `t+1`.

The target is a synthetic training proxy:

`total_activity(t+1) > 1.5 × trailing 24-hour median activity through t`

The model output is an operational attention signal and is not a confirmed network fault or congestion indicator.

## Data Split

Chronological split only.

- Train: {len(train):,} rows
- Validation: {len(validation):,} rows
- Test: {len(test):,} rows

### Train

- Start: {train["feature_timestamp"].min()}
- End: {train["feature_timestamp"].max()}

### Validation

- Start: {validation["feature_timestamp"].min()}
- End: {validation["feature_timestamp"].max()}

### Test

- Start: {test["feature_timestamp"].min()}
- End: {test["feature_timestamp"].max()}

## Test Metrics

- Accuracy: {metrics["accuracy"]:.4f}
- Precision: {metrics["precision"]:.4f}
- Recall: {metrics["recall"]:.4f}
- F1: {metrics["f1"]:.4f}
- Actual positive rate: {metrics["positive_rate"]:.4%}
- Predicted positive rate: {metrics["predicted_positive_rate"]:.4%}

## Feature Importance

"""

    for feature, value in importance:
        report += f"- {feature}: {value:.6f}\n"

    report += f"""
## NP3 Comparison

The NP3 comparison uses the actual current-activity HIGH_ACTIVITY condition:

`current total_activity(t) > 1.5 × trailing 24-hour median activity through t`

- Model high-risk rows at operational threshold {OPERATIONAL_THRESHOLD:.2f}: {np3_metrics["model_high_risk"]:,}
- NP3 high-activity rows: {np3_metrics["np3_high_activity"]:,}
- Comparable rows: {np3_metrics["comparable_rows"]:,}
- Agreement: {np3_metrics["agreement_count"]:,}
- Agreement rate: {np3_metrics["agreement_rate"]:.4%}

## Serving Artifact

The serving model is trained on train + validation only after the test evaluation is completed.

- Serving rows: {len(serving):,}
- Serving start: {serving["timestamp"].min()}
- Serving end: {serving["timestamp"].max()}
- Model version: ML3-v1

The serving predictions are application scores and must not be treated as additional evaluation data.

## Limitation

The available warehouse contains approximately seven days of hourly history. The Trainer Guide recommends a longer history for ML validation. Therefore, these results should be presented as a baseline evaluation on the available dataset, not as a production-quality performance guarantee.
"""

    REPORT_PATH.write_text(
        report,
        encoding="utf-8",
    )


def main() -> None:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    ml2 = load_ml2_features()
    activity = load_activity()

    leakage_pass = run_leakage_test(
        ml2,
        activity,
    )

    if not leakage_pass:
        raise RuntimeError(
            "Leakage test failed."
        )

    df = build_ml3_dataset(
        ml2,
        activity,
    )

    train, validation, test = chronological_split(
        df
    )

    model = train_model(
        train,
        validation,
    )

    metrics, test_predictions = evaluate_model(
        model,
        test,
    )

    np3_metrics = np3_comparison(
        test_predictions,
        activity,
    )

    serving = save_serving_predictions(
        model,
        df,
    )

    save_model(model)

    save_metrics(
        metrics,
        np3_metrics,
        train,
        validation,
        test,
        serving,
    )

    save_report(
        metrics,
        np3_metrics,
        model,
        train,
        validation,
        test,
        serving,
    )

    print()
    print("ML3 — Future High-Activity Risk Classifier")
    print("=" * 62)
    print(
        f"Leakage test        : {'PASS' if leakage_pass else 'FAIL'}"
    )
    print(
        f"Valid ML3 rows      : {len(df):,}"
    )
    print(
        f"Train rows          : {len(train):,}"
    )
    print(
        f"Validation rows     : {len(validation):,}"
    )
    print(
        f"Test rows           : {len(test):,}"
    )

    print()
    print(
        f"Train period        : "
        f"{train['feature_timestamp'].min()} → "
        f"{train['feature_timestamp'].max()}"
    )
    print(
        f"Validation period   : "
        f"{validation['feature_timestamp'].min()} → "
        f"{validation['feature_timestamp'].max()}"
    )
    print(
        f"Test period         : "
        f"{test['feature_timestamp'].min()} → "
        f"{test['feature_timestamp'].max()}"
    )

    print()
    print("Test metrics")
    print("-" * 62)
    print(
        f"Accuracy            : {metrics['accuracy']:.4f}"
    )
    print(
        f"Precision           : {metrics['precision']:.4f}"
    )
    print(
        f"Recall              : {metrics['recall']:.4f}"
    )
    print(
        f"F1                  : {metrics['f1']:.4f}"
    )
    print(
        f"Positive rate       : {metrics['positive_rate']:.4%}"
    )

    print()
    print("NP3 comparison")
    print("-" * 62)
    print(
        f"Model high-risk     : "
        f"{np3_metrics['model_high_risk']:,}"
    )
    print(
        f"NP3 high-activity   : "
        f"{np3_metrics['np3_high_activity']:,}"
    )
    print(
        f"Comparable rows     : "
        f"{np3_metrics['comparable_rows']:,}"
    )
    print(
        f"Agreement           : "
        f"{np3_metrics['agreement_count']:,} / "
        f"{np3_metrics['comparable_rows']:,}"
    )
    print(
        f"Agreement rate      : "
        f"{np3_metrics['agreement_rate']:.4%}"
    )

    print()
    print("Training final serving model")
    print("-" * 62)
    print(
        f"Serving rows        : {len(serving):,}"
    )
    print(
        f"Serving period      : "
        f"{serving['timestamp'].min()} → "
        f"{serving['timestamp'].max()}"
    )

    print()
    print("Output files")
    print("-" * 62)
    print(
        f"Model               : {MODEL_PATH}"
    )
    print(
        f"Predictions         : {PREDICTIONS_PATH}"
    )
    print(
        f"Metrics             : {METRICS_PATH}"
    )
    print(
        f"Report              : {REPORT_PATH}"
    )

    print()
    print("ML3 completed successfully.")


if __name__ == "__main__":
    main()