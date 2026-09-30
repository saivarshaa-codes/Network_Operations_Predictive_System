from __future__ import annotations

import pickle
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


DB_PATH = Path("data/warehouse/network_ops.db")

FEATURE_TABLE = "network_feature_table"
RISK_TABLE = "network_risk_scores"

MODEL_PATH = Path("ML/ML3/outputs/risk_classifier.pkl")
ANOMALY_PATH = Path("ML/ML4/network_anomaly_scores.csv")
REPORT_PATH = Path("ML/ML6/top20_operational_attention_report.md")

RISK_HIGH_THRESHOLD = 0.80
RISK_MEDIUM_THRESHOLD = 0.40

MODEL_FEATURES = [
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

REQUIRED_ACTIVITY_COLUMNS = [
    "grid_id",
    "timestamp",
    "total_activity",
    "internet_activity",
]


def fail(message: str) -> None:
    raise RuntimeError(message)


def validate_paths() -> None:
    if not DB_PATH.exists():
        fail(f"Warehouse database not found: {DB_PATH}")

    if not MODEL_PATH.exists():
        fail(f"ML3 model artifact not found: {MODEL_PATH}")

    if not ANOMALY_PATH.exists():
        fail(f"ML4 anomaly file not found: {ANOMALY_PATH}")


def validate_feature_table(conn: sqlite3.Connection) -> None:
    exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (FEATURE_TABLE,),
    ).fetchone()

    if exists is None:
        fail(
            f"Required feature table '{FEATURE_TABLE}' does not exist. "
            "ML6 cannot run before ML2 feature generation."
        )

    columns = {
        row[1]
        for row in conn.execute(
            f"PRAGMA table_info({FEATURE_TABLE})"
        ).fetchall()
    }

    required = {"grid_id", "feature_timestamp", *ML2_FEATURES}

    missing = sorted(required - columns)

    if missing:
        fail(
            "ML2 feature table is missing required columns: "
            + ", ".join(missing)
        )


def load_model() -> tuple[object, str]:
    with MODEL_PATH.open("rb") as handle:
        artifact = pickle.load(handle)

    if not isinstance(artifact, dict):
        fail("ML3 model artifact must be a dictionary.")

    required_keys = {
        "model",
        "model_version",
        "features",
        "threshold",
        "surge_factor",
    }

    missing = required_keys - set(artifact)

    if missing:
        fail(
            "ML3 model artifact is missing keys: "
            + ", ".join(sorted(missing))
        )

    model = artifact["model"]
    model_version = str(artifact["model_version"])
    features = list(artifact["features"])

    if features != MODEL_FEATURES:
        fail(
            "ML3 model feature contract mismatch.\n"
            f"Expected: {MODEL_FEATURES}\n"
            f"Found:    {features}"
        )

    if not hasattr(model, "predict_proba"):
        fail("ML3 model does not support predict_proba().")

    if not hasattr(model, "classes_"):
        fail("ML3 model does not expose classes_.")

    classes = list(model.classes_)

    if 1 not in classes:
        fail("ML3 model does not contain positive class 1.")

    if model_version != "ML3-v1":
        fail(
            f"Unexpected model version: {model_version}. "
            "ML6 expects the trained ML3-v1 artifact."
        )

    return model, model_version


def load_ml2_features(
    conn: sqlite3.Connection,
) -> pd.DataFrame:
    query = f"""
        SELECT
            grid_id,
            feature_timestamp,
            {", ".join(ML2_FEATURES)}
        FROM {FEATURE_TABLE}
        ORDER BY grid_id, feature_timestamp
    """

    df = pd.read_sql_query(query, conn)

    if df.empty:
        fail(
            "network_feature_table contains zero rows. "
            "ML6 cannot run before ML2 feature generation."
        )

    df["feature_timestamp"] = pd.to_datetime(
        df["feature_timestamp"],
        errors="raise",
    )

    duplicate_count = int(
        df.duplicated(
            subset=["grid_id", "feature_timestamp"]
        ).sum()
    )

    if duplicate_count > 0:
        fail(
            f"ML2 feature table contains {duplicate_count} "
            "duplicate grid/timestamp rows."
        )

    return df


def load_activity(
    conn: sqlite3.Connection,
) -> pd.DataFrame:
    query = """
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
        ORDER BY
            g.grid_id,
            t.timestamp
    """

    df = pd.read_sql_query(query, conn)

    if df.empty:
        fail("Warehouse activity query returned zero rows.")

    missing = [
        column
        for column in REQUIRED_ACTIVITY_COLUMNS
        if column not in df.columns
    ]

    if missing:
        fail(
            "Warehouse activity is missing required columns: "
            + ", ".join(missing)
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

    duplicate_count = int(
        df.duplicated(
            subset=["grid_id", "timestamp"]
        ).sum()
    )

    if duplicate_count > 0:
        fail(
            f"Warehouse activity contains {duplicate_count} "
            "duplicate grid/timestamp rows."
        )

    return df


def build_extra_ml3_features(
    activity: pd.DataFrame,
) -> pd.DataFrame:
    parts = []

    for grid_id, group in activity.groupby(
        "grid_id",
        sort=False,
    ):
        group = (
            group
            .sort_values("timestamp")
            .copy()
            .set_index("timestamp")
        )

        recent_mean = (
            group["total_activity"]
            .rolling(
                window=6,
                min_periods=6,
            )
            .mean()
        )

        recent_std = (
            group["total_activity"]
            .rolling(
                window=6,
                min_periods=6,
            )
            .std(ddof=0)
        )

        recent_peak = (
            group["total_activity"]
            .rolling(
                window=6,
                min_periods=6,
            )
            .max()
        )

        group["recent_6h_mean"] = recent_mean
        group["recent_6h_std"] = recent_std

        group["recent_6h_peak_ratio"] = np.where(
            recent_mean > 0,
            recent_peak / recent_mean,
            0.0,
        )

        hours = group.index.hour
        day_of_week = group.index.dayofweek

        group["hour_sin"] = np.sin(
            2 * np.pi * hours / 24
        )

        group["hour_cos"] = np.cos(
            2 * np.pi * hours / 24
        )

        group["day_sin"] = np.sin(
            2 * np.pi * day_of_week / 7
        )

        group["day_cos"] = np.cos(
            2 * np.pi * day_of_week / 7
        )

        group = group.reset_index()

        parts.append(
            group[
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
            ]
        )

    if not parts:
        fail("Unable to build ML3 scoring features.")

    return pd.concat(
        parts,
        ignore_index=True,
    )


def build_scoring_dataset(
    ml2: pd.DataFrame,
    extra: pd.DataFrame,
) -> pd.DataFrame:
    extra = extra.rename(
        columns={"timestamp": "feature_timestamp"}
    )

    df = ml2.merge(
        extra,
        on=["grid_id", "feature_timestamp"],
        how="inner",
        validate="one_to_one",
    )

    if df.empty:
        fail(
            "ML2 features and ML3 scoring features have no "
            "matching grid/timestamp rows."
        )

    missing_features = [
        column
        for column in MODEL_FEATURES
        if column not in df.columns
    ]

    if missing_features:
        fail(
            "Scoring dataset is missing model features: "
            + ", ".join(missing_features)
        )

    df = df.dropna(
        subset=MODEL_FEATURES
    ).copy()

    if df.empty:
        fail(
            "No rows remain after removing incomplete model features."
        )

    values = df[MODEL_FEATURES].to_numpy(
        dtype=float
    )

    finite_mask = np.isfinite(values).all(axis=1)

    df = df.loc[finite_mask].copy()

    if df.empty:
        fail(
            "No finite rows remain after model feature validation."
        )

    df.sort_values(
        ["grid_id", "feature_timestamp"],
        inplace=True,
    )

    df.reset_index(drop=True, inplace=True)

    return df


def score_dataset(
    model: object,
    model_version: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    probabilities = model.predict_proba(
        df[MODEL_FEATURES]
    )

    classes = list(model.classes_)

    positive_class_index = classes.index(1)

    risk_probability = probabilities[
        :,
        positive_class_index,
    ]

    if not np.isfinite(risk_probability).all():
        fail(
            "Model produced non-finite risk probabilities."
        )

    if (
        (risk_probability < 0)
        | (risk_probability > 1)
    ).any():
        fail(
            "Model produced risk probabilities outside [0, 1]."
        )

    result = pd.DataFrame(
        {
            "grid_id": df["grid_id"].astype(str),
            "timestamp": df["feature_timestamp"],
            "risk_score": risk_probability.astype(float),
            "model_version": model_version,
        }
    )

    result["risk_level"] = np.select(
        [
            result["risk_score"] >= RISK_HIGH_THRESHOLD,
            result["risk_score"] >= RISK_MEDIUM_THRESHOLD,
        ],
        [
            "HIGH",
            "MEDIUM",
        ],
        default="LOW",
    )

    return result[
        [
            "grid_id",
            "timestamp",
            "risk_score",
            "risk_level",
            "model_version",
        ]
    ]


def write_risk_table(
    conn: sqlite3.Connection,
    scores: pd.DataFrame,
) -> None:
    conn.execute(
        f"DROP TABLE IF EXISTS {RISK_TABLE}"
    )

    conn.execute(
        f"""
        CREATE TABLE {RISK_TABLE} (
            grid_id TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            risk_score REAL NOT NULL,
            risk_level TEXT NOT NULL,
            model_version TEXT NOT NULL,
            PRIMARY KEY (grid_id, timestamp)
        )
        """
    )

    rows = [
        (
            row.grid_id,
            row.timestamp.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            float(row.risk_score),
            row.risk_level,
            row.model_version,
        )
        for row in scores.itertuples(index=False)
    ]

    conn.executemany(
        f"""
        INSERT INTO {RISK_TABLE}
        (
            grid_id,
            timestamp,
            risk_score,
            risk_level,
            model_version
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )

    conn.execute(
        f"""
        CREATE INDEX idx_{RISK_TABLE}_timestamp
        ON {RISK_TABLE}(timestamp)
        """
    )

    conn.execute(
        f"""
        CREATE INDEX idx_{RISK_TABLE}_grid
        ON {RISK_TABLE}(grid_id)
        """
    )

    conn.commit()


def validate_risk_table(
    conn: sqlite3.Connection,
    expected_rows: int,
) -> dict:
    row_count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {RISK_TABLE}
        """
    ).fetchone()[0]

    duplicate_count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                grid_id,
                timestamp,
                COUNT(*) AS row_count
            FROM {RISK_TABLE}
            GROUP BY grid_id, timestamp
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    invalid_scores = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {RISK_TABLE}
        WHERE risk_score IS NULL
           OR risk_score < 0
           OR risk_score > 1
        """
    ).fetchone()[0]

    invalid_levels = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {RISK_TABLE}
        WHERE risk_level NOT IN (
            'LOW',
            'MEDIUM',
            'HIGH'
        )
        """
    ).fetchone()[0]

    invalid_model_versions = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {RISK_TABLE}
        WHERE model_version IS NULL
           OR TRIM(model_version) = ''
        """
    ).fetchone()[0]

    null_values = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {RISK_TABLE}
        WHERE grid_id IS NULL
           OR timestamp IS NULL
           OR risk_score IS NULL
           OR risk_level IS NULL
           OR model_version IS NULL
        """
    ).fetchone()[0]

    if row_count != expected_rows:
        fail(
            f"Risk table row mismatch: expected "
            f"{expected_rows}, found {row_count}."
        )

    if duplicate_count != 0:
        fail(
            f"Risk table contains {duplicate_count} duplicate "
            "grid/timestamp groups."
        )

    if invalid_scores != 0:
        fail(
            f"Risk table contains {invalid_scores} invalid scores."
        )

    if invalid_levels != 0:
        fail(
            f"Risk table contains {invalid_levels} invalid levels."
        )

    if invalid_model_versions != 0:
        fail(
            f"Risk table contains {invalid_model_versions} "
            "missing model versions."
        )

    if null_values != 0:
        fail(
            f"Risk table contains {null_values} rows with null values."
        )

    score_range = conn.execute(
        f"""
        SELECT
            MIN(risk_score),
            MAX(risk_score)
        FROM {RISK_TABLE}
        """
    ).fetchone()

    return {
        "row_count": row_count,
        "duplicate_count": duplicate_count,
        "invalid_scores": invalid_scores,
        "invalid_levels": invalid_levels,
        "invalid_model_versions": invalid_model_versions,
        "null_values": null_values,
        "min_score": float(score_range[0]),
        "max_score": float(score_range[1]),
    }


def load_latest_anomalies(
    latest_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    anomaly = pd.read_csv(
        ANOMALY_PATH
    )

    required = {
        "grid_id",
        "timestamp",
        "total_activity",
        "baseline_activity",
        "percentage_deviation",
        "anomaly_score",
        "direction",
        "reason",
    }

    missing = sorted(
        required - set(anomaly.columns)
    )

    if missing:
        fail(
            "ML4 anomaly file is missing columns: "
            + ", ".join(missing)
        )

    anomaly["timestamp"] = pd.to_datetime(
        anomaly["timestamp"],
        errors="raise",
    )

    anomaly = anomaly[
        anomaly["timestamp"] == latest_timestamp
    ].copy()

    return anomaly


def build_top20_report(
    conn: sqlite3.Connection,
    latest_timestamp: pd.Timestamp,
) -> None:
    risk = pd.read_sql_query(
        f"""
        SELECT
            grid_id,
            timestamp,
            risk_score,
            risk_level,
            model_version
        FROM {RISK_TABLE}
        WHERE timestamp = ?
        ORDER BY risk_score DESC, grid_id
        LIMIT 20
        """,
        conn,
        params=(
            latest_timestamp.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        ),
    )

    if risk.empty:
        fail(
            "No risk scores found for the latest scored timestamp."
        )

    anomaly = load_latest_anomalies(
        latest_timestamp
    )

    anomaly = anomaly.rename(
        columns={
            "timestamp": "anomaly_timestamp"
        }
    )

    risk["grid_id"] = risk["grid_id"].astype(str)
    anomaly["grid_id"] = anomaly["grid_id"].astype(str)

    risk["timestamp"] = pd.to_datetime(risk["timestamp"])
    anomaly["anomaly_timestamp"] = pd.to_datetime(
        anomaly["anomaly_timestamp"]
    )

    merged = risk.merge(
        anomaly[
            [
                "grid_id",
                "anomaly_timestamp",
                "total_activity",
                "baseline_activity",
                "percentage_deviation",
                "anomaly_score",
                "direction",
                "reason",
            ]
        ],
        left_on=["grid_id", "timestamp"],
        right_on=["grid_id", "anomaly_timestamp"],
        how="left",
    )

    lines = []

    lines.append(
        "# Top-20 Operational Attention Report"
    )
    lines.append("")
    lines.append(
        f"**Scored timestamp:** "
        f"{latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    lines.append("")
    lines.append(
        "This report ranks grids by ML3 future high-activity "
        "risk score. A high score is an operational attention "
        "signal and should be investigated; it is not a "
        "confirmed network fault."
    )
    lines.append("")
    lines.append(
        "ML3 risk and ML4 anomaly results are kept as separate "
        "signals. ML4 describes deviation from its historical "
        "hour-of-day baseline, while ML3 estimates future "
        "high-activity risk."
    )
    lines.append("")

    for rank, row in enumerate(
        merged.itertuples(index=False),
        start=1,
    ):
        risk_score = float(row.risk_score)

        if row.risk_level == "HIGH":
            risk_reason = (
                "High future high-activity risk. "
                "Investigate the grid."
            )
        elif row.risk_level == "MEDIUM":
            risk_reason = (
                "Medium future high-activity risk. "
                "Consider investigation."
            )
        else:
            risk_reason = (
                "Low future high-activity risk. "
                "No immediate model-driven investigation signal."
            )

        lines.append(
            f"## {rank}. Grid {row.grid_id}"
        )
        lines.append("")
        lines.append(
            f"- **Risk score:** {risk_score:.6f}"
        )
        lines.append(
            f"- **Risk level:** {row.risk_level}"
        )
        lines.append(
            f"- **Model version:** {row.model_version}"
        )
        lines.append(
            f"- **Operational reason:** {risk_reason}"
        )

        if pd.notna(
            getattr(row, "direction", np.nan)
        ):
            lines.append(
                f"- **ML4 direction:** {row.direction}"
            )
            lines.append(
                f"- **ML4 anomaly score:** "
                f"{float(row.anomaly_score):.6f}"
            )
            lines.append(
                f"- **ML4 deviation:** "
                f"{float(row.percentage_deviation):.2f}%"
            )
            lines.append(
                f"- **ML4 reason:** {row.reason}"
            )
        else:
            lines.append(
                "- **ML4 evidence:** No matching anomaly "
                "record was found for this timestamp."
            )

        lines.append("")

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_PATH.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

def main() -> None:
    print("ML6 — Batch Risk Scoring")
    print("=" * 60)

    validate_paths()

    model, model_version = load_model()

    print(
        f"Model version       : {model_version}"
    )

    with sqlite3.connect(DB_PATH) as conn:
        validate_feature_table(conn)

        print("Loading ML2 features...")

        ml2 = load_ml2_features(conn)

        print(
            f"ML2 rows            : {len(ml2):,}"
        )

        print("Loading warehouse activity...")

        activity = load_activity(conn)

        print(
            f"Activity rows       : {len(activity):,}"
        )

        print("Building ML3 scoring features...")

        extra = build_extra_ml3_features(
            activity
        )

        scoring_df = build_scoring_dataset(
            ml2,
            extra,
        )

        print(
            f"Scoring rows        : {len(scoring_df):,}"
        )

        print("Running ML3-v1 scoring...")

        scores = score_dataset(
            model,
            model_version,
            scoring_df,
        )

        print(
            f"Predictions created : {len(scores):,}"
        )

        latest_timestamp = scores["timestamp"].max()

        print("Writing risk table...")

        write_risk_table(
            conn,
            scores,
        )

        print("Validating risk table...")

        validation = validate_risk_table(
            conn,
            expected_rows=len(scores),
        )

        print("Building top-20 report...")

        build_top20_report(
            conn,
            latest_timestamp,
        )

    print("-" * 60)
    print(
        f"Risk table rows     : "
        f"{validation['row_count']:,}"
    )
    print(
        f"Duplicate rows      : "
        f"{validation['duplicate_count']:,}"
    )
    print(
        f"Score range         : "
        f"{validation['min_score']:.6f} "
        f"to {validation['max_score']:.6f}"
    )
    print(
        f"Invalid scores      : "
        f"{validation['invalid_scores']:,}"
    )
    print(
        f"Invalid levels      : "
        f"{validation['invalid_levels']:,}"
    )
    print(
        f"Invalid model ver.  : "
        f"{validation['invalid_model_versions']:,}"
    )
    print(
        f"Null values         : "
        f"{validation['null_values']:,}"
    )
    print("Validation          : PASS")
    print(
        f"Report              : {REPORT_PATH}"
    )


if __name__ == "__main__":
    main()