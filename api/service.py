from __future__ import annotations

import csv
import json
import pickle
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .models import (
    NetworkSummaryResponse,
    GridActivityResponse,
    GridActivityPoint,
    HotspotResponse,
    HotspotPoint,
    AlertResponse,
    AlertPoint,
    GridFeatureResponse,
    PredictionResponse,
    PipelineStatusResponse,
    GridLocationResponse,
    GridNeighbour,
    GridNeighboursResponse,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

MODEL_PATH = (
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

ML4_ANOMALY_PATH = (
    PROJECT_ROOT
    / "ML"
    / "ML4"
    / "network_anomaly_scores.csv"
)

FEATURE_TABLE = "network_feature_table"

ML2_FEATURES = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

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

RISK_HIGH_THRESHOLD = 0.80
RISK_MEDIUM_THRESHOLD = 0.40
_cached_risk_model = None
_model_version = None


def _load_risk_model() -> None:
    global _cached_risk_model
    global _model_version

    if not MODEL_PATH.exists():
        raise RuntimeError(
            f"ML3 risk classifier artifact not found at {MODEL_PATH}. "
            "Run ML3 training before starting the API."
        )

    try:
        with MODEL_PATH.open("rb") as file:
            artifact = pickle.load(file)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load ML3 risk classifier: {exc}"
        ) from exc

    if not isinstance(artifact, dict):
        raise RuntimeError(
            "Invalid ML3 model artifact. "
            "Expected a dictionary containing model metadata."
        )

    model = artifact.get("model")
    model_features = artifact.get("features")
    model_version = artifact.get("model_version")

    if model is None:
        raise RuntimeError(
            "Invalid ML3 model artifact: missing 'model'."
        )

    if model_features is None:
        raise RuntimeError(
            "Invalid ML3 model artifact: missing 'features'."
        )

    if list(model_features) != MODEL_FEATURES:
        raise RuntimeError(
            "ML3/API feature schema mismatch. "
            f"Expected: {MODEL_FEATURES}. "
            f"Model artifact: {list(model_features)}."
        )

    if not hasattr(model, "predict_proba"):
        raise RuntimeError(
            "ML3 risk classifier does not support predict_proba()."
        )

    if not hasattr(model, "classes_"):
        raise RuntimeError(
            "ML3 risk classifier does not expose classes_."
        )

    if 1 not in list(model.classes_):
        raise RuntimeError(
            "ML3 risk classifier does not contain the positive class 1."
        )

    _cached_risk_model = model
    _model_version = str(
        model_version if model_version is not None else "ML3-v1"
    )


def load_ml5_model_at_startup() -> None:
    _load_risk_model()


def _get_risk_model():
    if _cached_risk_model is None:
        raise RuntimeError(
            "ML3 risk classifier has not been loaded."
        )

    return _cached_risk_model


def _parse_as_of(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            "as_of must be a valid ISO timestamp, "
            "for example YYYY-MM-DD HH:MM:SS"
        ) from exc


def _resolve_as_of(
    connection: sqlite3.Connection,
    requested_as_of: str | None,
) -> datetime:

    row = connection.execute(
        """
        SELECT MAX(timestamp) AS max_timestamp
        FROM dim_time
        """
    ).fetchone()

    if row is None or row["max_timestamp"] is None:
        raise RuntimeError(
            "Analytics warehouse contains no timestamps."
        )

    configured_as_of = datetime.fromisoformat(
        row["max_timestamp"]
    )

    if requested_as_of is None:
        return configured_as_of

    effective_as_of = _parse_as_of(requested_as_of)

    exists = connection.execute(
        """
        SELECT 1
        FROM dim_time
        WHERE timestamp = ?
        """,
        (
            effective_as_of.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        ),
    ).fetchone()

    if exists is None:
        raise ValueError(
            "as_of is not represented in the analytics layer: "
            f"{requested_as_of}"
        )

    return effective_as_of


def get_network_summary(
    connection: sqlite3.Connection,
    requested_as_of: str | None = None,
) -> NetworkSummaryResponse:
    effective_as_of = _resolve_as_of(connection, requested_as_of)

    as_of_text = effective_as_of.strftime("%Y-%m-%d %H:%M:%S")

    row = connection.execute(
        """
        SELECT
            total_activity,
            active_grids,
            peak_hour,
            top_grid,
            as_of
        FROM network_summary
        WHERE as_of = ?
        """,
        (as_of_text,),
    ).fetchone()

    if row is None:
        raise RuntimeError(
            "No precomputed network summary exists for "
            f"as_of={as_of_text}."
        )

    return NetworkSummaryResponse(
        total_activity=float(row["total_activity"]),
        active_grids=int(row["active_grids"]),
        peak_hour=datetime.fromisoformat(row["peak_hour"]),
        top_grid=str(row["top_grid"]),
        as_of=datetime.fromisoformat(row["as_of"]),
    )


def get_grid_activity(
    connection: sqlite3.Connection,
    grid_id: int,
    date: str | None = None,
    hour: int | None = None,
    requested_as_of: str | None = None,
) -> GridActivityResponse:

    if grid_id < 1 or grid_id > 10000:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    grid_exists = connection.execute(
        """
        SELECT 1
        FROM dim_grid
        WHERE grid_id = ?
        LIMIT 1
        """,
        (str(grid_id),),
    ).fetchone()

    if grid_exists is None:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    as_of = _resolve_as_of(
        connection,
        requested_as_of,
    )

    if hour is not None and not 0 <= hour <= 23:
        raise ValueError(
            "hour must be between 0 and 23."
        )

    if date is not None:
        try:
            datetime.strptime(
                date,
                "%Y-%m-%d",
            )
        except ValueError as exc:
            raise ValueError(
                "Invalid date. Use YYYY-MM-DD."
            ) from exc

    start_time = as_of - timedelta(hours=23)

    query = """
        SELECT
            t.timestamp,
            g.grid_id,
            f.sms_in,
            f.sms_out,
            f.call_in,
            f.call_out,
            f.internet_activity,
            f.total_sms,
            f.total_calls,
            f.total_activity,
            f.internet_share
        FROM fact_network_activity AS f
        JOIN dim_time AS t
            ON f.time_key = t.time_key
        JOIN dim_grid AS g
            ON f.grid_key = g.grid_key
        WHERE g.grid_id = ?
          AND t.timestamp BETWEEN ? AND ?
    """

    parameters: list[object] = [
        str(grid_id),
        start_time.isoformat(sep=" "),
        as_of.isoformat(sep=" "),
    ]

    if date is not None:
        query += " AND t.date = ?"
        parameters.append(date)

    if hour is not None:
        query += " AND t.hour = ?"
        parameters.append(hour)

    query += " ORDER BY t.timestamp"

    rows = connection.execute(
        query,
        parameters,
    ).fetchall()

    data = [
        GridActivityPoint(
            timestamp=datetime.fromisoformat(
                row["timestamp"]
            ),
            sms_in=row["sms_in"],
            sms_out=row["sms_out"],
            call_in=row["call_in"],
            call_out=row["call_out"],
            internet_activity=row["internet_activity"],
            total_sms=row["total_sms"],
            total_calls=row["total_calls"],
            total_activity=row["total_activity"],
            internet_share=row["internet_share"],
        )
        for row in rows
    ]

    timestamps = [
        point.timestamp
        for point in data
    ]

    if len(timestamps) != len(set(timestamps)):
        raise RuntimeError(
            f"Duplicate timestamps detected for grid {grid_id}."
        )

    return GridActivityResponse(
        grid_id=str(grid_id),
        as_of=as_of,
        data=data,
    )


def get_hotspots(
    connection: sqlite3.Connection,
    limit: int = 10,
    requested_as_of: str | None = None,
) -> HotspotResponse:

    if limit < 1:
        raise ValueError(
            "limit must be at least 1."
        )

    if limit > 10000:
        raise ValueError(
            "limit must not exceed 10000."
        )

    as_of = _resolve_as_of(
        connection,
        requested_as_of,
    )

    as_of_text = as_of.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    rows = connection.execute(
        """
        WITH ranked AS (
            SELECT
                g.grid_id,
                t.timestamp,
                f.total_activity,
                f.total_sms,
                f.total_calls,
                f.internet_activity,
                ROW_NUMBER() OVER (
                    ORDER BY
                        f.total_activity DESC,
                        g.grid_id ASC
                ) AS rank
            FROM fact_network_activity AS f
            JOIN dim_time AS t
                ON f.time_key = t.time_key
            JOIN dim_grid AS g
                ON f.grid_key = g.grid_key
            WHERE t.timestamp = ?
        )

        SELECT
            grid_id,
            timestamp,
            total_activity,
            total_sms,
            total_calls,
            internet_activity,
            rank
        FROM ranked
        WHERE rank <= ?
        ORDER BY
            rank ASC,
            grid_id ASC
        """,
        (as_of_text, limit),
    ).fetchall()

    data = [
        HotspotPoint(
            grid_id=str(row["grid_id"]),
            timestamp=datetime.fromisoformat(
                row["timestamp"]
            ),
            total_activity=float(
                row["total_activity"]
            ),
            total_sms=float(
                row["total_sms"]
            ),
            total_calls=float(
                row["total_calls"]
            ),
            internet_activity=float(
                row["internet_activity"]
            ),
            rank=int(row["rank"]),
            status="HOTSPOT",
            reason=(
                "Grid is among the highest activity areas "
                "for the effective reporting hour."
            ),
        )
        for row in rows
    ]

    return HotspotResponse(
        as_of=as_of,
        data=data,
    )


def get_alerts(
    connection: sqlite3.Connection,
    limit: int = 10,
    severity: str | None = None,
    requested_as_of: str | None = None,
) -> AlertResponse:

    if limit < 1:
        raise ValueError(
            "limit must be at least 1."
        )

    if limit > 10000:
        raise ValueError(
            "limit must not exceed 10000."
        )

    as_of = _resolve_as_of(
        connection,
        requested_as_of,
    )

    as_of_text = as_of.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    alerts_path = (
        PROJECT_ROOT
        / "outputs"
        / "network_alerts.csv"
    )

    if not alerts_path.exists():
        raise FileNotFoundError(
            f"NP3 alert output not found: {alerts_path}"
        )

    rows = []

    with alerts_path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            if row["timestamp"] > as_of_text:
                continue

            if severity is not None:
                if row["alert_type"] != severity:
                    continue

            rows.append(row)

    rows.sort(
        key=lambda row: (
            row["timestamp"],
            row["grid_id"],
            row["alert_type"],
        ),
        reverse=True,
    )

    rows = rows[:limit]

    data = [
        AlertPoint(
            grid_id=str(row["grid_id"]),
            timestamp=datetime.fromisoformat(
                row["timestamp"]
            ),
            alert_type=row["alert_type"],
            current_activity=float(
                row["current_activity"]
            ),
            baseline_activity=float(
                row["baseline_activity"]
            ),
            severity=row["alert_type"],
            reason=row["reason"],
        )
        for row in rows
    ]

    return AlertResponse(
        as_of=as_of,
        data=data,
    )


def get_grid_features(
    connection: sqlite3.Connection,
    grid_id: int,
) -> GridFeatureResponse:

    if grid_id < 1 or grid_id > 10000:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    table_exists = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'network_feature_table'
        LIMIT 1
        """
    ).fetchone()

    if table_exists is None:
        raise RuntimeError(
            "ML feature table is not available. "
            "API4 requires the stored ML2 feature output."
        )

    row = connection.execute(
        """
        SELECT
            grid_id,
            feature_timestamp,
            avg_activity,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share,
            'GOOD' AS data_quality,
            'CURRENT' AS freshness
        FROM network_feature_table
        WHERE grid_id = ?
        ORDER BY feature_timestamp DESC
        LIMIT 1
        """,
        (str(grid_id),),
    ).fetchone()

    if row is None:
        raise LookupError(
            f"No stored ML features found for grid_id: {grid_id}"
        )

    return GridFeatureResponse(
        grid_id=str(row["grid_id"]),
        feature_timestamp=datetime.fromisoformat(
            row["feature_timestamp"]
        ),
        avg_activity=float(
            row["avg_activity"]
        ),
        activity_growth=float(
            row["activity_growth"]
        ),
        active_hours=int(
            row["active_hours"]
        ),
        peak_ratio=float(
            row["peak_ratio"]
        ),
        variability=float(
            row["variability"]
        ),
        internet_share=float(
            row["internet_share"]
        ),
        data_quality=str(
            row["data_quality"]
        ),
        freshness=str(
            row["freshness"]
        ),
    )


# In api/service.py, change the ML3 prediction score column handling to:

def _parse_prediction_score(row: dict) -> float:
    """Read the ML3 risk probability from the prediction output."""
    value = row.get("risk_probability")

    if value is None:
        raise RuntimeError(
            "ML3 prediction output does not contain 'risk_probability'."
        )

    try:
        score = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Invalid ML3 risk_probability value: {value!r}"
        ) from exc

    if not 0.0 <= score <= 1.0:
        raise RuntimeError(
            f"ML3 risk_probability must be between 0 and 1, got {score}."
        )

    return score

def _find_prediction_row(
    grid_id: int,
    timestamp: datetime | None = None,
) -> tuple[dict, datetime]:
    if not ML3_PREDICTIONS_PATH.exists():
        raise FileNotFoundError(
            f"ML3 prediction file not found: {ML3_PREDICTIONS_PATH}"
        )

    matching_row = None
    latest_row = None
    latest_timestamp = None

    with ML3_PREDICTIONS_PATH.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                row_grid_id = int(row["grid_id"])
            except (KeyError, TypeError, ValueError):
                continue

            if row_grid_id != grid_id:
                continue

            row_timestamp = row.get("timestamp")

            if not row_timestamp:
                continue

            try:
                parsed_timestamp = datetime.fromisoformat(
                    row_timestamp
                )
            except ValueError:
                continue

            parsed_timestamp = parsed_timestamp.replace(
                tzinfo=None
            )

            if timestamp is not None:
                requested_timestamp = timestamp.replace(
                    tzinfo=None
                )

                if parsed_timestamp == requested_timestamp:
                    matching_row = row
                    break

            elif (
                latest_timestamp is None
                or parsed_timestamp > latest_timestamp
            ):
                latest_timestamp = parsed_timestamp
                latest_row = row

    if timestamp is not None:
        if matching_row is None:
            raise LookupError(
                f"No ML3 prediction found for grid {grid_id} "
                f"at {timestamp.isoformat()}."
            )

        prediction_timestamp = datetime.fromisoformat(
            matching_row["timestamp"]
        ).replace(tzinfo=None)

        return matching_row, prediction_timestamp

    if latest_row is None or latest_timestamp is None:
        raise LookupError(
            f"No ML3 prediction found for grid {grid_id}."
        )

    return latest_row, latest_timestamp


def _find_anomaly_context(
    grid_id: int,
    feature_timestamp: datetime,
) -> tuple[str, str] | None:

    if not ML4_ANOMALY_PATH.exists():
        return None

    target_timestamp = feature_timestamp.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    with ML4_ANOMALY_PATH.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:

        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            return None

        required_fields = {
            "grid_id",
            "timestamp",
            "direction",
            "reason",
        }

        if not required_fields.issubset(
            set(reader.fieldnames)
        ):
            return None

        for row in reader:
            try:
                row_grid_id = int(float(row["grid_id"]))
            except (TypeError, ValueError):
                continue

            if row_grid_id != grid_id:
                continue

            if row["timestamp"] != target_timestamp:
                continue

            return (
                str(row["direction"]),
                str(row["reason"]),
            )

    return None


def predict_risk(
    connection: sqlite3.Connection,
    grid_id: int,
    timestamp: datetime | None = None,
) -> PredictionResponse:

    if grid_id < 1 or grid_id > 10000:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    model = _get_risk_model()

    feature_table_exists = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        LIMIT 1
        """,
        (FEATURE_TABLE,),
    ).fetchone()

    if feature_table_exists is None:
        raise RuntimeError(
            "ML feature table is not available. "
            "Prediction requires the stored ML2 feature output."
        )

    feature_row = connection.execute(
        """
        SELECT
            grid_id,
            feature_timestamp,
            avg_activity,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share
        FROM network_feature_table
        WHERE grid_id = ?
        ORDER BY feature_timestamp DESC
        LIMIT 1
        """,
        (str(grid_id),),
    ).fetchone()

    if feature_row is None:
        raise LookupError(
            f"No stored ML features found for grid_id: {grid_id}"
        )

    missing_columns = [
        feature
        for feature in ML2_FEATURES
        if feature_row[feature] is None
    ]

    if missing_columns:
        raise RuntimeError(
            "Stored ML2 features contain NULL values for "
            f"grid_id {grid_id}: {missing_columns}"
        )

    prediction_row, prediction_timestamp = _find_prediction_row(
    grid_id=grid_id,
    timestamp=timestamp,
)

    risk_score = _parse_prediction_score(
        prediction_row
    )

    if risk_score >= RISK_HIGH_THRESHOLD:
        risk_level = "HIGH"
        explanation = (
            "High future high-activity risk. "
            "Investigate the grid."
        )
    elif risk_score >= RISK_MEDIUM_THRESHOLD:
        risk_level = "MEDIUM"
        explanation = (
            "Medium future high-activity risk. "
            "Monitor the grid and consider investigation."
        )
    else:
        risk_level = "LOW"
        explanation = (
            "Low future high-activity risk based on the "
            "ML3 prediction."
        )

    anomaly_context = _find_anomaly_context(
        grid_id=grid_id,
        feature_timestamp=prediction_timestamp,
    )

    if anomaly_context is not None:
        direction, reason = anomaly_context

        if direction.lower() != "normal":
            explanation = (
                f"{explanation} "
                f"ML4 anomaly context: {reason}"
            )

    return PredictionResponse(
        grid_id=str(grid_id),
        risk_score=risk_score,
        risk_level=risk_level,
        model_version=str(
            _model_version or "ML3-v1"
        ),
        feature_timestamp=prediction_timestamp,
        explanation_note=explanation,
    )


def get_pipeline_status() -> PipelineStatusResponse:

    status_path = (
        PROJECT_ROOT
        / "logs"
        / "pipeline_status.json"
    )

    if not status_path.exists():
        raise FileNotFoundError(
            f"Pipeline status record not found: {status_path}"
        )

    try:
        status = json.loads(
            status_path.read_text(
                encoding="utf-8"
            )
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Unable to read pipeline status record: {exc}"
        ) from exc

    pipeline_status = str(
        status.get("status", "UNKNOWN")
    ).upper()

    task_status = status.get(
        "per_task_status",
        {},
    )

    reasons = []

    if pipeline_status != "SUCCESS":
        reasons.append(
            f"Pipeline status is {pipeline_status}."
        )

    for task_name, task_state in task_status.items():
        if str(task_state).upper() not in {
            "SUCCESS",
            "SKIPPED",
        }:
            reasons.append(
                f"Task '{task_name}' status is "
                f"{task_state}."
            )

    healthy = len(reasons) == 0

    run_timestamp = datetime.fromisoformat(
        status["run_timestamp"]
    )

    as_of = status.get("AS_OF")

    as_of_datetime = (
        datetime.fromisoformat(as_of)
        if as_of
        else None
    )

    if run_timestamp.tzinfo is None:
        run_timestamp_utc = run_timestamp.replace(
            tzinfo=timezone.utc
        )
    else:
        run_timestamp_utc = run_timestamp.astimezone(
            timezone.utc
        )

    age_seconds = (
        datetime.now(timezone.utc)
        - run_timestamp_utc
    ).total_seconds()

    if age_seconds < 3600:
        freshness = "FRESH"
    elif age_seconds < 86400:
        freshness = "STALE"
    else:
        freshness = "VERY_STALE"

    if freshness != "FRESH":
        reasons.append(
            f"Analytics status record is {freshness}."
        )
        healthy = False

    return PipelineStatusResponse(
        healthy=healthy,
        run_id=str(status["run_id"]),
        run_timestamp=run_timestamp,
        status=pipeline_status,
        per_task_status={
            str(key): str(value)
            for key, value in task_status.items()
        },
        rows_in=int(
            status.get("rows_in", 0)
        ),
        rows_rejected=int(
            status.get("rows_rejected", 0)
        ),
        nulls_handled=int(
            status.get("nulls_handled", 0)
        ),
        rows_published=int(
            status.get("rows_published", 0)
        ),
        as_of=as_of_datetime,
        freshness=freshness,
        reasons=reasons,
    )


def get_grid_location(
    connection: sqlite3.Connection,
    grid_id: int,
) -> GridLocationResponse:

    if grid_id < 1 or grid_id > 10000:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    row = connection.execute(
        """
        SELECT
            grid_id,
            centroid_latitude,
            centroid_longitude,
            geometry_reference
        FROM dim_grid
        WHERE grid_id = ?
        LIMIT 1
        """,
        (str(grid_id),),
    ).fetchone()

    if row is None:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    if (
        row["centroid_latitude"] is None
        or row["centroid_longitude"] is None
    ):
        raise RuntimeError(
            f"Centroid is unavailable for grid_id: {grid_id}"
        )

    return GridLocationResponse(
        grid_id=str(row["grid_id"]),
        centroid_latitude=float(
            row["centroid_latitude"]
        ),
        centroid_longitude=float(
            row["centroid_longitude"]
        ),
        geometry_reference=str(
            row["geometry_reference"]
        ),
    )


def get_grid_neighbours(
    connection: sqlite3.Connection,
    grid_id: int,
    limit: int = 8,
) -> GridNeighboursResponse:

    if grid_id < 1 or grid_id > 10000:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    if limit < 1:
        raise ValueError(
            "limit must be at least 1."
        )

    if limit > 100:
        raise ValueError(
            "limit must not exceed 100."
        )

    origin = connection.execute(
        """
        SELECT
            grid_id,
            centroid_latitude,
            centroid_longitude
        FROM dim_grid
        WHERE grid_id = ?
        LIMIT 1
        """,
        (str(grid_id),),
    ).fetchone()

    if origin is None:
        raise LookupError(
            f"Unknown grid_id: {grid_id}"
        )

    if (
        origin["centroid_latitude"] is None
        or origin["centroid_longitude"] is None
    ):
        raise RuntimeError(
            f"Centroid is unavailable for grid_id: {grid_id}"
        )

    latitude = float(
        origin["centroid_latitude"]
    )

    longitude = float(
        origin["centroid_longitude"]
    )

    rows = connection.execute(
        """
        SELECT
            grid_id,
            centroid_latitude,
            centroid_longitude,
            (
                (centroid_latitude - ?) *
                (centroid_latitude - ?)
                +
                (centroid_longitude - ?) *
                (centroid_longitude - ?)
            ) AS distance
        FROM dim_grid
        WHERE grid_id <> ?
          AND centroid_latitude IS NOT NULL
          AND centroid_longitude IS NOT NULL
        ORDER BY
            distance ASC,
            grid_id ASC
        LIMIT ?
        """,
        (
            latitude,
            latitude,
            longitude,
            longitude,
            str(grid_id),
            limit,
        ),
    ).fetchall()

    neighbours = [
        GridNeighbour(
            grid_id=str(row["grid_id"]),
            centroid_latitude=float(
                row["centroid_latitude"]
            ),
            centroid_longitude=float(
                row["centroid_longitude"]
            ),
            distance=float(
                row["distance"]
            ),
        )
        for row in rows
    ]

    return GridNeighboursResponse(
        grid_id=str(origin["grid_id"]),
        neighbours=neighbours,
    )


load_ml5_model_at_startup()