from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class NetworkSummaryResponse(BaseModel):
    total_activity: float
    active_grids: int
    peak_hour: datetime
    top_grid: str
    as_of: datetime


class GridActivityPoint(BaseModel):
    timestamp: datetime
    sms_in: float
    sms_out: float
    call_in: float
    call_out: float
    internet_activity: float
    total_sms: float
    total_calls: float
    total_activity: float
    internet_share: float


class GridActivityResponse(BaseModel):
    grid_id: str
    as_of: datetime
    data: list[GridActivityPoint]


class HotspotPoint(BaseModel):
    grid_id: str
    timestamp: datetime
    total_activity: float
    total_sms: float
    total_calls: float
    internet_activity: float
    rank: int
    status: str
    reason: str
    risk_score: float | None = None


class HotspotResponse(BaseModel):
    as_of: datetime
    data: list[HotspotPoint]


class AlertPoint(BaseModel):
    grid_id: str
    timestamp: datetime
    alert_type: str
    current_activity: float
    baseline_activity: float
    severity: str
    reason: str
    risk_score: float | None = None


class AlertResponse(BaseModel):
    as_of: datetime
    data: list[AlertPoint]


class GridFeatureResponse(BaseModel):
    grid_id: str
    feature_timestamp: datetime

    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float

    data_quality: str
    freshness: str


class PredictionRequest(BaseModel):
    grid_id: int
    timestamp: datetime | None = None

class PredictionResponse(BaseModel):
    grid_id: str
    risk_score: float
    risk_level: str
    model_version: str
    feature_timestamp: datetime
    explanation_note: str

class PipelineStatusResponse(BaseModel):
    healthy: bool
    run_id: str
    run_timestamp: datetime
    status: str
    per_task_status: dict[str, str]
    rows_in: int
    rows_rejected: int
    nulls_handled: int
    rows_published: int
    as_of: datetime | None
    freshness: str
    reasons: list[str]


class GridLocationResponse(BaseModel):
    grid_id: str
    centroid_latitude: float
    centroid_longitude: float
    geometry_reference: str


class GridNeighbour(BaseModel):
    grid_id: str
    centroid_latitude: float
    centroid_longitude: float
    distance: float


class GridNeighboursResponse(BaseModel):
    grid_id: str
    neighbours: list[GridNeighbour]