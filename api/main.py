from __future__ import annotations

import sqlite3

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .database import get_db

from .models import (
    NetworkSummaryResponse,
    GridActivityResponse,
    HotspotResponse,
    AlertResponse,
    GridFeatureResponse,
    PredictionRequest,
    PredictionResponse,
    PipelineStatusResponse,
    GridLocationResponse,
    GridNeighboursResponse,
)

from .service import (
    get_network_summary,
    get_grid_activity,
    get_hotspots,
    get_alerts,
    get_grid_features,
    predict_risk,
    get_pipeline_status,
    get_grid_location,
    get_grid_neighbours,
)


app = FastAPI(
    title="Network Operations Intelligence API",
    version="1.0.0",
    description=(
        "REST API over the curated network analytics warehouse."
    ),
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get(
    "/network/summary",
    response_model=NetworkSummaryResponse,
    summary="Network summary",
    tags=["Network"],
)
def network_summary(
    as_of: str | None = Query(
        default=None,
        description="Optional reporting-window end timestamp.",
    ),
    connection: sqlite3.Connection = Depends(get_db),
) -> NetworkSummaryResponse:

    try:
        return get_network_summary(
            connection=connection,
            requested_as_of=as_of,
        )

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    except (
        FileNotFoundError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics data source unavailable: "
                f"{exc}"
            ),
        ) from exc


@app.get(
    "/network/grid/{grid_id}",
    response_model=GridActivityResponse,
    summary="Grid activity history",
    tags=["Network"],
)
def grid_activity(
    grid_id: int,
    date: str | None = None,
    hour: int | None = None,
    as_of: str | None = None,
    connection: sqlite3.Connection = Depends(get_db),
) -> GridActivityResponse:

    try:
        return get_grid_activity(
            connection=connection,
            grid_id=grid_id,
            date=date,
            hour=hour,
            requested_as_of=as_of,
        )

    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=str(exc),
        ) from exc

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    except (
        FileNotFoundError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics data source error: "
                f"{exc}"
            ),
        ) from exc


@app.get(
    "/network/hotspots",
    response_model=HotspotResponse,
    summary="Network hotspots",
    tags=["Network"],
)
def network_hotspots(
    limit: int = Query(
        default=10,
        ge=1,
        le=10000,
    ),
    as_of: str | None = Query(default=None),
    connection: sqlite3.Connection = Depends(get_db),
) -> HotspotResponse:

    try:
        return get_hotspots(
            connection=connection,
            limit=limit,
            requested_as_of=as_of,
        )

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    except (
        FileNotFoundError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics data source error: "
                f"{exc}"
            ),
        ) from exc


@app.get(
    "/network/alerts",
    response_model=AlertResponse,
    summary="Network alerts",
    tags=["Network"],
)
def network_alerts(
    limit: int = Query(
        default=10,
        ge=1,
        le=10000,
    ),
    severity: str | None = Query(default=None),
    as_of: str | None = Query(default=None),
    connection: sqlite3.Connection = Depends(get_db),
) -> AlertResponse:

    try:
        return get_alerts(
            connection=connection,
            limit=limit,
            severity=severity,
            requested_as_of=as_of,
        )

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    except (
        FileNotFoundError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics data source error: "
                f"{exc}"
            ),
        ) from exc


@app.get(
    "/network/grid/{grid_id}/features",
    response_model=GridFeatureResponse,
    summary="ML features for a grid",
    tags=["Network"],
)
def grid_features(
    grid_id: int,
    connection: sqlite3.Connection = Depends(get_db),
) -> GridFeatureResponse:

    try:
        return get_grid_features(
            connection=connection,
            grid_id=grid_id,
        )

    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=str(exc),
        ) from exc

    except (
        FileNotFoundError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Feature data source unavailable: "
                f"{exc}"
            ),
        ) from exc


@app.post(
    "/network/predict-risk",
    response_model=PredictionResponse,
    summary="Predict network risk",
    tags=["Network"],
)
def network_predict_risk(
    request: PredictionRequest,
    connection: sqlite3.Connection = Depends(get_db),
) -> PredictionResponse:

    try:
        return predict_risk(
            connection=connection,
            grid_id=request.grid_id,
            timestamp=request.timestamp,
        )

    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=str(exc),
        ) from exc

    except (
        FileNotFoundError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Prediction data source error: "
                f"{exc}"
            ),
        ) from exc


@app.get(
    "/pipeline/status",
    response_model=PipelineStatusResponse,
    summary="Current pipeline health and status",
    tags=["Operations"],
)
def pipeline_status() -> PipelineStatusResponse:

    try:
        return get_pipeline_status()

    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc

    except (
        RuntimeError,
        ValueError,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get(
    "/network/grid/{grid_id}/location",
    response_model=GridLocationResponse,
    summary="Geographic location of a network grid",
    tags=["Network"],
)
def grid_location(
    grid_id: int,
    connection: sqlite3.Connection = Depends(get_db),
) -> GridLocationResponse:

    try:
        return get_grid_location(
            connection=connection,
            grid_id=grid_id,
        )

    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=str(exc),
        ) from exc

    except (
        RuntimeError,
        sqlite3.Error,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get(
    "/network/grid/{grid_id}/neighbours",
    response_model=GridNeighboursResponse,
    summary="Nearby network grids by centroid distance",
    tags=["Network"],
)
def grid_neighbours(
    grid_id: int,
    limit: int = Query(
        default=8,
        ge=1,
        le=100,
    ),
    connection: sqlite3.Connection = Depends(get_db),
) -> GridNeighboursResponse:

    try:
        return get_grid_neighbours(
            connection=connection,
            grid_id=grid_id,
            limit=limit,
        )

    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=str(exc),
        ) from exc

    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        ) from exc

    except (
        RuntimeError,
        sqlite3.Error,
    ) as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc